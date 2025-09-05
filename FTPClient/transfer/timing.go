package transfer

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// LockResult contains the results of a lock operation including timing information
type LockResult struct {
	LockAcquired   bool          // Whether the lock was successfully acquired
	WaitTime       time.Duration // Time spent waiting for the lock
	TransferStart  time.Time     // When the actual transfer should start timing
	LockStartTime  time.Time     // When lock acquisition started
}

// TimedFileLocker provides file locking with separate timing for lock waits vs transfers
type TimedFileLocker struct {
	file         *os.File
	exclusive    bool
	lockResult   *LockResult
	unlockFunc   func() error
}

// NewTimedFileLocker creates a new timed file locker
func NewTimedFileLocker(file *os.File, exclusive bool) *TimedFileLocker {
	return &TimedFileLocker{
		file:      file,
		exclusive: exclusive,
	}
}


// AcquireLockWithNotification acquires lock with user notification during wait and timeout
func (tfl *TimedFileLocker) AcquireLockWithNotification(notifyFunc func(waitTime time.Duration)) (*LockResult, error) {
	lockStartTime := time.Now()
	
	handle := windows.Handle(tfl.file.Fd())
	var flags uint32
	if tfl.exclusive {
		flags = windows.LOCKFILE_EXCLUSIVE_LOCK
	}

	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)

	// Try immediate lock first
	flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	err := windows.LockFileEx(handle, flags, 0, maxUint32, maxUint32, ol)
	
	if err == nil {
		// Lock acquired immediately
		transferStart := time.Now()
		result := &LockResult{
			LockAcquired:  true,
			WaitTime:      time.Since(lockStartTime),
			TransferStart: transferStart,
			LockStartTime: lockStartTime,
		}
		
		tfl.lockResult = result
		tfl.unlockFunc = func() error {
			return windows.UnlockFileEx(handle, 0, maxUint32, maxUint32, ol)
		}
		
		return result, nil
	}

	// Need to wait for lock - notify user
	if notifyFunc != nil {
		notifyFunc(0) // Initial notification
	}

	// Remove fail immediately flag for blocking acquisition
	flags &^= windows.LOCKFILE_FAIL_IMMEDIATELY
	
	// Set up timeout and notification channels
	timeout := time.NewTimer(30 * time.Second) // 30 second timeout
	defer timeout.Stop()
	
	stopNotifications := make(chan bool, 1)
	lockAcquired := make(chan error, 1)
	
	// Start a goroutine to provide periodic notifications
	if notifyFunc != nil {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			
			for {
				select {
				case <-stopNotifications:
					return
				case <-ticker.C:
					notifyFunc(time.Since(lockStartTime))
				}
			}
		}()
	}

	// Start lock acquisition in a goroutine
	go func() {
		err := windows.LockFileEx(handle, flags, 0, maxUint32, maxUint32, ol)
		lockAcquired <- err
	}()

	// Wait for either lock acquisition or timeout
	var lockErr error
	select {
	case lockErr = <-lockAcquired:
		// Lock acquired or failed
	case <-timeout.C:
		// Timeout occurred
		lockErr = fmt.Errorf("lock acquisition timed out after 30 seconds")
	}
	
	// Stop notifications
	if notifyFunc != nil {
		select {
		case stopNotifications <- true:
		default:
		}
	}

	if lockErr != nil {
		return &LockResult{
			LockAcquired:  false,
			WaitTime:      time.Since(lockStartTime),
			TransferStart: time.Time{},
			LockStartTime: lockStartTime,
		}, fmt.Errorf("failed to acquire file lock: %v", lockErr)
	}

	// Lock acquired after waiting
	transferStart := time.Now()
	waitTime := transferStart.Sub(lockStartTime)
	
	result := &LockResult{
		LockAcquired:  true,
		WaitTime:      waitTime,
		TransferStart: transferStart,
		LockStartTime: lockStartTime,
	}
	
	tfl.lockResult = result
	tfl.unlockFunc = func() error {
		return windows.UnlockFileEx(handle, 0, maxUint32, maxUint32, ol)
	}
	
	return result, nil
}

// ReleaseLock releases the acquired file lock
func (tfl *TimedFileLocker) ReleaseLock() error {
	if tfl.unlockFunc != nil {
		err := tfl.unlockFunc()
		tfl.unlockFunc = nil
		return err
	}
	return nil
}

// GetLockResult returns the lock result information
func (tfl *TimedFileLocker) GetLockResult() *LockResult {
	return tfl.lockResult
}

// TransferTimer helps calculate accurate transfer speeds excluding lock wait time
type TransferTimer struct {
	transferStart time.Time
	lockWaitTime  time.Duration
	totalBytes    int64
}

// NewTransferTimer creates a new transfer timer
func NewTransferTimer(lockResult *LockResult) *TransferTimer {
	return &TransferTimer{
		transferStart: lockResult.TransferStart,
		lockWaitTime:  lockResult.WaitTime,
	}
}

// CalculateSpeed calculates transfer speed excluding lock wait time
func (tt *TransferTimer) CalculateSpeed(bytesTransferred int64) (float64, time.Duration) {
	transferDuration := time.Since(tt.transferStart)
	if transferDuration.Seconds() == 0 {
		return 0, transferDuration
	}
	
	speedMBps := float64(bytesTransferred) / transferDuration.Seconds() / (1024 * 1024)
	return speedMBps, transferDuration
}

// GetTimingReport returns a detailed timing report
func (tt *TransferTimer) GetTimingReport(bytesTransferred int64) TimingReport {
	transferDuration := time.Since(tt.transferStart)
	speedMBps, _ := tt.CalculateSpeed(bytesTransferred)
	
	return TimingReport{
		LockWaitTime:     tt.lockWaitTime,
		TransferTime:     transferDuration,
		TotalTime:        tt.lockWaitTime + transferDuration,
		BytesTransferred: bytesTransferred,
		TransferSpeed:    speedMBps,
	}
}

// TimingReport contains detailed timing information for a file operation
type TimingReport struct {
	LockWaitTime     time.Duration // Time spent waiting for file lock
	TransferTime     time.Duration // Time spent in actual transfer (excluding lock wait)
	TotalTime        time.Duration // Total time including lock wait
	BytesTransferred int64         // Number of bytes transferred
	TransferSpeed    float64       // Transfer speed in MB/s (excluding lock wait time)
}

// String returns a formatted string representation of the timing report
func (tr *TimingReport) String() string {
	return fmt.Sprintf(
		"Lock Wait: %v, Transfer: %v (%.2f MB/s), Total: %v, Bytes: %d",
		tr.LockWaitTime.Round(time.Millisecond),
		tr.TransferTime.Round(time.Millisecond),
		tr.TransferSpeed,
		tr.TotalTime.Round(time.Millisecond),
		tr.BytesTransferred,
	)
}
