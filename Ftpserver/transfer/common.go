package transfer

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// SessionInterface defines the interface for session objects
type SessionInterface interface {
	LogPrintf(format string, args ...interface{})
	GetRestartPos() int64
	GetTransferMode() string
}

// Memory-mapped file structure for high-performance transfers
type MmapFile struct {
	file   *os.File
	data   []byte
	size   int64
	handle syscall.Handle
}

// Close closes the memory-mapped file with comprehensive cleanup
func (mmf *MmapFile) Close() error {
	var firstErr error
	
	// Ensure memory mapping is unmapped first
	if mmf.handle != 0 {
		if len(mmf.data) > 0 {
			// Safely unmap the view
			if err := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&mmf.data[0]))); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to unmap view of file: %v", err)
				}
			}
			// Clear the data slice to prevent further access
			mmf.data = nil
		}
		
		// Close the mapping handle
		if err := syscall.CloseHandle(mmf.handle); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close mapping handle: %v", err)
			}
		}
		mmf.handle = 0
	}
	
	// Close the file handle
	if mmf.file != nil {
		if err := mmf.file.Close(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close file: %v", err)
			}
		}
		mmf.file = nil
	}
	
	// Reset size to indicate closed state
	mmf.size = 0
	
	return firstErr
}

// File locking structures and functions

// LockResult contains information about a file lock acquisition
type LockResult struct {
	LockType     string        // "shared" or "exclusive"
	WaitTime     time.Duration // Time spent waiting for the lock
	AcquiredAt   time.Time     // When the lock was acquired
	ThreadInfo   string        // Thread/goroutine information
	FileInfo     string        // File information
	LockDuration time.Duration // How long the lock has been held (for reporting)
}

// TimedFileLocker provides file locking with timing information
type TimedFileLocker struct {
	file       *os.File
	exclusive  bool
	lockResult *LockResult
	acquired   bool
	mutex      sync.Mutex
}

// NewTimedFileLocker creates a new timed file locker
func NewTimedFileLocker(file *os.File, exclusive bool) *TimedFileLocker {
	return &TimedFileLocker{
		file:      file,
		exclusive: exclusive,
		acquired:  false,
	}
}

// AcquireLockWithNotification acquires a file lock with notification callback
func (tfl *TimedFileLocker) AcquireLockWithNotification(notifyFunc func(time.Duration)) (*LockResult, error) {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	if tfl.acquired {
		return tfl.lockResult, fmt.Errorf("lock already acquired")
	}

	startTime := time.Now()
	var notificationSent bool
	const notificationDelay = 2 * time.Second

	for {
		// Try to acquire the lock
		success := TryExclusiveLock(tfl.file)
		if success {
			// Lock acquired successfully
			waitTime := time.Since(startTime)
			tfl.lockResult = &LockResult{
				LockType:   func() string { if tfl.exclusive { return "exclusive" } else { return "shared" } }(),
				WaitTime:   waitTime,
				AcquiredAt: time.Now(),
				ThreadInfo: fmt.Sprintf("goroutine-%d", time.Now().UnixNano()%10000),
				FileInfo:   tfl.file.Name(),
			}
			tfl.acquired = true
			return tfl.lockResult, nil
		}

		// Lock not available, check if we should send notification
		waitTime := time.Since(startTime)
		if !notificationSent && waitTime >= notificationDelay {
			if notifyFunc != nil {
				notifyFunc(waitTime)
			}
			notificationSent = true
		}

		// Wait a bit before retrying
		time.Sleep(100 * time.Millisecond)

		// Check for timeout (optional - you can remove this if you want indefinite waiting)
		if waitTime > 30*time.Second {
			return nil, fmt.Errorf("timeout waiting for file lock after %v", waitTime)
		}
	}
}

// ReleaseLock releases the acquired lock
func (tfl *TimedFileLocker) ReleaseLock() error {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	if !tfl.acquired {
		return fmt.Errorf("no lock to release")
	}

	err := UnlockFile(tfl.file)
	if err == nil {
		tfl.acquired = false
		if tfl.lockResult != nil {
			tfl.lockResult.LockDuration = time.Since(tfl.lockResult.AcquiredAt)
		}
	}
	return err
}

// GetLockResult returns the current lock result
func (tfl *TimedFileLocker) GetLockResult() *LockResult {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()
	return tfl.lockResult
}

// TryExclusiveLock attempts to acquire an exclusive lock on the file
func TryExclusiveLock(file *os.File) bool {
	handle := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)
	
	// Try to lock the entire file exclusively (non-blocking)
	err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, maxUint32, maxUint32, ol)
	return err == nil
}

// TrySharedLock attempts to acquire a shared lock on the file for read operations
func TrySharedLock(file *os.File) bool {
	handle := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)
	
	// Try to lock the entire file for shared access (non-blocking)
	err := windows.LockFileEx(handle, windows.LOCKFILE_FAIL_IMMEDIATELY, 0, maxUint32, maxUint32, ol)
	return err == nil
}

// VerifyLockState verifies the current lock state of a file
func VerifyLockState(filename string) (bool, string, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return false, "", fmt.Errorf("cannot access file: %v", err)
	}
	defer file.Close()
	
	// Test exclusive lock
	if TryExclusiveLock(file) {
		UnlockFile(file)
		return false, "unlocked", nil
	}
	
	// Test shared lock
	if TrySharedLock(file) {
		UnlockFile(file)
		return true, "shared_lock", nil
	}
	
	return true, "exclusive_lock", nil
}

// UnlockFile unlocks the file
func UnlockFile(file *os.File) error {
	handle := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)
	return windows.UnlockFileEx(handle, 0, maxUint32, maxUint32, ol)
}

// Transfer timing utilities

// TransferTimer helps track transfer timing excluding lock wait time
type TransferTimer struct {
	transferStart time.Time
	lockResult    *LockResult
}

// TimingReport contains transfer timing information
type TimingReport struct {
	TransferTime  time.Duration // Actual transfer time (excluding lock wait)
	TotalTime     time.Duration // Total time including lock wait
	LockWaitTime  time.Duration // Time spent waiting for lock
	TransferSpeed float64       // Speed in MB/s (based on transfer time only)
}

// NewTransferTimer creates a new transfer timer
func NewTransferTimer(lockResult *LockResult) *TransferTimer {
	return &TransferTimer{
		transferStart: time.Now(),
		lockResult:    lockResult,
	}
}

// CalculateSpeed calculates current transfer speed excluding lock wait time
func (tt *TransferTimer) CalculateSpeed(bytesTransferred int64) (float64, time.Duration) {
	elapsed := time.Since(tt.transferStart)
	if elapsed > 0 {
		speedMBps := float64(bytesTransferred) / (1024 * 1024) / elapsed.Seconds()
		return speedMBps, elapsed
	}
	return 0, 0
}

// GetTimingReport generates a comprehensive timing report
func (tt *TransferTimer) GetTimingReport(totalBytes int64) TimingReport {
	transferTime := time.Since(tt.transferStart)
	var lockWaitTime time.Duration
	var totalTime time.Duration

	if tt.lockResult != nil {
		lockWaitTime = tt.lockResult.WaitTime
		totalTime = lockWaitTime + transferTime
	} else {
		totalTime = transferTime
	}

	var transferSpeed float64
	if transferTime > 0 {
		transferSpeed = float64(totalBytes) / (1024 * 1024) / transferTime.Seconds()
	}

	return TimingReport{
		TransferTime:  transferTime,
		TotalTime:     totalTime,
		LockWaitTime:  lockWaitTime,
		TransferSpeed: transferSpeed,
	}
}

// SyncMmapFile syncs memory-mapped file data to disk
func SyncMmapFile(data []byte) {
	if len(data) > 0 {
		addr := uintptr(unsafe.Pointer(&data[0]))
		size := uintptr(len(data))
		windows.FlushViewOfFile(addr, size)
	}
}

// WriteRequest represents a write operation for parallel transfers
type WriteRequest struct {
	buffer []byte
	data   []byte
	size   int
}
