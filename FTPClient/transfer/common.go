package transfer

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

// Minimum file size to use optimized I/O (100MB)
const OptimizedIOThreshold = 100 * 1024 * 1024

// ProgressReader wraps an io.Reader to track progress with intelligent speed calculation
type ProgressReader struct {
	Reader      io.Reader
	Total       int64
	Transferred int64
	StartTime   time.Time
	LastUpdate  time.Time
	LastBytes   int64
	OnProgress  func(transferred int64, total int64, speed float64, elapsed time.Duration)

	// Enhanced speed tracking to exclude pauses
	ActiveTransferTime time.Duration
	SpeedSamples       []float64
	LastActiveTime     time.Time
	PauseThreshold     time.Duration
	MinSpeed           float64
	MaxSamples         int
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	if pr.StartTime.IsZero() {
		pr.StartTime = time.Now()
		pr.LastUpdate = pr.StartTime
		pr.LastActiveTime = pr.StartTime
		pr.LastBytes = 0
		pr.PauseThreshold = 500 * time.Millisecond // Consider anything > 500ms as a pause
		pr.MinSpeed = 1024 * 1024                  // 1 MB/s minimum to be considered active
		pr.MaxSamples = 100                        // Keep last 100 speed samples
		pr.SpeedSamples = make([]float64, 0, pr.MaxSamples)
	}

	n, err = pr.Reader.Read(p)
	if n > 0 {
		pr.Transferred += int64(n)

		now := time.Now()
		if now.Sub(pr.LastUpdate) >= 100*time.Millisecond {
			bytesDiff := pr.Transferred - pr.LastBytes
			timeDiff := now.Sub(pr.LastUpdate).Seconds()
			speed := float64(bytesDiff) / timeDiff

			// Track active transfer time and speed samples
			pr.updateSpeedTracking(speed, timeDiff, now)

			if pr.OnProgress != nil {
				pr.OnProgress(pr.Transferred, pr.Total, speed, now.Sub(pr.StartTime))
			}

			pr.LastUpdate = now
			pr.LastBytes = pr.Transferred
		}
	}
	return
}

// progressBar creates a visual progress bar
func progressBar(progress float64) string {
	const width = 50
	pos := int(float64(width) * progress / 100)
	bar := make([]rune, width)
	for i := range bar {
		switch {
		case i < pos:
			bar[i] = '='
		case i == pos:
			bar[i] = '>'
		default:
			bar[i] = ' '
		}
	}
	return string(bar)
}

// calculateOptimalChunkSize determines the best chunk size based on file size
func calculateOptimalChunkSize(fileSize int64) int64 {
	switch {
	case fileSize < 1*1024*1024: // < 1MB
		return 64 * 1024 // 64KB chunk
	case fileSize < 10*1024*1024: // < 10MB
		return 2 * 1024 * 1024 // 2MB chunk
	case fileSize < 100*1024*1024: // < 100MB
		return 4 * 1024 * 1024 // 4MB chunk
	case fileSize < 1024*1024*1024: // < 1GB
		return 16 * 1024 * 1024 // 16MB chunk
	case fileSize < 10*1024*1024*1024: // < 10GB
		return 32 * 1024 * 1024 // 32Mb chunk
	default: // >= 10GB
		return 64 * 1024 * 1024 // 64MB
	}
}

// RetryCounter tracks the number of retries for the current operation
var RetryCounter int64

// RetryWithExponentialBackoff executes a function with exponential backoff retries
func RetryWithExponentialBackoff(operation string, fn func() error) error {
	maxRetries := 5
	baseRetryDelay := 500 * time.Millisecond
	maxRetryDelay := 30 * time.Second

	// Reset retry counter at the start of each operation using atomic operations
	atomic.StoreInt64(&RetryCounter, 0)

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		atomic.AddInt64(&RetryCounter, 1)

		if attempt < maxRetries {
			delay := baseRetryDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}

			// Add jitter (±20%)
			jitter := float64(delay) * (0.8 + rand.Float64()*0.4)
			delay = time.Duration(jitter)

			fmt.Printf("\nRetry %d/%d for %s after %v: %v\n",
				attempt, maxRetries, operation, delay, err)

			time.Sleep(delay)
		}
	}

	return fmt.Errorf("operation %s failed after %d attempts: %v",
		operation, maxRetries, err)
}

// updateSpeedTracking tracks active transfer speeds and excludes pauses
func (pr *ProgressReader) updateSpeedTracking(speed float64, timeDiff float64, now time.Time) {
	// Check if this looks like an active transfer (not a pause)
	if speed >= pr.MinSpeed && timeDiff < pr.PauseThreshold.Seconds() {
		// This is active transfer time
		pr.ActiveTransferTime += time.Duration(timeDiff * float64(time.Second))
		pr.LastActiveTime = now

		// Add speed sample
		pr.SpeedSamples = append(pr.SpeedSamples, speed)

		// Keep only recent samples
		if len(pr.SpeedSamples) > pr.MaxSamples {
			pr.SpeedSamples = pr.SpeedSamples[1:]
		}
	} else if now.Sub(pr.LastActiveTime) > pr.PauseThreshold {
		// This looks like a pause period - don't count it towards active time
		// but update the last active time to avoid accumulating pause time
		pr.LastActiveTime = now
	}
}

// GetAverageActiveSpeed calculates average speed based only on active transfer periods
func (pr *ProgressReader) GetAverageActiveSpeed() float64 {
	if len(pr.SpeedSamples) == 0 {
		// Fallback to total time calculation if no samples
		if pr.ActiveTransferTime > 0 {
			return float64(pr.Transferred) / pr.ActiveTransferTime.Seconds()
		}
		return 0
	}

	// Calculate average from speed samples (this excludes pause periods)
	totalSpeed := 0.0
	validSamples := 0

	for _, speed := range pr.SpeedSamples {
		if speed >= pr.MinSpeed { // Only count meaningful speeds
			totalSpeed += speed
			validSamples++
		}
	}

	if validSamples > 0 {
		return totalSpeed / float64(validSamples)
	}

	// Final fallback
	if pr.ActiveTransferTime > 0 {
		return float64(pr.Transferred) / pr.ActiveTransferTime.Seconds()
	}
	return 0
}

// GetTransferStats returns both traditional and intelligent speed calculations
func (pr *ProgressReader) GetTransferStats() (traditionalSpeed, activeSpeed float64, activeTime time.Duration) {
	totalElapsed := time.Since(pr.StartTime)
	traditionalSpeed = float64(pr.Transferred) / totalElapsed.Seconds()
	activeSpeed = pr.GetAverageActiveSpeed()
	activeTime = pr.ActiveTransferTime
	return
}

// getCurrentDirectory returns the current working directory
func getCurrentDirectory() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	return dir
}
