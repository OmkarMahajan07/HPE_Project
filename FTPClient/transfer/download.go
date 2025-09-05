package transfer

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ftp/config"
	"ftp/perfmetrics"

	"github.com/jlaffaye/ftp"
)

// External connection interface to access global FTP connection
type FTPConnectionInterface interface {
	GetCompressionStatus() bool
	GetWindowSize() int
	GetRawConnection() net.Conn
}

// Constants for parallel downloads
const (
	maxParallelDownloads = 4 // Maximum number of parallel chunk downloads
)


// ChunkTask represents a single chunk download task
type ChunkTask struct {
	Conn       *ftp.ServerConn
	RemoteFile string
	Offset     int64
	Length     int64
	ChunkID    int
}

// Global variables for accessing main package data
var (
	MainFTPConn FTPConnectionInterface
)

// SetMainFTPConnection allows the main package to provide access to the FTP connection
func SetMainFTPConnection(conn FTPConnectionInterface) {
	MainFTPConn = conn
}

// ProgressTracker manages download progress reporting
type ProgressTracker struct {
	totalBytes       int64
	transferredBytes *int64
	startTime        time.Time
	lastUpdate       time.Time
	lastBytes        int64
	updateInterval   time.Duration
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(totalBytes int64, transferredBytes *int64) *ProgressTracker {
	now := time.Now()
	return &ProgressTracker{
		totalBytes:       totalBytes,
		transferredBytes: transferredBytes,
		startTime:        now,
		lastUpdate:       now,
		lastBytes:        0,
		updateInterval:   100 * time.Millisecond,
	}
}

// UpdateProgress updates and displays progress if enough time has passed
func (pt *ProgressTracker) UpdateProgress() {
	now := time.Now()
	if now.Sub(pt.lastUpdate) < pt.updateInterval {
		return
	}

	currentBytes := atomic.LoadInt64(pt.transferredBytes)
	bytesDiff := currentBytes - pt.lastBytes
	timeDiff := now.Sub(pt.lastUpdate).Seconds()
	speed := float64(bytesDiff) / timeDiff
	progress := float64(currentBytes) / float64(pt.totalBytes) * 100
	if progress > 100 {
		progress = 100
	}

	fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
		progressBar(progress),
		progress,
		speed/1024/1024,
		int(now.Sub(pt.startTime).Seconds()))

	pt.lastUpdate = now
	pt.lastBytes = currentBytes
}

// FinalReport displays the final progress report
func (pt *ProgressTracker) FinalReport() {
	elapsed := time.Since(pt.startTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond
	}
	finalBytes := atomic.LoadInt64(pt.transferredBytes)
	avgSpeed := float64(finalBytes) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100), avgSpeed, int(elapsed.Seconds()))
	fmt.Printf("Download completed - Average speed: %.2f MB/s\n", avgSpeed)
}

// MmapWriter wraps memory-mapped byte slice to implement io.Writer
type MmapWriter struct {
	data []byte
	offset int64
}

// NewMmapWriter creates a new MmapWriter
func NewMmapWriter(data []byte) *MmapWriter {
	return &MmapWriter{data: data, offset: 0}
}

// Write implements io.Writer for memory-mapped data
func (mw *MmapWriter) Write(p []byte) (n int, err error) {
	if mw.offset+int64(len(p)) > int64(len(mw.data)) {
		return 0, fmt.Errorf("write would exceed memory-mapped region")
	}
	copy(mw.data[mw.offset:mw.offset+int64(len(p))], p)
	mw.offset += int64(len(p))
	return len(p), nil
}

// copyWithProgress copies data with progress reporting
func copyWithProgress(src io.Reader, dst io.Writer, totalBytes int64, startTime time.Time) error {
	var transferredBytes int64
	pt := NewProgressTracker(totalBytes, &transferredBytes)
	buf := make([]byte, 4*1024*1024) // 4MB buffer

	for transferredBytes < totalBytes {
		n, err := src.Read(buf)
		if n > 0 {
			// Ensure we don't exceed total bytes
			if transferredBytes+int64(n) > totalBytes {
				n = int(totalBytes - transferredBytes)
			}
			_, writeErr := dst.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write error: %v", writeErr)
			}
			atomic.AddInt64(&transferredBytes, int64(n))
			pt.UpdateProgress()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read error: %v", err)
		}
	}
	return nil
}

// measureRTT measures Round Trip Time by sending a NOOP command
func measureRTT(client *ftp.ServerConn) time.Duration {
	start := time.Now()

	// Use NOOP command for RTT measurement
	err := client.NoOp()
	if err != nil {
		// Fallback to default if NOOP fails
		return 50 * time.Millisecond
	}

	return time.Since(start)
}

// logDownloadMetrics centralizes performance metrics logging for all download methods
func logDownloadMetrics(client *ftp.ServerConn, remoteFile string, fileSize int64, avgSpeed float64, elapsed time.Duration, clientType string) {
	// Measure RTT for performance metrics
	rtt := measureRTT(client)

	// Get compression status and window size from main FTP connection
	compression := false
	windowSize := 1
	if MainFTPConn != nil {
		compression = MainFTPConn.GetCompressionStatus()
		windowSize = MainFTPConn.GetWindowSize()
	}

	// Performance metrics logging with real-time values
	metrics := map[string]interface{}{
		"Client":         clientType,
		"FileName":       filepath.Base(remoteFile),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    compression,
		"RTTms":          int(float64(rtt.Nanoseconds()) / 1e6), // Convert to milliseconds as int
		"WindowSize":     windowSize,
		"ThroughputMBps": avgSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        int(atomic.LoadInt64(&RetryCounter)), // Get actual retry count as int
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}
}
// DownloadFile handles the file download process (legacy function for backward compatibility)
func DownloadFile(client *ftp.ServerConn, remoteFile, localPath string, useMemoryMappedIO bool) error {
	// Determine legacy transfer mode
	var transferMode string
	if useMemoryMappedIO {
		transferMode = "auto-mmap" // Legacy mmap preference
	} else {
		transferMode = "auto" // Legacy auto selection
	}
	
	return downloadFileInternal(client, remoteFile, localPath, transferMode)
}

// DownloadFileWithMode handles the file download process using explicit I/O mode
func DownloadFileWithMode(client *ftp.ServerConn, remoteFile, localPath string, ioMode string) error {
	return downloadFileInternal(client, remoteFile, localPath, ioMode)
}

// downloadFileInternal contains the unified implementation for both legacy and mode-based downloads
func downloadFileInternal(client *ftp.ServerConn, remoteFile, localPath, transferMode string) error {
	// Get file size
	fileSize, err := client.FileSize(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to get file size: %v", err)
	}

	// Create parent directories if needed
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create directories: %v", err)
	}

	fmt.Printf("Downloading %s (%d bytes)...\n", remoteFile, fileSize)

	// Execute transfer based on determined mode
	return executeDownloadWithMode(client, remoteFile, localPath, fileSize, transferMode)
}

// executeDownloadWithMode contains the core logic for mode-based download execution
func executeDownloadWithMode(client *ftp.ServerConn, remoteFile, localPath string, fileSize int64, transferMode string) error {
    switch transferMode {
    case "mmap":
        if fileSize >= OptimizedIOThreshold {
            fmt.Println("Using memory-mapped I/O for file transfer...")
            return downloadWithOptimizedIO(client, remoteFile, localPath, fileSize)
        } else {
            fmt.Printf("File size (%d bytes) is below memory-mapped I/O threshold (%d bytes). Falling back to chunked I/O...\n", fileSize, OptimizedIOThreshold)
            fmt.Println("Using chunked I/O for file transfer...")
            return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)
        }

    case "mthread":
        if fileSize >= 1*1024*1024 { // 1MB minimum for parallel to be effective
            fmt.Println("Using multi-threaded parallel I/O for file transfer...")
            return downloadWithParallelChunksIO(client, remoteFile, localPath, fileSize)
        } else {
            fmt.Printf("File size (%d bytes) is too small for parallel I/O. Falling back to chunked I/O...\n", fileSize)
            fmt.Println("Using chunked I/O for file transfer...")
            return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)
        }

    case "chunked":
        fmt.Println("Using chunked I/O for file transfer...")
        return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)

    case "auto-mmap":
        // Legacy mode: prefer mmap for large files
        if fileSize >= OptimizedIOThreshold {
            fmt.Println("Using memory-mapped I/O for large file transfer...")
            return downloadWithOptimizedIO(client, remoteFile, localPath, fileSize)
        } else if fileSize >= 50*1024*1024 { // 50MB threshold for parallel mode
            fmt.Println("Using parallel chunked I/O for large file transfer...")
            return downloadWithParallelChunksIO(client, remoteFile, localPath, fileSize)
        } else {
            fmt.Println("Using chunked I/O for file transfer...")
            return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)
        }

    case "auto":
        fallthrough
    default:
        // Auto mode: smart selection based on file size
        if fileSize >= 100*1024*1024 { // 100MB+ -> parallel
            fmt.Println("Using parallel chunked I/O for large file transfer...")
            return downloadWithParallelChunksIO(client, remoteFile, localPath, fileSize)
        } else {
            fmt.Println("Using chunked I/O for file transfer...")
            return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)
        }
    }
}

// downloadWithOptimizedIO downloads a file using memory-mapped I/O
func downloadWithOptimizedIO(client *ftp.ServerConn, remoteFile, localPath string, fileSize int64) error {
	// Create memory-mapped file
	mmapData, file, err := CreateMmapFile(localPath, fileSize)
	if err != nil {
		fmt.Printf("Memory mapping failed (%v), falling back to chunked I/O...\n", err)
		return downloadWithChunkedIO(client, remoteFile, localPath, fileSize)
	}
	defer func() {
		if mmapData != nil {
			SyncMmapFile(mmapData)
			MunmapFile(mmapData)
		}
		if file != nil {
			file.Close()
		}
	}()

	// Download file
	resp, err := client.Retr(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to download file: %v", err)
	}
	// Enhanced response closing to filter known non-issues
	defer func() {
		if closeErr := resp.Close(); closeErr != nil {
			if !strings.Contains(closeErr.Error(), "200 NOOP command successful") {
				fmt.Printf("Warning: Failed to close response: %v\n", closeErr)
			}
		}
	}()

	// Progress tracking
	var transferredBytes int64
	startTime := time.Now()
	lastUpdate := startTime
	var lastBytes int64

	// Copy data to memory-mapped file with proper error handling
	buf := make([]byte, 4*1024*1024) // 4MB buffer
	for transferredBytes < fileSize {
		n, err := resp.Read(buf)
		if n > 0 {
			// Ensure we don't exceed file size
			if transferredBytes+int64(n) > fileSize {
				n = int(fileSize - transferredBytes)
			}
			copy(mmapData[transferredBytes:transferredBytes+int64(n)], buf[:n])
			transferredBytes += int64(n)

			// Update progress
			now := time.Now()
			if now.Sub(lastUpdate) >= 100*time.Millisecond {
				bytesDiff := transferredBytes - lastBytes
				timeDiff := now.Sub(lastUpdate).Seconds()
				speed := float64(bytesDiff) / timeDiff
				progress := float64(transferredBytes) / float64(fileSize) * 100
				if progress > 100 {
					progress = 100
				}

				fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
					progressBar(progress),
					progress,
					speed/1024/1024,
					int(now.Sub(startTime).Seconds()))

				lastUpdate = now
				lastBytes = transferredBytes
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error during download: %v", err)
		}
	}

	// Calculate and print final statistics
	elapsed := time.Since(startTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond // Prevent division by zero
	}
	avgSpeed := float64(transferredBytes) / elapsed.Seconds() / 1024 / 1024 // MB/s
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		avgSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Download completed - Average speed: %.2f MB/s\n", avgSpeed)

	// Log performance metrics using centralized function
	logDownloadMetrics(client, remoteFile, fileSize, avgSpeed, elapsed, "Download_Mmap")

	return nil
}

// downloadWithChunkedIO downloads a file using chunked I/O with resume support
func downloadWithChunkedIO(client *ftp.ServerConn, remoteFile, localPath string, fileSize int64) error {
	// Check if local file exists and resume if possible
	var startOffset int64 = 0
	var localFile *os.File
	var err error

	if _, err := os.Stat(localPath); err == nil {
		// File exists, check its size and resume if valid
		if stat, err := os.Stat(localPath); err == nil && stat.Size() < fileSize {
			startOffset = stat.Size()
			fmt.Printf("Resuming download from offset: %d bytes\n", startOffset)
			localFile, err = os.OpenFile(localPath, os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open existing file: %v", err)
			}
		} else if stat.Size() >= fileSize {
			// File is already complete or larger, recreate it
			localFile, err = os.Create(localPath)
			if err != nil {
				return fmt.Errorf("failed to create local file: %v", err)
			}
			startOffset = 0
		} else {
			localFile, err = os.Create(localPath)
			if err != nil {
				return fmt.Errorf("failed to create local file: %v", err)
			}
			startOffset = 0
		}
	} else {
		// File doesn't exist, create new
		localFile, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create local file: %v", err)
		}
	}
	defer localFile.Close()

	// Calculate optimal chunk size based on file size
	chunkSize := calculateOptimalChunkSize(fileSize)
	fmt.Printf("Using chunk size: %d bytes\n", chunkSize)

	// Progress tracking
	var transferredBytes int64 = startOffset
	startTime := time.Now()
	lastUpdate := startTime
	var lastBytes int64 = startOffset

	// Download file in chunks
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		// Calculate current chunk size
		currentChunkSize := chunkSize
		if offset+chunkSize > fileSize {
			currentChunkSize = fileSize - offset
		}

		// Download chunk with retry logic
		err = RetryWithExponentialBackoff("download chunk", func() error {
			resp, err := client.RetrFrom(remoteFile, uint64(offset))
			if err != nil {
				return fmt.Errorf("failed to download chunk: %v", err)
			}
			defer resp.Close()

			// Read chunk data with progress tracking, writing directly to file
			buffer := make([]byte, 2*1024*1024) // 1MB buffer for streaming
			var chunkBytesRead int64

			for chunkBytesRead < currentChunkSize {
				bytesToRead := int64(len(buffer))
				if chunkBytesRead+bytesToRead > currentChunkSize {
					bytesToRead = currentChunkSize - chunkBytesRead
				}

				n, readErr := resp.Read(buffer[:bytesToRead])
				if n > 0 {
					// Write directly to file at correct offset
					_, writeErr := localFile.WriteAt(buffer[:n], offset+chunkBytesRead)
					if writeErr != nil {
						return fmt.Errorf("error writing to file: %v", writeErr)
					}
					chunkBytesRead += int64(n)
					transferredBytes += int64(n)

					// Update progress more frequently
					now := time.Now()
					if now.Sub(lastUpdate) >= 100*time.Millisecond {
						bytesDiff := transferredBytes - lastBytes
						timeDiff := now.Sub(lastUpdate).Seconds()
						speed := float64(bytesDiff) / timeDiff
						progress := float64(transferredBytes) / float64(fileSize) * 100
						if progress > 100 {
							progress = 100
						}

						fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
							progressBar(progress),
							progress,
							speed/1024/1024,
							int(now.Sub(startTime).Seconds()))

						lastUpdate = now
						lastBytes = transferredBytes
					}
				}
				if readErr != nil {
					if readErr == io.EOF {
						break
					}
					return fmt.Errorf("failed to read chunk data: %v", readErr)
				}
			}
			return nil
		})

		if err != nil {
			return fmt.Errorf("error downloading chunk at offset %d: %v", offset, err)
		}

		// Note: transferredBytes is already updated inside the chunk reading loop
	}

	// Calculate and print final statistics
	elapsed := time.Since(startTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond // Prevent division by zero
	}
	avgSpeed := float64(transferredBytes) / elapsed.Seconds() / 1024 / 1024 // MB/s
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		avgSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Download completed - Average speed: %.2f MB/s\n", avgSpeed)

	// Log performance metrics using centralized function
	logDownloadMetrics(client, remoteFile, fileSize, avgSpeed, elapsed, "Download_Chunked")

	return nil
}

func downloadWithParallelChunksIO(client *ftp.ServerConn, remoteFile, localPath string, fileSize int64) error {
	return downloadWithParallelChunksIOWithConfig(client, remoteFile, localPath, fileSize, nil)
}

// downloadWithParallelChunksIOWithConfig downloads a file using parallel connections with config
func downloadWithParallelChunksIOWithConfig(client *ftp.ServerConn, remoteFile, localPath string, fileSize int64, cfg *config.FTPLoginConfig) error {

	// ✅ Step 4: Ensure output file can be created (create and immediately close to test permissions)
	// Each goroutine will open its own file handle for thread-safe writes
	testFile, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create local file: %v", err)
	}
	testFile.Close() // Close immediately - each goroutine will open its own handle

	// ✅ Step 2: File Metadata Preparation
	chunkSize := calculateOptimalChunkSize(fileSize)
	numChunks := (fileSize + chunkSize - 1) / chunkSize // Ceiling division
	fmt.Printf("📦 File Size: %d bytes, Chunk Size: %d bytes, Number of Chunks: %d\n", fileSize, chunkSize, numChunks)

	// ✅ Step 3: Initialize Connection Pool
	connPool, err := createFTPConnectionPoolWithConfig(client, maxParallelDownloads, cfg)
	if err != nil {
		return err
	}
	defer func() {
		// Only quit connections that are not the base client to avoid multiple quits
		for i, conn := range connPool {
			if i > 0 && conn != client { // Skip base client (index 0) and avoid duplicates
				if err := conn.Quit(); err != nil {
					fmt.Printf("Warning: Failed to close connection %d: %v\n", i, err)
				}
			}
		}
	}()


	// Create chunk tasks with assigned connections
	var tasks []ChunkTask
	chunkID := 0
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		length := chunkSize
		if offset+chunkSize > fileSize {
			length = fileSize - offset
		}
		conn := connPool[chunkID%len(connPool)] // Round-robin connection assignment
		tasks = append(tasks, ChunkTask{
			Conn:       conn,
			RemoteFile: remoteFile,
			Offset:     offset,
			Length:     length,
			ChunkID:    chunkID,
		})
		chunkID++
	}

	// ✅ Step 6: Track Progress / Speed
	var transferredBytes int64
	startTime := time.Now()
	progressDone := make(chan bool)

	// Enhanced progress reporting with proper termination
	go func() {
		lastUpdate := startTime
		lastBytes := int64(0)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				done := atomic.LoadInt64(&transferredBytes)
				now := time.Now()
				speed := float64(done-lastBytes) / now.Sub(lastUpdate).Seconds()
				progress := float64(done) / float64(fileSize) * 100
				if progress > 100 {
					progress = 100
				}
				fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
					progressBar(progress), progress, speed/1024/1024, int(now.Sub(startTime).Seconds()))
				lastUpdate = now
				lastBytes = done
			case <-progressDone:
				return
			}
		}
	}()

	// ✅ Step 5: Launch Goroutines Per Chunk
	var wg sync.WaitGroup
	var errorCount int64
	var hasErrors bool
	var errorMutex sync.Mutex

	bufferPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 256*1024) // 256KB buffer as per your example
		},
	}

	// Launch parallel download tasks
	for _, task := range tasks {
		wg.Add(1)
		go func(t ChunkTask) {
			defer wg.Done()
			
			// ✅ Step 8: Enhanced Retry Logic
			err := retryChunk(t, localPath, &transferredBytes, &bufferPool)
			
			if err != nil {
				fmt.Printf("\nERROR: Chunk %d failed at offset %d after retries: %v\n", t.ChunkID, t.Offset, err)
				atomic.AddInt64(&errorCount, 1)
				errorMutex.Lock()
				hasErrors = true
				errorMutex.Unlock()
			}
		}(task)
	}

	// ✅ Step 7: Join All Threads & Finalize
	wg.Wait()
	progressDone <- true // Stop progress reporting

	// Check for errors after all chunks complete
	if hasErrors {
		failedChunks := atomic.LoadInt64(&errorCount)
		return fmt.Errorf("parallel download failed: %d chunks failed after retries", failedChunks)
	}

	// Verify we got all the data
	if transferredBytes != fileSize {
		return fmt.Errorf("download incomplete: expected %d bytes, got %d bytes", fileSize, transferredBytes)
	}

	// Final stats
	elapsed := time.Since(startTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond
	}
	avgSpeed := float64(transferredBytes) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100), avgSpeed, int(elapsed.Seconds()))
	fmt.Printf("Download completed successfully - Average speed: %.2f MB/s\n", avgSpeed)

	// Log performance metrics using centralized function
	logDownloadMetrics(client, remoteFile, fileSize, avgSpeed, elapsed, "Download_Parallel")

	return nil
}

// downloadChunkWithOptions downloads a single chunk using REST + RETR commands
// This function implements the core parallel download logic with optional zero-initialization
func downloadChunkWithOptions(task ChunkTask, targetPath string, transferredBytes *int64, bufferPool *sync.Pool, zeroInitialize bool) error {
	// ✅ Step 5: Each goroutine opens its own file handle for safe concurrent writes
	file, err := os.OpenFile(targetPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for chunk %d: %v", task.ChunkID, err)
	}
	defer file.Close()

	// ✅ Step 5: Use REST command to seek to offset, then RETR
	// The RetrFrom function in the ftp library handles REST + RETR automatically
	resp, err := task.Conn.RetrFrom(task.RemoteFile, uint64(task.Offset))
	if err != nil {
		return fmt.Errorf("failed to retrieve chunk %d from offset %d: %v", task.ChunkID, task.Offset, err)
	}
	defer func() {
		if closeErr := resp.Close(); closeErr != nil {
			// Filter out known non-issues
			if !strings.Contains(closeErr.Error(), "200") && !strings.Contains(closeErr.Error(), "226") {
				fmt.Printf("Warning: Failed to close response for chunk %d: %v\n", task.ChunkID, closeErr)
			}
		}
	}()

	// Get buffer from pool
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	// ✅ Step 5: Download chunk data with progress tracking
	var bytesRead int64

	// Zero-initialize chunk range if requested
	if zeroInitialize {
		zeroBuffer := make([]byte, task.Length)
		_, zeroErr := file.WriteAt(zeroBuffer, task.Offset)
		if zeroErr != nil {
			return fmt.Errorf("error zero-initializing chunk %d: %v", task.ChunkID, zeroErr)
		}
	}

	// Continue with download
	for bytesRead < task.Length {
		toRead := int64(len(buffer))
		if bytesRead+toRead > task.Length {
			toRead = task.Length - bytesRead
		}

		n, readErr := resp.Read(buffer[:toRead])
		if n > 0 {
			// ✅ Step 5: Write to correct offset in the output file
			_, writeErr := file.WriteAt(buffer[:n], task.Offset+bytesRead)
			if writeErr != nil {
				return fmt.Errorf("write error for chunk %d: %v", task.ChunkID, writeErr)
			}
			bytesRead += int64(n)

			// ✅ Step 6: Update progress atomically
			atomic.AddInt64(transferredBytes, int64(n))
		}

		if readErr != nil {
			if readErr == io.EOF {
				break // End of chunk
			}
			return fmt.Errorf("read error for chunk %d: %v", task.ChunkID, readErr)
		}
	}

	// ✅ Step 9: Verify chunk completion
	if bytesRead != task.Length {
		return fmt.Errorf("chunk %d incomplete: expected %d bytes, got %d bytes", task.ChunkID, task.Length, bytesRead)
	}

	return nil
}

// downloadChunk downloads a single chunk with zero-initialization (legacy wrapper)
func downloadChunk(task ChunkTask, targetPath string, transferredBytes *int64, bufferPool *sync.Pool) error {
	return downloadChunkWithOptions(task, targetPath, transferredBytes, bufferPool, true)
}

// retryChunk implements enhanced retry logic for chunk downloads
func retryChunk(task ChunkTask, targetPath string, transferredBytes *int64, bufferPool *sync.Pool) error {
	maxRetries := 5
	baseRetryDelay := 500 * time.Millisecond
	maxRetryDelay := 30 * time.Second

	// Reset retry counter at the start
	atomic.StoreInt64(&RetryCounter, 0)

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Only zero-initialize on retries (not first attempt)
		if attempt > 1 {
			// Zero-initialize chunk range to prevent partial overwrites
			if zeroErr := zeroInitializeChunkRange(targetPath, task.Offset, task.Length); zeroErr != nil {
				fmt.Printf("\nWARNING: Failed to zero-initialize chunk %d for retry: %v\n", task.ChunkID, zeroErr)
			}
		}

		// Attempt chunk download
		err = downloadChunkWithOptions(task, targetPath, transferredBytes, bufferPool, false)
		if err == nil {
			return nil // Success
		}

		atomic.AddInt64(&RetryCounter, 1)

		if attempt < maxRetries {
			delay := baseRetryDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}

			// Add jitter (±20%)
			jitter := float64(delay) * (0.8 + 0.4*float64(attempt%100)/100.0) // Pseudo-random jitter
			delay = time.Duration(jitter)

			fmt.Printf("\nRetry %d/%d for chunk %d after %v: %v\n",
				attempt, maxRetries, task.ChunkID, delay, err)

			time.Sleep(delay)
		}
	}

	return fmt.Errorf("chunk %d failed after %d attempts: %v", task.ChunkID, maxRetries, err)
}

// zeroInitializeChunkRange zeros out a specific range in the file to prevent partial overwrites
func zeroInitializeChunkRange(filePath string, offset, length int64) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for zero-initialization: %v", err)
	}
	defer file.Close()

	// Create zero buffer (limit to reasonable size to avoid excessive memory usage)
	bufferSize := int64(1024 * 1024) // 1MB buffer
	if length < bufferSize {
		bufferSize = length
	}
	zeroBuffer := make([]byte, bufferSize)

	// Zero out the chunk range in chunks
	for written := int64(0); written < length; written += bufferSize {
		toWrite := bufferSize
		if written+bufferSize > length {
			toWrite = length - written
		}

		_, err := file.WriteAt(zeroBuffer[:toWrite], offset+written)
		if err != nil {
			return fmt.Errorf("failed to zero-initialize at offset %d: %v", offset+written, err)
		}
	}

	return nil
}


// downloadChunkSafe downloads a single chunk without zero-initialization (legacy wrapper)
func downloadChunkSafe(task ChunkTask, targetPath string, transferredBytes *int64, bufferPool *sync.Pool) error {
	return downloadChunkWithOptions(task, targetPath, transferredBytes, bufferPool, false)
}

func createFTPConnectionPool(baseClient *ftp.ServerConn, count int) ([]*ftp.ServerConn, error) {
	return createFTPConnectionPoolWithConfig(baseClient, count, nil)
}

// DownloadFileParallelWithConfig downloads a file using multiple real FTP connections
// This is the main function that external code should call for parallel downloads
func DownloadFileParallelWithConfig(baseClient *ftp.ServerConn, remoteFile, localPath string, cfg *config.FTPLoginConfig) error {
	// Get file size
	fileSize, err := baseClient.FileSize(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to get file size: %v", err)
	}

	// Create parent directories if needed
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create directories: %v", err)
	}

	fmt.Printf("Downloading %s (%d bytes) using multiple real FTP connections...\n", remoteFile, fileSize)

	// Use parallel chunks I/O with multiple real connections
	return downloadWithParallelChunksIOWithConfig(baseClient, remoteFile, localPath, fileSize, cfg)
}

// createFTPConnectionPoolWithConfig creates a pool of FTP connections using the provided config
// Enhanced with better error handling, connection validation, and timeout management
func createFTPConnectionPoolWithConfig(baseClient *ftp.ServerConn, count int, cfg *config.FTPLoginConfig) ([]*ftp.ServerConn, error) {
	var pool []*ftp.ServerConn
	var newConnections []*ftp.ServerConn  // Track newly created connections for proper cleanup
	var connectionErrors []string          // Collect all connection errors for better diagnostics

	// Validate base client connection health before proceeding
	if err := validateConnectionHealth(baseClient); err != nil {
		return nil, fmt.Errorf("base FTP connection is unhealthy: %v", err)
	}

	// Add the base client as the first connection
	pool = append(pool, baseClient)
	fmt.Printf("Using base FTP connection (1/%d) - connection validated\n", count)

	// Increase delay to ensure server is ready for connections (improved timing)
	time.Sleep(200 * time.Millisecond) // Adding delay for server readiness

	// If no config provided, limit to single connection to avoid unsafe sharing
	if cfg == nil {
		fmt.Println("No connection config provided. Limiting to single connection for safety.")
		return pool, nil
	}

	// Validate configuration before attempting connections
	if err := validateFTPConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid FTP configuration: %v", err)
	}

	// Create additional connections using the provided config with enhanced error handling
	fmt.Printf("Creating %d additional FTP connections to %s with timeout %v...\n", count-1, cfg.Address, cfg.Timeout)
	successfulConnections := 1  // Count base connection
	maxRetries := 3             // Maximum retries per connection attempt
	baseDelay := 250 * time.Millisecond // Base delay between connection attempts
	
	// CORE CORRECTION: Connection creation loop with proper rate limiting
	for i := 1; i < count && successfulConnections < count; i++ {
		var conn *ftp.ServerConn
		var lastErr error
		connectionCreated := false

		// Rate limiting: Don't create too many connections simultaneously
		if i > 1 && i%2 == 0 {
			time.Sleep(500 * time.Millisecond) // Stagger connection creation
		}

		// Retry logic for each connection with exponential backoff
		for attempt := 1; attempt <= maxRetries && !connectionCreated; attempt++ {
			// Progressive delay between attempts
			if attempt > 1 {
				delay := baseDelay * time.Duration(1<<uint(attempt-1)) // Exponential backoff
				if delay > 5*time.Second {
					delay = 5 * time.Second // Cap at 5 seconds
				}
				fmt.Printf("Retrying connection %d (attempt %d/%d) after %v...\n", i, attempt, maxRetries, delay)
				time.Sleep(delay)
			}

			// Enhanced connection creation with better timeout handling
			conn, lastErr = createFTPConnectionWithTimeout(cfg, fmt.Sprintf("connection_%d", i))
			if lastErr != nil {
				fmt.Printf("Attempt %d/%d failed for connection %d: %v\n", attempt, maxRetries, i, lastErr)
				continue
			}

			// Validate the newly created connection
			if err := validateConnectionHealth(conn); err != nil {
				conn.Quit() // Clean up invalid connection
				lastErr = fmt.Errorf("connection health check failed: %v", err)
				fmt.Printf("Connection %d failed health check on attempt %d/%d: %v\n", i, attempt, maxRetries, lastErr)
				continue
			}

			connectionCreated = true
		}

		// Handle connection creation result
		if !connectionCreated {
			errorMsg := fmt.Sprintf("Failed to create connection %d after %d attempts: %v", i, maxRetries, lastErr)
			connectionErrors = append(connectionErrors, errorMsg)
			fmt.Printf("⚠️  %s\n", errorMsg)
			continue
		}

		// Successfully created and validated connection
		pool = append(pool, conn)
		newConnections = append(newConnections, conn)  // Track for cleanup
		successfulConnections++
		fmt.Printf("✓ Created and validated FTP connection %d (total: %d)\n", i, successfulConnections)

		// Small delay between successful connections to avoid overwhelming server
		if i < count-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	fmt.Printf("Connection pool ready with %d connections (requested: %d)\n", len(pool), count)
	
	// CORE CORRECTION: Enhanced error reporting and graceful degradation
	if len(pool) < count {
		fmt.Printf("⚠️  WARNING: Only %d out of %d requested connections available for parallel downloads\n", len(pool), count)
		
		if len(connectionErrors) > 0 {
			fmt.Printf("Connection creation errors encountered:\n")
			for idx, errMsg := range connectionErrors {
				fmt.Printf("  %d. %s\n", idx+1, errMsg)
			}
		}
		
		// If we have very few connections, gracefully degrade to single connection
		if len(pool) == 1 {
			fmt.Printf("ℹ️  NOTICE: Falling back to single-connection download (still fast!)\n")
			return pool, nil
		}
		
		// If we have at least 2 connections, proceed with reduced parallelism
		if len(pool) >= 2 {
			fmt.Printf("✅ Proceeding with %d connections (reduced parallelism but still effective)\n", len(pool))
		}
	}
	
	// Final connection pool validation
	if len(pool) == 0 {
		return nil, fmt.Errorf("failed to create any FTP connections: no valid connections in pool")
	}
	
	return pool, nil
}

// validateFTPConfig validates the FTP configuration parameters
func validateFTPConfig(cfg *config.FTPLoginConfig) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil")
	}
	if cfg.Address == "" {
		return fmt.Errorf("server address cannot be empty")
	}
	if cfg.Username == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got: %v", cfg.Timeout)
	}
	return nil
}

// createFTPConnectionWithTimeout creates a single FTP connection with enhanced timeout management
func createFTPConnectionWithTimeout(cfg *config.FTPLoginConfig, connName string) (*ftp.ServerConn, error) {
	// Use a context with timeout for the entire connection process
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout*2) // Double timeout for safety
	defer cancel()

	// Channel to receive connection result
	type connResult struct {
		conn *ftp.ServerConn
		err  error
	}
	resultChan := make(chan connResult, 1)

	// Create connection in a goroutine to enable timeout handling
	go func() {
		conn, err := ftp.Dial(cfg.Address, ftp.DialWithTimeout(cfg.Timeout))
		if err != nil {
			resultChan <- connResult{nil, fmt.Errorf("dial failed: %v", err)}
			return
		}

		// Attempt login with timeout
		if err := conn.Login(cfg.Username, cfg.Password); err != nil {
			conn.Quit() // Clean up connection on login failure
			resultChan <- connResult{nil, fmt.Errorf("login failed: %v", err)}
			return
		}

		resultChan <- connResult{conn, nil}
	}()

	// Wait for connection or timeout
	select {
	case result := <-resultChan:
		if result.err != nil {
			return nil, fmt.Errorf("%s creation failed: %v", connName, result.err)
		}
		return result.conn, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%s creation timed out after %v", connName, cfg.Timeout*2)
	}
}

// validateConnectionHealth performs a health check on an FTP connection
func validateConnectionHealth(conn *ftp.ServerConn) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Use NOOP command to test connection health with timeout
	type noopResult struct {
		err error
	}
	resultChan := make(chan noopResult, 1)

	go func() {
		err := conn.NoOp()
		resultChan <- noopResult{err}
	}()

	// Wait for NOOP result with timeout
	select {
	case result := <-resultChan:
		if result.err != nil {
			return fmt.Errorf("NOOP command failed: %v", result.err)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("connection health check timed out")
	}
}
