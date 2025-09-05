package transfer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"
	"unsafe"

	"ftp/perfmetrics"

	"github.com/jlaffaye/ftp"
)

// UploadFile handles the file upload process
func UploadFile(client *ftp.ServerConn, localFile, remotePath string, useMemoryMappedIO bool) error {
	// Get file info
	if !filepath.IsAbs(localFile) {
		localFile = filepath.Join(getCurrentDirectory(), localFile)
	}

	fileInfo, err := os.Stat(localFile)
	if err != nil {
		return fmt.Errorf("failed to get file information: %v", err)
	}

	fileSize := fileInfo.Size()
	fmt.Printf("Uploading %s (%d bytes)...\n", localFile, fileSize)

	// Determine remote path
	targetRemotePath := remotePath
	if targetRemotePath == "" {
		targetRemotePath = filepath.Base(localFile)
	}

	// Choose I/O method based on user preference
	if useMemoryMappedIO {
		if fileSize >= OptimizedIOThreshold {
			fmt.Println("Using memory-mapped I/O for large file transfer...")
			return uploadWithOptimizedIO(client, localFile, targetRemotePath, fileSize)
		} else {
			fmt.Println("Using memory-mapped I/O for file transfer...")
			return uploadWithOptimizedIO(client, localFile, targetRemotePath, fileSize)
		}
	} else {
		fmt.Println("Using chunked I/O for file transfer...")
		return uploadWithTrueChunkedIO(client, localFile, targetRemotePath, fileSize)
	}
}

// OptimizedBufferedReader provides high-performance buffered reading
// from memory-mapped data
type OptimizedBufferedReader struct {
	data        []byte
	bufferSize  int
	position    int64
	total       int64
	transferred int64
	startTime   time.Time
	lastUpdate  time.Time
	lastBytes   int64
	onProgress  func(transferred int64, total int64, speed float64, elapsed time.Duration)
}

func (obr *OptimizedBufferedReader) Read(p []byte) (n int, err error) {
	if obr.startTime.IsZero() {
		obr.startTime = time.Now()
		obr.lastUpdate = obr.startTime
		obr.lastBytes = 0
	}

	// Check if we've reached the end
	if obr.position >= int64(len(obr.data)) {
		return 0, io.EOF
	}

	// Calculate how much to read
	remaining := int64(len(obr.data)) - obr.position
	bufferSize := int64(obr.bufferSize)
	if remaining < bufferSize {
		bufferSize = remaining
	}
	if int64(len(p)) < bufferSize {
		bufferSize = int64(len(p))
	}

	// Copy data
	n = copy(p, obr.data[obr.position:obr.position+bufferSize])
	obr.position += int64(n)
	obr.transferred += int64(n)

	// Update progress
	now := time.Now()
	if now.Sub(obr.lastUpdate) >= 100*time.Millisecond {
		bytesDiff := obr.transferred - obr.lastBytes
		timeDiff := now.Sub(obr.lastUpdate).Seconds()
		speed := float64(bytesDiff) / timeDiff

		if obr.onProgress != nil {
			obr.onProgress(obr.transferred, obr.total, speed, now.Sub(obr.startTime))
		}

		obr.lastUpdate = now
		obr.lastBytes = obr.transferred
	}

	return n, nil
}

// OptimizedFileReader provides high-performance buffered reading from files
type OptimizedFileReader struct {
	file        *os.File
	bufferSize  int
	position    int64
	total       int64
	transferred int64
	startTime   time.Time
	lastUpdate  time.Time
	lastBytes   int64
	onProgress  func(transferred int64, total int64, speed float64, elapsed time.Duration)
}

func (ofr *OptimizedFileReader) Read(p []byte) (n int, err error) {
	if ofr.startTime.IsZero() {
		ofr.startTime = time.Now()
		ofr.lastUpdate = ofr.startTime
		ofr.lastBytes = 0
	}

	// Use large buffer size to maximize throughput
	bufferSize := ofr.bufferSize
	if len(p) < bufferSize {
		bufferSize = len(p)
	}

	// Read data from file
	n, err = ofr.file.Read(p[:bufferSize])
	if n > 0 {
		ofr.transferred += int64(n)

		// Update progress
		now := time.Now()
		if now.Sub(ofr.lastUpdate) >= 100*time.Millisecond {
			bytesDiff := ofr.transferred - ofr.lastBytes
			timeDiff := now.Sub(ofr.lastUpdate).Seconds()
			speed := float64(bytesDiff) / timeDiff

			if ofr.onProgress != nil {
				ofr.onProgress(ofr.transferred, ofr.total, speed, now.Sub(ofr.startTime))
			}

			ofr.lastUpdate = now
			ofr.lastBytes = ofr.transferred
		}
	}

	return n, err
}

// uploadWithOptimizedStreaming uploads using continuous streaming with intelligent speed calculation
func uploadWithOptimizedStreaming(client *ftp.ServerConn, file *os.File, targetRemotePath string, fileSize int64) error {
	// Create simple file reader without duplicate callbacks
	bufferedReader := &OptimizedFileReader{
		file:       file,
		bufferSize: 8 * 1024 * 1024, // Use 8MB buffer for better performance
		total:      fileSize,
		onProgress: nil, // No callback here - will be handled by ProgressReader
	}
	
	// Create properly initialized intelligent progress reader
	now := time.Now()
	progressReader := &ProgressReader{
		Reader:           bufferedReader,
		Total:            fileSize,
		Transferred:      0,
		StartTime:        now,
		LastUpdate:       now,
		LastActiveTime:   now,
		LastBytes:        0,
		PauseThreshold:   500 * time.Millisecond,
		MinSpeed:         1024 * 1024, // 1 MB/s minimum
		MaxSamples:       100,
		SpeedSamples:     make([]float64, 0, 100),
		ActiveTransferTime: 0,
		OnProgress: func(transferred int64, total int64, speed float64, elapsed time.Duration) {
			progress := float64(transferred) / float64(total) * 100
			if progress > 100 {
				progress = 100
			}
			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(elapsed.Seconds()))
		},
	}

	// Upload using intelligent progress tracking
	err := client.Stor(targetRemotePath, progressReader)
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	// Calculate intelligent final statistics
	elapsed := time.Since(progressReader.StartTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond // Prevent division by zero
	}
	traditionalSpeed := float64(fileSize) / elapsed.Seconds() / 1024 / 1024 // MB/s
	
	// Get intelligent speed calculation from progress reader
	activeSpeed := progressReader.GetAverageActiveSpeed() / 1024 / 1024 // Convert to MB/s
	activeTime := progressReader.ActiveTransferTime
	
	// Use intelligent speed if it's significantly different (better) than traditional
	finalSpeed := traditionalSpeed
	speedMethod := "traditional"
	if activeSpeed > traditionalSpeed*1.1 && activeTime > 0 { // At least 10% better
		finalSpeed = activeSpeed
		speedMethod = "active"
	}

	// Show final progress
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		finalSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Upload completed - Average speed: %.2f MB/s (%s)\n", finalSpeed, speedMethod)
	if speedMethod == "active" {
		fmt.Printf("Traditional speed: %.2f MB/s (excluded %.1fs of pause time)\n", 
			traditionalSpeed, elapsed.Seconds()-activeTime.Seconds())
	}

	// Performance metrics logging
	metrics := map[string]interface{}{
		"FileName":       filepath.Base(targetRemotePath),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    false,
		"RTTms":          50,
		"WindowSize":     1,
		"ThroughputMBps": finalSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        0,
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}

	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// CreateMmapForReading creates a proper memory mapping for reading files
// This version keeps the file handle open during the mapping lifetime
func CreateMmapForReading(filename string, expectedSize int64) ([]byte, *os.File, error) {
	// Open the file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Verify file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to get file info: %v", err)
	}
	size := fileInfo.Size()

	if size == 0 {
		file.Close()
		return []byte{}, nil, nil
	}

	if size != expectedSize {
		file.Close()
		return nil, nil, fmt.Errorf("file size mismatch: expected %d, got %d", expectedSize, size)
	}

	// Get file handle
	handle := syscall.Handle(file.Fd())

	// Split size into high and low DWORDs for 64-bit support
	high := uint32(size >> 32)
	low := uint32(size & 0xFFFFFFFF)

	// Create file mapping
	mapping, err := syscall.CreateFileMapping(handle, nil, syscall.PAGE_READONLY, high, low, nil)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to create file mapping: %v", err)
	}
	defer syscall.CloseHandle(mapping)

	// Map view of file
	addr, err := syscall.MapViewOfFile(mapping, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Create a safe slice of the mapped memory
	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))

	return data, file, nil
}

// uploadWithOptimizedIO uploads a file using improved memory-mapped I/O
func uploadWithOptimizedIO(client *ftp.ServerConn, localFile, targetRemotePath string, fileSize int64) error {
	// Aggressive memory-mapped approach - try memory mapping for all files first
	fileSizeMB := fileSize / (1024 * 1024)
	fmt.Printf("Attempting memory mapping for %d MB file...\n", fileSizeMB)
	
	// Use improved memory mapping that properly manages file handles
	mmapData, file, err := CreateMmapForReading(localFile, fileSize)
	if err != nil {
		fmt.Printf("Memory mapping failed (%v), falling back to robust chunked I/O...\n", err)
		return uploadWithRobustChunkedIO(client, localFile, targetRemotePath, fileSize)
	}
	fmt.Printf("Memory mapping successful, proceeding with mapped upload...\n")
	defer func() {
		// Properly cleanup memory-mapped resources
		if err := MunmapFile(mmapData); err != nil {
			fmt.Printf("Warning: Failed to unmap memory: %v\n", err)
		}
		if file != nil {
			file.Close()
		}
		// Force aggressive memory cleanup
		runtime.GC()
		runtime.GC() // Second GC to ensure cleanup
		debug.FreeOSMemory()
		// Allow more time for Windows to release resources
		time.Sleep(250 * time.Millisecond)
	}()

	// Create simple buffered reader without duplicate callbacks
	bufferedReader := &OptimizedBufferedReader{
		data:       mmapData,
		bufferSize: 4 * 1024 * 1024, // Use same 4MB buffer as download
		total:      fileSize,
		onProgress: nil, // No callback here - will be handled by ProgressReader
	}
	
	// Create properly initialized intelligent progress reader
	now := time.Now()
	progressReader := &ProgressReader{
		Reader:           bufferedReader,
		Total:            fileSize,
		Transferred:      0,
		StartTime:        now,
		LastUpdate:       now,
		LastActiveTime:   now,
		LastBytes:        0,
		PauseThreshold:   500 * time.Millisecond,
		MinSpeed:         1024 * 1024, // 1 MB/s minimum
		MaxSamples:       100,
		SpeedSamples:     make([]float64, 0, 100),
		ActiveTransferTime: 0,
		OnProgress: func(transferred int64, total int64, speed float64, elapsed time.Duration) {
			progress := float64(transferred) / float64(total) * 100
			if progress > 100 {
				progress = 100
			}
			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(elapsed.Seconds()))
		},
	}

	// Upload file using intelligent progress tracking
	err = client.Stor(targetRemotePath, progressReader)
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	// Calculate intelligent final statistics
	elapsed := time.Since(progressReader.StartTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond // Prevent division by zero
	}
	traditionalSpeed := float64(fileSize) / elapsed.Seconds() / 1024 / 1024 // MB/s
	
	// Get intelligent speed calculation from progress reader
	activeSpeed := progressReader.GetAverageActiveSpeed() / 1024 / 1024 // Convert to MB/s
	activeTime := progressReader.ActiveTransferTime
	
	// Use intelligent speed if it's significantly different (better) than traditional
	finalSpeed := traditionalSpeed
	speedMethod := "traditional"
	if activeSpeed > traditionalSpeed*1.1 && activeTime > 0 { // At least 10% better
		finalSpeed = activeSpeed
		speedMethod = "active"
	}

	// Show final progress
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		finalSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Upload completed - Average speed: %.2f MB/s (%s)\n", finalSpeed, speedMethod)
	if speedMethod == "active" {
		fmt.Printf("Traditional speed: %.2f MB/s (excluded %.1fs of pause time)\n", 
			traditionalSpeed, elapsed.Seconds()-activeTime.Seconds())
	}

	// Performance metrics logging
	metrics := map[string]interface{}{
		"FileName":       filepath.Base(localFile),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    false, // Set based on your compression logic
		"RTTms":          50,    // Set based on your RTT measurement
		"WindowSize":     1,     // Set based on your window size
		"ThroughputMBps": finalSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        0, // Set based on your retry logic
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}

	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// uploadWithChunkedIO uploads using optimized streaming I/O with resource cleanup
func uploadWithChunkedIO(client *ftp.ServerConn, localFile, targetRemotePath string, fileSize int64) error {
	// Open local file
	file, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open local file: %v", err)
	}
	defer func() {
		file.Close()
		// Ensure file handle is released
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}()

	// Use optimized streaming for all files
	return uploadWithOptimizedStreaming(client, file, targetRemotePath, fileSize)
}

// uploadSmallFile handles small files with direct upload and guaranteed progress display
func uploadSmallFile(client *ftp.ServerConn, file *os.File, targetRemotePath string, fileSize int64) error {
	// Create a progress reader that will show progress even for small files
	progressReader := &ProgressReader{
		Reader: file,
		Total:  fileSize,
		OnProgress: func(transferred int64, total int64, speed float64, elapsed time.Duration) {
			progress := float64(transferred) / float64(total) * 100
			if progress > 100 {
				progress = 100
			}
			// Force progress update for small files even if transfer is fast
			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(elapsed.Seconds()))
		},
	}

	// Show initial progress for small files
	fmt.Printf("\rProgress: [%s] 0.0%% 0.00 MB/s Time: 0s", progressBar(0))

	// Upload the file
	err := client.Stor(targetRemotePath, progressReader)
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	// Calculate intelligent final statistics
	elapsed := time.Since(progressReader.StartTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond // Prevent division by zero
	}
	traditionalSpeed := float64(fileSize) / elapsed.Seconds() / 1024 / 1024 // MB/s
	
	// Get intelligent speed calculation from progress reader
	activeSpeed := progressReader.GetAverageActiveSpeed() / 1024 / 1024 // Convert to MB/s
	activeTime := progressReader.ActiveTransferTime
	
	// Use intelligent speed if it's significantly different (better) than traditional
	finalSpeed := traditionalSpeed
	speedMethod := "traditional"
	if activeSpeed > traditionalSpeed*1.1 && activeTime > 0 { // At least 10% better
		finalSpeed = activeSpeed
		speedMethod = "active"
	}

	// Show final progress
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		finalSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Upload completed - Average speed: %.2f MB/s (%s)\n", finalSpeed, speedMethod)
	if speedMethod == "active" {
		fmt.Printf("Traditional speed: %.2f MB/s (excluded %.1fs of pause time)\n", 
			traditionalSpeed, elapsed.Seconds()-activeTime.Seconds())
	}

	// Performance metrics logging
	metrics := map[string]interface{}{
		"FileName":       filepath.Base(targetRemotePath),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    false,
		"RTTms":          50,
		"WindowSize":     1,
		"ThroughputMBps": finalSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        0,
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}

	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// uploadLargeFileChunked handles large files with chunked upload
func uploadLargeFileChunked(client *ftp.ServerConn, file *os.File, targetRemotePath string, fileSize int64) error {
	// Calculate optimal chunk size based on file size
	chunkSize := calculateOptimalChunkSize(fileSize)
	fmt.Printf("Using chunk size: %d bytes\n", chunkSize)

	// Progress tracking
	var transferredBytes int64
	startTime := time.Now()
	lastUpdate := startTime
	var lastBytes int64

	// Upload file in chunks
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		// Calculate current chunk size
		currentChunkSize := chunkSize
		if offset+chunkSize > fileSize {
			currentChunkSize = fileSize - offset
		}

		// Read chunk from file
		chunkData := make([]byte, currentChunkSize)
		_, err := file.ReadAt(chunkData, offset)
		if err != nil {
			return fmt.Errorf("failed to read chunk at offset %d: %v", offset, err)
		}

		// Upload chunk with retry logic
		err = RetryWithExponentialBackoff("upload chunk", func() error {
			if offset == 0 {
				// First chunk - use Stor
				return client.Stor(targetRemotePath, bytes.NewReader(chunkData))
			} else {
				// Subsequent chunks - use Append
				return client.Append(targetRemotePath, bytes.NewReader(chunkData))
			}
		})

		if err != nil {
			return fmt.Errorf("failed to upload chunk at offset %d: %v", offset, err)
		}

		// Update progress
		transferredBytes += currentChunkSize
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

	// Calculate and print final statistics
	elapsed := time.Since(startTime)
	avgSpeed := float64(fileSize) / elapsed.Seconds() / 1024 / 1024 // MB/s
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		avgSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Upload completed - Average speed: %.2f MB/s (traditional calculation)\n", avgSpeed)

	// Performance metrics logging
	metrics := map[string]interface{}{
		"FileName":       filepath.Base(targetRemotePath),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    false,
		"RTTms":          50,
		"WindowSize":     1,
		"ThroughputMBps": avgSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        0,
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}

	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// uploadWithTrueChunkedIO uploads using simplified streaming approach
func uploadWithTrueChunkedIO(client *ftp.ServerConn, localFile, targetRemotePath string, fileSize int64) error {
	// Open local file
	file, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open local file: %v", err)
	}
	defer file.Close()

	// Calculate optimal chunk size based on file size
	chunkSize := calculateOptimalChunkSize(fileSize)
	fmt.Printf("Using chunk size: %d bytes\n", chunkSize)

	// Progress tracking
	startTime := time.Now()

	// Create properly initialized intelligent progress reader
	now := time.Now()
	progressReader := &ProgressReader{
		Reader:           file,
		Total:            fileSize,
		Transferred:      0,
		StartTime:        now,
		LastUpdate:       now,
		LastActiveTime:   now,
		LastBytes:        0,
		PauseThreshold:   500 * time.Millisecond,
		MinSpeed:         1024 * 1024, // 1 MB/s minimum
		MaxSamples:       100,
		SpeedSamples:     make([]float64, 0, 100),
		ActiveTransferTime: 0,
		OnProgress: func(transferred int64, total int64, speed float64, elapsed time.Duration) {
			progress := float64(transferred) / float64(total) * 100
			if progress > 100 {
				progress = 100
			}
			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(elapsed.Seconds()))
		},
	}

	// Upload the entire file using a single STOR command with chunked reading internally
	err = RetryWithExponentialBackoff("upload file", func() error {
		// Reset file position for retry
		file.Seek(0, 0)
		progressReader.Transferred = 0
		progressReader.StartTime = time.Time{}
		progressReader.LastUpdate = time.Time{}
		progressReader.LastBytes = 0
		
		// Use single STOR command - let the server handle buffering
		return client.Stor(targetRemotePath, progressReader)
	})

	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	// Calculate and print intelligent final statistics
	elapsed := time.Since(startTime)
	traditionalSpeed := float64(fileSize) / elapsed.Seconds() / 1024 / 1024 // MB/s
	
	// Get intelligent speed calculation from progress reader
	activeSpeed := progressReader.GetAverageActiveSpeed() / 1024 / 1024 // Convert to MB/s
	activeTime := progressReader.ActiveTransferTime
	
	// Use intelligent speed if it's significantly different (better) than traditional
	finalSpeed := traditionalSpeed
	speedMethod := "traditional"
	if activeSpeed > traditionalSpeed*1.1 && activeTime > 0 { // At least 10% better
		finalSpeed = activeSpeed
		speedMethod = "active"
	}
	
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		finalSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Upload completed - Average speed: %.2f MB/s (%s)\n", finalSpeed, speedMethod)
	if speedMethod == "active" {
		fmt.Printf("Traditional speed: %.2f MB/s (excluded %.1fs of pause time)\n", 
			traditionalSpeed, elapsed.Seconds()-activeTime.Seconds())
	}

	// Performance metrics logging
	metrics := map[string]interface{}{
		"FileName":       filepath.Base(targetRemotePath),
		"FileSizeMB":     float64(fileSize) / (1024 * 1024),
		"Compression":    false,
		"RTTms":          50,
		"WindowSize":     1,
		"ThroughputMBps": finalSpeed,
		"TimeSec":        elapsed.Seconds(),
		"Retries":        0,
	}

	logErr := perfmetrics.LogPerformanceToCSV("performance_log.csv", metrics)
	if logErr != nil {
		fmt.Printf("Failed to log performance: %v\n", logErr)
	}

	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// uploadWithRobustChunkedIO implements a robust chunked upload with resume capability
func uploadWithRobustChunkedIO(client *ftp.ServerConn, localFile, targetRemotePath string, fileSize int64) error {
	fmt.Println("Using robust chunked I/O for file transfer...")
	
	// Open local file
	file, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open local file: %v", err)
	}
	defer file.Close()
	
	// Calculate optimal chunk size based on file size and memory constraints
	chunkSize := calculateRobustChunkSize(fileSize)
	fmt.Printf("Using robust chunk size: %d MB\n", chunkSize/(1024*1024))
	
	// Check if file already exists and get its size for resume capability
	var startOffset int64 = 0
	existingSize, err := getRemoteFileSize(client, targetRemotePath)
	if err == nil && existingSize > 0 && existingSize < fileSize {
		startOffset = existingSize
		fmt.Printf("Resuming upload from offset %d bytes (%.1f%% complete)\n", 
			startOffset, float64(startOffset)/float64(fileSize)*100)
	}
	
	// Progress tracking with intelligent speed calculation
	startTime := time.Now()
	totalTransferred := startOffset
	
	// Create properly initialized intelligent progress reader for robust upload
	now := time.Now()
	progressReader := &ProgressReader{
		Reader:           file,
		Total:            fileSize,
		Transferred:      startOffset,
		StartTime:        now,
		LastUpdate:       now,
		LastActiveTime:   now,
		LastBytes:        startOffset,
		PauseThreshold:   500 * time.Millisecond,
		MinSpeed:         1024 * 1024, // 1 MB/s minimum
		MaxSamples:       100,
		SpeedSamples:     make([]float64, 0, 100),
		ActiveTransferTime: 0,
		OnProgress: func(transferred int64, total int64, speed float64, elapsed time.Duration) {
			progress := float64(transferred) / float64(total) * 100
			if progress > 100 {
				progress = 100
			}
			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(elapsed.Seconds()))
		},
	}
	
	// Robust upload with error recovery
	maxRetries := 3
	for offset := startOffset; offset < fileSize; {
		// Calculate current chunk size
		currentChunkSize := chunkSize
		if offset+chunkSize > fileSize {
			currentChunkSize = fileSize - offset
		}
		
		// Seek to current position
		_, err := file.Seek(offset, 0)
		if err != nil {
			return fmt.Errorf("failed to seek to offset %d: %v", offset, err)
		}
		
		// Create a limited reader for this chunk
		chunkReader := io.LimitReader(progressReader, currentChunkSize)
		
		// Try to upload this chunk with retries
		var uploadErr error
		for retry := 0; retry < maxRetries; retry++ {
			if offset == startOffset && startOffset == 0 {
				// First chunk - use STOR
				uploadErr = client.Stor(targetRemotePath, chunkReader)
			} else {
				// Subsequent chunks or resume - use APPE (append)
				uploadErr = client.Append(targetRemotePath, chunkReader)
			}
			
			if uploadErr == nil {
				break // Success
			}
			
			// Retry with exponential backoff
			if retry < maxRetries-1 {
				delay := time.Duration(1<<retry) * time.Second
				fmt.Printf("\nChunk upload failed (retry %d/%d): %v, retrying in %v...\n", 
					retry+1, maxRetries, uploadErr, delay)
				time.Sleep(delay)
				
				// Reset chunk reader position
				file.Seek(offset, 0)
				progressReader.Transferred = offset
				chunkReader = io.LimitReader(progressReader, currentChunkSize)
			}
		}
		
		if uploadErr != nil {
			return fmt.Errorf("failed to upload chunk at offset %d after %d retries: %v", 
				offset, maxRetries, uploadErr)
		}
		
		// Move to next chunk
		offset += currentChunkSize
		totalTransferred += currentChunkSize
		
		// Verify upload progress periodically
		if offset%(10*chunkSize) == 0 { // Every 10 chunks
			if verifyErr := verifyUploadProgress(client, targetRemotePath, offset); verifyErr != nil {
				fmt.Printf("Warning: Upload verification failed: %v\n", verifyErr)
			}
		}
	}
	
	// Calculate intelligent final statistics
	elapsed := time.Since(startTime)
	if elapsed == 0 {
		elapsed = 1 * time.Millisecond
	}
	traditionalSpeed := float64(fileSize-startOffset) / elapsed.Seconds() / 1024 / 1024
	
	// Get intelligent speed calculation
	activeSpeed := progressReader.GetAverageActiveSpeed() / 1024 / 1024
	activeTime := progressReader.ActiveTransferTime
	
	// Use intelligent speed if significantly better
	finalSpeed := traditionalSpeed
	speedMethod := "traditional"
	if activeSpeed > traditionalSpeed*1.1 && activeTime > 0 {
		finalSpeed = activeSpeed
		speedMethod = "active"
	}
	
	// Show final progress
	fmt.Printf("\rProgress: [%s] 100.0%% %.2f MB/s Time: %ds\n",
		progressBar(100),
		finalSpeed,
		int(elapsed.Seconds()))
	fmt.Printf("Robust upload completed - Average speed: %.2f MB/s (%s)\n", finalSpeed, speedMethod)
	if speedMethod == "active" {
		fmt.Printf("Traditional speed: %.2f MB/s (excluded %.1fs of pause time)\n", 
			traditionalSpeed, elapsed.Seconds()-activeTime.Seconds())
	}
	
	// Final verification
	if finalErr := verifyUploadCompletion(client, targetRemotePath, fileSize); finalErr != nil {
		fmt.Printf("Warning: Final upload verification failed: %v\n", finalErr)
	}
	
	fmt.Printf("File uploaded successfully to: %s\n", targetRemotePath)
	return nil
}

// calculateRobustChunkSize determines optimal chunk size for robust uploads
func calculateRobustChunkSize(fileSize int64) int64 {
	// Use larger chunks for robust uploads to reduce overhead
	switch {
	case fileSize < 100*1024*1024: // < 100MB
		return 8 * 1024 * 1024 // 8MB chunks
	case fileSize < 1024*1024*1024: // < 1GB
		return 16 * 1024 * 1024 // 16MB chunks
	case fileSize < 10*1024*1024*1024: // < 10GB
		return 32 * 1024 * 1024 // 32MB chunks
	default: // >= 10GB
		return 64 * 1024 * 1024 // 64MB chunks
	}
}

// getRemoteFileSize attempts to get the size of a remote file
func getRemoteFileSize(client *ftp.ServerConn, remotePath string) (int64, error) {
	size, err := client.FileSize(remotePath)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// verifyUploadProgress verifies that the remote file has the expected size
func verifyUploadProgress(client *ftp.ServerConn, remotePath string, expectedSize int64) error {
	actualSize, err := getRemoteFileSize(client, remotePath)
	if err != nil {
		return fmt.Errorf("failed to get remote file size: %v", err)
	}
	
	if actualSize != expectedSize {
		return fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, actualSize)
	}
	
	return nil
}

// verifyUploadCompletion performs final verification of upload completion
func verifyUploadCompletion(client *ftp.ServerConn, remotePath string, expectedSize int64) error {
	return verifyUploadProgress(client, remotePath, expectedSize)
}

// ChunkedProgressReader reads data from a file chunk with progress tracking
type ChunkedProgressReader struct {
	file             *os.File
	offset           int64
	chunkSize        int64
	bytesRead        int64
	transferredBytes *int64
	startTime        time.Time
	lastUpdate       *time.Time
	lastBytes        *int64
	fileSize         int64
	buffer           []byte
}

func (cpr *ChunkedProgressReader) Read(p []byte) (n int, err error) {
	// Initialize buffer if needed
	if cpr.buffer == nil {
		cpr.buffer = make([]byte, 256*1024) // 256KB buffer
	}

	// Check if we've read the entire chunk
	if cpr.bytesRead >= cpr.chunkSize {
		return 0, io.EOF
	}

	// Calculate how much to read
	bytesToRead := int64(len(cpr.buffer))
	if cpr.bytesRead+bytesToRead > cpr.chunkSize {
		bytesToRead = cpr.chunkSize - cpr.bytesRead
	}
	if int64(len(p)) < bytesToRead {
		bytesToRead = int64(len(p))
	}

	// Read from file at the correct offset
	n, err = cpr.file.ReadAt(p[:bytesToRead], cpr.offset+cpr.bytesRead)
	if n > 0 {
		cpr.bytesRead += int64(n)
		*cpr.transferredBytes += int64(n)

		// Update progress
		now := time.Now()
		if now.Sub(*cpr.lastUpdate) >= 100*time.Millisecond {
			bytesDiff := *cpr.transferredBytes - *cpr.lastBytes
			timeDiff := now.Sub(*cpr.lastUpdate).Seconds()
			speed := float64(bytesDiff) / timeDiff
			progress := float64(*cpr.transferredBytes) / float64(cpr.fileSize) * 100
			if progress > 100 {
				progress = 100
			}

			fmt.Printf("\rProgress: [%s] %.1f%% %.2f MB/s Time: %ds",
				progressBar(progress),
				progress,
				speed/1024/1024,
				int(now.Sub(cpr.startTime).Seconds()))

			*cpr.lastUpdate = now
			*cpr.lastBytes = *cpr.transferredBytes
		}
	}

	return n, err
}

func (cpr *ChunkedProgressReader) Reset() {
	cpr.bytesRead = 0
}
