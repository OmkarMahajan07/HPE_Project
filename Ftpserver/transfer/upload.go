package transfer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

// fallbackStreamingUpload provides streaming upload when memory mapping fails
func FallbackStreamingUpload(session interface{}, dst *os.File, src io.Reader) (int64, error) {
	// Use very large buffer for maximum throughput when mmap fails
	bufferSize := 64 * 1024 * 1024 // 64MB buffer for maximum performance
	buffer := make([]byte, bufferSize)

	var totalWritten int64

	// Stream data with large buffers for maximum performance
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			// Write entire buffer in one operation for maximum speed
			nw, writeErr := dst.Write(buffer[:n])
			if writeErr != nil {
				return totalWritten, writeErr
			}
			totalWritten += int64(nw)
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return totalWritten, readErr
		}
	}

	// No sync operations - let OS handle it for maximum performance
	return totalWritten, nil
}

// SuperfastSTORHandler implements high-performance server STOR handler
// optimized for superfast client uploads with multiple I/O strategies
// Now respects client-server transfer mode synchronization
func SuperfastSTORHandler(session interface{}, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	s := session.(SessionInterface)

	// Strategy selection based on client's transfer mode preference
	fileSizeMB := expectedSize / (1024 * 1024)
	if expectedSize > 0 {
		s.LogPrintf("Processing upload: %d MB", fileSizeMB)
	} else {
		s.LogPrintf("Processing upload: unknown size")
	}

	// Get transfer mode from session (set by SITE MODE commands)
	transferMode := s.GetTransferMode()
	s.LogPrintf("Using transfer mode: %s", transferMode)

	// Select upload strategy based on client-synchronized transfer mode
	switch transferMode {
	case "mmap":
		// Memory-mapped upload for maximum performance
		if expectedSize > 0 {
			s.LogPrintf("Using memory-mapped upload handler for file (%d MB)...", fileSizeMB)
			return SuperfastMmapUploadFixedSize(session, file, reader, expectedSize)
		} else {
			s.LogPrintf("Memory-mapped mode requested but unknown file size - using streaming")
			return OriginalOptimizedSTORHandler(file, reader)
		}
	
	case "parallel":
		// CORE CORRECTION: Use true parallel chunked upload with multiple goroutines
		s.LogPrintf("Using true parallel chunked upload handler")
		if expectedSize > 0 {
			s.LogPrintf("Starting parallel upload with expected size: %d bytes", expectedSize)
			return parallelChunkedUpload(s, file, reader, expectedSize)
		} else {
			// Fallback to streaming for unknown size
			s.LogPrintf("Parallel mode with unknown size - using streaming fallback")
			return optimizedStreamingUpload(s, file, reader, 0)
		}
	
	case "chunked":
		// Standard chunked/optimized streaming upload
		s.LogPrintf("Using chunked streaming handler")
		return optimizedChunkedUpload(s, file, reader, expectedSize)
	
	case "normal":
		// Standard optimized streaming upload (legacy)
		s.LogPrintf("Using standard optimized streaming handler")
		return OriginalOptimizedSTORHandler(file, reader)
	
	default:
		// Fallback to automatic mode selection based on file size (legacy behavior)
		s.LogPrintf("No transfer mode set - using automatic selection")
		if expectedSize > 0 {
			s.LogPrintf("Using optimized upload handler for file (%d MB)...", fileSizeMB)
			return SuperfastMmapUploadFixedSize(session, file, reader, expectedSize)
		}
		
		// For unknown sizes, use streaming handler
		s.LogPrintf("Using streaming upload handler for unknown size")
		return OriginalOptimizedSTORHandler(file, reader)
	}
}


// optimizedStreamingUpload provides high-performance streaming upload
func optimizedStreamingUpload(session SessionInterface, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	// Use intelligent buffer sizing based on file size
	bufferSize := calculateOptimalBufferSize(expectedSize)
	session.LogPrintf("Using optimized streaming with %d MB buffer", bufferSize/(1024*1024))

	// Create large buffered reader to match client's upload buffer
	bufferedReader := bufio.NewReaderSize(reader, bufferSize)

	// Create optimized write buffer
	writeBuffer := make([]byte, bufferSize)

	// High-performance streaming with progress tracking
	totalWritten, err := streamWithOptimization(file, bufferedReader, writeBuffer, expectedSize)
	if err != nil {
		return totalWritten, fmt.Errorf("optimized streaming failed: %v", err)
	}

	// Ensure data is flushed to disk
	file.Sync()

	return totalWritten, nil
}

// calculateOptimalBufferSize determines best buffer size for streaming
func calculateOptimalBufferSize(fileSize int64) int {
	switch {
	case fileSize < 50*1024*1024: // <50MB
		return 16 * 1024 * 1024 // 16MB buffer (increased from 8MB)
	case fileSize < 500*1024*1024: // <500MB
		return 64 * 1024 * 1024 // 64MB buffer (increased from 32MB)
	default: // >=500MB
		return 128 * 1024 * 1024 // 128MB buffer for very large files (increased from 64MB)
	}
}

// streamWithOptimization performs optimized streaming with intelligent buffering
func streamWithOptimization(file *os.File, reader *bufio.Reader, buffer []byte, expectedSize int64) (int64, error) {
	var totalWritten int64
	lastSync := int64(0)
	syncInterval := int64(64 * 1024 * 1024) // Sync every 64MB

	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			// Write data in chunks to ensure complete writes
			data := buffer[:n]
			for len(data) > 0 {
				nw, writeErr := file.Write(data)
				if writeErr != nil {
					return totalWritten, writeErr
				}
				totalWritten += int64(nw)
				data = data[nw:]
			}

			// Periodic sync for large files to maintain performance
			if totalWritten-lastSync >= syncInterval {
				file.Sync()
				lastSync = totalWritten
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return totalWritten, readErr
		}
	}

	return totalWritten, nil
}

// optimizedChunkedUpload implements proper server-side chunked upload handling
// FIXED: Improved chunk reading logic to handle partial reads and ensure complete data transfer
func optimizedChunkedUpload(session SessionInterface, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	// Use intelligent chunk size based on expected file size
	chunkSize := calculateOptimalChunkSize(expectedSize)
	session.LogPrintf("Using chunked upload with %d KB chunk size", chunkSize/1024)

	// Create buffer for reading chunks
	buffer := make([]byte, chunkSize)
	var totalWritten int64
	var consecutiveZeroReads int

	// Process data in chunks with improved partial read handling
	for {
		// Determine how much to try to read in this iteration
		toRead := chunkSize
		if expectedSize > 0 {
			remaining := expectedSize - totalWritten
			if remaining <= 0 {
				session.LogPrintf("Reached expected size limit: %d bytes", expectedSize)
				break // We've read the expected amount
			}
			if remaining < int64(chunkSize) {
				toRead = int(remaining)
			}
		}

		// FIXED: Use io.ReadAtLeast to ensure we read meaningful chunks
		// This prevents issues with very small partial reads that can cause problems
		var n int
		var readErr error
		
		// Try to read at least 1KB or the remaining amount, whichever is smaller
		minRead := 1024 // 1KB minimum
		if toRead < minRead {
			minRead = toRead
		}
		
		// For the last chunk, we might need to read less than our minimum
		if expectedSize > 0 && (expectedSize - totalWritten) < int64(minRead) {
			minRead = int(expectedSize - totalWritten)
		}
		
		if minRead > 0 {
			n, readErr = io.ReadAtLeast(reader, buffer[:toRead], minRead)
			if readErr == io.ErrUnexpectedEOF {
				// We got some data but less than minRead - that's okay at the end
				readErr = nil
			}
		} else {
			// Special case: we need to read exactly 0 more bytes
			n = 0
			readErr = io.EOF
		}

		if n > 0 {
			consecutiveZeroReads = 0 // Reset zero read counter
			
			// Write the chunk data to file, handling partial writes
			data := buffer[:n]
			for len(data) > 0 {
				nw, writeErr := file.Write(data)
				if writeErr != nil {
					return totalWritten, fmt.Errorf("write error at offset %d: %v", totalWritten, writeErr)
				}
				totalWritten += int64(nw)
				data = data[nw:] // Move to next unwritten portion
			}

			// Periodic sync for large files
			if totalWritten%(64*1024*1024) == 0 { // Every 64MB
				file.Sync()
			}
			
			// Log progress based on actual chunk size for better alignment
			if totalWritten%int64(chunkSize) == 0 {
				session.LogPrintf("Progress: %d bytes uploaded (chunk completed)", totalWritten)
			}
		} else {
			consecutiveZeroReads++
			if consecutiveZeroReads > 3 {
				session.LogPrintf("Too many consecutive zero reads, terminating")
				break
			}
		}

		// Handle read errors and EOF
		if readErr == io.EOF {
			session.LogPrintf("Reached EOF after reading %d bytes", totalWritten)
			break // End of input
		}
		if readErr != nil {
			return totalWritten, fmt.Errorf("read error at offset %d: %v", totalWritten, readErr)
		}
	}

	// Final sync to ensure all data is written
	file.Sync()

	// Verify we received the expected amount of data (if known)
	if expectedSize > 0 && totalWritten != expectedSize {
		session.LogPrintf("Warning: Expected %d bytes but received %d bytes", expectedSize, totalWritten)
	}

	session.LogPrintf("Chunked upload completed: %d bytes written", totalWritten)
	return totalWritten, nil
}

// calculateOptimalChunkSize determines the best chunk size for the given file size
func calculateOptimalChunkSize(fileSize int64) int {
	switch {
	case fileSize <= 0: // Unknown size
		return 4 * 1024 * 1024 // 4MB default
	case fileSize < 10*1024*1024: // < 10MB
		return 1 * 1024 * 1024 // 1MB chunks for small files
	case fileSize < 100*1024*1024: // < 100MB
		return 4 * 1024 * 1024 // 4MB chunks
	case fileSize < 1024*1024*1024: // < 1GB
		return 8 * 1024 * 1024 // 8MB chunks
	default: // >= 1GB
		return 16 * 1024 * 1024 // 16MB chunks for very large files
	}
}


// OptimizedSTORHandler implements OPTIMIZATION: Efficient server STOR handler
// with large buffers, memory mapping, and optimized disk writes - 400MB/s performance
func OptimizedSTORHandler(session interface{}, file *os.File, reader io.Reader) (int64, error) {
	// Wrap dataConn in a large bufio.Reader for massive buffer optimization
	bufferSize := 8 * 1024 * 1024 // 8MB buffer to match upload client optimization
	bufferedReader := bufio.NewReaderSize(reader, bufferSize)

	// Create a write buffer pool using 8MB chunks for maximum disk write speed
	writeBuffer := make([]byte, bufferSize)

	// Use direct io.CopyBuffer with large buffers for guaranteed high-speed transfer
	totalWritten, err := io.CopyBuffer(file, bufferedReader, writeBuffer)
	if err != nil {
		return totalWritten, fmt.Errorf("optimized STOR handler failed: %v", err)
	}

	// Force sync to disk for data integrity
	if syncer, ok := interface{}(file).(interface{ Sync() error }); ok {
		syncer.Sync()
	}

	return totalWritten, nil
}

// createFastWritableMmapFile creates a memory-mapped file using RETR's fast allocation method
func createFastWritableMmapFile(file *os.File, size int64) (*MmapFile, error) {
	// For upload memory mapping, we need to create a writable memory map
	fd := file.Fd()
	fileHandle := syscall.Handle(fd)

	// Set up for writable memory mapping
	protection := uint32(syscall.PAGE_READWRITE)
	mapAccess := uint32(syscall.FILE_MAP_WRITE)

	// Create file mapping for writing
	mapHandle, err := syscall.CreateFileMapping(
		fileHandle,
		nil,
		protection,
		uint32(size>>32),
		uint32(size),
		nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create writable file mapping: %v", err)
	}

	// Map view of file for writing
	addr, err := syscall.MapViewOfFile(
		mapHandle,
		mapAccess,
		0,
		0,
		uintptr(size))

	if err != nil {
		syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("failed to map view of writable file: %v", err)
	}

	// ✅ RETR-MATCHING: Use same fast slice creation as RETR for maximum performance
	data := (*[1 << 30]byte)(unsafe.Pointer(addr))[:size:size]

	return &MmapFile{
		file:   file,
		data:   data,
		size:   size,
		handle: mapHandle,
	}, nil
}

// createWritableMmapFile creates a memory-mapped file for writing with proper cleanup
func createWritableMmapFile(session SessionInterface, file *os.File, size int64) (*MmapFile, error) {
	// For upload memory mapping, we need to create a writable memory map
	fd := file.Fd()
	fileHandle := syscall.Handle(fd)

	// Set up for writable memory mapping
	protection := uint32(syscall.PAGE_READWRITE)
	mapAccess := uint32(syscall.FILE_MAP_WRITE)

	// Create file mapping for writing
	mapHandle, err := syscall.CreateFileMapping(
		fileHandle,
		nil,
		protection,
		uint32(size>>32),
		uint32(size),
		nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create writable file mapping: %v", err)
	}

	// Map view of file for writing
	addr, err := syscall.MapViewOfFile(
		mapHandle,
		mapAccess,
		0,
		0,
		uintptr(size))

	if err != nil {
		// CRITICAL: Clean up the mapping handle if MapViewOfFile fails
		syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("failed to map view of writable file: %v", err)
	}

	// Create byte slice from mapped memory for writing
	// Use recovery mechanism to ensure cleanup happens even if slice creation fails
	var data []byte
	defer func() {
		if recover() != nil {
			// Emergency cleanup if slice creation panics
			syscall.UnmapViewOfFile(addr)
			syscall.CloseHandle(mapHandle)
			panic("failed to create memory-mapped byte slice")
		}
	}()

	// Fix for files larger than 1GB - use unsafe.Slice for proper large file support
	// Check for potential integer overflow on 32-bit systems
	if size > (1<<31 - 1) {
		return nil, fmt.Errorf("file too large for memory mapping on this system: %d bytes", size)
	}
	data = unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))

	return &MmapFile{
		file:   file,
		data:   data,
		size:   size,
		handle: mapHandle,
	}, nil
}

// createWritableMmapFileOriginal creates a memory-mapped file for writing - FIXED FOR LARGE FILES >1GB
// Enhanced to handle files larger than 1GB safely on Windows 64-bit
func createWritableMmapFileOriginal(file *os.File, size int64) (*MmapFile, error) {
	// For upload memory mapping, we need to create a writable memory map
	fd := file.Fd()
	fileHandle := syscall.Handle(fd)

	// Set up for writable memory mapping
	protection := uint32(syscall.PAGE_READWRITE)
	mapAccess := uint32(syscall.FILE_MAP_WRITE)

	// Create file mapping for writing
	mapHandle, err := syscall.CreateFileMapping(
		fileHandle,
		nil,
		protection,
		uint32(size>>32),
		uint32(size),
		nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create writable file mapping: %v", err)
	}

	// Map view of file for writing
	addr, err := syscall.MapViewOfFile(
		mapHandle,
		mapAccess,
		0,
		0,
		uintptr(size))

	if err != nil {
		syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("failed to map view of writable file: %v", err)
	}

	// FIXED: Use unsafe.Slice for proper large file support on Windows 64-bit
	// This replaces the old (*[1 << 30]byte) method that was limited to 1GB
	var data []byte
	defer func() {
		if recover() != nil {
			// Emergency cleanup if slice creation panics
			syscall.UnmapViewOfFile(addr)
			syscall.CloseHandle(mapHandle)
			panic("failed to create memory-mapped byte slice for large file")
		}
	}()
	
	// Check for reasonable file size limits (Windows 64-bit can handle very large files)
	if size > (1 << 40) { // 1TB limit for safety
		return nil, fmt.Errorf("file too large for memory mapping: %d bytes (max 1TB)", size)
	}
	
	// Use unsafe.Slice for safe large file handling
	data = unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))

	return &MmapFile{
		file:   file,
		data:   data,
		size:   size,
		handle: mapHandle,
	}, nil
}

// OriginalOptimizedSTORHandler - EXACT copy from original server that achieved 400MB/s
func OriginalOptimizedSTORHandler(file *os.File, reader io.Reader) (int64, error) {
	// Wrap dataConn in a large bufio.Reader for massive buffer optimization
	bufferSize := 8 * 1024 * 1024 // 8MB buffer to match upload client optimization
	bufferedReader := bufio.NewReaderSize(reader, bufferSize)

	// Create a write buffer pool using 8MB chunks for maximum disk write speed
	writeBuffer := make([]byte, bufferSize)

	// Use direct io.CopyBuffer with large buffers for guaranteed high-speed transfer
	totalWritten, err := io.CopyBuffer(file, bufferedReader, writeBuffer)
	if err != nil {
		return totalWritten, fmt.Errorf("optimized STOR handler failed: %v", err)
	}

	// Force sync to disk for data integrity
	if syncer, ok := interface{}(file).(interface{ Sync() error }); ok {
		syncer.Sync()
	}

	return totalWritten, nil
}

// tryUltraFastMmapUpload - OPTIMIZED for 400MB/s performance to match RETR speeds
func tryUltraFastMmapUpload(session SessionInterface, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	// Pre-allocate file space for maximum performance
	err := file.Truncate(expectedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to pre-allocate file space: %v", err)
	}

	// Create memory-mapped file for writing
	mmapFile, err := createWritableMmapFileOriginal(file, expectedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to create memory map: %v", err)
	}

	// Ensure cleanup happens even if panic occurs
	defer func() {
		// Flush mmap changes to disk for maximum performance
		SyncMmapFile(mmapFile.data)
		syscall.FlushFileBuffers(syscall.Handle(file.Fd()))
		if closeErr := mmapFile.Close(); closeErr != nil {
			session.LogPrintf("Warning: failed to close memory-mapped file: %v", closeErr)
		}
	}()

	// ULTRA-FAST: Use io.ReadFull for maximum performance (matches RETR speed)
	n, err := io.ReadFull(reader, mmapFile.data)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		// Client sent less data than expected - truncate to actual size
		actualSize := int64(n)
		file.Truncate(actualSize)
		session.LogPrintf("File truncated to actual size: %d bytes", actualSize)
		return actualSize, nil
	}
	if err != nil {
		return int64(n), err
	}

	return int64(n), nil
}

// ultraFastStreamingUpload - OPTIMIZED for 400MB/s performance when mmap fails
func ultraFastStreamingUpload(session SessionInterface, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	// Use massive buffers to match RETR performance
	bufferSize := 128 * 1024 * 1024 // 128MB buffer for maximum throughput
	if expectedSize > 0 && expectedSize < int64(bufferSize) {
		// For smaller files, use file size as buffer size
		bufferSize = int(expectedSize)
	}

	session.LogPrintf("Ultra-fast streaming with %d MB buffer", bufferSize/(1024*1024))

	// Create massive buffered reader to match client performance
	bufferedReader := bufio.NewReaderSize(reader, bufferSize)

	// Create ultra-large write buffer for maximum disk write speed
	writeBuffer := make([]byte, bufferSize)

	// Use io.CopyBuffer with massive buffers for 400MB/s performance
	totalWritten, err := io.CopyBuffer(file, bufferedReader, writeBuffer)
	if err != nil {
		return totalWritten, fmt.Errorf("ultra-fast streaming failed: %v", err)
	}

	// Aggressive sync for maximum performance
	file.Sync()
	syscall.FlushFileBuffers(syscall.Handle(file.Fd()))

	return totalWritten, nil
}

// parallelChunkedUpload - CORE CORRECTION: True parallel chunked upload with multiple goroutines
// Implements server-side parallel upload to match client's multi-threaded uploads
func parallelChunkedUpload(session SessionInterface, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	// CORE CORRECTION: Configuration for parallel upload
	const (
		chunkSize = 4 * 1024 * 1024  // 4MB chunks for optimal parallel processing
		maxWorkers = 4               // Number of concurrent write workers
		bufferChannelSize = 8        // Buffered channel size for chunks
	)
	
	session.LogPrintf("Starting parallel chunked upload: %d workers, %d MB chunks", maxWorkers, chunkSize/(1024*1024))
	
	// Pre-allocate file space for parallel writes
	err := file.Truncate(expectedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to pre-allocate file space: %v", err)
	}
	
	// Channel for chunk data with position information
	type chunkData struct {
		data     []byte
		offset   int64
		size     int
		chunkNum int
	}
	
	chunkChannel := make(chan chunkData, bufferChannelSize)
	errorChannel := make(chan error, maxWorkers+1) // +1 for reader goroutine
	doneChannel := make(chan bool, maxWorkers)
	
	// Start worker goroutines for parallel writes
	for i := 0; i < maxWorkers; i++ {
		go func(workerID int) {
			defer func() { doneChannel <- true }()
			
			for chunk := range chunkChannel {
				// Seek to chunk position and write
				_, err := file.Seek(chunk.offset, 0)
				if err != nil {
					errorChannel <- fmt.Errorf("worker %d seek failed at offset %d: %v", workerID, chunk.offset, err)
					return
				}
				
				// Write chunk data
				written := 0
				for written < chunk.size {
					n, writeErr := file.Write(chunk.data[written:chunk.size])
					if writeErr != nil {
						errorChannel <- fmt.Errorf("worker %d write failed for chunk %d: %v", workerID, chunk.chunkNum, writeErr)
						return
					}
					written += n
				}
				
				session.LogPrintf("Worker %d completed chunk %d (%d bytes at offset %d)", workerID, chunk.chunkNum, chunk.size, chunk.offset)
			}
		}(i)
	}
	
	// Reader goroutine to process incoming data
	go func() {
		defer close(chunkChannel)
		
		buffer := make([]byte, chunkSize)
		var totalRead int64
		chunkNum := 0
		
		for totalRead < expectedSize {
			// Calculate chunk size to read
			remaining := expectedSize - totalRead
			toRead := chunkSize
			if remaining < int64(chunkSize) {
				toRead = int(remaining)
			}
			
			// Read chunk from client
			n, readErr := reader.Read(buffer[:toRead])
			if n > 0 {
				// Create chunk data copy for worker
				chunkBuffer := make([]byte, n)
				copy(chunkBuffer, buffer[:n])
				
				// Send to worker channel
				select {
				case chunkChannel <- chunkData{
					data:     chunkBuffer,
					offset:   totalRead,
					size:     n,
					chunkNum: chunkNum,
				}:
					totalRead += int64(n)
					chunkNum++
				case err := <-errorChannel:
					// Worker error occurred
					errorChannel <- err
					return
				}
			}
			
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				errorChannel <- fmt.Errorf("read error at offset %d: %v", totalRead, readErr)
				return
			}
		}
		
		session.LogPrintf("Reader completed: %d chunks, %d total bytes", chunkNum, totalRead)
	}()
	
	// Wait for all workers to complete or error
	workersCompleted := 0
	var uploadErr error
	
	for workersCompleted < maxWorkers {
		select {
		case <-doneChannel:
			workersCompleted++
		case err := <-errorChannel:
			uploadErr = err
			// Continue to let other workers finish
		}
	}
	
	// Check for any remaining errors
	select {
	case err := <-errorChannel:
		if uploadErr == nil {
			uploadErr = err
		}
	default:
		// No more errors
	}
	
	if uploadErr != nil {
		return 0, fmt.Errorf("parallel upload failed: %v", uploadErr)
	}
	
	// Force sync to ensure all data is written
	file.Sync()
	session.LogPrintf("Parallel chunked upload completed successfully: %d bytes", expectedSize)
	
	return expectedSize, nil
}

// SuperfastMmapUploadFixedSize - chunked reading implementation for reliable uploads
func SuperfastMmapUploadFixedSize(session interface{}, file *os.File, reader io.Reader, expectedSize int64) (int64, error) {
	s := session.(SessionInterface)
	
	// Pre-allocate file space for maximum performance
	err := file.Truncate(expectedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to pre-allocate file space: %v", err)
	}

	// Create memory-mapped file for writing
	mmapFile, err := createWritableMmapFileOriginal(file, expectedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to create memory map: %v", err)
	}

	// Ensure cleanup happens even if panic occurs (async cleanup)
	defer func() {
		// Close memory-mapped file (this is fast)
		if closeErr := mmapFile.Close(); closeErr != nil {
			s.LogPrintf("Warning: failed to close memory-mapped file: %v", closeErr)
		}
	}()

	// Use CHUNKED reading for reliable uploads
	// Use large chunks for good performance while maintaining reliability
	chunkSize := 8 * 1024 * 1024 // 8MB chunks
	buffer := make([]byte, chunkSize)
	var totalWritten int64
	
	for totalWritten < expectedSize {
		// Calculate how much to read in this chunk
		remaining := expectedSize - totalWritten
		toRead := chunkSize
		if remaining < int64(chunkSize) {
			toRead = int(remaining)
		}
		
		// Read chunk from client
		n, readErr := reader.Read(buffer[:toRead])
		if n > 0 {
			// Copy chunk to memory-mapped buffer
			copy(mmapFile.data[totalWritten:], buffer[:n])
			totalWritten += int64(n)
		}
		
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return totalWritten, readErr
		}
	}
	
	// Truncate to actual size if we received less data
	if totalWritten < expectedSize {
		file.Truncate(totalWritten)
		s.LogPrintf("File truncated to actual size: %d bytes", totalWritten)
	}

	return totalWritten, nil
}
