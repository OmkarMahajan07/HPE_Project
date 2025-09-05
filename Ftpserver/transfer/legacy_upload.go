package transfer

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// ==================== LEGACY UPLOAD FUNCTIONS ====================
// These functions are not currently used in the main server but are
// kept for potential future use or reference.

// OptimizedMmapUpload implements true memory-mapped upload to match download performance
// Status: NOT USED - kept for reference
func OptimizedMmapUpload(session interface{}, dst *os.File, src io.Reader) (int64, error) {
	// Access session methods through interface casting
	s := session.(SessionInterface)
	
	// Pre-allocate file space for memory mapping - start with large size
	initialSize := int64(1024 * 1024 * 1024) // 1GB initial allocation
	err := dst.Truncate(initialSize)
	if err != nil {
		// If we can't pre-allocate, fall back to streaming
		return FallbackStreamingUpload(session, dst, src)
	}

	// Create memory-mapped file for writing (like download uses for reading)
	mmapFile, err := createWritableMmapFile(s, dst, initialSize)
	if err != nil {
		// If memory mapping fails, fall back to streaming
		s.LogPrintf("Memory mapping failed, using streaming fallback: %v", err)
		return FallbackStreamingUpload(session, dst, src)
	}
	defer mmapFile.Close()

	// Direct network-to-memory mapping (eliminate buffer copy overhead)
	chunkSize := 64 * 1024 * 1024 // 64MB chunks for optimal direct transfer
	var totalWritten int64

	// Stream data directly into memory-mapped region (NO intermediate buffer)
	for {
		// Calculate remaining space in current mapping
		remainingSpace := int64(len(mmapFile.data)) - totalWritten

		// If we need more space, expand the mapping
		if remainingSpace < int64(chunkSize) {
			// Expand the file and remap for more space
			newSize := int64(len(mmapFile.data)) * 2 // Double the current size
			if newSize-totalWritten < int64(chunkSize) {
				newSize = totalWritten + int64(chunkSize)*2 // Ensure enough space
			}

			mmapFile.Close()

			err := dst.Truncate(newSize)
			if err != nil {
				return totalWritten, fmt.Errorf("failed to expand file: %v", err)
			}

			mmapFile, err = createWritableMmapFile(s, dst, newSize)
			if err != nil {
				return totalWritten, fmt.Errorf("failed to remap file: %v", err)
			}
			remainingSpace = int64(len(mmapFile.data)) - totalWritten
		}

		// Read directly into memory-mapped region (ZERO COPY)
		readSize := legacyMin(int(remainingSpace), chunkSize)
		directBuffer := mmapFile.data[totalWritten : totalWritten+int64(readSize)]

		n, readErr := src.Read(directBuffer)
		if n > 0 {
			totalWritten += int64(n)
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return totalWritten, readErr
		}
	}

	// Truncate file to actual size and close memory mapping
	mmapFile.Close()
	err = dst.Truncate(totalWritten)
	if err != nil {
		s.LogPrintf("Warning: failed to truncate file to final size: %v", err)
	}

	return totalWritten, nil
}

// ChunkedMmapWrite writes large data in optimized chunks
// Status: NOT USED - kept for reference
func ChunkedMmapWrite(dst *os.File, data []byte) (int64, error) {
	chunkSize := 32 * 1024 * 1024 // 32MB chunks for optimal performance
	totalWritten := int64(0)

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[offset:end]
		n, err := dst.Write(chunk)
		totalWritten += int64(n)

		if err != nil {
			return totalWritten, err
		}

		// Sync every 64MB to maintain data integrity without killing performance
		if totalWritten%(64*1024*1024) == 0 {
			if syncer, ok := interface{}(dst).(interface{ Sync() error }); ok {
				syncer.Sync()
			}
		}
	}

	// Final sync
	if syncer, ok := interface{}(dst).(interface{ Sync() error }); ok {
		syncer.Sync()
	}

	return totalWritten, nil
}

// OptimizedCopyToFile provides high-performance file writing with adaptive buffering
// Status: NOT USED - kept for reference
func OptimizedCopyToFile(session interface{}, dst *os.File, src io.Reader) (int64, error) {
	s := session.(SessionInterface)
	
	// Use large buffer for high-speed transfers - matching download performance
	bufferSize := 8 * 1024 * 1024 // 8MB buffer to match download optimization
	buffer := make([]byte, bufferSize)

	var written int64
	var err error

	// Pre-allocate file space if possible (Windows optimization)
	if seeker, ok := interface{}(dst).(io.Seeker); ok {
		if currentPos, seekErr := seeker.Seek(0, io.SeekCurrent); seekErr == nil {
			// Try to estimate file size and pre-allocate
			if s.GetRestartPos() == 0 {
				// For new files, try to get size hint from REST command or estimate
				if estimatedSize := int64(100 * 1024 * 1024); estimatedSize > 0 { // 100MB default
					dst.Truncate(currentPos + estimatedSize)
				}
			}
		}
	}

	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			// Write the data in chunks to ensure reliability
			data := buffer[:n]
			for len(data) > 0 {
				nw, writeErr := dst.Write(data)
				if writeErr != nil {
					return written, writeErr
				}
				written += int64(nw)
				data = data[nw:]
			}

			// Force sync to disk every 64MB for data integrity
			if written%(64*1024*1024) == 0 {
				if syncer, ok := interface{}(dst).(interface{ Sync() error }); ok {
					syncer.Sync()
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			err = readErr
			break
		}
	}

	// Final sync to ensure all data is written
	if syncer, ok := interface{}(dst).(interface{ Sync() error }); ok {
		syncer.Sync()
	}

	return written, err
}

// UltraOptimizedBufferedUpload implements Option 2: Large buffer approach for maximum performance
// Status: NOT USED - kept for reference
func UltraOptimizedBufferedUpload(dst *os.File, src io.Reader) (int64, error) {
	// Use ultra-large buffers to minimize system calls and maximize throughput
	bufferSize := 128 * 1024 * 1024 // 128MB buffer - massive for maximum speed
	buffer := make([]byte, bufferSize)

	var totalWritten int64

	// Stream data with massive buffers to reduce overhead
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

// hybridParallelUpload implements Option 3: Hybrid approach with parallel streaming and zero-copy
// Status: NOT USED - kept for reference (moved from main file)
func HybridParallelUpload(dst *os.File, src io.Reader) (int64, error) {
	// Use multiple parallel buffers for optimal throughput
	bufferSize := 64 * 1024 * 1024 // 64MB per buffer
	numBuffers := 3                // Triple buffering for overlapped I/O

	// Create channel for buffer coordination
	bufferChan := make(chan []byte, numBuffers)
	writeChan := make(chan WriteRequest, numBuffers)
	errorChan := make(chan error, 2)

	// Initialize buffers
	for i := 0; i < numBuffers; i++ {
		bufferChan <- make([]byte, bufferSize)
	}

	var totalWritten int64
	var wg sync.WaitGroup

	// Start async writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		var writeOffset int64

		for req := range writeChan {
			if req.data == nil {
				break // EOF signal
			}

			// Write data to file
			_, err := dst.WriteAt(req.data[:req.size], writeOffset)
			if err != nil {
				errorChan <- err
				return
			}

			writeOffset += int64(req.size)
			atomic.AddInt64(&totalWritten, int64(req.size))

			// Return buffer to pool
			bufferChan <- req.buffer
		}
	}()

	// Start async reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writeChan)

		for {
			// Get buffer from pool
			buffer := <-bufferChan

			// Read data into buffer
			n, err := src.Read(buffer)
			if n > 0 {
				// Send to writer
				writeChan <- WriteRequest{
					buffer: buffer,
					data:   buffer,
					size:   n,
				}
			} else {
				// Return unused buffer
				bufferChan <- buffer
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				errorChan <- err
				return
			}
		}

		// Send EOF signal
		writeChan <- WriteRequest{data: nil}
	}()

	// Wait for completion or error
	go func() {
		wg.Wait()
		errorChan <- nil // Signal completion
	}()

	// Wait for first result (completion or error)
	err := <-errorChan
	if err != nil {
		return atomic.LoadInt64(&totalWritten), err
	}

	return atomic.LoadInt64(&totalWritten), nil
}

// Legacy helper function
func legacyMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
