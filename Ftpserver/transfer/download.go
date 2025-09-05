package transfer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

// SuperfastOpenMmapFile provides high-performance file access optimized for superfast client downloads
func SuperfastOpenMmapFile(session interface{}, filename string, writable bool) (*MmapFile, error) {
	s := session.(SessionInterface)
	
	// Open file using standard library
	var flag int = os.O_RDONLY
	if writable {
		flag = os.O_RDWR
	}

	file, err := os.OpenFile(filename, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	fileSize := fileInfo.Size()
	fileSizeMB := fileSize / (1024 * 1024)
	s.LogPrintf("Opening file for download: %s (%d MB)", filename, fileSizeMB)
	
	if fileSize == 0 {
		file.Close()
		return &MmapFile{file: nil, data: []byte{}, size: 0, handle: 0}, nil
	}

	// Strategy selection based on file size for optimal performance
	if fileSize >= 100*1024*1024 { // Files >= 100MB use memory mapping
		s.LogPrintf("Large file detected, attempting memory mapping...")
		mmapFile, err := tryWindowsMmap(s, file, fileSize, writable)
		if err == nil {
			s.LogPrintf("Memory mapping successful for %d MB file", fileSizeMB)
			return mmapFile, nil
		}
		s.LogPrintf("Memory mapping failed (%v), using optimized streaming", err)
	}

	// For smaller files or memory mapping failures, use optimized streaming approach
	return createOptimizedStreamingFile(s, file, fileSize, filename)
}

// createOptimizedStreamingFile creates a high-performance streaming file handler
func createOptimizedStreamingFile(session SessionInterface, file *os.File, fileSize int64, filename string) (*MmapFile, error) {
	// For files that don't benefit from memory mapping or when mapping fails,
	// read the entire file into memory for maximum download speed
	// This eliminates disk I/O during transfer and matches client's expectations
	
	fileSizeMB := fileSize / (1024 * 1024)
	session.LogPrintf("Using optimized streaming for %d MB file: %s", fileSizeMB, filename)
	
	// Use large buffer for fast file reading
	bufferSize := calculateOptimalReadBufferSize(fileSize)
	bufferedReader := bufio.NewReaderSize(file, bufferSize)
	
	// Read entire file into memory with optimized buffering
	data, err := io.ReadAll(bufferedReader)
	file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read file into memory: %v", err)
	}

	return &MmapFile{
		file:   nil,
		data:   data,
		size:   fileSize,
		handle: 0,
	}, nil
}

// calculateOptimalReadBufferSize determines the best buffer size for file reading
func calculateOptimalReadBufferSize(fileSize int64) int {
	switch {
	case fileSize < 10*1024*1024: // <10MB
		return 2 * 1024 * 1024 // 2MB buffer
	case fileSize < 100*1024*1024: // <100MB
		return 8 * 1024 * 1024 // 8MB buffer
	case fileSize < 1024*1024*1024: // <1GB
		return 16 * 1024 * 1024 // 16MB buffer
	default: // >=1GB
		return 32 * 1024 * 1024 // 32MB buffer
	}
}

// Legacy function for backward compatibility
func OpenMmapFile(session interface{}, filename string, writable bool) (*MmapFile, error) {
	return SuperfastOpenMmapFile(session, filename, writable)
}

// Windows-specific memory mapping (best effort)
func tryWindowsMmap(session SessionInterface, file *os.File, fileSize int64, writable bool) (*MmapFile, error) {
	// For large files, try actual memory mapping
	if fileSize > 100*1024*1024 { // Only use mmap for files > 100MB
		return actualWindowsMmap(file, fileSize, writable)
	}

	// For smaller files, return error to use fallback
	return nil, fmt.Errorf("file too small for memory mapping")
}

// Actual Windows memory mapping implementation with proper cleanup
func actualWindowsMmap(file *os.File, fileSize int64, writable bool) (*MmapFile, error) {
	// Get file handle from the file descriptor
	fd := file.Fd()
	fileHandle := syscall.Handle(fd)

	// Set up protection and access flags
	var protection uint32 = syscall.PAGE_READONLY
	var mapAccess uint32 = syscall.FILE_MAP_READ

	if writable {
		protection = syscall.PAGE_READWRITE
		mapAccess = syscall.FILE_MAP_WRITE
	}

	// Create file mapping
	mapHandle, err := syscall.CreateFileMapping(
		fileHandle,
		nil,
		protection,
		uint32(fileSize>>32),
		uint32(fileSize),
		nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create file mapping: %v", err)
	}

	// Map view of file
	addr, err := syscall.MapViewOfFile(
		mapHandle,
		mapAccess,
		0,
		0,
		uintptr(fileSize))

	if err != nil {
		// CRITICAL: Clean up the mapping handle if MapViewOfFile fails
		syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Create byte slice from mapped memory with panic recovery
	var data []byte
	defer func() {
		if recover() != nil {
			// Emergency cleanup if slice creation panics
			syscall.UnmapViewOfFile(addr)
			syscall.CloseHandle(mapHandle)
			panic("failed to create memory-mapped byte slice for download")
		}
	}()
	
	data = (*[1 << 30]byte)(unsafe.Pointer(addr))[:fileSize:fileSize]

	return &MmapFile{
		file:   file,
		data:   data,
		size:   fileSize,
		handle: mapHandle,
	}, nil
}
