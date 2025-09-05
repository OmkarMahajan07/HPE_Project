//go:build windows
// +build windows

package transfer

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// CreateMmapFile creates a memory-mapped file for efficient I/O
func CreateMmapFile(path string, size int64) ([]byte, *os.File, error) {
	// Create or truncate the file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Set file size
	if err := file.Truncate(size); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to set file size: %v", err)
	}

	// Get file handle
	handle := syscall.Handle(file.Fd())

	// Split size into high and low DWORDs for 64-bit support
	high := uint32(size >> 32)
	low := uint32(size & 0xFFFFFFFF)

	// Create file mapping
	mapping, err := syscall.CreateFileMapping(handle, nil, syscall.PAGE_READWRITE, high, low, nil)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to create file mapping: %v", err)
	}
	defer syscall.CloseHandle(mapping)

	// Map view of file
	addr, err := syscall.MapViewOfFile(mapping, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Create a safe slice of the mapped memory
	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))

	return data, file, nil
}

// MmapFile maps an existing file into memory
func MmapFile(filename string) ([]byte, error) {
	// Open the file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}
	size := fileInfo.Size()

	if size == 0 {
		return []byte{}, nil
	}

	// Get file handle
	handle := syscall.Handle(file.Fd())

	// Split size into high and low DWORDs for 64-bit support
	high := uint32(size >> 32)
	low := uint32(size & 0xFFFFFFFF)

	// Create file mapping
	h, err := syscall.CreateFileMapping(handle, nil, syscall.PAGE_READONLY, high, low, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create file mapping: %v", err)
	}
	defer syscall.CloseHandle(h)

	// Map view of file
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		return nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Create a safe slice of the mapped memory
	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))

	return data, nil
}

// SyncMmapFile flushes mmap changes to disk
func SyncMmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)))
}

// MunmapFile unmaps the memory-mapped file
func MunmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&data[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return fmt.Errorf("failed to unmap view of file: %v", err)
	}
	return nil
}

// CreateMmapForUpload creates a memory-mapped view of an existing file for reading
// This adapts the proven download approach for upload use
func CreateMmapForUpload(filename string) ([]byte, *os.File, error) {
	// Open the existing file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Get file size
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

	// Get file handle
	handle := syscall.Handle(file.Fd())

	// Split size into high and low DWORDs for 64-bit support
	high := uint32(size >> 32)
	low := uint32(size & 0xFFFFFFFF)

	// Create file mapping for reading
	mapping, err := syscall.CreateFileMapping(handle, nil, syscall.PAGE_READONLY, high, low, nil)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to create file mapping: %v", err)
	}
	defer syscall.CloseHandle(mapping)

	// Map view of file for reading
	addr, err := syscall.MapViewOfFile(mapping, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// ✅ SIZE-AWARE: Use fast method for <=1GB, safe method for >1GB
	var data []byte
	if size <= (1 << 30) { // 1GB limit for fast method
		// ULTRA-FAST: Use server's proven fast slice creation for files <=1GB
		data = (*[1 << 30]byte)(unsafe.Pointer(addr))[:size:size]
	} else {
		// SAFE: Use unsafe.Slice for files >1GB to prevent crashes
		data = unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))
	}

	return data, file, nil
}
