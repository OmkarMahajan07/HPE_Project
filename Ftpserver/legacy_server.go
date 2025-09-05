package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"strings"
	"ftpserver/auth"
)

// ==================== LEGACY SERVER FUNCTIONS ====================
// These functions are not currently used in the main server but are
// kept for potential future use or reference.

// ==================== DUPLICATE FUNCTIONS ====================

// optimizedSTORHandler - DUPLICATE of transfer.OptimizedSTORHandler()
// Status: REDUNDANT - main file calls transfer.OptimizedSTORHandler() instead
// Location: Originally at line 2557 in ftp_server1.go
func (session *ClientSession) legacyOptimizedSTORHandler(file *os.File, reader io.Reader) (int64, error) {
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

// ==================== UNUSED PARALLEL UPLOAD ====================

// writeRequest represents a write operation
type legacyWriteRequest struct {
	buffer []byte
	data   []byte
	size   int
}

// hybridParallelUpload implements Option 3: Hybrid approach with parallel streaming and zero-copy
// Status: NOT USED - complex parallel upload never called
// Location: Originally at line 2452 in ftp_server1.go
func (session *ClientSession) legacyHybridParallelUpload(dst *os.File, src io.Reader) (int64, error) {
	// Use multiple parallel buffers for optimal throughput
	bufferSize := 64 * 1024 * 1024 // 64MB per buffer
	numBuffers := 3                // Triple buffering for overlapped I/O

	// Create channel for buffer coordination
	bufferChan := make(chan []byte, numBuffers)
	writeChan := make(chan legacyWriteRequest, numBuffers)
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
				writeChan <- legacyWriteRequest{
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
		writeChan <- legacyWriteRequest{data: nil}
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

// ==================== UNUSED INFRASTRUCTURE ====================

// NetworkCondition represents network performance characteristics
type LegacyNetworkCondition struct {
	Latency   time.Duration
	Bandwidth int64
	PacketLoss float64
}

// BufferPool manages a pool of reusable buffers
// Status: NOT USED - created but never used
type LegacyBufferPool struct {
	buffers    chan []byte
	bufferSize int
	maxBuffers int
}

// NewBufferPool creates a new buffer pool
func NewLegacyBufferPool(bufferSize int, maxBuffers int) *LegacyBufferPool {
	bp := &LegacyBufferPool{
		buffers:    make(chan []byte, maxBuffers),
		bufferSize: bufferSize,
		maxBuffers: maxBuffers,
	}

	// Pre-allocate buffers
	for i := 0; i < maxBuffers; i++ {
		bp.buffers <- make([]byte, bufferSize)
	}

	return bp
}

// GetBuffer retrieves a buffer from the pool
func (bp *LegacyBufferPool) GetBuffer() []byte {
	select {
	case buf := <-bp.buffers:
		return buf
	default:
		// Pool is empty, create new buffer
		return make([]byte, bp.bufferSize)
	}
}

// PutBuffer returns a buffer to the pool
func (bp *LegacyBufferPool) PutBuffer(buf []byte) {
	if len(buf) != bp.bufferSize {
		return // Wrong size, discard
	}

	select {
	case bp.buffers <- buf:
		// Buffer returned to pool
	default:
		// Pool is full, discard buffer
	}
}

// TransferOptimizer optimizes transfer parameters based on conditions
// Status: NOT USED - created but never used
type LegacyTransferOptimizer struct {
	mu              sync.RWMutex
	adaptiveEnabled bool
	lastAnalysis    time.Time
}

// NewTransferOptimizer creates a new transfer optimizer
func NewLegacyTransferOptimizer() *LegacyTransferOptimizer {
	return &LegacyTransferOptimizer{
		adaptiveEnabled: true,
		lastAnalysis:    time.Now(),
	}
}

// OptimizeTransfer calculates optimal transfer parameters
func (to *LegacyTransferOptimizer) OptimizeTransfer(fileSize int64, networkCondition LegacyNetworkCondition) (chunkSize int64, threads int) {
	to.mu.RLock()
	defer to.mu.RUnlock()

	// Run adaptive analysis if needed
	if time.Since(to.lastAnalysis) > 30*time.Second {
		go to.analyzeAndOptimize()
	}

	// Default optimization logic
	if fileSize < 1024*1024 { // < 1MB
		return 64 * 1024, 1 // 64KB chunks, single thread
	} else if fileSize < 100*1024*1024 { // < 100MB
		return 1024 * 1024, 2 // 1MB chunks, 2 threads
	} else {
		return 8 * 1024 * 1024, 4 // 8MB chunks, 4 threads
	}
}

// analyzeAndOptimize performs adaptive optimization analysis
func (to *LegacyTransferOptimizer) analyzeAndOptimize() {
	to.mu.Lock()
	defer to.mu.Unlock()

	// Placeholder for adaptive optimization logic
	// In a real implementation, this would analyze recent transfer
	// performance and adjust parameters accordingly

	to.lastAnalysis = time.Now()
}

// ==================== UNUSED CONNECTION MANAGEMENT ====================

// ConnectionPool manages a pool of reusable connections
// Status: NOT USED - created but never used
type LegacyConnectionPool struct {
	connections    chan interface{}
	maxConnections int
	maxIdleTime    time.Duration
	mu             sync.RWMutex
	closed         bool
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewConnectionPool creates a new connection pool
func NewLegacyConnectionPool(ctx context.Context, maxConnections int, maxIdleTime time.Duration) *LegacyConnectionPool {
	poolCtx, cancel := context.WithCancel(ctx)
	pool := &LegacyConnectionPool{
		connections:    make(chan interface{}, maxConnections),
		maxConnections: maxConnections,
		maxIdleTime:    maxIdleTime,
		ctx:            poolCtx,
		cancel:         cancel,
	}

	// Start cleanup routine
	go pool.cleanupRoutine()

	return pool
}

// cleanupRoutine periodically cleans up idle connections
func (pool *LegacyConnectionPool) cleanupRoutine() {
	ticker := time.NewTicker(pool.maxIdleTime / 2)
	defer ticker.Stop()

	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-ticker.C:
			pool.cleanup()
		}
	}
}

// cleanup removes idle connections
func (pool *LegacyConnectionPool) cleanup() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		return
	}

	// Drain and close idle connections
	for {
		select {
		case conn := <-pool.connections:
			// Close connection (implementation depends on connection type)
			_ = conn
		default:
			return
		}
	}
}

// SessionManager manages active FTP sessions
// Status: NOT USED - created but never used
type LegacySessionManager struct {
	sessions map[string]*ClientSession
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewSessionManager creates a new session manager
func NewLegacySessionManager(ctx context.Context) *LegacySessionManager {
	smCtx, cancel := context.WithCancel(ctx)
	sm := &LegacySessionManager{
		sessions: make(map[string]*ClientSession),
		ctx:      smCtx,
		cancel:   cancel,
	}

	// Start cleanup routine
	go sm.cleanupSessions()

	return sm
}

// cleanupSessions periodically cleans up inactive sessions
func (sm *LegacySessionManager) cleanupSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performCleanup()
		}
	}
}

// performCleanup removes inactive sessions
func (sm *LegacySessionManager) performCleanup() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	for sessionID, session := range sm.sessions {
		// Remove sessions inactive for more than 30 minutes
		if now.Sub(session.lastActivity) > 30*time.Minute {
			delete(sm.sessions, sessionID)
		}
	}
}

// ==================== UNUSED UTILITY FUNCTIONS ====================

// unlockFileRange unlocks a file range
// Status: NOT USED - function defined but never called
func legacyUnlockFileRange(file *os.File) error {
	// Implementation for unlocking file ranges
	// This is platform-specific and would need proper implementation
	return nil
}

// getKeys extracts keys from a user profile map
// Status: NOT USED - helper function never called
func legacyGetKeys(m map[string]*auth.UserProfile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getThreadID returns a simple thread identifier for logging
// Status: NOT USED - logging helper never called
func legacyGetThreadID() int {
	// Simple implementation using goroutine ID approximation
	// In production, you might want to use a more sophisticated approach
	return int(time.Now().UnixNano() % 10000)
}

// Helper functions that might be used internally
func legacyMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func legacyMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// createFileWithRetry creates a file with retry logic for Windows compatibility
func (session *ClientSession) createFileWithRetry(fullPath string) (*os.File, error) {
	maxRetries := 5
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		// First, check if file already exists and try to remove it
		if _, err := os.Stat(fullPath); err == nil {
			session.logger.Printf("File exists, attempting to remove: %s", fullPath)

			// Try to remove the existing file
			if removeErr := os.Remove(fullPath); removeErr != nil {
				session.logger.Printf("Failed to remove existing file (attempt %d): %v", attempt+1, removeErr)

				// If removal fails due to sharing violation, wait and retry
				if attempt < maxRetries-1 {
					delay := baseDelay * time.Duration(1<<uint(attempt))
					session.logger.Printf("Waiting %v before retry...", delay)
					time.Sleep(delay)
					continue
				}
				return nil, fmt.Errorf("failed to remove existing file after %d attempts: %v", maxRetries, removeErr)
			}

			// File removed successfully, wait a moment for Windows to release handles
			time.Sleep(50 * time.Millisecond)
		}

		// Try to create the new file
		file, err := os.Create(fullPath)
		if err == nil {
			session.logger.Printf("File created successfully: %s", fullPath)
			return file, nil
		}

		// Check if it's a sharing violation error
		if strings.Contains(err.Error(), "sharing violation") ||
			strings.Contains(err.Error(), "user-mapped section open") ||
			strings.Contains(err.Error(), "being used by another process") {

			session.logger.Printf("File creation failed due to sharing violation (attempt %d): %v", attempt+1, err)

			if attempt < maxRetries-1 {
				// Exponential backoff with jitter
				delay := baseDelay * time.Duration(1<<uint(attempt))
				// Add some randomness to avoid thundering herd
				jitter := time.Duration(float64(delay) * (0.5 + 0.5*float64(attempt)/float64(maxRetries)))
				session.logger.Printf("Waiting %v before retry...", jitter)
				time.Sleep(jitter)
				continue
			}
		}

		// For other errors, return immediately
		return nil, fmt.Errorf("failed to create file after %d attempts: %v", maxRetries, err)
	}

	return nil, fmt.Errorf("failed to create file after %d attempts", maxRetries)
}