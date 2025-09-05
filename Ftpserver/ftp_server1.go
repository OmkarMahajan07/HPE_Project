package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"ftpserver/auth"
	"ftpserver/transfer"
	"golang.org/x/crypto/bcrypt"
)

// FTPServer represents the main FTP server with enhanced features
type FTPServer struct {
	listenPort       int
	dataPortStart    int
	dataPortEnd      int
	rootDir          string
	userManager      *auth.UserManager     // User management module
	authService      *auth.AuthService     // Authentication service
	siteCommandHandler *auth.SiteCommandHandler // SITE commands for user management
	users            map[string]*auth.UserProfile // Local copy of users for backward compatibility
	listener         net.Listener
	tlsListener      net.Listener
	portMutex        sync.Mutex
	nextDataPort     int
	config           *ServerConfig
	connectionPool   *ConnectionPool
	performanceStats *PerformanceStats
	bandwidthLimiter *GlobalBandwidthLimiter
	sessionManager   *SessionManager
	mutex            sync.RWMutex
	running          bool
	ctx              context.Context
	cancel           context.CancelFunc
	// Global session registry to share state between control and data connections
	globalSessions map[string]*ClientSession
	globalMutex    sync.RWMutex
}


// ServerConfig contains server configuration
type ServerConfig struct {
	MaxConnections       int
	MaxConnectionsPerIP  int
	ConnectionTimeout    time.Duration
	IdleTimeout          time.Duration
	MaxFileSize          int64
	EnableCompression    bool
	CompressionLevel     int
	EnableTLS            bool
	TLSCertFile          string
	TLSKeyFile           string
	BufferSize           int
	EnableRateLimit      bool
	GlobalBandwidthLimit int64
	EnableLogging        bool
	LogLevel             int
	EnableMetrics        bool
	MetricsInterval      time.Duration
	TrustedSubnets       []string // List of allowed CIDR blocks (e.g. ["192.168.1.0/24", "10.0.0.0/8"])
	EnableIPRestriction  bool     // Whether to enable IP restriction
}

// ClientSession represents an individual client session with enhanced features
type ClientSession struct {
	server             *FTPServer
	controlConn        net.Conn
	dataConn           net.Conn
	dataListener       net.Listener
	currentDir         string
	username           string
	userProfile        *auth.UserProfile
	authenticated      bool
	transferType       string // A (ASCII) or I (Binary)
	mode               string // S (Stream) or Z (Compressed)
	passiveMode        bool
	passiveAddr        string
	utf8Enabled        bool   // UTF-8 filename support
	restartPos         int64  // REST command restart position
	renameFrom         string // RNFR source path for rename operations
	compressionLevel   int    // Current compression level
	sessionStart       time.Time
	lastActivity       time.Time
	connectionID       string
	clientIP           string
	bandwidthLimiter   *BandwidthLimiter
	performanceMetrics *SessionMetrics
	transferManager    *TransferManager
	networkCondition   NetworkCondition
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *log.Logger
	mutex              sync.RWMutex

	// Advanced transfer features
	usingTLS          bool
	tlsConn           *tls.Conn
	parallelTransfers int
	maxParallel       int
	adaptiveBuffer    *AdaptiveBuffer

	// For mmap upload optimization
	expectedUploadSize int64
	// Memory-mapped upload is now enabled by default for high performance

	// Transfer mode synchronization with client
	transferMode string // "mmap", "parallel", "normal"
}

// Implement SessionInterface for ClientSession
func (session *ClientSession) LogPrintf(format string, args ...interface{}) {
	session.logger.Printf(format, args...)
}

func (session *ClientSession) GetRestartPos() int64 {
	return session.restartPos
}

func (session *ClientSession) GetTransferMode() string {
	session.mutex.RLock()
	defer session.mutex.RUnlock()
	return session.transferMode
}

// ConnectionPool manages reusable connections
type ConnectionPool struct {
	connections    map[string]*PooledConnection
	maxConnections int
	maxIdleTime    time.Duration
	mutex          sync.RWMutex
	cleaner        *time.Ticker
	ctx            context.Context
	cancel         context.CancelFunc
}

// PooledConnection represents a reusable connection
type PooledConnection struct {
	conn         net.Conn
	lastUsed     time.Time
	inUse        bool
	connectionID string
	useCount     int64
	errorCount   int64
}

// PerformanceStats tracks server performance
type PerformanceStats struct {
	startTime           time.Time
	activeConnections   int64
	totalConnections    int64
	totalTransfers      int64
	totalBytesServed    int64
	currentBandwidth    int64
	peakBandwidth       int64
	averageResponseTime time.Duration
	errorRate           float64
	uptime              time.Duration
	mutex               sync.RWMutex
}

// GlobalBandwidthLimiter controls overall server bandwidth
type GlobalBandwidthLimiter struct {
	maxRate     int64   // bytes per second
	currentRate int64   // current usage
	tokens      float64 // token bucket
	lastUpdate  time.Time
	mutex       sync.Mutex
}

// BandwidthLimiter controls per-session bandwidth
type BandwidthLimiter struct {
	maxRate    int64   // bytes per second
	tokens     float64 // token bucket
	lastUpdate time.Time
	mutex      sync.Mutex
}

// SessionManager manages all active sessions
type SessionManager struct {
	sessions      map[string]*ClientSession
	ipConnections map[string]int // IP -> connection count
	mutex         sync.RWMutex
	cleanupTicker *time.Ticker
	ctx           context.Context
	cancel        context.CancelFunc
}

// SessionMetrics tracks session performance
type SessionMetrics struct {
	filesTransferred int64
	bytesTransferred int64
	transferTime     time.Duration
	averageSpeed     float64
	peakSpeed        float64
	rtt              time.Duration
	packetLoss       float64
	compressionRatio float64
	errorCount       int64
	retryCount       int64
	lastMeasurement  time.Time
	mutex            sync.RWMutex
}

// TransferManager handles optimized transfers
type TransferManager struct {
	chunkSize        int64
	maxConcurrency   int
	compression      bool
	compressionLevel int
	useMemoryMapping bool
	bufferPool       *BufferPool
	optimizer        *TransferOptimizer
	mutex            sync.RWMutex
}

// BufferPool provides reusable buffers
type BufferPool struct {
	buffers    chan []byte
	bufferSize int
	maxBuffers int
	allocated  int64
	reused     int64
	mutex      sync.RWMutex
}

// TransferOptimizer dynamically optimizes transfers
type TransferOptimizer struct {
	history          []TransferRecord
	maxHistory       int
	optimalChunkSize int64
	optimalThreads   int
	lastOptimization time.Time
	mutex            sync.RWMutex
}

// TransferRecord contains transfer metrics
type TransferRecord struct {
	FileSize       int64
	ChunkSize      int64
	Threads        int
	Compression    bool
	Throughput     float64
	Duration       time.Duration
	NetworkQuality NetworkCondition
	Timestamp      time.Time
}

// NetworkCondition represents network quality
type NetworkCondition int

const (
	NetworkExcellent NetworkCondition = iota
	NetworkGood
	NetworkFair
	NetworkPoor
)

// AdaptiveBuffer automatically adjusts buffer size
type AdaptiveBuffer struct {
	currentSize   int
	minSize       int
	maxSize       int
	optimalSize   int
	lastAdjust    time.Time
	adjustMetrics BufferMetrics
	mutex         sync.RWMutex
}

// BufferMetrics tracks buffer performance
type BufferMetrics struct {
	Throughput   float64
	Latency      time.Duration
	CPUUsage     float64
	MemoryUsage  int64
	LastMeasured time.Time
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

	// Note: Don't close the file handle here as it's managed by the caller
	// mmf.file = nil

	// Reset size to indicate closed state
	mmf.size = 0

	return firstErr
}

// Constants for performance optimization
const (
	// Buffer sizes
	DefaultBufferSize = 4 * 1024 * 1024  // 4MB
	MinBufferSize     = 64 * 1024        // 64KB
	MaxBufferSize     = 64 * 1024 * 1024 // 64MB

	// Transfer optimization
	DefaultChunkSize       = 8 * 1024 * 1024   // 8MB
	MinChunkSize           = 256 * 1024        // 256KB
	MaxChunkSize           = 128 * 1024 * 1024 // 128MB
	MaxConcurrentTransfers = 16

	// Connection limits
	DefaultMaxConnections = 1000
	DefaultIdleTimeout    = 60 * time.Minute  // Increased from 15 to 60 minutes for large file transfers
	DefaultCommandTimeout = 600 * time.Second // Increased from 60 to 600 seconds (10 minutes) for large file transfers

	// Memory mapping thresholds
	MmapThreshold = 100 * 1024 * 1024 // 100MB
	MmapMaxFiles  = 100

	// Performance monitoring
	MetricsHistorySize    = 1000
	MetricsUpdateInterval = 5 * time.Second

	// Compression settings
	DefaultCompressionLevel = 6
	CompressionThreshold    = 1024 * 1024 // 1MB
)

// NewFTPServer creates a new enhanced FTP server instance
func NewFTPServer(listenPort, dataPortStart, dataPortEnd int, rootDir string) *FTPServer {
	ctx, cancel := context.WithCancel(context.Background())

	// Create user manager and auth service
	userManager := auth.NewUserManager("users.json")
	authService := auth.NewAuthService(userManager)
	siteCommandHandler := auth.NewSiteCommandHandler(userManager, rootDir)

	// Load users from file
	if err := userManager.LoadUsersFromFile(rootDir); err != nil {
		log.Printf("Warning: Could not load users.json: %v", err)
	}

	// Ultra high-performance configuration optimized for maximum throughput
	config := &ServerConfig{
		MaxConnections:       DefaultMaxConnections,
		MaxConnectionsPerIP:  100, // Increased connection limits
		ConnectionTimeout:    DefaultCommandTimeout,
		IdleTimeout:          DefaultIdleTimeout,
		MaxFileSize:          1000 * 1024 * 1024 * 1024, // 1TB
		EnableCompression:    false,                     // Disable compression for maximum speed
		CompressionLevel:     0,
		EnableTLS:            false,            // Can be enabled with certificates
		BufferSize:           16 * 1024 * 1024, // 16MB buffer for maximum performance
		EnableRateLimit:      false,            // Disable rate limiting for maximum speed
		GlobalBandwidthLimit: 0,                // unlimited - no throttling
		EnableLogging:        true,
		LogLevel:             2, // Info level
		EnableMetrics:        true,
		MetricsInterval:      MetricsUpdateInterval,
		TrustedSubnets:       []string{"192.168.1.0/24", "10.0.0.0/8", "127.0.0.0/10"}, // Default trusted subnets
		EnableIPRestriction:  true,                                                     // Enable by default
	}

	// Initialize connection pool
	connectionPool := NewConnectionPool(ctx, 100, 5*time.Minute)

	// Initialize performance stats
	perfStats := &PerformanceStats{
		startTime: time.Now(),
	}

	// Initialize global bandwidth limiter
	bandwidthLimiter := &GlobalBandwidthLimiter{
		maxRate:    config.GlobalBandwidthLimit,
		lastUpdate: time.Now(),
	}

	// Initialize session manager
	sessionManager := NewSessionManager(ctx)

	server := &FTPServer{
		listenPort:       listenPort,
		dataPortStart:    dataPortStart,
		dataPortEnd:      dataPortEnd,
		rootDir:          rootDir,
		userManager:      userManager,
		authService:      authService,
		siteCommandHandler: siteCommandHandler,
		users:            make(map[string]*auth.UserProfile), // Initialize users map
		nextDataPort:     dataPortStart,
		config:           config,
		connectionPool:   connectionPool,
		performanceStats: perfStats,
		bandwidthLimiter: bandwidthLimiter,
		sessionManager:   sessionManager,
		ctx:              ctx,
		cancel:           cancel,
		globalSessions:   make(map[string]*ClientSession),
	}


	return server
}

// Start begins listening for FTP connections
func (server *FTPServer) Start() error {
	// Force IPv4 for FTP compatibility
	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", server.listenPort))
	if err != nil {
		return fmt.Errorf("failed to start FTP server: %v", err)
	}

	server.listener = listener
	log.Printf("FTP Server started on port %d", server.listenPort)
	log.Printf("Data port range: %d-%d", server.dataPortStart, server.dataPortEnd)
	log.Printf("Root directory: %s", server.rootDir)
	log.Printf("Trusted subnets: %v", server.config.TrustedSubnets)

	//Infinite Loop accepts incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// Get client IP and check against trusted subnets
		clientIP := strings.Split(conn.RemoteAddr().String(), ":")[0]
		if !server.isIPAllowed(clientIP) {
			log.Printf("Connection from %s rejected (not in trusted subnets)", clientIP)
			conn.Close()
			continue
		}

		// Handle each client in a separate goroutine
		go server.handleClient(conn)
	}
}

// handleClient processes a single client connection
func (server *FTPServer) handleClient(conn net.Conn) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	clientIP := strings.Split(clientAddr, ":")[0]
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", clientAddr), log.LstdFlags)

	// Check if this IP already has a control session (for data connection inheritance)
	server.globalMutex.RLock()
	existingSession, hasControlSession := server.globalSessions[clientIP]
	server.globalMutex.RUnlock()

	var session *ClientSession
	if hasControlSession && existingSession != nil {
		// This is likely a data connection - inherit settings from control session
		session = &ClientSession{
			server:        server,
			controlConn:   conn,
			currentDir:    existingSession.currentDir,
			transferType:  existingSession.transferType,
			mode:          existingSession.mode,
			authenticated: existingSession.authenticated,
			logger:        logger,
			clientIP:      clientIP,
			// INHERIT BOTH EXPECTED SIZE AND TRANSFER MODE
			expectedUploadSize: existingSession.expectedUploadSize, // INHERIT EXPECTED SIZE
			transferMode:       existingSession.transferMode,       // INHERIT TRANSFER MODE
			username:           existingSession.username,
			userProfile:        existingSession.userProfile,
			restartPos:         existingSession.restartPos,
		}
		logger.Printf("🔄 [INHERIT] Data connection inherited session state from control %p -> %p (expectedSize: %d, transferMode: %s)",
			existingSession, session, session.expectedUploadSize, session.transferMode)
	} else {
		// This is a new control connection
		session = &ClientSession{
			server:        server,
			controlConn:   conn,
			currentDir:    "/",
			transferType:  "I", // Binary by default
			mode:          "S", // Stream mode by default
			authenticated: false,
			logger:        logger,
			clientIP:      clientIP,
		}
		// Register this as the control session for this IP
		server.globalMutex.Lock()
		server.globalSessions[clientIP] = session
		server.globalMutex.Unlock()
		logger.Printf("🆕 [NEW] New control session %p registered for IP %s", session, clientIP)
	}

	// Log client connection once
	session.logger.Printf("🌐 [INFO] Client connected from IP: %s", clientIP)

	// Send welcome message
	session.sendResponse(220, "Welcome to High-Performance FTP Server")

	// Process commands
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		// Log command with password redaction for security
		loggedCommand := command
		if strings.HasPrefix(strings.ToUpper(command), "PASS ") {
			loggedCommand = "PASS [REDACTED]"
		}
		session.logger.Printf("➡️  [COMMAND] %s", loggedCommand)
		session.handleCommand(command)
	}

	if err := scanner.Err(); err != nil {
		session.logger.Printf("❌ [ERROR] Connection error: %v", err)
	}

	session.logger.Printf("🔌 [INFO] Client disconnected")
}

// handleCommand processes individual FTP commands
func (session *ClientSession) handleCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToUpper(parts[0])
	args := ""
	if len(parts) > 1 {
		args = strings.Join(parts[1:], " ")
	}

	switch cmd {
	case "USER":
		session.username = args
		if session.server.authService.ValidateUser(args) {
			session.sendResponse(331, fmt.Sprintf("User %s OK. Password required", args))
		} else {
			session.sendResponse(530, "User not found")
		}
	case "PASS":
		if session.username == "" {
			session.sendResponse(503, "Login with USER first")
			return
		}

		authResult, err := session.server.authService.AuthenticateUser(session.username, args)
		if err != nil {
			session.sendResponse(530, "Authentication error")
			session.logger.Printf("❌ [AUTH] Authentication error for user %s: %v", session.username, err)
			return
		}

		if authResult.Success {
			session.authenticated = true
			session.userProfile = authResult.UserProfile

			// Initialize session metrics and limits
			session.initializeSession()

			session.sendResponse(230, fmt.Sprintf("User %s logged in", session.username))
			session.logger.Printf("🔑 [AUTH] User %s authenticated successfully", session.username)
		} else {
			if authResult.AccountLocked {
				session.logger.Printf("🔒 [LOCK] User %s locked after %d failed attempts", session.username, authResult.FailedAttempts)
			}
			session.sendResponse(530, authResult.Message)
		}
	case "SYST":
		session.handleSYST()
	case "TYPE":
		session.handleTYPE(args)
	case "MODE":
		session.handleMODE(args)
	case "PWD":
		session.handlePWD()
	case "CWD":
		session.handleCWD(args)
	case "LIST":
		session.handleLIST(args)
	case "PASV":
		session.handlePASV()
	case "EPSV":
		session.handleEPSV(args)
	case "PORT":
		session.handlePORT(args)
	case "RETR":
		session.handleRETR(args)
	case "STOR":
		session.handleSTOR(args)
	case "QUIT":
		// Decrement user connection count before disconnecting
		if session.authenticated && session.userProfile != nil {
			session.server.authService.DecrementUserConnection(session.userProfile)
			session.logger.Printf("User %s disconnected, active connections: %d",
				session.username, session.userProfile.ActiveConnections)
		}

		session.sendResponse(221, "Goodbye")
		session.controlConn.Close()
	case "FEAT":
		session.handleFEAT()
	case "OPTS":
		session.handleOPTS(args)
	case "SIZE":
		session.handleSIZE(args)
	case "MDTM":
		session.handleMDTM(args)
	case "REST":
		session.handleREST(args)
	case "MKD", "XMKD":
		session.handleMKD(args)
	case "RMD", "XRMD":
		session.handleRMD(args)
	case "DELE":
		session.handleDELE(args)
	case "RNFR":
		session.handleRNFR(args)
	case "RNTO":
		session.handleRNTO(args)
	case "CDUP":
		session.handleCDUP()
	case "ABOR":
		session.handleABOR()
	case "STRU":
		session.handleSTRU(args)
	case "ALLO":
		session.handleALLO(args)
	case "APPE":
		session.handleAPPE(args)
	case "STAT":
		session.handleSTAT(args)
	case "HELP":
		session.handleHELP(args)
	case "NOOP":
		session.sendResponse(200, "NOOP command successful")
	case "AUTH":
		session.handleAUTH(args)
	case "SITE":
		// First check if this is a SITE command
		if strings.HasPrefix(strings.ToUpper(args), "SITE ") {
			session.sendResponse(501, "Syntax error: SITE command should not include 'SITE' prefix")
			return
		}

		// Handle various SITE commands
		siteCmd := strings.TrimSpace(args)
		siteCmdUpper := strings.ToUpper(siteCmd)

		// Now check SITE sub-commands
		switch {
		case strings.HasPrefix(siteCmdUpper, "MODE MMAP"):
			// Set server to use memory-mapped I/O mode
			session.transferMode = "mmap"
			session.logger.Printf("🗂️  [MMAP] Server set to memory-mapped upload mode for session %p", session)
			// Update the global session registry to ensure data connections inherit the correct mode
			server := session.server
			server.globalMutex.Lock()
			server.globalSessions[session.clientIP] = session
			server.globalMutex.Unlock()
			session.sendResponse(200, "Server synchronized to memory-mapped mode")
			return

		case strings.HasPrefix(siteCmdUpper, "MODE PARALLEL"):
			// Set server to use parallel/multi-threaded I/O mode
			session.transferMode = "parallel"
			session.logger.Printf("⚡ [PARALLEL] Server set to multi-threaded parallel mode for session %p", session)
			// Update the global session registry to ensure data connections inherit the correct mode
			server := session.server
			server.globalMutex.Lock()
			server.globalSessions[session.clientIP] = session
			server.globalMutex.Unlock()
			session.sendResponse(200, "Server synchronized to multi-threaded parallel mode")
			return

		case strings.HasPrefix(siteCmdUpper, "MODE CHUNKED"):
			// Set server to use chunked I/O mode
			session.transferMode = "chunked"
			session.logger.Printf("📦 [CHUNKED] Server set to chunked mode for session %p", session)
			// Update the global session registry to ensure data connections inherit the correct mode
			server := session.server
			server.globalMutex.Lock()
			server.globalSessions[session.clientIP] = session
			server.globalMutex.Unlock()
			session.sendResponse(200, "Server synchronized to chunked mode")
			return

		case strings.HasPrefix(siteCmdUpper, "CHECKLOCK "):
			parts := strings.Fields(siteCmd)
			if len(parts) < 2 {
				session.sendResponse(501, "Usage: SITE CHECKLOCK <filename>")
				return
			}
			filename := parts[1]
			filePath := session.resolvePath(filename)
			fullPath := filepath.Join(session.server.rootDir, filePath)
			fullPath = filepath.FromSlash(fullPath)
			// Try to open file for writing (exclusive lock test)
			file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				session.sendResponse(450, fmt.Sprintf("File '%s' is locked or unavailable", filename))
				return
			}
			defer file.Close()
			// Try to lock exclusively (non-blocking)
			locked := transfer.TryExclusiveLock(file)
			if locked {
				transfer.UnlockFile(file)
				session.sendResponse(200, fmt.Sprintf("File '%s' is not locked", filename))
			} else {
				session.sendResponse(450, fmt.Sprintf("File '%s' is currently locked", filename))
			}
			return

		// All other SITE commands delegate to the site command handler from auth module
		default:
			// Parse SITE command and delegate to appropriate handler
			parts := strings.Fields(siteCmd)
			if len(parts) == 0 {
				session.sendResponse(501, "SITE command requires arguments")
				return
			}

			siteSubCmd := strings.ToUpper(parts[0])
			switch siteSubCmd {
			case "ADDUSER":
				if len(parts) < 4 {
					session.sendResponse(501, "Syntax: SITE ADDUSER <username> <password> <homeDir> [permissions]")
					return
				}
				args := strings.Join(parts[1:], " ")
				result, err := session.server.siteCommandHandler.HandleAddUser(session.username, args)
				if err != nil {
					session.sendResponse(550, err.Error())
				} else {
					session.sendResponse(200, result)
				}

			case "DELUSER":
				if len(parts) < 2 {
					session.sendResponse(501, "Syntax: SITE DELUSER <username>")
					return
				}
				result, err := session.server.siteCommandHandler.HandleDelUser(session.username, parts[1])
				if err != nil {
					session.sendResponse(550, err.Error())
				} else {
					session.sendResponse(200, result)
				}

			case "LISTUSERS":
				result, err := session.server.siteCommandHandler.HandleListUsers(session.username)
				if err != nil {
					session.sendResponse(550, err.Error())
				} else {
					session.controlConn.Write([]byte(result))
				}

			case "USERINFO":
				if len(parts) < 2 {
					session.sendResponse(501, "Syntax: SITE USERINFO <username>")
					return
				}
				result, err := session.server.siteCommandHandler.HandleUserInfo(session.username, parts[1])
				if err != nil {
					session.sendResponse(550, err.Error())
				} else {
					session.controlConn.Write([]byte(result))
				}

			default:
				session.sendResponse(502, "SITE command not recognized")
			}
			return
		}
	default:
		session.sendResponse(502, fmt.Sprintf("Command %s not implemented", cmd))
	}
}

// Authentication commands are now handled in auth_handlers.go

// Helper functions to reduce handler implementation overlap
func (session *ClientSession) withAuth(handler func()) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}
	handler()
}

func (session *ClientSession) withValidParam(param string, handler func()) {
	if param == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}
	handler()
}

func (session *ClientSession) withExistingFile(filename string, handler func(string, string, os.FileInfo)) {
	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "File not found")
		return
	}

	handler(filePath, fullPath, fileInfo)
}

func (session *ClientSession) withExistingDirectory(dirname string, handler func(string, string, os.FileInfo)) {
	dirPath := session.resolvePath(dirname)
	fullPath := filepath.Join(session.server.rootDir, dirPath)
	fullPath = filepath.FromSlash(fullPath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "Directory not found")
		return
	}

	if !fileInfo.IsDir() {
		session.sendResponse(550, "Not a directory")
		return
	}

	handler(dirPath, fullPath, fileInfo)
}

func (session *ClientSession) withResolvedPath(filename string, handler func(string, string)) {
	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	handler(filePath, fullPath)
}

// System commands
func (session *ClientSession) handleSYST() {
	session.sendResponse(215, "UNIX Type: L8")
}

func (session *ClientSession) handleTYPE(typeStr string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	switch strings.ToUpper(typeStr) {
	case "A":
		session.transferType = "A"
		session.sendResponse(200, "Switching to ASCII mode")
	case "I":
		session.transferType = "I"
		session.sendResponse(200, "Switching to Binary mode")
	default:
		session.sendResponse(504, "Command not implemented for that parameter")
	}
}

func (session *ClientSession) handleMODE(modeStr string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	switch strings.ToUpper(modeStr) {
	case "S":
		session.mode = "S"
		session.sendResponse(200, "Mode set to Stream")
	case "Z":
		session.mode = "Z"
		session.sendResponse(200, "Mode set to Compressed")
	case "M":
		session.mode = "M"
		session.sendResponse(200, "Mode set to Memory-mapped")
	default:
		session.sendResponse(504, "Command not implemented for that parameter")
	}
}

// Directory commands
func (session *ClientSession) handlePWD() {
	session.withAuth(func() {
		session.sendResponse(257, fmt.Sprintf("\"%s\" is the current directory", session.currentDir))
	})
}

func (session *ClientSession) handleCWD(path string) {
	session.withAuth(func() {
		session.withValidParam(path, func() {
			newPath := session.resolvePath(path)
			fullPath := filepath.Join(session.server.rootDir, newPath)
			fullPath = filepath.FromSlash(fullPath)

			if stat, err := os.Stat(fullPath); err == nil && stat.IsDir() {
				session.currentDir = newPath
				session.sendResponse(250, fmt.Sprintf("CWD command successful. \"%s\" is current directory", newPath))
				session.logger.Printf("📁 [DIR] Changed directory to: %s", newPath)
			} else {
				session.sendResponse(550, "Failed to change directory")
			}
		})
	})
}

// Data connection commands
func (session *ClientSession) handlePASV() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Close existing data connection to ensure clean state
	session.closeDataConnection()

	// Try to get next available port with retry logic
	var listener net.Listener
	var port int
	var err error

	for retries := 0; retries < 10; retries++ {
		port = session.server.getNextDataPort()
		listener, err = net.Listen("tcp4", fmt.Sprintf(":%d", port))
		if err == nil {
			break // Success
		}
		session.logger.Printf("⚠️ [PASV] Port %d failed, trying next: %v", port, err)
	}

	if err != nil {
		session.logger.Printf("❌ [PASV] Failed to bind to any port after 10 retries: %v", err)
		session.sendResponse(425, "Can't open data connection - no available ports")
		return
	}

	session.dataListener = listener
	session.passiveMode = true

	// Get server IP address - force IPv4 for FTP compatibility
	host, _, _ := net.SplitHostPort(session.controlConn.LocalAddr().String())
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		// Default to localhost for IPv4
		ip = net.ParseIP("127.0.0.1")
	} else {
		// Ensure we use the IPv4 representation
		ip = ip.To4()
	}

	// Format response (h1,h2,h3,h4,p1,p2)
	p1 := port / 256
	p2 := port % 256
	ipParts := strings.Split(ip.String(), ".")

	session.passiveAddr = fmt.Sprintf("%s,%s,%s,%s,%d,%d",
		ipParts[0], ipParts[1], ipParts[2], ipParts[3], p1, p2)

	session.sendResponse(227, fmt.Sprintf("Entering Passive Mode (%s)", session.passiveAddr))
	session.logger.Printf("✅ [PASV] Listener ready on %s (port %d) for session %p", listener.Addr().String(), port, session)
}

// EPSV command - Extended Passive Mode (fallback to PASV)
func (session *ClientSession) handleEPSV(protocol string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// For simplicity, we'll implement EPSV by falling back to PASV
	// This is compatible and will work with most clients
	session.handlePASV()
}

func (session *ClientSession) handlePORT(portCmd string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Close existing data connection
	session.closeDataConnection()

	// Parse PORT command (h1,h2,h3,h4,p1,p2)
	parts := strings.Split(portCmd, ",")
	if len(parts) != 6 {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	// Build IP address
	ip := strings.Join(parts[0:4], ".")

	// Calculate port
	p1, err1 := strconv.Atoi(parts[4])
	p2, err2 := strconv.Atoi(parts[5])
	if err1 != nil || err2 != nil {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}
	port := p1*256 + p2

	session.passiveMode = false
	session.passiveAddr = fmt.Sprintf("%s:%d", ip, port)
	session.sendResponse(200, "PORT command successful")
}

// Enhanced lockFileRange with waiting notification using new timing utilities
func (session *ClientSession) lockFileRangeWithNotification(file *os.File, exclusive bool) (*transfer.LockResult, *transfer.TimedFileLocker, error) {
	locker := transfer.NewTimedFileLocker(file, exclusive)

	// Acquire lock with notification callback
	notifyFunc := func(waitTime time.Duration) {
		if waitTime == 0 {
			session.sendResponse(450, fmt.Sprintf("File '%s' is currently locked by another user. Waiting for access...", filepath.Base(file.Name())))
			session.logger.Printf("User %s waiting for file lock: %s", session.username, filepath.Base(file.Name()))
		} else {
			session.logger.Printf("User %s still waiting for file lock after %v: %s", session.username, waitTime.Round(time.Second), filepath.Base(file.Name()))
		}
	}

	lockResult, err := locker.AcquireLockWithNotification(notifyFunc)
	if err != nil {
		return nil, nil, err
	}

	if lockResult.WaitTime > 0 {
		session.logger.Printf("Lock acquired after %v wait time for user %s: %s",
			lockResult.WaitTime.Round(time.Millisecond), session.username, filepath.Base(file.Name()))
	}

	return lockResult, locker, nil
}

// File transfer commands
func (session *ClientSession) handleLIST(path string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	dataConn, err := session.openDataConnection()
	if err != nil {
		session.sendResponse(425, "Can't open data connection")
		return
	}
	defer session.closeDataConnection()

	session.sendResponse(150, "Here comes the directory listing")

	// Resolve the path for listing
	listPath := session.currentDir
	if path != "" {
		listPath = session.resolvePath(path)
	}

	// Use the enhanced path resolution method
	fullPath := session.getFullSystemPath(listPath)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		session.sendResponse(550, "Failed to list directory")
		return
	}

	var listing bytes.Buffer
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Format: permissions, links, owner, group, size, date, name
		perms := "-rw-r--r--"
		if info.IsDir() {
			perms = "drwxr-xr-x"
		}

		line := fmt.Sprintf("%s %3d %-8s %-8s %8d %s %s\r\n",
			perms, 1, "owner", "group", info.Size(),
			info.ModTime().Format("Jan 02 15:04"), info.Name())
		listing.WriteString(line)
	}

	// Send listing through data connection
	var writer io.Writer = dataConn
	if session.mode == "Z" {
		zWriter := zlib.NewWriter(dataConn)
		defer zWriter.Close()
		writer = zWriter
	}

	_, err = writer.Write(listing.Bytes())
	if err != nil {
		session.sendResponse(426, "Connection closed; transfer aborted")
		return
	}

	session.sendResponse(226, "Directory send OK")
	session.logger.Printf("📜 [LIST] Directory listing sent for: %s", listPath)
}

func (session *ClientSession) handleRETR(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	// Check if file exists
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "File not found")
		return
	}

	if fileInfo.IsDir() {
		session.sendResponse(550, "Is a directory")
		return
	}

	dataConn, err := session.openDataConnection()
	if err != nil {
		session.sendResponse(425, "Can't open data connection")
		return
	}
	defer session.closeDataConnection()

	session.sendResponse(150, fmt.Sprintf("Opening data connection for %s (%d bytes)", filename, fileInfo.Size()))

	// Open file for reading with large buffer
	file, err := os.Open(fullPath)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Failed to open file: %v", err))
		return
	}
	defer file.Close()

	// For RETR operations, allow concurrent reads - no exclusive locking needed
	// Multiple read operations can happen simultaneously on the same file
	session.logger.Printf("📖 [RETR] Opening file for concurrent read access (offset: %d) - session %p", session.restartPos, session)

	// Create a simple transfer timer without file locking for concurrent reads
	transferTimer := transfer.NewTransferTimer(nil)

	// Log the REST offset being used for this specific session
	if session.restartPos > 0 {
		session.logger.Printf("🔄 [REST] Session %p using REST offset: %d bytes", session, session.restartPos)
	}

	// No lock cleanup needed for read-only operations

	// Setup writer (with optional compression)
	var writer io.Writer = dataConn
	if session.mode == "Z" {
		zWriter := zlib.NewWriter(dataConn)
		defer zWriter.Close()
		writer = zWriter
	}

	// Handle REST (resume) position
	if session.restartPos > 0 {
		_, err = file.Seek(session.restartPos, 0)
		if err != nil {
			session.sendResponse(554, "Failed to seek to restart position")
			return
		}
		session.logger.Printf("(Thread-%d) Resume transfer from position %d", getThreadID(), session.restartPos)
	}

	// Use buffered transfer with large chunks
	buf := make([]byte, 32*1024*1024) // 32MB buffer
	var totalWritten int64
	// Note: startTime is now managed by transferTimer, which excludes lock wait time

	// For files over 2GB, use percentage-based progress logging
	const twoGB = 2 * 1024 * 1024 * 1024
	isLargeFile := fileInfo.Size() > twoGB
	var nextMilestone int
	var lastLogTime time.Time
	if isLargeFile {
		nextMilestone = 25 // Start with 25%
	} else {
		lastLogTime = time.Now() // Initialize for small files
	}

	for {
		n, err := file.Read(buf)
		if n > 0 {
			_, writeErr := writer.Write(buf[:n])
			if writeErr != nil {
				session.sendResponse(426, "Connection closed; transfer aborted")
				return
			}
			totalWritten += int64(n)

			// Update progress
			speed, elapsed := transferTimer.CalculateSpeed(totalWritten)
			if isLargeFile {
				// For large files (>2GB), use percentage-based logging
				percent := int((totalWritten * 100) / fileInfo.Size())
				if percent >= nextMilestone {
					session.logger.Printf("Transfer progress: %d%% (%.2f MB/s, transfer time: %v)",
						percent, speed, elapsed.Round(time.Millisecond))
					nextMilestone += 25
				}
			} else {
				// For smaller files, use time-based logging (every 5 seconds)
				if elapsed >= 5*time.Second && (lastLogTime.IsZero() || time.Since(lastLogTime) >= 5*time.Second) {
					session.logger.Printf("Transfer progress: %d/%d bytes (%.2f MB/s, transfer time: %v)",
						totalWritten, fileInfo.Size(), speed, elapsed.Round(time.Millisecond))
					lastLogTime = time.Now()
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			session.sendResponse(426, "Read error during transfer")
			return
		}
	}

	// Reset restart position after successful transfer
	session.restartPos = 0

	// Calculate final transfer speed using timer (excludes lock wait time)
	timingReport := transferTimer.GetTimingReport(totalWritten)

	session.sendResponse(226, "Transfer complete")
	session.logger.Printf("(Thread-%d) File sent: %s (%d bytes, %.2f MB/s, %v total time)",
		getThreadID(), filename, totalWritten, timingReport.TransferSpeed, timingReport.TotalTime.Round(time.Millisecond))
}
func (session *ClientSession) handleSTOR(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	dataConn, err := session.openDataConnection()
	if err != nil {
		session.sendResponse(425, "Can't open data connection")
		return
	}
	defer session.closeDataConnection()

	session.sendResponse(150, fmt.Sprintf("Opening data connection for %s", filename))

	var file *os.File

	if session.restartPos > 0 {
		// Resume mode - open existing file for writing at specific position
		file, err = os.OpenFile(fullPath, os.O_RDWR, 0644)
		if err != nil {
			// If file doesn't exist for resume, create it
			file, err = os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				session.sendResponse(550, fmt.Sprintf("Failed to open file for resume: %v", err))
				return
			}
		}
		defer file.Close()

		// Get current file size to validate restart position
		fileInfo, err := file.Stat()
		if err == nil && session.restartPos > fileInfo.Size() {
			// Extend file to restart position if needed
			if err := file.Truncate(session.restartPos); err != nil {
				session.sendResponse(550, "Failed to extend file for restart position")
				return
			}
		}

		// Seek to restart position
		_, err = file.Seek(session.restartPos, 0)
		if err != nil {
			session.sendResponse(550, "Failed to seek to restart position")
			return
		}

		session.logger.Printf("(Thread-%d) Resume upload from position %d", getThreadID(), session.restartPos)
	} else {
		// Normal mode - create new file or truncate existing
		file, err = os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			session.sendResponse(550, fmt.Sprintf("Failed to open file: %v", err))
			return
		}
		defer file.Close()
	}

	// Acquire exclusive (write) lock on the file with timing
	lockResult, locker, err := session.lockFileRangeWithNotification(file, true)
	if err != nil {
		session.sendResponse(550, "Failed to lock file")
		return
	}

	// Create transfer timer to exclude lock wait time from speed calculations
	transferTimer := transfer.NewTransferTimer(lockResult)

	// Use the original locker that acquired the lock for cleanup
	defer func() {
		if unlockErr := locker.ReleaseLock(); unlockErr != nil {
			session.logger.Printf("Warning: failed to unlock file: %v", unlockErr)
		}
	}()

	// Setup reader (with optional decompression)
	var reader io.Reader = dataConn
	if session.mode == "Z" {
		zReader, err := zlib.NewReader(dataConn)
		if err != nil {
			session.sendResponse(550, "Failed to initialize decompression")
			return
		}
		defer zReader.Close()
		reader = zReader
	}

	var bytesWritten int64

	// Get the current expectedUploadSize and transferMode from the control session (inheritance fix)
	server := session.server
	server.globalMutex.RLock()
	controlSession, hasControlSession := server.globalSessions[session.clientIP]
	server.globalMutex.RUnlock()

	expectedSize := session.expectedUploadSize
	transferMode := session.transferMode

	if hasControlSession && controlSession != nil && controlSession != session {
		// This is a data session - inherit BOTH expectedSize AND transferMode from control session
		controlExpectedSize := controlSession.expectedUploadSize
		controlTransferMode := controlSession.transferMode

		// Inherit expectedUploadSize
		if controlExpectedSize > 0 {
			expectedSize = controlExpectedSize
			session.logger.Printf("🔄 [INHERIT] Data connection inherited expectedUploadSize from control session: %d bytes (%.2f MB)", expectedSize, float64(expectedSize)/(1024*1024))
		} else {
			session.logger.Printf("🔄 [INHERIT] Control session has expectedUploadSize=0, using local value: %d bytes", expectedSize)
		}

		// Inherit transferMode (this was the missing piece!)
		if controlTransferMode != "" {
			transferMode = controlTransferMode
			session.transferMode = controlTransferMode // Update data session's transfer mode
			session.logger.Printf("🔄 [INHERIT] Data connection inherited transferMode from control %p -> %p: %s", controlSession, session, transferMode)
		} else {
			session.logger.Printf("🔄 [INHERIT] Control session has empty transferMode, using default: %s", transferMode)
		}
	} else {
		session.logger.Printf("🔄 [LOCAL] Using local session state: expectedSize=%d bytes, transferMode=%s", expectedSize, transferMode)
	}

	// Log the final transfer mode and expected size being used
	session.logger.Printf("🔧 [MODE] Using transfer mode: %s", transferMode)
	if expectedSize > 0 {
		session.logger.Printf("💾 [SIZE] Expected upload size: %d bytes (%.2f MB)", expectedSize, float64(expectedSize)/(1024*1024))
	}

	// Don't reset expectedUploadSize until after the upload is complete
	// Store the control session reference for later cleanup
	var controlSessionForCleanup *ClientSession
	if hasControlSession && controlSession != nil && controlSession != session {
		controlSessionForCleanup = controlSession
	}

	// 400MB/s HIGH-PERFORMANCE upload method selection based on file size and conditions
	if session.restartPos > 0 {
		// Resume uploads always use streaming for safety and simplicity
		session.logger.Printf("📤 Using streaming upload (resume mode from position %d)", session.restartPos)
		bytesWritten, err = transfer.FallbackStreamingUpload(session, file, reader)
	} else {
		// ALWAYS attempt 400MB/s upload methods first for maximum performance
		// The SuperfastSTORHandler will automatically choose the best method
		if expectedSize > 0 {
			session.logger.Printf("Starting optimized upload for %d MB file", expectedSize/(1024*1024))
		} else {
			session.logger.Printf("Starting optimized upload (size unknown)")
		}
		bytesWritten, err = transfer.SuperfastSTORHandler(session, file, reader, expectedSize)
		// Reset expectedUploadSize after successful upload
		if controlSessionForCleanup != nil {
			controlSessionForCleanup.expectedUploadSize = 0
		} else {
			session.expectedUploadSize = 0
		}
	}
	if err != nil {
		session.sendResponse(426, "Connection closed; transfer aborted")
		return
	}

	// Calculate final transfer speed using timer (excludes lock wait time)
	timingReport := transferTimer.GetTimingReport(bytesWritten)

	session.sendResponse(226, "Transfer complete")
	session.logger.Printf("(Thread-%d) File received: %s (%d bytes, %.2f MB/s, %v total time)",
		getThreadID(), filename, bytesWritten, timingReport.TransferSpeed, timingReport.TotalTime.Round(time.Millisecond))

	// Reset restart position after successful transfer
	session.restartPos = 0
	// Removed blocking sleep for maximum performance - let OS handle write cache asynchronously
}

// FEAT command - advertise server features
func (session *ClientSession) handleFEAT() {
	features := "211-Features:\r\n"
	features += " MODE Z\r\n"      // MODE Z compression support
	features += " UTF8\r\n"        // UTF-8 filename support
	features += " SIZE\r\n"        // SIZE command support
	features += " MDTM\r\n"        // Modification time support
	features += " REST STREAM\r\n" // Resume support
	features += "211 End\r\n"
	session.controlConn.Write([]byte(features))
	session.logger.Printf("⚙️ [INFO] FEAT command processed - advertised server capabilities")
}

// OPTS command - set options for FTP extensions
func (session *ClientSession) handleOPTS(args string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Handle empty arguments
	if args == "" {
		session.sendResponse(501, "OPTS command requires arguments")
		return
	}

	// Parse the OPTS command arguments more flexibly
	args = strings.TrimSpace(args)
	parts := strings.Fields(strings.ToUpper(args))
	if len(parts) == 0 {
		session.sendResponse(501, "OPTS command requires arguments")
		return
	}

	option := parts[0]

	switch option {
	case "UTF8":
		// Handle UTF8 option - can be "OPTS UTF8 ON" or just "OPTS UTF8"
		if len(parts) >= 2 {
			value := parts[1]
			switch value {
			case "ON":
				session.utf8Enabled = true
				session.sendResponse(200, "UTF8 set to on")
				session.logger.Printf("🔤 [UTF-8] UTF-8 support enabled")
			case "OFF":
				session.utf8Enabled = false
				session.sendResponse(200, "UTF8 set to off")
				session.logger.Printf("🔤 [UTF-8] UTF-8 support disabled")
			default:
				session.sendResponse(501, "Invalid UTF8 value. Use ON or OFF")
			}
		} else {
			// Default to ON if no value specified (common client behavior)
			session.utf8Enabled = true
			session.sendResponse(200, "UTF8 set to on")
			session.logger.Printf("UTF-8 support enabled (default)")
		}

	case "MLST":
		// Handle MLST fact selection
		if len(parts) >= 2 {
			// Join remaining parts as MLST facts
			facts := strings.Join(parts[1:], " ")
			session.sendResponse(200, fmt.Sprintf("MLST OPTS %s", facts))
			session.logger.Printf("MLST facts set: %s", facts)
		} else {
			// Return current MLST facts
			defaultFacts := "Type;Size;Modify;Perm"
			session.sendResponse(200, fmt.Sprintf("MLST OPTS %s", defaultFacts))
			session.logger.Printf("MLST facts queried")
		}

	case "MODE":
		// Handle MODE options
		if len(parts) >= 2 {
			modeType := parts[1]
			switch modeType {
			case "Z":
				if len(parts) >= 4 && parts[2] == "LEVEL" {
					// Set compression level: OPTS MODE Z LEVEL 6
					level, err := strconv.Atoi(parts[3])
					if err != nil || level < 0 || level > 9 {
						session.sendResponse(501, "Invalid compression level. Use 0-9")
						return
					}
					session.sendResponse(200, fmt.Sprintf("MODE Z compression level set to %d", level))
					session.logger.Printf("Compression level set to %d", level)
				} else {
					// General MODE Z acknowledgment
					session.sendResponse(200, "MODE Z options accepted")
					session.logger.Printf("MODE Z options processed")
				}
			default:
				session.sendResponse(501, fmt.Sprintf("MODE %s not supported", modeType))
			}
		} else {
			session.sendResponse(501, "MODE option requires a mode type")
		}

	case "PASV":
		// Handle PASV options - some clients send this
		session.sendResponse(200, "PASV options accepted")
		session.logger.Printf("PASV options processed")

	case "PORT":
		// Handle PORT options - some clients send this
		session.sendResponse(200, "PORT options accepted")
		session.logger.Printf("PORT options processed")

	case "RETR":
		// Handle RETR options - some clients send this for resume/partial transfers
		session.sendResponse(200, "RETR options accepted")
		session.logger.Printf("RETR options processed")

	case "STOR":
		// Handle STOR options - some clients send this
		session.sendResponse(200, "STOR options accepted")
		session.logger.Printf("STOR options processed")

	case "HASH":
		// Handle HASH options - for file integrity checking
		if len(parts) >= 2 {
			hashType := parts[1]
			switch hashType {
			case "SHA-1", "SHA-256", "MD5", "CRC32":
				session.sendResponse(200, fmt.Sprintf("HASH %s accepted", hashType))
				session.logger.Printf("HASH type set to %s", hashType)
			default:
				session.sendResponse(501, "Hash type not supported")
			}
		} else {
			session.sendResponse(200, "HASH SHA-1 CRC32 MD5")
		}

	default:
		// More graceful handling of unknown options
		session.sendResponse(451, fmt.Sprintf("Option %s not supported", option))
		session.logger.Printf("Unsupported OPTS option: %s", option)
	}
}

// SIZE command - get file size
func (session *ClientSession) handleSIZE(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	// Check if file exists
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "File not found")
		return
	}

	if fileInfo.IsDir() {
		session.sendResponse(550, "Is a directory")
		return
	}

	session.sendResponse(213, fmt.Sprintf("%d", fileInfo.Size()))

	// Set expectedUploadSize for subsequent STOR operations (performance optimization)
	session.expectedUploadSize = fileInfo.Size()
	session.logger.Printf("SIZE command: %s (%d bytes) - expectedUploadSize set to %d", filename, fileInfo.Size(), session.expectedUploadSize)
}

// MDTM command - get file modification time
func (session *ClientSession) handleMDTM(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	// Check if file exists
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "File not found")
		return
	}

	if fileInfo.IsDir() {
		session.sendResponse(550, "Is a directory")
		return
	}

	// Format: YYYYMMDDHHMMSS
	modTime := fileInfo.ModTime().UTC().Format("20060102150405")
	session.sendResponse(213, modTime)
	session.logger.Printf("MDTM command: %s (%s)", filename, modTime)
}

// REST command - set restart position for resume
func (session *ClientSession) handleREST(position string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if position == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	pos, err := strconv.ParseInt(position, 10, 64)
	if err != nil || pos < 0 {
		session.sendResponse(501, "Invalid restart position")
		return
	}

	session.restartPos = pos
	session.sendResponse(350, fmt.Sprintf("Restarting at %d. Send RETR to initiate transfer", pos))
	session.logger.Printf("(Thread-%d) REST command: restart position set to %d", getThreadID(), pos)
}

// MKD command - create directory
func (session *ClientSession) handleMKD(dirName string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if dirName == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	dirPath := session.resolvePath(dirName)
	fullPath := filepath.Join(session.server.rootDir, dirPath)
	fullPath = filepath.FromSlash(fullPath)

	err := os.MkdirAll(fullPath, 0755)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Failed to create directory: %v", err))
		return
	}

	session.sendResponse(257, fmt.Sprintf("\"%s\" directory created", dirPath))
	session.logger.Printf("Directory created: %s", dirPath)
}

// RMD command - remove directory
func (session *ClientSession) handleRMD(dirName string) {
	session.withAuth(func() {
		session.withValidParam(dirName, func() {
			session.withExistingDirectory(dirName, func(dirPath, fullPath string, fileInfo os.FileInfo) {
				// Check if directory is empty
				entries, err := os.ReadDir(fullPath)
				if err != nil {
					session.sendResponse(550, "Failed to read directory")
					return
				}

				if len(entries) > 0 {
					session.sendResponse(550, "Directory not empty")
					return
				}

				err = os.Remove(fullPath)
				if err != nil {
					session.sendResponse(550, fmt.Sprintf("Failed to remove directory: %v", err))
					return
				}

				session.sendResponse(250, "Directory removed")
				session.logger.Printf("Directory removed: %s", dirPath)
			})
		})
	})
}

// DELE command - delete file
func (session *ClientSession) handleDELE(filename string) {
	session.withAuth(func() {
		session.withValidParam(filename, func() {
			session.withExistingFile(filename, func(filePath, fullPath string, fileInfo os.FileInfo) {
				if fileInfo.IsDir() {
					session.sendResponse(550, "Is a directory, use RMD")
					return
				}

				err := os.Remove(fullPath)
				if err != nil {
					session.sendResponse(550, fmt.Sprintf("Failed to delete file: %v", err))
					return
				}

				session.sendResponse(250, "File deleted")
				session.logger.Printf("File deleted: %s", filePath)
			})
		})
	})
}

// RNFR command - rename from (first part of rename operation)
func (session *ClientSession) handleRNFR(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	// Check if file/directory exists
	if _, err := os.Stat(fullPath); err != nil {
		session.sendResponse(550, "File or directory not found")
		return
	}

	// Store the source path for RNTO command
	session.renameFrom = fullPath
	session.sendResponse(350, "Ready for RNTO")
	session.logger.Printf("RNFR: marked for rename: %s", filePath)
}

// RNTO command - rename to (second part of rename operation)
func (session *ClientSession) handleRNTO(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	if session.renameFrom == "" {
		session.sendResponse(503, "Bad sequence of commands, use RNFR first")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	// Check if target already exists
	if _, err := os.Stat(fullPath); err == nil {
		session.sendResponse(550, "Target file already exists")
		session.renameFrom = "" // Reset
		return
	}

	// Perform the rename
	err := os.Rename(session.renameFrom, fullPath)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Rename failed: %v", err))
		session.renameFrom = "" // Reset
		return
	}

	session.sendResponse(250, "Rename successful")
	session.logger.Printf("Renamed: %s -> %s", session.renameFrom, fullPath)
	session.renameFrom = "" // Reset after successful rename
}

// CDUP command - change to parent directory
func (session *ClientSession) handleCDUP() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Navigate to parent directory
	if session.currentDir == "/" {
		session.sendResponse(250, "Already in root directory")
		return
	}

	// Go up one level
	lastSlash := strings.LastIndex(session.currentDir, "/")
	if lastSlash > 0 {
		session.currentDir = session.currentDir[:lastSlash]
	} else {
		session.currentDir = "/"
	}

	session.sendResponse(250, fmt.Sprintf("CDUP command successful. \"%s\" is current directory", session.currentDir))
	session.logger.Printf("Changed to parent directory: %s", session.currentDir)
}

// ABOR command - abort current transfer
func (session *ClientSession) handleABOR() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Close any active data connections
	session.closeDataConnection()

	// Send appropriate response
	session.sendResponse(226, "No transfer to abort")
	session.logger.Printf("ABOR command processed")
}

// STRU command - set file structure
func (session *ClientSession) handleSTRU(structure string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	switch strings.ToUpper(structure) {
	case "F":
		session.sendResponse(200, "Structure set to File")
	case "R":
		session.sendResponse(504, "Record structure not supported")
	case "P":
		session.sendResponse(504, "Page structure not supported")
	default:
		session.sendResponse(501, "Unknown structure type")
	}
}

// ALLO command - allocate storage space
func (session *ClientSession) handleALLO(args string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}
	size, err := strconv.ParseInt(strings.TrimSpace(args), 10, 64)
	if err == nil && size > 0 {
		session.expectedUploadSize = size
		session.logger.Printf("💾 [ALLO] expectedUploadSize set to %d bytes (%.2f MB) for upcoming upload", size, float64(size)/(1024*1024))
		session.sendResponse(200, fmt.Sprintf("Will allocate %d bytes for upload", size))
	} else {
		session.logger.Printf("💾 [ALLO] Invalid size '%s' or size <= 0, ignoring ALLO command", args)
		session.sendResponse(202, "ALLO command ignored (storage allocation automatic)")
	}
}

// APPE command - append to file
func (session *ClientSession) handleAPPE(filename string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if filename == "" {
		session.sendResponse(501, "Syntax error in parameters")
		return
	}

	filePath := session.resolvePath(filename)
	fullPath := filepath.Join(session.server.rootDir, filePath)
	fullPath = filepath.FromSlash(fullPath)

	dataConn, err := session.openDataConnection()
	if err != nil {
		session.sendResponse(425, "Can't open data connection")
		return
	}
	defer session.closeDataConnection()

	session.sendResponse(150, fmt.Sprintf("Opening data connection for append to %s", filename))

	// Open file in append mode
	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Failed to open file for append: %v", err))
		return
	}
	defer file.Close()

	// Acquire exclusive (write) lock on the file with timing
	lockResult, locker, err := session.lockFileRangeWithNotification(file, true)
	if err != nil {
		session.sendResponse(550, "Failed to lock file")
		return
	}

	// Create transfer timer to exclude lock wait time from speed calculations
	transferTimer := transfer.NewTransferTimer(lockResult)

	// Use the original locker that acquired the lock for cleanup
	defer func() {
		if unlockErr := locker.ReleaseLock(); unlockErr != nil {
			session.logger.Printf("Warning: failed to unlock file: %v", unlockErr)
		}
	}()

	// Setup reader (with optional decompression)
	var reader io.Reader = dataConn
	if session.mode == "Z" {
		zReader, err := zlib.NewReader(dataConn)
		if err != nil {
			session.sendResponse(550, "Failed to initialize decompression")
			return
		}
		defer zReader.Close()
		reader = zReader
	}

	// Determine starting position for append
	restartPos := session.restartPos
	if restartPos == 0 {
		// Append mode - use file size as starting point
		fileInfo, err := file.Stat()
		if err == nil {
			restartPos = fileInfo.Size()
		}
	}

	// Seek to starting position for append
	_, err = file.Seek(restartPos, io.SeekStart)
	if err != nil {
		session.sendResponse(550, "Failed to seek to append position")
		return
	}

	// Inherit expectedUploadSize and transferMode
	expectedSize := session.expectedUploadSize
	session.logger.Printf("🔄 [APPE] Using transfer mode: %s", session.transferMode)
	if expectedSize > 0 {
		session.logger.Printf("💾 [APPE] Expected append size: %d bytes (%.2f MB)", expectedSize, float64(expectedSize)/(1024*1024))
	}

	// Use the SuperfastSTORHandler with proper mode
	bytesWritten, err := transfer.SuperfastSTORHandler(session, file, reader, expectedSize)
	if err != nil {
		session.sendResponse(426, "Connection closed; transfer aborted")
		return
	}

	// Calculate final transfer speed using timer (excludes lock wait time)
	timingReport := transferTimer.GetTimingReport(bytesWritten)

	session.sendResponse(226, "Append complete")
	session.logger.Printf("(Thread-%d) File appended: %s (%d bytes, %.2f MB/s, %v total time)",
		getThreadID(), filename, bytesWritten, timingReport.TransferSpeed, timingReport.TotalTime.Round(time.Millisecond))
}

// STAT command - show server/file status
func (session *ClientSession) handleSTAT(args string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if args == "" {
		// Show server status
		status := "211-FTP Server Status:\r\n"
		status += fmt.Sprintf(" Connected from: %s\r\n", session.controlConn.RemoteAddr().String())
		status += fmt.Sprintf(" Logged in as: %s\r\n", session.username)
		status += fmt.Sprintf(" Current directory: %s\r\n", session.currentDir)
		status += fmt.Sprintf(" Transfer type: %s\r\n", session.transferType)
		status += fmt.Sprintf(" Transfer mode: %s\r\n", session.mode)
		if session.mode == "Z" {
			status += " MODE Z compression: enabled\r\n"
		} else {
			status += " MODE Z compression: disabled\r\n"
		}
		status += fmt.Sprintf(" Data connection: %s\r\n", func() string {
			if session.passiveMode {
				return "PASV"
			}
			return "PORT"
		}())
		status += "211 End of status\r\n"
		session.controlConn.Write([]byte(status))
	} else {
		// Show file status
		filePath := session.resolvePath(args)
		fullPath := filepath.Join(session.server.rootDir, filePath)
		fullPath = filepath.FromSlash(fullPath)

		fileInfo, err := os.Stat(fullPath)
		if err != nil {
			session.sendResponse(550, "File not found")
			return
		}

		// Format file info similar to LIST
		perms := "-rw-r--r--"
		if fileInfo.IsDir() {
			perms = "drwxr-xr-x"
		}

		statusLine := fmt.Sprintf("213-%s %3d %-8s %-8s %8d %s %s\r\n213 End\r\n",
			perms, 1, "owner", "group", fileInfo.Size(),
			fileInfo.ModTime().Format("Jan 02 15:04"), fileInfo.Name())
		session.controlConn.Write([]byte(statusLine))
	}

	session.logger.Printf("(Thread-%d) STAT command processed for: %s", getThreadID(), args)
}

// HELP command - show available commands
// handleAUTH handles the AUTH command for unsupported TLS
func (session *ClientSession) handleAUTH(args string) {
	session.sendResponse(502, "AUTH command not supported")
	session.logger.Printf("AUTH command received and rejected")
}

func (session *ClientSession) handleHELP(args string) {
	if args == "" {
		// Show general help
		help := "214-The following commands are recognized:\r\n"
		help += " USER PASS QUIT SYST TYPE MODE PWD CWD LIST\r\n"
		help += " PASV PORT RETR STOR APPE DELE RNFR RNTO\r\n"
		help += " MKD RMD CDUP SIZE MDTM REST STAT FEAT\r\n"
		help += " OPTS STRU ALLO ABOR NOOP HELP\r\n"
		help += "214 Help OK\r\n"
		session.controlConn.Write([]byte(help))
	} else {
		// Show help for specific command
		cmd := strings.ToUpper(args)
		switch cmd {
		case "MODE":
			session.sendResponse(214, "MODE [S|Z] - Set transfer mode (S=Stream, Z=Compressed)")
		case "TYPE":
			session.sendResponse(214, "TYPE [A|I] - Set transfer type (A=ASCII, I=Binary)")
		case "PASV":
			session.sendResponse(214, "PASV - Enter passive mode")
		case "REST":
			session.sendResponse(214, "REST position - Set restart position for resume")
		case "SIZE":
			session.sendResponse(214, "SIZE filename - Get file size")
		case "MDTM":
			session.sendResponse(214, "MDTM filename - Get file modification time")
		default:
			session.sendResponse(502, fmt.Sprintf("No help available for %s", cmd))
		}
	}

	session.logger.Printf("HELP command processed for: %s", args)
}

// QUIT command is now handled in auth_handlers.go

// Helper methods
func (session *ClientSession) sendResponse(code int, message string) {
	// Thread-safe response sending with proper locking
	session.mutex.Lock()
	defer session.mutex.Unlock()

	response := fmt.Sprintf("%d %s\r\n", code, message)
	_, err := session.controlConn.Write([]byte(response))
	if err != nil {
		session.logger.Printf("Failed to send response %d: %v", code, err)
	} else {
		session.logger.Printf("→ Sent: %d %s", code, message)
	}
}

func (session *ClientSession) resolvePath(path string) string {
	// Start with current directory
	resolvedPath := session.currentDir

	// Handle absolute vs relative paths
	if strings.HasPrefix(path, "/") {
		// Absolute path - use as is
		resolvedPath = path
	} else if path != "" {
		// Relative path - append to current directory
		if session.currentDir == "/" {
			resolvedPath = "/" + path
		} else {
			resolvedPath = session.currentDir + "/" + path
		}
	}

	// Clean the path and ensure it starts with /
	resolvedPath = filepath.Clean(strings.ReplaceAll(resolvedPath, "\\", "/"))
	if !strings.HasPrefix(resolvedPath, "/") {
		resolvedPath = "/" + resolvedPath
	}

	return resolvedPath
}

// getFullSystemPath converts FTP path to full system path
func (session *ClientSession) getFullSystemPath(ftpPath string) string {
	// Clean the FTP path
	cleanPath := filepath.Clean(strings.ReplaceAll(ftpPath, "/", string(filepath.Separator)))

	// Remove leading separator to make it relative
	if strings.HasPrefix(cleanPath, string(filepath.Separator)) {
		cleanPath = strings.TrimPrefix(cleanPath, string(filepath.Separator))
	}

	// Join with server root directory
	fullPath := filepath.Join(session.server.rootDir, cleanPath)

	// Ensure we stay within the server root directory (security check)
	absRoot, _ := filepath.Abs(session.server.rootDir)
	absPath, _ := filepath.Abs(fullPath)

	if !strings.HasPrefix(absPath, absRoot) {
		// Path tries to escape root directory, return root
		return absRoot
	}

	return fullPath
}

func (session *ClientSession) openDataConnection() (net.Conn, error) {
	if session.passiveMode {
		// Accept connection on passive listener
		if session.dataListener == nil {
			return nil, fmt.Errorf("no passive listener available")
		}

		session.logger.Printf("🔄 [DATA] Waiting for data connection on %s...", session.dataListener.Addr().String())

		// Set a reasonable timeout for data connection accept
		if tcpListener, ok := session.dataListener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(60 * time.Second)) // Increased timeout
		}

		// Give client time to establish connection after PASV response
		time.Sleep(100 * time.Millisecond)

		// Accept the incoming data connection
		conn, err := session.dataListener.Accept()
		if err != nil {
			session.logger.Printf("❌ [DATA] Failed to accept data connection on %s: %v", session.dataListener.Addr().String(), err)
			return nil, fmt.Errorf("data connection accept failed: %v", err)
		}

		session.dataConn = conn
		session.logger.Printf("✅ [DATA] Data connection established from %s to %s (session %p)",
			conn.RemoteAddr().String(), conn.LocalAddr().String(), session)
		return conn, nil
	} else {
		// Connect to client's PORT
		if session.passiveAddr == "" {
			return nil, fmt.Errorf("no PORT specified")
		}

		session.logger.Printf("🔄 [DATA] Connecting to client PORT %s...", session.passiveAddr)
		conn, err := net.Dial("tcp", session.passiveAddr)
		if err != nil {
			session.logger.Printf("❌ [DATA] Failed to connect to PORT %s: %v", session.passiveAddr, err)
			return nil, fmt.Errorf("failed to connect to PORT: %v", err)
		}

		session.dataConn = conn
		session.logger.Printf("✅ [DATA] Data connection established to %s (session %p)",
			session.passiveAddr, session)
		return conn, nil
	}
}

func (session *ClientSession) closeDataConnection() {
	if session.dataConn != nil {
		session.dataConn.Close()
		session.dataConn = nil
	}
	if session.dataListener != nil {
		session.dataListener.Close()
		session.dataListener = nil
	}
}

func (server *FTPServer) getNextDataPort() int {
	server.portMutex.Lock()
	defer server.portMutex.Unlock()

	port := server.nextDataPort
	server.nextDataPort++
	if server.nextDataPort > server.dataPortEnd {
		server.nextDataPort = server.dataPortStart
	}
	return port
}

// isIPAllowed checks if the given IP address is in the trusted subnets
func (server *FTPServer) isIPAllowed(ipStr string) bool {
	if !server.config.EnableIPRestriction {
		return true // All allowed if restriction is disabled
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false // Invalid IP
	}

	for _, cidr := range server.config.TrustedSubnets {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			log.Printf("Invalid CIDR in trusted subnets: %s", cidr)
			continue
		}
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// Stop gracefully shuts down the server
func (server *FTPServer) Stop() error {
	if server.listener != nil {
		return server.listener.Close()
	}
	return nil
}


// Enhanced session initialization
func (session *ClientSession) initializeSession() {
	// Initialize session context
	ctx, cancel := context.WithTimeout(context.Background(), session.server.config.IdleTimeout)
	session.ctx = ctx
	session.cancel = cancel

	// Set session timing
	session.sessionStart = time.Now()
	session.lastActivity = time.Now()

	// Initialize connection ID
	clientAddr := session.controlConn.RemoteAddr().String()
	session.connectionID = fmt.Sprintf("%s-%d", clientAddr, time.Now().UnixNano())
	session.clientIP = strings.Split(clientAddr, ":")[0]

	// Initialize bandwidth limiter if user has limits
	if session.userProfile.BandwidthLimit > 0 {
		session.bandwidthLimiter = &BandwidthLimiter{
			maxRate:    session.userProfile.BandwidthLimit,
			tokens:     float64(session.userProfile.BandwidthLimit),
			lastUpdate: time.Now(),
		}
	}

	// Initialize performance metrics
	session.performanceMetrics = &SessionMetrics{
		lastMeasurement: time.Now(),
	}

	// Initialize transfer manager
	session.transferManager = &TransferManager{
		chunkSize:        DefaultChunkSize,
		maxConcurrency:   4, // Start conservative
		compression:      session.server.config.EnableCompression,
		compressionLevel: session.server.config.CompressionLevel,
		useMemoryMapping: true,
		bufferPool:       NewBufferPool(session.server.config.BufferSize, 10),
		optimizer:        NewTransferOptimizer(),
	}

	// Initialize adaptive buffer
	session.adaptiveBuffer = &AdaptiveBuffer{
		currentSize: session.server.config.BufferSize,
		minSize:     MinBufferSize,
		maxSize:     MaxBufferSize,
		optimalSize: session.server.config.BufferSize,
		lastAdjust:  time.Now(),
	}

	// Set parallel transfer limits based on user permissions
	if session.userProfile.Permissions.Parallel {
		session.maxParallel = MaxConcurrentTransfers
	} else {
		session.maxParallel = 1
	}

	// Set compression level
	session.compressionLevel = session.server.config.CompressionLevel

	// Assess initial network condition
	session.networkCondition = NetworkGood

	session.logger.Printf("Session initialized for user %s with limits: BW=%d B/s, Parallel=%d",
		session.username, session.userProfile.BandwidthLimit, session.maxParallel)
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(ctx context.Context, maxConnections int, maxIdleTime time.Duration) *ConnectionPool {
	poolCtx, cancel := context.WithCancel(ctx)

	pool := &ConnectionPool{
		connections:    make(map[string]*PooledConnection),
		maxConnections: maxConnections,
		maxIdleTime:    maxIdleTime,
		ctx:            poolCtx,
		cancel:         cancel,
	}

	// Start cleanup goroutine
	pool.cleaner = time.NewTicker(time.Minute)
	go pool.cleanupRoutine()

	return pool
}

// cleanupRoutine removes idle connections
func (pool *ConnectionPool) cleanupRoutine() {
	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-pool.cleaner.C:
			pool.cleanup()
		}
	}
}

func (pool *ConnectionPool) cleanup() {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	now := time.Now()
	for id, conn := range pool.connections {
		if !conn.inUse && now.Sub(conn.lastUsed) > pool.maxIdleTime {
			conn.conn.Close()
			delete(pool.connections, id)
		}
	}
}

// NewSessionManager creates a new session manager
func NewSessionManager(ctx context.Context) *SessionManager {
	sessionCtx, cancel := context.WithCancel(ctx)

	sm := &SessionManager{
		sessions:      make(map[string]*ClientSession),
		ipConnections: make(map[string]int),
		ctx:           sessionCtx,
		cancel:        cancel,
	}

	// Start cleanup ticker
	sm.cleanupTicker = time.NewTicker(30 * time.Second)
	go sm.cleanupSessions()

	return sm
}

func (sm *SessionManager) cleanupSessions() {
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-sm.cleanupTicker.C:
			sm.performCleanup()
		}
	}
}

func (sm *SessionManager) performCleanup() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	now := time.Now()
	for id, session := range sm.sessions {
		// Remove sessions that have been idle too long
		if now.Sub(session.lastActivity) > 30*time.Minute {
			session.logger.Printf("Session %s timed out due to inactivity", id)
			session.controlConn.Close()
			delete(sm.sessions, id)

			// Decrement IP connection count
			if count, exists := sm.ipConnections[session.clientIP]; exists {
				if count <= 1 {
					delete(sm.ipConnections, session.clientIP)
				} else {
					sm.ipConnections[session.clientIP] = count - 1
				}
			}
		}
	}
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(bufferSize int, maxBuffers int) *BufferPool {
	return &BufferPool{
		buffers:    make(chan []byte, maxBuffers),
		bufferSize: bufferSize,
		maxBuffers: maxBuffers,
	}
}

// GetBuffer gets a buffer from the pool or creates a new one
func (bp *BufferPool) GetBuffer() []byte {
	select {
	case buf := <-bp.buffers:
		atomic.AddInt64(&bp.reused, 1)
		return buf
	default:
		atomic.AddInt64(&bp.allocated, 1)
		return make([]byte, bp.bufferSize)
	}
}

// PutBuffer returns a buffer to the pool
func (bp *BufferPool) PutBuffer(buf []byte) {
	if len(buf) != bp.bufferSize {
		return // Wrong size buffer
	}

	select {
	case bp.buffers <- buf:
		// Buffer returned to pool
	default:
		// Pool is full, let GC handle it
	}
}

// NewTransferOptimizer creates a new transfer optimizer
func NewTransferOptimizer() *TransferOptimizer {
	return &TransferOptimizer{
		history:          make([]TransferRecord, 0, 100),
		maxHistory:       100,
		optimalChunkSize: DefaultChunkSize,
		optimalThreads:   4,
		lastOptimization: time.Now(),
	}
}

// OptimizeTransfer analyzes transfer history and optimizes parameters
func (to *TransferOptimizer) OptimizeTransfer(fileSize int64, networkCondition NetworkCondition) (chunkSize int64, threads int) {
	to.mutex.Lock()
	defer to.mutex.Unlock()

	// If we have enough history, analyze and optimize
	if len(to.history) >= 10 && time.Since(to.lastOptimization) > 30*time.Second {
		to.analyzeAndOptimize()
		to.lastOptimization = time.Now()
	}

	// Adjust based on file size and network condition
	chunkSize = to.optimalChunkSize
	threads = to.optimalThreads

	// Adjust for file size
	if fileSize < 10*1024*1024 { // Files < 10MB
		chunkSize = MinChunkSize
		threads = 1
	} else if fileSize > 1024*1024*1024 { // Files > 1GB
		chunkSize = MaxChunkSize
		threads = min(threads*2, MaxConcurrentTransfers)
	}

	// Adjust for network condition
	switch networkCondition {
	case NetworkExcellent:
		threads = min(threads*2, MaxConcurrentTransfers)
	case NetworkGood:
		// Keep current values
	case NetworkFair:
		chunkSize = chunkSize / 2
		threads = max(threads/2, 1)
	case NetworkPoor:
		chunkSize = MinChunkSize
		threads = 1
	}

	return chunkSize, threads
}

func (to *TransferOptimizer) analyzeAndOptimize() {
	// Analyze recent transfers to find optimal parameters
	recentTransfers := to.history[max(0, len(to.history)-20):]

	var bestThroughput float64
	var bestChunkSize int64 = DefaultChunkSize
	var bestThreads int = 4

	for _, record := range recentTransfers {
		if record.Throughput > bestThroughput {
			bestThroughput = record.Throughput
			bestChunkSize = record.ChunkSize
			bestThreads = record.Threads
		}
	}

	// Update optimal values with some smoothing
	to.optimalChunkSize = (to.optimalChunkSize + bestChunkSize) / 2
	to.optimalThreads = (to.optimalThreads + bestThreads) / 2
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func getKeys(m map[string]*auth.UserProfile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getThreadID returns a simple thread identifier for logging
func getThreadID() int {
	// Simple implementation using goroutine ID approximation
	// In production, you might want to use a more sophisticated approach
	return int(time.Now().UnixNano() % 10000)
}

// User Management SITE Commands Implementation

// handleAddUser implements SITE ADDUSER command for creating new users
func (session *ClientSession) handleAddUser(args string) {
	// Check if user is authenticated and has admin privileges
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if session.username != "admin" {
		session.sendResponse(530, "Access denied. Administrator privileges required")
		session.logger.Printf("🚫 [SECURITY] Non-admin user %s attempted to create user", session.username)
		return
	}

	// Parse arguments: username password homeDir [permissions]
	parts := strings.Fields(args)
	if len(parts) < 3 {
		session.sendResponse(501, "Syntax: SITE ADDUSER <username> <password> <homeDir> [permissions]")
		return
	}

	username := parts[0]
	password := parts[1]
	homeDir := parts[2]

	// Validate username
	if !isValidUsername(username) {
		session.sendResponse(501, "Invalid username. Use alphanumeric characters and underscores only")
		return
	}

	// Validate password strength
	if !isValidPassword(password) {
		session.sendResponse(501, "Password too weak. Must be at least 6 characters with letters and numbers")
		return
	}

	// Validate and sanitize home directory
	if !isValidDirectory(homeDir) {
		session.sendResponse(501, "Invalid home directory path")
		return
	}

	// Check if user already exists
	session.server.mutex.Lock()
	defer session.server.mutex.Unlock()

	if _, exists := session.server.users[username]; exists {
		session.sendResponse(550, "User already exists")
		return
	}

	// Parse permissions if provided
	permissions := auth.UserPermissions{
		Read: true, Write: true, Delete: false, Create: true,
		List: true, Rename: false, Resume: true, Compress: true, Parallel: true,
	}

	if len(parts) >= 4 {
		permStr := strings.ToUpper(parts[3])
		permissions = parsePermissions(permStr)
	}

	// Hash the password using bcrypt
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		session.sendResponse(550, "Failed to hash password")
		return
	}

	// Create the new user profile
	newUser := &auth.UserProfile{
		PasswordHash:      string(passwordHash),
		HomeDir:           homeDir,
		Permissions:       permissions,
		MaxConnections:    10, // Default limit
		ActiveConnections: 0,
		BandwidthLimit:    0, // Unlimited by default
		TransferStats:     &auth.TransferStats{},
		LastLogin:         time.Time{},
		LoginAttempts:     0,
		Locked:            false,
	}

	// Add user to server
	session.server.users[username] = newUser

	// Save users to file for persistence
	err = session.server.SaveUsersToFile("users.json")
	if err != nil {
		session.logger.Printf("⚠️ [WARNING] Failed to persist user data: %v", err)
		session.sendResponse(451, fmt.Sprintf("User '%s' created but failed to save to persistent storage", username))
		return
	}

	// Create home directory if it doesn't exist
	fullHomeDir := filepath.Join(session.server.rootDir, homeDir)
	fullHomeDir = filepath.FromSlash(fullHomeDir)
	if err := os.MkdirAll(fullHomeDir, 0755); err != nil {
		session.logger.Printf("⚠️ [WARNING] Failed to create home directory for user %s: %v", username, err)
	}

	session.sendResponse(200, fmt.Sprintf("User '%s' created successfully with home directory '%s' and saved to persistent storage", username, homeDir))
	session.logger.Printf("👤 [USER] Admin %s created new user: %s (Home: %s) - persisted to file", session.username, username, homeDir)
}

// handleDelUser implements SITE DELUSER command for deleting users
func (session *ClientSession) handleDelUser(args string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if session.username != "admin" {
		session.sendResponse(530, "Access denied. Administrator privileges required")
		session.logger.Printf("🚫 [SECURITY] Non-admin user %s attempted to delete user", session.username)
		return
	}

	args = strings.TrimSpace(args)
	if args == "" {
		session.sendResponse(501, "Syntax: SITE DELUSER <username>")
		return
	}

	username := args

	// Prevent deletion of admin user
	if username == "admin" {
		session.sendResponse(550, "Cannot delete admin user")
		return
	}

	// Validate username
	if !isValidUsername(username) {
		session.sendResponse(501, "Invalid username")
		return
	}

	// Check if user exists
	session.server.mutex.Lock()
	defer session.server.mutex.Unlock()

	userProfile, exists := session.server.users[username]
	if !exists {
		session.sendResponse(550, "User not found")
		return
	}

	// Check if user has active connections
	if atomic.LoadInt32(&userProfile.ActiveConnections) > 0 {
		session.sendResponse(550, "Cannot delete user with active connections")
		return
	}

	// Delete the user
	delete(session.server.users, username)

	// Save users to file for persistence
	err := session.server.SaveUsersToFile("users.json")
	if err != nil {
		session.logger.Printf("⚠️ [WARNING] Failed to persist user data after deletion: %v", err)
		session.sendResponse(451, fmt.Sprintf("User '%s' deleted but failed to save to persistent storage", username))
		return
	}

	session.sendResponse(200, fmt.Sprintf("User '%s' deleted successfully and removed from persistent storage", username))
	session.logger.Printf("🗑️ [USER] Admin %s deleted user: %s - persisted to file", session.username, username)
}



// Helper functions for user validation

// isValidUsername checks if username contains only allowed characters
func isValidUsername(username string) bool {
	if len(username) < 2 || len(username) > 32 {
		return false
	}
	for _, char := range username {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

// isValidPassword checks password strength
func isValidPassword(password string) bool {
	if len(password) < 6 {
		return false
	}
	hasLetter := false
	hasNumber := false
	for _, char := range password {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
			hasLetter = true
		} else if char >= '0' && char <= '9' {
			hasNumber = true
		}
	}
	return hasLetter && hasNumber
}

// isValidDirectory checks if directory path is valid and safe
func isValidDirectory(dir string) bool {
	if dir == "" || strings.Contains(dir, "..") {
		return false
	}
	// Ensure it starts with / for consistency
	if !strings.HasPrefix(dir, "/") {
		dir = "/" + dir
	}
	// Check for invalid characters
	for _, char := range dir {
		if char < 32 || char == '|' || char == '*' || char == '?' || char == '<' || char == '>' {
			return false
		}
	}
	return true
}

// parsePermissions parses permission string (e.g., "RWDCLR" for Read,Write,Delete,Create,List,Rename)
func parsePermissions(permStr string) auth.UserPermissions {
	perms := auth.UserPermissions{}
	permStr = strings.ToUpper(permStr)

	perms.Read = strings.Contains(permStr, "R")
	perms.Write = strings.Contains(permStr, "W")
	perms.Delete = strings.Contains(permStr, "D")
	perms.Create = strings.Contains(permStr, "C")
	perms.List = strings.Contains(permStr, "L")
	perms.Rename = strings.Contains(permStr, "N")   // N for reNaming
	perms.Resume = strings.Contains(permStr, "S")   // S for reSume
	perms.Compress = strings.Contains(permStr, "Z") // Z for compress
	perms.Parallel = strings.Contains(permStr, "P") // P for Parallel

	return perms
}

// SaveUsersToFile saves all users to a JSON file for persistence
func (server *FTPServer) SaveUsersToFile(filePath string) error {
	server.mutex.RLock()
	defer server.mutex.RUnlock()

	// Create a map to store serializable user data
	userData := make(map[string]*auth.UserProfile)

	// Copy user data (excluding runtime fields)
	for username, profile := range server.users {
		// Create a copy with only persistent fields
	userCopy := &auth.UserProfile{
			Username:       username,
			PasswordHash:   profile.PasswordHash,
			HomeDir:        profile.HomeDir,
			Permissions:    profile.Permissions,
			MaxConnections: profile.MaxConnections,
			BandwidthLimit: profile.BandwidthLimit,
			// Runtime fields are excluded (ActiveConnections, TransferStats, etc.)
		}
		userData[username] = userCopy
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(userData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal users to JSON: %v", err)
	}

	// Write to file
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write users file: %v", err)
	}

	log.Printf("💾 [PERSISTENCE] Saved %d users to %s", len(userData), filePath)
	return nil
}

// LoadUsersFromFile loads users from a JSON file
func (server *FTPServer) LoadUsersFromFile(filePath string) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("📂 [PERSISTENCE] Users file %s does not exist, will be created on first user save", filePath)
		return nil // Not an error - file will be created when users are saved
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read users file: %v", err)
	}

	// Parse JSON
	var userData map[string]*auth.UserProfile
	err = json.Unmarshal(data, &userData)
	if err != nil {
		return fmt.Errorf("failed to parse users JSON: %v", err)
	}

	server.mutex.Lock()
	defer server.mutex.Unlock()

	// Load users into server, initializing runtime fields
	loadedCount := 0
	for username, profile := range userData {
		// Initialize runtime fields that aren't persisted
		profile.Username = username // Ensure username is set
		profile.ActiveConnections = 0
		profile.TransferStats = &auth.TransferStats{}
		profile.LastLogin = time.Time{}
		profile.LoginAttempts = 0
		profile.Locked = false

		// Validate loaded user data
		if profile.PasswordHash == "" && username != "anonymous" {
			log.Printf("⚠️ [WARNING] User %s has empty password hash, skipping", username)
			continue
		}

		if profile.HomeDir == "" {
			log.Printf("⚠️ [WARNING] User %s has empty home directory, skipping", username)
			continue
		}

		// Add to server users (this will override defaults if they exist)
		server.users[username] = profile
		loadedCount++

		// Ensure user's home directory exists
		fullHomeDir := filepath.Join(server.rootDir, profile.HomeDir)
		fullHomeDir = filepath.FromSlash(fullHomeDir)
		if err := os.MkdirAll(fullHomeDir, 0755); err != nil {
			log.Printf("⚠️ [WARNING] Failed to create home directory for user %s: %v", username, err)
		}
	}

	log.Printf("📥 [PERSISTENCE] Loaded %d users from %s", loadedCount, filePath)
	return nil
}
