package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
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

	"ftp/transfer"

	"golang.org/x/sys/windows"
)

// FTPServer represents the main FTP server with enhanced features
type FTPServer struct {
	listenPort       int
	dataPortStart    int
	dataPortEnd      int
	rootDir          string
	users            map[string]*UserProfile // Enhanced user management
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
}

// UserProfile contains enhanced user information
type UserProfile struct {
	Password          string
	HomeDir           string
	Permissions       UserPermissions
	MaxConnections    int
	ActiveConnections int32
	BandwidthLimit    int64 // bytes per second
	TransferStats     *TransferStats
	LastLogin         time.Time
	LoginAttempts     int32
	Locked            bool
}

// UserPermissions defines what operations a user can perform
type UserPermissions struct {
	Read     bool
	Write    bool
	Delete   bool
	Create   bool
	List     bool
	Rename   bool
	Resume   bool
	Compress bool
	Parallel bool
}

// TransferStats tracks transfer statistics
type TransferStats struct {
	FilesUploaded   int64
	FilesDownloaded int64
	BytesUploaded   int64
	BytesDownloaded int64
	TotalTransfers  int64
	LastTransfer    time.Time
	mutex           sync.RWMutex
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
	userProfile        *UserProfile
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
	DefaultIdleTimeout    = 15 * time.Minute
	DefaultCommandTimeout = 60 * time.Second

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

	// Create enhanced user profiles with granular permissions
	users := map[string]*UserProfile{
		"admin": {
			Password: "password123",
			HomeDir:  "/",
			Permissions: UserPermissions{
				Read: true, Write: true, Delete: true, Create: true,
				List: true, Rename: true, Resume: true, Compress: true, Parallel: true,
			},
			MaxConnections: 100, // Increased from 10 to 100
			BandwidthLimit: 0,   // unlimited
			TransferStats:  &TransferStats{},
		},
		"user": {
			Password: "userpass",
			HomeDir:  "/uploads",
			Permissions: UserPermissions{
				Read: true, Write: true, Delete: false, Create: true,
				List: true, Rename: false, Resume: true, Compress: true, Parallel: true,
			},
			MaxConnections: 50, // Increased from 5 to 50
			BandwidthLimit: 0,  // Unlimited
			TransferStats:  &TransferStats{},
		},
		"anonymous": {
			Password: "",
			HomeDir:  "/public",
			Permissions: UserPermissions{
				Read: true, Write: false, Delete: false, Create: false,
				List: true, Rename: false, Resume: true, Compress: true, Parallel: false,
			},
			MaxConnections: 25, // Increased from 3 to 25
			BandwidthLimit: 0,  // Unlimited
			TransferStats:  &TransferStats{},
		},
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

	return &FTPServer{
		listenPort:       listenPort,
		dataPortStart:    dataPortStart,
		dataPortEnd:      dataPortEnd,
		rootDir:          rootDir,
		users:            users,
		nextDataPort:     dataPortStart,
		config:           config,
		connectionPool:   connectionPool,
		performanceStats: perfStats,
		bandwidthLimiter: bandwidthLimiter,
		sessionManager:   sessionManager,
		ctx:              ctx,
		cancel:           cancel,
	}
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
	logger.Printf("🌐 [INFO] Client connected from IP: %s", clientIP)

	session := &ClientSession{
		server:        server,
		controlConn:   conn,
		currentDir:    "/",
		transferType:  "I", // Binary by default
		mode:          "S", // Stream mode by default
		authenticated: false,
		logger:        logger,
	}

	session.logger.Printf("🌐 [INFO] Client connected from IP: %s", session.clientIP)

	// Send welcome message
	session.sendResponse(220, "Welcome to High-Performance FTP Server")

	// Process commands
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		session.logger.Printf("➡️  [COMMAND] %s", command)
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
		session.handleUSER(args)
	case "PASS":
		session.handlePASS(args)
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
	case "PORT":
		session.handlePORT(args)
	case "RETR":
		session.handleRETR(args)
	case "STOR":
		session.handleSTOR(args)
	case "QUIT":
		session.handleQUIT()
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
		// Handle various SITE commands
		argsUpper := strings.ToUpper(args)
		if strings.HasPrefix(argsUpper, "ADDUSER") {
			// SITE ADDUSER command for creating new users
			parts := strings.Fields(args)
			if len(parts) >= 2 {
				remaining := strings.Join(parts[1:], " ")
				session.handleAddUser(remaining)
			} else {
				session.sendResponse(501, "Syntax: SITE ADDUSER <username> <password> <homeDir> [permissions]")
			}
			return
		} else if strings.HasPrefix(argsUpper, "DELUSER") {
			// SITE DELUSER command for deleting users
			parts := strings.Fields(args)
			if len(parts) >= 2 {
				remaining := parts[1]
				session.handleDelUser(remaining)
			} else {
				session.sendResponse(501, "Syntax: SITE DELUSER <username>")
			}
			return
		} else if strings.HasPrefix(argsUpper, "LISTUSERS") {
			// SITE LISTUSERS command for listing all users
			session.handleListUsers()
			return
		} else if strings.HasPrefix(argsUpper, "USERINFO") {
			// SITE USERINFO command for getting user information
			parts := strings.Fields(args)
			if len(parts) >= 2 {
				remaining := parts[1]
				session.handleUserInfo(remaining)
			} else {
				session.sendResponse(501, "Syntax: SITE USERINFO <username>")
			}
			return
		} else if strings.HasPrefix(argsUpper, "CHECKLOCK") {
			parts := strings.Fields(args)
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
		} else {
			session.sendResponse(502, "SITE command not recognized")
			return
		}
	default:
		session.sendResponse(502, fmt.Sprintf("Command %s not implemented", cmd))
	}
}

// Authentication commands
func (session *ClientSession) handleUSER(username string) {
	session.username = username
	if _, exists := session.server.users[username]; exists {
		session.sendResponse(331, fmt.Sprintf("User %s OK. Password required", username))
	} else {
		session.sendResponse(530, "User not found")
	}
}

func (session *ClientSession) handlePASS(password string) {
	if session.username == "" {
		session.sendResponse(503, "Login with USER first")
		return
	}

	userProfile, exists := session.server.users[session.username]
	if !exists {
		session.sendResponse(530, "User not found")
		return
	}

	// Check if user is locked
	if userProfile.Locked {
		session.sendResponse(530, "Account is locked")
		return
	}

	// Check connection limit
	if int(atomic.LoadInt32(&userProfile.ActiveConnections)) >= userProfile.MaxConnections {
		session.sendResponse(421, "Too many connections for this user")
		return
	}

	if userProfile.Password == password || (session.username == "anonymous" && password != "") {
		session.authenticated = true
		session.userProfile = userProfile

		// Increment active connections
		atomic.AddInt32(&userProfile.ActiveConnections, 1)

		// Update last login
		userProfile.LastLogin = time.Now()

		// Reset failed login attempts
		atomic.StoreInt32(&userProfile.LoginAttempts, 0)

		// Initialize session metrics and limits
		session.initializeSession()

		session.sendResponse(230, fmt.Sprintf("User %s logged in", session.username))
		session.logger.Printf("🔑 [AUTH] User %s authenticated successfully", session.username)
	} else {
		// Increment failed attempts
		attempts := atomic.AddInt32(&userProfile.LoginAttempts, 1)

		// Lock account after 5 failed attempts
		if attempts >= 5 {
			userProfile.Locked = true
			session.logger.Printf("🔒 [LOCK] User %s locked after %d failed attempts", session.username, attempts)
		}

		session.sendResponse(530, "Login incorrect")
	}
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
	default:
		session.sendResponse(504, "Command not implemented for that parameter")
	}
}

// Directory commands
func (session *ClientSession) handlePWD() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	session.sendResponse(257, fmt.Sprintf("\"%s\" is the current directory", session.currentDir))
}

func (session *ClientSession) handleCWD(path string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	newPath := session.resolvePath(path)
	fullPath := filepath.Join(session.server.rootDir, newPath)

	// Convert to Windows path format
	fullPath = filepath.FromSlash(fullPath)

	if stat, err := os.Stat(fullPath); err == nil && stat.IsDir() {
		session.currentDir = newPath
		session.sendResponse(250, fmt.Sprintf("CWD command successful. \"%s\" is current directory", newPath))
		session.logger.Printf("📁 [DIR] Changed directory to: %s", newPath)
	} else {
		session.sendResponse(550, "Failed to change directory")
	}
}

// Data connection commands
func (session *ClientSession) handlePASV() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	// Close existing data connection
	session.closeDataConnection()

	// Get next available port
	port := session.server.getNextDataPort()

	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		session.sendResponse(425, "Can't open data connection")
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
func (session *ClientSession) lockFileRangeWithNotification(file *os.File, exclusive bool) (*transfer.LockResult, error) {
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
		return nil, err
	}

	if lockResult.WaitTime > 0 {
		session.logger.Printf("Lock acquired after %v wait time for user %s: %s",
			lockResult.WaitTime.Round(time.Millisecond), session.username, filepath.Base(file.Name()))
	}

	return lockResult, nil
}

// Unlock the file.
func unlockFileRange(file *os.File) error {
	handle := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)
	return windows.UnlockFileEx(handle, 0, maxUint32, maxUint32, ol)
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

	// Acquire shared (read) lock on the file with timing
	lockResult, err := session.lockFileRangeWithNotification(file, false)
	if err != nil {
		session.sendResponse(550, "Failed to lock file")
		return
	}

	// Create transfer timer to exclude lock wait time from speed calculations
	transferTimer := transfer.NewTransferTimer(lockResult)

	// Create locker for cleanup
	locker := transfer.NewTimedFileLocker(file, false)
	locker.GetLockResult() // Set the result from our acquisition
	defer func() {
		if unlockErr := locker.ReleaseLock(); unlockErr != nil {
			session.logger.Printf("⚠️ [WARNING] Failed to unlock file: %v", unlockErr)
		}
	}()

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

	for {
		n, err := file.Read(buf)
		if n > 0 {
			_, writeErr := writer.Write(buf[:n])
			if writeErr != nil {
				session.sendResponse(426, "Connection closed; transfer aborted")
				return
			}
			totalWritten += int64(n)

			// Update progress periodically using transfer timer (excludes lock wait)
			speed, elapsed := transferTimer.CalculateSpeed(totalWritten)
			if elapsed > 5*time.Second {
				session.logger.Printf("Transfer progress: %d/%d bytes (%.2f MB/s, transfer time: %v)",
					totalWritten, fileInfo.Size(), speed, elapsed.Round(time.Millisecond))
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
	lockResult, err := session.lockFileRangeWithNotification(file, true)
	if err != nil {
		session.sendResponse(550, "Failed to lock file")
		return
	}

	// Create transfer timer to exclude lock wait time from speed calculations
	transferTimer := transfer.NewTransferTimer(lockResult)

	// Create locker for cleanup
	locker := transfer.NewTimedFileLocker(file, true)
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
	if session.expectedUploadSize > 0 && session.restartPos == 0 {
		// Use mmap only if we know the size and not resuming
		file.Truncate(session.expectedUploadSize)
		bytesWritten, err = session.optimizedMmapUploadFixedSize(file, reader, session.expectedUploadSize)
		session.expectedUploadSize = 0 // Reset after use
	} else {
		bytesWritten, err = session.optimizedSTORHandler(file, reader)
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
	time.Sleep(3 * time.Second) // Allow OS write cache to drain between transfers
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
		// Handle MLST fact selection - FileZilla and modern clients use this
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
	session.logger.Printf("SIZE command: %s (%d bytes)", filename, fileInfo.Size())
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
	session.sendResponse(350, fmt.Sprintf("Restarting at %d. Send STORE or RETRIEVE to initiate transfer", pos))
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

	// Check if directory exists and is actually a directory
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		session.sendResponse(550, "Directory not found")
		return
	}

	if !fileInfo.IsDir() {
		session.sendResponse(550, "Not a directory")
		return
	}

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
}

// DELE command - delete file
func (session *ClientSession) handleDELE(filename string) {
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
		session.sendResponse(550, "Is a directory, use RMD")
		return
	}

	err = os.Remove(fullPath)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Failed to delete file: %v", err))
		return
	}

	session.sendResponse(250, "File deleted")
	session.logger.Printf("File deleted: %s", filePath)
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
		session.sendResponse(200, fmt.Sprintf("Will allocate %d bytes for upload", size))
	} else {
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
	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		session.sendResponse(550, fmt.Sprintf("Failed to open file for append: %v", err))
		return
	}
	defer file.Close()

	// Acquire exclusive (write) lock on the file with timing
	lockResult, err := session.lockFileRangeWithNotification(file, true)
	if err != nil {
		session.sendResponse(550, "Failed to lock file")
		return
	}

	// Create transfer timer to exclude lock wait time from speed calculations
	transferTimer := transfer.NewTransferTimer(lockResult)

	// Create locker for cleanup
	locker := transfer.NewTimedFileLocker(file, true)
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

	// Transfer data using transfer timer (excludes lock wait time)
	// Note: timing now managed by transferTimer
	bytesWritten, err := io.Copy(file, reader)
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

func (session *ClientSession) handleQUIT() {
	// Decrement user connection count before disconnecting
	if session.authenticated && session.userProfile != nil {
		atomic.AddInt32(&session.userProfile.ActiveConnections, -1)
		session.logger.Printf("User %s disconnected, active connections: %d",
			session.username, atomic.LoadInt32(&session.userProfile.ActiveConnections))
	}

	session.sendResponse(221, "Goodbye")
	session.controlConn.Close()
}

// Helper methods
func (session *ClientSession) sendResponse(code int, message string) {
	response := fmt.Sprintf("%d %s\r\n", code, message)
	session.controlConn.Write([]byte(response))
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
			return nil, fmt.Errorf("no passive listener")
		}

		conn, err := session.dataListener.Accept()
		if err != nil {
			return nil, err
		}

		session.dataConn = conn
		return conn, nil
	} else {
		// Connect to client's PORT
		if session.passiveAddr == "" {
			return nil, fmt.Errorf("no PORT specified")
		}

		conn, err := net.Dial("tcp", session.passiveAddr)
		if err != nil {
			return nil, err
		}

		session.dataConn = conn
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

// High-performance file operations with fallback for compatibility
func (session *ClientSession) openMmapFile(filename string, writable bool) (*MmapFile, error) {
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
	if fileSize == 0 {
		file.Close()
		return &MmapFile{file: nil, data: []byte{}, size: 0, handle: 0}, nil
	}

	// Try Windows memory mapping first, fallback to reading into memory
	mmapFile, err := session.tryWindowsMmap(file, fileSize, writable)
	if err == nil {
		return mmapFile, nil
	}

	// Fallback: read entire file into memory for high performance
	session.logger.Printf("Memory mapping failed (%v), using memory fallback for file: %s", err, filename)
	data, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	return &MmapFile{
		file:   nil,
		data:   data,
		size:   fileSize,
		handle: 0,
	}, nil
}

// Windows-specific memory mapping (best effort)
func (session *ClientSession) tryWindowsMmap(file *os.File, fileSize int64, writable bool) (*MmapFile, error) {
	// For large files, try actual memory mapping
	if fileSize > 100*1024*1024 { // Only use mmap for files > 100MB
		return session.actualWindowsMmap(file, fileSize, writable)
	}

	// For smaller files, return error to use fallback
	return nil, fmt.Errorf("file too small for memory mapping")
}

// Actual Windows memory mapping implementation
func (session *ClientSession) actualWindowsMmap(file *os.File, fileSize int64, writable bool) (*MmapFile, error) {
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
		syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Create byte slice from mapped memory
	data := (*[1 << 30]byte)(unsafe.Pointer(addr))[:fileSize:fileSize]

	return &MmapFile{
		file:   file,
		data:   data,
		size:   fileSize,
		handle: mapHandle,
	}, nil
}

func (mmf *MmapFile) Close() error {
	if mmf.handle != 0 {
		if len(mmf.data) > 0 {
			syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&mmf.data[0])))
		}
		syscall.CloseHandle(mmf.handle)
		mmf.handle = 0
	}
	if mmf.file != nil {
		mmf.file.Close()
		mmf.file = nil
	}
	return nil
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

func main() {
	// Configure server parameters
	listenPort := 2121
	dataPortStart := 20000
	dataPortEnd := 20100
	rootDir := "."

	// Parse command line arguments if provided
	if len(os.Args) > 1 {
		if port, err := strconv.Atoi(os.Args[1]); err == nil {
			listenPort = port
		}
	}
	if len(os.Args) > 2 {
		rootDir = os.Args[2]
	}

	// Ensure root directory exists
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		log.Fatalf("Root directory does not exist: %s", rootDir)
	}

	// Create and start server
	server := NewFTPServer(listenPort, dataPortStart, dataPortEnd, rootDir)

	log.Printf("Starting High-Performance FTP Server...")
	log.Printf("Listening on port: %d", listenPort)
	log.Printf("Data port range: %d-%d", dataPortStart, dataPortEnd)
	log.Printf("Root directory: %s", rootDir)
	log.Printf("Configured users: %v", getKeys(server.users))

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
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

// optimizedMmapUpload implements true memory-mapped upload to match download performance
func (session *ClientSession) optimizedMmapUpload(dst *os.File, src io.Reader) (int64, error) {
	// Pre-allocate file space for memory mapping - start with large size
	initialSize := int64(1024 * 1024 * 1024) // 1GB initial allocation
	err := dst.Truncate(initialSize)
	if err != nil {
		// If we can't pre-allocate, fall back to streaming
		return session.fallbackStreamingUpload(dst, src)
	}

	// Create memory-mapped file for writing (like download uses for reading)
	mmapFile, err := session.createWritableMmapFile(dst, initialSize)
	if err != nil {
		// If memory mapping fails, fall back to streaming
		session.logger.Printf("Memory mapping failed, using streaming fallback: %v", err)
		return session.fallbackStreamingUpload(dst, src)
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

			mmapFile, err = session.createWritableMmapFile(dst, newSize)
			if err != nil {
				return totalWritten, fmt.Errorf("failed to remap file: %v", err)
			}
			remainingSpace = int64(len(mmapFile.data)) - totalWritten
		}

		// Read directly into memory-mapped region (ZERO COPY)
		readSize := min(int(remainingSpace), chunkSize)
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
		session.logger.Printf("Warning: failed to truncate file to final size: %v", err)
	}

	return totalWritten, nil
}

// chunkedMmapWrite writes large data in optimized chunks
func (session *ClientSession) chunkedMmapWrite(dst *os.File, data []byte) (int64, error) {
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

// optimizedCopyToFile provides high-performance file writing with adaptive buffering
func (session *ClientSession) optimizedCopyToFile(dst *os.File, src io.Reader) (int64, error) {
	// Use large buffer for high-speed transfers - matching download performance
	bufferSize := 8 * 1024 * 1024 // 8MB buffer to match download optimization
	buffer := make([]byte, bufferSize)

	var written int64
	var err error

	// Pre-allocate file space if possible (Windows optimization)
	if seeker, ok := interface{}(dst).(io.Seeker); ok {
		if currentPos, seekErr := seeker.Seek(0, io.SeekCurrent); seekErr == nil {
			// Try to estimate file size and pre-allocate
			if session.restartPos == 0 {
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

// fallbackStreamingUpload provides streaming upload when memory mapping fails
func (session *ClientSession) fallbackStreamingUpload(dst *os.File, src io.Reader) (int64, error) {
	// Use very large buffer for maximum throughput when mmap fails
	bufferSize := 16 * 1024 * 1024 // 16MB buffer for maximum performance
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

// createWritableMmapFile creates a memory-mapped file for writing
func (session *ClientSession) createWritableMmapFile(file *os.File, size int64) (*MmapFile, error) {
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

	// Create byte slice from mapped memory for writing
	data := (*[1 << 30]byte)(unsafe.Pointer(addr))[:size:size]

	return &MmapFile{
		file:   file,
		data:   data,
		size:   size,
		handle: mapHandle,
	}, nil
}

// ultraOptimizedBufferedUpload implements Option 2: Large buffer approach for maximum performance
func (session *ClientSession) ultraOptimizedBufferedUpload(dst *os.File, src io.Reader) (int64, error) {
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
func (session *ClientSession) hybridParallelUpload(dst *os.File, src io.Reader) (int64, error) {
	// Use multiple parallel buffers for optimal throughput
	bufferSize := 64 * 1024 * 1024 // 64MB per buffer
	numBuffers := 3                // Triple buffering for overlapped I/O

	// Create channel for buffer coordination
	bufferChan := make(chan []byte, numBuffers)
	writeChan := make(chan writeRequest, numBuffers)
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
				writeChan <- writeRequest{
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
		writeChan <- writeRequest{data: nil}
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

// writeRequest represents a write operation
type writeRequest struct {
	buffer []byte
	data   []byte
	size   int
}

// optimizedSTORHandler implements OPTIMIZATION #2: Efficient server STOR handler
// with large buffers, memory mapping, and optimized disk writes
func (session *ClientSession) optimizedSTORHandler(file *os.File, reader io.Reader) (int64, error) {
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

func getKeys(m map[string]*UserProfile) []string {
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
	permissions := UserPermissions{
		Read: true, Write: true, Delete: false, Create: true,
		List: true, Rename: false, Resume: true, Compress: true, Parallel: true,
	}

	if len(parts) >= 4 {
		permStr := strings.ToUpper(parts[3])
		permissions = parsePermissions(permStr)
	}

	// Create the new user profile
	newUser := &UserProfile{
		Password:          password,
		HomeDir:           homeDir,
		Permissions:       permissions,
		MaxConnections:    10, // Default limit
		ActiveConnections: 0,
		BandwidthLimit:    0, // Unlimited by default
		TransferStats:     &TransferStats{},
		LastLogin:         time.Time{},
		LoginAttempts:     0,
		Locked:            false,
	}

	// Add user to server
	session.server.users[username] = newUser

	// Create home directory if it doesn't exist
	fullHomeDir := filepath.Join(session.server.rootDir, homeDir)
	fullHomeDir = filepath.FromSlash(fullHomeDir)
	if err := os.MkdirAll(fullHomeDir, 0755); err != nil {
		session.logger.Printf("⚠️ [WARNING] Failed to create home directory for user %s: %v", username, err)
	}

	session.sendResponse(200, fmt.Sprintf("User '%s' created successfully with home directory '%s'", username, homeDir))
	session.logger.Printf("👤 [USER] Admin %s created new user: %s (Home: %s)", session.username, username, homeDir)
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

	session.sendResponse(200, fmt.Sprintf("User '%s' deleted successfully", username))
	session.logger.Printf("🗑️ [USER] Admin %s deleted user: %s", session.username, username)
}

// handleListUsers implements SITE LISTUSERS command for listing all users
func (session *ClientSession) handleListUsers() {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if session.username != "admin" {
		session.sendResponse(530, "Access denied. Administrator privileges required")
		return
	}

	session.server.mutex.RLock()
	defer session.server.mutex.RUnlock()

	userList := "211-User List:\r\n"
	for username, profile := range session.server.users {
		statusStr := "Active"
		if profile.Locked {
			statusStr = "Locked"
		}

		activeConns := atomic.LoadInt32(&profile.ActiveConnections)
		lastLogin := "Never"
		if !profile.LastLogin.IsZero() {
			lastLogin = profile.LastLogin.Format("2006-01-02 15:04:05")
		}

		userList += fmt.Sprintf(" %-12s | Status: %-6s | Connections: %d/%d | Home: %-15s | Last: %s\r\n",
			username, statusStr, activeConns, profile.MaxConnections, profile.HomeDir, lastLogin)
	}
	userList += "211 End of user list\r\n"

	session.controlConn.Write([]byte(userList))
	session.logger.Printf("📋 [INFO] Admin %s requested user list", session.username)
}

// handleUserInfo implements SITE USERINFO command for getting detailed user information
func (session *ClientSession) handleUserInfo(args string) {
	if !session.authenticated {
		session.sendResponse(530, "Not logged in")
		return
	}

	if session.username != "admin" {
		session.sendResponse(530, "Access denied. Administrator privileges required")
		return
	}

	args = strings.TrimSpace(args)
	if args == "" {
		session.sendResponse(501, "Syntax: SITE USERINFO <username>")
		return
	}

	username := args

	session.server.mutex.RLock()
	defer session.server.mutex.RUnlock()

	userProfile, exists := session.server.users[username]
	if !exists {
		session.sendResponse(550, "User not found")
		return
	}

	statusStr := "Active"
	if userProfile.Locked {
		statusStr = "Locked"
	}

	activeConns := atomic.LoadInt32(&userProfile.ActiveConnections)
	lastLogin := "Never"
	if !userProfile.LastLogin.IsZero() {
		lastLogin = userProfile.LastLogin.Format("2006-01-02 15:04:05")
	}

	bandwidthLimit := "Unlimited"
	if userProfile.BandwidthLimit > 0 {
		bandwidthLimit = fmt.Sprintf("%d B/s", userProfile.BandwidthLimit)
	}

	userInfo := fmt.Sprintf("213-User Information for '%s':\r\n", username)
	userInfo += fmt.Sprintf(" Status: %s\r\n", statusStr)
	userInfo += fmt.Sprintf(" Home Directory: %s\r\n", userProfile.HomeDir)
	userInfo += fmt.Sprintf(" Active Connections: %d/%d\r\n", activeConns, userProfile.MaxConnections)
	userInfo += fmt.Sprintf(" Bandwidth Limit: %s\r\n", bandwidthLimit)
	userInfo += fmt.Sprintf(" Last Login: %s\r\n", lastLogin)
	userInfo += fmt.Sprintf(" Failed Login Attempts: %d\r\n", atomic.LoadInt32(&userProfile.LoginAttempts))
	userInfo += fmt.Sprintf(" Permissions: Read=%t Write=%t Delete=%t Create=%t List=%t Rename=%t\r\n",
		userProfile.Permissions.Read, userProfile.Permissions.Write, userProfile.Permissions.Delete,
		userProfile.Permissions.Create, userProfile.Permissions.List, userProfile.Permissions.Rename)

	if userProfile.TransferStats != nil {
		userProfile.TransferStats.mutex.RLock()
		userInfo += fmt.Sprintf(" Transfer Stats: Files Up=%d Down=%d | Bytes Up=%d Down=%d\r\n",
			userProfile.TransferStats.FilesUploaded, userProfile.TransferStats.FilesDownloaded,
			userProfile.TransferStats.BytesUploaded, userProfile.TransferStats.BytesDownloaded)
		userProfile.TransferStats.mutex.RUnlock()
	}

	userInfo += "213 End of user information\r\n"

	session.controlConn.Write([]byte(userInfo))
	session.logger.Printf("ℹ️ [INFO] Admin %s requested info for user: %s", session.username, username)
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
func parsePermissions(permStr string) UserPermissions {
	perms := UserPermissions{}
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

// Implement optimizedMmapUploadFixedSize:
func (session *ClientSession) optimizedMmapUploadFixedSize(dst *os.File, src io.Reader, size int64) (int64, error) {
	mmapFile, err := session.createWritableMmapFile(dst, size)
	if err != nil {
		return session.optimizedSTORHandler(dst, src)
	}
	defer mmapFile.Close()
	// Read exactly size bytes from src into mmap
	n, err := io.ReadFull(src, mmapFile.data)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		// Client sent less data than expected
		dst.Truncate(int64(n))
		// Flush mmap changes to disk
		transfer.SyncMmapFile(mmapFile.data)
		syscall.FlushFileBuffers(syscall.Handle(dst.Fd()))
		return int64(n), nil
	}
	// Flush mmap changes to disk
	transfer.SyncMmapFile(mmapFile.data)
	syscall.FlushFileBuffers(syscall.Handle(dst.Fd()))
	return int64(n), err
}
