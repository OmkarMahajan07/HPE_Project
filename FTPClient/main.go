package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/c-bata/go-prompt"
	"github.com/fatih/color"
	"github.com/jlaffaye/ftp"

	"ftp/config"
	"ftp/terminal"
	"ftp/transfer"
)

// FTPConnection represents the FTP connection state
type FTPConnection struct {
	client              *ftp.ServerConn
	isConnected         int32  // Use int32 for atomic operations (0=false, 1=true)
	isSecure            bool
	server              string
	username            string
	password            string
	port                int
	currentRemoteDir    string
	supportsCompression bool     // Whether server supports MODE Z compression
	usingCompression    bool     // Whether compression is currently active
	rawConn             net.Conn // Raw connection for sending direct FTP commands
	compressionLevel    int      // Current compression level (0-9)
	connMutex           sync.RWMutex // Mutex for thread-safe connection operations
}

// TransferProgress tracks file transfer progress
type TransferProgress struct {
	startTime            time.Time
	lastUpdate           time.Time
	totalBytes           int64
	transferredBytes     int64
	lastTransferredBytes int64
	transferSpeed        float64
	isResumed            bool
	resumeOffset         int64
	fileName             string
	fileSize             int64
	updateMutex          *sync.Mutex
}

// NetworkMetrics stores measurements of network performance
type NetworkMetrics struct {
	RTT           time.Duration   // Round-trip time
	RTTHistory    []time.Duration // History of RTT measurements
	MaxRTTHistory int             // Maximum number of RTT measurements to store
	AvgRTT        time.Duration   // Average RTT
	MinRTT        time.Duration   // Minimum RTT
	MaxRTT        time.Duration   // Maximum RTT

	Throughput           float64   // Current throughput in bytes/sec
	ThroughputHistory    []float64 // History of throughput measurements
	MaxThroughputHistory int       // Maximum number of throughput measurements to store
	AvgThroughput        float64   // Average throughput
	MaxThroughput        float64   // Maximum throughput
	MinThroughput        float64   // Minimum throughput

	LastMeasurement time.Time // Time of the last measurement

	// Network condition classification
	NetworkCondition string // "excellent", "good", "fair", "poor"

	metricsMutex sync.Mutex // Mutex for thread-safe updates
}

// NetworkCondition constants
const (
	NetworkExcellent = "excellent"
	NetworkGood      = "good"
	NetworkFair      = "fair"
	NetworkPoor      = "poor"
)

// TransferWindow represents a sliding window for optimized transfers
type TransferWindow struct {
	bufferSize      int        // Buffer size for transfers
	windowSize      int        // Current number of parallel transfers
	minWindowSize   int        // Minimum window size
	maxWindowSize   int        // Maximum window size
	activeTransfers int        // Current active transfers
	transferMutex   sync.Mutex // Mutex for thread-safe access

	// Dynamic window adjustment parameters
	metrics         NetworkMetrics // Network performance metrics
	adaptiveEnabled bool           // Whether adaptive window sizing is enabled
	lastAdjustment  time.Time      // Time of last window size adjustment
	adjustInterval  time.Duration  // Minimum time between window size adjustments

	// Connection pooling
	connectionPool []*ftp.ServerConn // Pool of reusable FTP connections
	poolMutex      sync.Mutex        // Mutex for connection pool access
	maxPoolSize    int               // Maximum size of the connection pool

	// Connection health monitoring
	connectionHealth map[string]float64 // Health score for each connection (0-1)
	healthMutex      sync.Mutex         // Mutex for health map

	// Retry management
	maxRetries     int           // Maximum number of retries
	baseRetryDelay time.Duration // Base delay for exponential backoff
	maxRetryDelay  time.Duration // Maximum delay for exponential backoff

	// Session restoration
	sessionInfo  map[string]interface{} // Stores session information for restoration
	sessionMutex sync.Mutex             // Mutex for session info

	// Data integrity
	useChecksums      bool   // Whether to use checksums for integrity
	checksumAlgorithm string // Algorithm to use for checksums

	// Compression
	useCompression    bool  // Whether to use MODE Z compression
	compressionLevel  int   // Compression level (0-9, 0=no compression, 9=max)
	compressThreshold int64 // Minimum file size to use compression (bytes)

	// Performance monitoring
	successfulTransfers   int64         // Count of successful transfers
	failedTransfers       int64         // Count of failed transfers
	totalBytesTransferred int64         // Total bytes transferred
	totalTransferTime     time.Duration // Total time spent transferring
}

// FTPClientWrapper wraps FTPConnection to implement terminal.FTPClient interface
type FTPClientWrapper struct {
	conn *FTPConnection
}

func (w *FTPClientWrapper) List(path string) ([]*ftp.Entry, error) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		return nil, fmt.Errorf("not connected")
	}
	return w.conn.client.List(path)
}

func (w *FTPClientWrapper) IsConnected() bool {
	return atomic.LoadInt32(&w.conn.isConnected) == 1
}

func (w *FTPClientWrapper) GetCurrentDir() string {
	return w.conn.currentRemoteDir
}

// FTPConnectionWrapper implements the transfer.FTPConnectionInterface
type FTPConnectionWrapper struct {
	conn           *FTPConnection
	transferWindow *TransferWindow
}

// GetCompressionStatus returns the current compression status
func (fcw *FTPConnectionWrapper) GetCompressionStatus() bool {
	return fcw.conn.usingCompression
}

// GetWindowSize returns the current window size
func (fcw *FTPConnectionWrapper) GetWindowSize() int {
	return fcw.transferWindow.windowSize
}

// GetRawConnection returns the raw connection
func (fcw *FTPConnectionWrapper) GetRawConnection() net.Conn {
	return fcw.conn.rawConn
}

// getAPIKey retrieves the API key from environment variable
func getAPIKey() string {
	if key := os.Getenv("IPSTACK_API_KEY"); key != "" {
		return key
	}
	// Fallback to default key if environment variable is not set
	// In production, this should be removed and key should be required
	return "f0c12e91dc23eac708252e3434af0fc3"
}

var (
	ipstackAPIKey = getAPIKey() // Get API key from environment
	ftpConn       = FTPConnection{
		currentRemoteDir: "/",
	}
	ftpClientWrapper = &FTPClientWrapper{conn: &ftpConn}
	transferProg     = TransferProgress{}
	transferWindow   = TransferWindow{
		bufferSize:        4 * 1024 * 1024, // 4MB buffer
		windowSize:        8,               // 8 parallel transfers by default
		minWindowSize:     2,               // Minimum 2 connections
		maxWindowSize:     16,              // Maximum 16 connections
		activeTransfers:   0,
		adaptiveEnabled:   true,                           // Enable adaptive window sizing by default
		adjustInterval:    time.Second * 3,                // Adjust window size every 3 seconds
		maxPoolSize:       32,                             // Maximum connection pool size
		connectionPool:    make([]*ftp.ServerConn, 0, 32), // Initialize connection pool
		connectionHealth:  make(map[string]float64),       // Initialize connection health map
		maxRetries:        5,                              // Maximum of 5 retries
		baseRetryDelay:    time.Millisecond * 500,         // Start with 500ms delay
		maxRetryDelay:     time.Second * 30,               // Maximum 30 second delay
		sessionInfo:       make(map[string]interface{}),   // Initialize session info
		useChecksums:      true,                           // Enable checksums by default
		checksumAlgorithm: "crc32",                        // Use CRC32 by default (options: crc32, md5)
		useCompression:    true,                           // Enable compression by default
		compressionLevel:  6,                              // Default compression level (medium)
		compressThreshold: 1024 * 1024,                    // Only compress files larger than 1MB
		metrics: NetworkMetrics{
			MaxRTTHistory:        10,          // Store last 10 RTT measurements
			MaxThroughputHistory: 10,          // Store last 10 throughput measurements
			MinRTT:               time.Hour,   // Initialize to a high value
			NetworkCondition:     NetworkFair, // Start with fair network condition
		},
	}
	// I/O Mode Selection
	// Values: "mmap", "chunked", "mthread"
	ioMode            = "chunked" // Default to chunked I/O
	
	// Track server sync state to avoid redundant syncing
	lastSyncedMode = ""           // Last synced mode with server
	serverSynced   = false        // Whether server is in sync

	// Terminal components
	themeManager     *terminal.ThemeManager
	commandCompleter *terminal.CommandCompleter
	tableFormatter   *terminal.TableFormatter
	
	// TEMPORARY FLAGS TO DISABLE SPECIFIC COMMANDS
	// Set to true to disable sending these commands to the server
	disableSiteModeMMAPCommand = false  // Enable SITE MODE MMAP command
	disableSiteCHECKLOCKCommand = false  // Enable SITE CHECKLOCK command
)

var allowedCountries = map[string]bool{
	"India":         true,
	"United States": true, // ipstack
}

// This function resolves domain names to IPs
func resolveDomainToIP(domain string) ([]string, error) {
	ips, err := net.LookupHost(domain)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve domain: %v", err)
	}
	return ips, nil
}

// Update the isAllowedIP function to handle domains
func isAllowedHost(host string) (bool, string, error) {
	// Check if it's an IP address
	if ip := net.ParseIP(host); ip != nil {
		return isAllowedIP(host)
	}

	// It's a domain name - resolve it
	ips, err := resolveDomainToIP(host)
	if err != nil {
		return false, "", err
	}

	// Check all resolved IPs
	for _, ip := range ips {
		allowed, location, err := isAllowedIP(ip)
		if err != nil {
			return false, "", err
		}
		if !allowed {
			return false, location, nil
		}
	}
	return true, "Domain resolved to allowed IPs", nil
}

func isAllowedIP(ip string) (bool, string, error) {
	// Check if it's a private IP
	if isPrivateIP(net.ParseIP(ip)) {
		return true, "Private IP", nil
	}

	// Look up country for public IPs
	country, _, err := getIPLocation(ip)
	if err != nil {
		return false, "", fmt.Errorf("location lookup failed: %v", err)
	}

	// Check if country is in allow list
	if allowedCountries[country] {
		return true, country, nil
	}

	return false, country, nil
}

func getIPLocation(ip string) (string, string, error) {
	// Skip private IP addresses
	if isPrivateIP(net.ParseIP(ip)) {
		return "", "", fmt.Errorf("private IP address")
	}

	url := fmt.Sprintf("http://api.ipstack.com/%s?access_key=%s", ip, ipstackAPIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", "", fmt.Errorf("network error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 404 {
			return "", "", fmt.Errorf("invalid IP address")
		}
		if resp.StatusCode == 401 {
			return "", "", fmt.Errorf("invalid API key")
		}
		if resp.StatusCode == 429 {
			return "", "", fmt.Errorf("API request limit reached")
		}
		return "", "", fmt.Errorf("API returned status: %s", resp.Status)
	}

	var result struct {
		CountryName string `json:"country_name"`
		RegionName  string `json:"region_name"`
		Error       struct {
			Code int    `json:"code"`
			Info string `json:"info"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("failed to decode response: %v", err)
	}

	if result.Error.Info != "" {
		if result.Error.Code == 101 {
			return "", "", fmt.Errorf("invalid API key")
		}
		if result.Error.Code == 103 {
			return "", "", fmt.Errorf("invalid IP address")
		}
		if result.Error.Code == 104 {
			return "", "", fmt.Errorf("usage limit reached")
		}
		return "", "", fmt.Errorf("API error: %s", result.Error.Info)
	}

	if result.CountryName == "" {
		return "", "", fmt.Errorf("no location data available")
	}

	return result.CountryName, result.RegionName, nil
}

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}

	// Check for private IP ranges
	_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
	_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
	_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")

	return private24BitBlock.Contains(ip) ||
		private20BitBlock.Contains(ip) ||
		private16BitBlock.Contains(ip) ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast()
}

// Command represents a parsed command
type Command struct {
	name string
	args []string
}

func main() {
	// Initialize terminal components
	var err error
	themeManager, err = terminal.NewThemeManager()
	if err != nil {
		fmt.Printf("Warning: Failed to initialize theme manager: %v\n", err)
	}

	commandCompleter = terminal.NewCommandCompleter() // autocomplete
	tableFormatter = terminal.NewTableFormatter()

	// Set FTP client for autocomplete
	commandCompleter.SetFTPClient(ftpClientWrapper)

	// Set console title
	if runtime.GOOS == "windows" {
		exec.Command("cmd", "/C", "title", "Enhanced Go Terminal with FTP").Run()
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)
	go func() {
		sig := <-sigChan
	if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
		fmt.Println("\nReceived interrupt signal. Disconnecting from FTP server...")
		disconnectFTP()
	}
		fmt.Printf("\nReceived signal: %v. Exiting...\n", sig)
		os.Exit(0)
	}()

	// Welcome message
	themeManager.GetPromptColor().Println("Welcome to SuperfastFTP Client")
	themeManager.GetTextColor().Println("Type 'HELP' for available commands")
	fmt.Println()

	// Initialize prompt with improved configuration
	p := prompt.New(
		executor,
		commandCompleter.Completer,
		prompt.OptionTitle("Enhanced Go Terminal with FTP"),
		prompt.OptionLivePrefix(func() (string, bool) {
			dir := getCurrentDirectory()
			if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
				return "[FTP] " + dir + "> ", true
			}
			return dir + "> ", true
		}),
		prompt.OptionPrefixTextColor(prompt.Green),
		prompt.OptionPreviewSuggestionTextColor(prompt.Blue),
		prompt.OptionSelectedSuggestionBGColor(prompt.LightGray),
		prompt.OptionSuggestionBGColor(prompt.DarkGray),
		prompt.OptionCompletionWordSeparator(" "),
		// Remove OptionShowCompletionAtStart to prevent showing all files at once
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlSpace,
			Fn: func(buf *prompt.Buffer) {
				// Show suggestions when Ctrl+Space is pressed
				buf.InsertText("", false, true)
			},
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.Tab,
			Fn: func(buf *prompt.Buffer) {
				// Complete the current word when Tab is pressed
				buf.InsertText("", false, true)
			},
		}),
		// Add Ctrl+C handling to properly exit
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlC,
			Fn: func(buf *prompt.Buffer) {
			if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
				fmt.Println("\nDisconnecting from FTP server...")
				disconnectFTP()
			}
				fmt.Println("\nExiting...")
				os.Exit(0)
			},
		}),
	)

	// Start prompt
	p.Run()
}

// executor handles command execution
func executor(input string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return
	}

	if input == "exit" {
	if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
		disconnectFTP()
	}
	os.Exit(0)
	}

	cmd := parseCommand(input)
	processCommand(cmd)
}

func parseCommand(input string) Command {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return Command{}
	}

	cmd := Command{
		name: parts[0],
		args: parts[1:],
	}

	// Handle quoted arguments
	for i := 0; i < len(cmd.args); i++ {
		if strings.HasPrefix(cmd.args[i], "\"") {
			// Find the closing quote
			for j := i + 1; j < len(cmd.args); j++ {
				if strings.HasSuffix(cmd.args[j], "\"") {
					// Combine all parts between quotes
					cmd.args[i] = strings.Trim(cmd.args[i], "\"") + " " + strings.Join(cmd.args[i+1:j+1], " ")
					cmd.args[i] = strings.Trim(cmd.args[i], "\"")
					// Remove the combined parts
					cmd.args = append(cmd.args[:i+1], cmd.args[j+1:]...)
					break
				}
			}
		}
	}

	return cmd
}

func processCommand(cmd Command) {
	commands := map[string]func([]string){
		"clear": func(args []string) {
			if runtime.GOOS == "windows" {
				exec.Command("cmd", "/C", "cls").Run()
			} else {
				exec.Command("clear").Run()
			}
		},
		"MODEZ": func(args []string) {
		if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
			fmt.Println("Not connected to FTP server.")
			return
		}
			if len(args) == 0 {
				fmt.Println("Usage: MODEZ [ON|OFF]")
				return
			}
			state := strings.ToUpper(args[0])
			switch state {
			case "ON":
				err := enableModeZ(&ftpConn)
				if err != nil {
					fmt.Printf("Error enabling MODE Z: %v\n", err)
				} else {
					fmt.Println("MODE Z compression enabled.")
				}
			case "OFF":
				err := disableModeZ(&ftpConn)
				if err != nil {
					fmt.Printf("Error disabling MODE Z: %v\n", err)
				} else {
					fmt.Println("MODE Z compression disabled.")
				}
			default:
				fmt.Println("Invalid option. Use MODEZ [ON|OFF]")
			}
		},
		"setio": func(args []string) {
			if len(args) == 0 {
				fmt.Printf("Current I/O mode: %s\n", getIOModeString())
				fmt.Println("Usage: setio <mode>")
				fmt.Println("Available modes:")
				fmt.Println("  mmap    - Use memory-mapped I/O (fastest for very large files >= 100MB)")
				fmt.Println("  chunked - Use chunked I/O (stable, lower memory usage)")
				fmt.Println("  mthread - Use multi-threaded parallel I/O (faster for large files >= 50MB)")
				return
			}

			mode := strings.ToLower(args[0])
			switch mode {
		case "mmap":
				ioMode = "mmap"
				fmt.Println("I/O mode set to: Memory-mapped")
				// Sync with server when connected
			if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
					fmt.Println("🔄 Syncing server to Memory-mapped mode...")
if err := syncTransferModeWithServer("mmap"); err != nil {
						fmt.Printf("Warning: Failed to sync with server: %v\n", err)
						fmt.Println("Server-side mmap mode may not be enabled. Upload will use client-side mmap only.")
						serverSynced = false
					} else {
						lastSyncedMode = "mmap"
						serverSynced = true
						fmt.Println("✅ Server synchronized to Memory-mapped mode")
					}
				} else {
					fmt.Println("Note: Connect to FTP server to enable server-side memory-mapped mode")
					serverSynced = false
				}
		case "chunked":
			ioMode = "chunked"
			fmt.Println("I/O mode set to: Chunked")
				// Sync with server when connected
				if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
					fmt.Println("🔄 Syncing server to Chunked mode...")
if err := syncTransferModeWithServer("chunked"); err != nil {
						fmt.Printf("Warning: Failed to sync with server: %v\n", err)
						serverSynced = false
					} else {
						lastSyncedMode = "chunked"
						serverSynced = true
						fmt.Println("✅ Server synchronized to Chunked mode")
					}
				} else {
					fmt.Println("Note: Connect to FTP server to sync chunked mode")
					serverSynced = false
				}
		case "mthread":
			ioMode = "mthread"
			fmt.Println("I/O mode set to: Multi-threaded parallel")
				// Sync with server when connected
				if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
					fmt.Println("🔄 Syncing server to Multi-threaded parallel mode...")
if err := syncTransferModeWithServer("mthread"); err != nil {
						fmt.Printf("Warning: Failed to sync with server: %v\n", err)
						serverSynced = false
					} else {
						lastSyncedMode = "mthread"
						serverSynced = true
						fmt.Println("✅ Server synchronized to Multi-threaded parallel mode")
					}
				} else {
					fmt.Println("Note: Connect to FTP server to sync parallel mode")
					serverSynced = false
				}
			default:
				fmt.Println("Invalid I/O mode. Use 'mmap', 'chunked', or 'mthread'")
			}
		},
		"color": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Available colors:")
				fmt.Println("1. Green")
				fmt.Println("2. Red")
				fmt.Println("3. Blue")
				fmt.Println("4. Yellow")
				fmt.Print("Enter color number (1-4): ")
				var choice int
				fmt.Scanln(&choice)
				switch choice {
				case 1:
					color.Set(color.FgGreen)
				case 2:
					color.Set(color.FgRed)
				case 3:
					color.Set(color.FgBlue)
				case 4:
					color.Set(color.FgYellow)
				default:
					fmt.Println("Invalid color choice!")
				}
			} else {
				// Handle direct color setting
				colorChoice := args[0]
				switch colorChoice {
				case "green":
					color.Set(color.FgGreen)
				case "red":
					color.Set(color.FgRed)
				case "blue":
					color.Set(color.FgBlue)
				case "yellow":
					color.Set(color.FgYellow)
				default:
					fmt.Println("Invalid color choice!")
				}
			}
		},

		"HELP": func(args []string) {
			showHelp()
		},
		"dir": func(args []string) {
			path := "."
			if len(args) > 0 {
				path = args[0]
			}
			listDirectory(path)
		},
		"cd": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Current directory:", getCurrentDirectory())
			} else {
				changeDirectory(args[0])
			}
		},
		"mkdir": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: mkdir <directory_name>")
			} else {
				createDirectory(args[0])
			}
		},
		"rmdir": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: rmdir <directory_name>")
			} else {
				removeDirectory(args[0])
			}
		},
		"pwd": func(args []string) {
			fmt.Println(getCurrentDirectory())
		},
		"HOST": func(args []string) {
			if len(args) < 1 {
				fmt.Println("Usage: HOST <server>")
				return
			}

			server := args[0]

			// Check if host is allowed
			allowed, location, err := isAllowedHost(server)
			if err != nil {
				color.Set(color.FgRed)
				fmt.Printf("Access Denied: %v\n", err)
				color.Set(color.FgGreen)
				return
			}

			if !allowed {
				color.Set(color.FgRed)
				fmt.Printf("Access Denied: %s is not allowed (Location: %s)\n", server, location)
				color.Set(color.FgGreen)
				return
			}

			color.Set(color.FgGreen)
			if net.ParseIP(server) != nil {
				fmt.Printf("IP Location: %s (Allowed)\n", location)
			} else {
				fmt.Printf("Domain resolved to allowed location: %s\n", location)
			}
			color.Set(color.FgGreen)

			// Interactive login prompt
			fmt.Printf("USER : ")
			scanner := bufio.NewScanner(os.Stdin)
			if !scanner.Scan() {
				fmt.Println("Error reading username")
				return
			}
			username := scanner.Text()

			fmt.Print("PASS : ")
			if runtime.GOOS == "windows" {
				disableConsoleEcho()
				defer enableConsoleEcho()
			}
			password, err := readPasswordWithAsterisks()
			if err != nil {
				fmt.Printf("\nError reading password: %v\n", err)
				return
			}
			fmt.Println()

			port := 21 // Default FTP port
			if len(args) > 1 {
				fmt.Sscanf(args[1], "%d", &port)
			}

			connectToFTP(server, username, password, port)
		},
		"QUIT": func(args []string) {
			if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
				disconnectFTP()
			} else {
				fmt.Println("Not connected to FTP server.")
			}
		},
		"LIST": func(args []string) {
			path := ""
			if len(args) > 0 {
				path = args[0]
			}
			listFTPDirectory(path)
		},
		"CWD": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: CWD <path>")
			} else {
				changeFTPDirectory(args[0])
			}
		},
		"PWD": func(args []string) {
			if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
				fmt.Println("FTP Current directory:", ftpConn.currentRemoteDir)
			} else {
				fmt.Println("Not connected to FTP server.")
			}
		},
		"RETR": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: RETR <remoteFile> [localPath]")
				return
			}

			remoteFile := args[0]
			localPath := getCurrentDirectory()
			if len(args) > 1 {
				localPath = args[1]
			}

			// Automatically append filename if localPath is directory
			if stat, err := os.Stat(localPath); err == nil && stat.IsDir() {
				localPath = filepath.Join(localPath, filepath.Base(remoteFile))
			}

			fmt.Printf("Downloading %s to %s...\n", remoteFile, localPath)
			downloadFile(remoteFile, localPath)
		},
		"STOR": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: STOR <localFile> [remotePath]")
				return
			}

			localFile := args[0]
			remotePath := ""
			if len(args) > 1 {
				remotePath = args[1]
			}

			uploadFile(localFile, remotePath)
		},
		"del": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: del <filename>")
				return
			}

			filename := args[0]
			if !filepath.IsAbs(filename) {
				filename = filepath.Join(getCurrentDirectory(), filename)
			}

			// Check if file exists
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				fmt.Printf("Error: File '%s' does not exist\n", filename)
				return
			}

			// Confirm deletion
			fmt.Printf("Are you sure you want to delete '%s'? (y/n): ", filename)
			scanner := bufio.NewScanner(os.Stdin)
			if !scanner.Scan() {
				fmt.Println("Error reading input")
				return
			}
			response := strings.ToLower(scanner.Text())
			if response != "y" && response != "yes" {
				fmt.Println("Deletion cancelled")
				return
			}

			// Delete the file
			err := os.Remove(filename)
			if err != nil {
				fmt.Printf("Error deleting file: %v\n", err)
				return
			}
			fmt.Printf("File '%s' deleted successfully\n", filename)
		},
		"DEL": func(args []string) {
			if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
				fmt.Println("Not connected to FTP server.")
				return
			}

			if len(args) == 0 {
				fmt.Println("Usage: DEL <remote_filename>")
				return
			}

			remoteFile := args[0]
			if !strings.HasPrefix(remoteFile, "/") {
				// If relative path, prepend current directory
				remoteFile = ftpConn.currentRemoteDir
				if !strings.HasSuffix(remoteFile, "/") {
					remoteFile += "/"
				}
				remoteFile += args[0]
			}

			// Confirm deletion
			fmt.Printf("Are you sure you want to delete '%s' from the server? (y/n): ", remoteFile)
			scanner := bufio.NewScanner(os.Stdin)
			if !scanner.Scan() {
				fmt.Println("Error reading input")
				return
			}
			response := strings.ToLower(scanner.Text())
			if response != "y" && response != "yes" {
				fmt.Println("Deletion cancelled")
				return
			}

			// Delete the file from server
			err := ftpConn.client.Delete(remoteFile)
			if err != nil {
				fmt.Printf("Error deleting file from server: %v\n", err)
				return
			}
			fmt.Printf("File '%s' deleted successfully from server\n", remoteFile)
		},
		"theme": func(args []string) {
			if len(args) == 0 {
				themeManager.GetTextColor().Printf("Current theme: %s\n", themeManager.GetThemeName())
				themeManager.GetTextColor().Println("Available themes: light, dark")
				return
			}

			theme := args[0]
			if err := themeManager.SetTheme(theme); err != nil {
				themeManager.GetErrorColor().Printf("Failed to set theme: %v\n", err)
				return
			}

			themeManager.GetSuccessColor().Printf("Theme set to: %s\n", theme)
		},
		"CD": func(args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: CD <path>")
			} else {
				changeFTPDirectory(args[0])
			}
		},
		"SITE": func(args []string) {
			if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
				fmt.Println("Not connected to FTP server.")
				return
			}
			if len(args) == 0 {
				fmt.Println("Usage: SITE <command> [arguments]")
				fmt.Println("Available SITE commands:")
				fmt.Println("  ADDUSER <username> <password> <homeDir> [permissions]")
				fmt.Println("  DELUSER <username>")
				fmt.Println("  LISTUSERS")
				fmt.Println("  USERINFO <username>")
				return
			}
			siteCommand := strings.Join(args, " ")
			sendSiteCommand(siteCommand)
		},
	}

	if handler, ok := commands[cmd.name]; ok {
		handler(cmd.args)
	} else {
		// Execute as system command
		exec.Command(cmd.name, cmd.args...).Run()
	}
}

func showHelp() {
	themeManager.GetTextColor().Println("\nFTP Commands (RFC 959):")
	themeManager.GetTextColor().Println("USER <server> - Connect to FTP server (interactive login)")
	themeManager.GetTextColor().Println("QUIT - Disconnect from FTP server")
	themeManager.GetTextColor().Println("LIST [path] - List files on FTP server")
	themeManager.GetTextColor().Println("CD <path> - Change directory on FTP server")
	themeManager.GetTextColor().Println("PWD - Show current FTP directory")
	themeManager.GetTextColor().Println("RETR <remoteFile> [localPath] - Download file from FTP")
	themeManager.GetTextColor().Println("STOR <localFile> [remotePath] - Upload file to FTP")
	themeManager.GetTextColor().Println("\nAdditional Commands:")
	themeManager.GetTextColor().Println("setio <mode> - Set I/O mode (mmap/chunked/mthread)")
	themeManager.GetTextColor().Println("  mmap    - Use memory-mapped I/O (fastest for very large files >= 100MB)")
	themeManager.GetTextColor().Println("  chunked - Use chunked I/O (stable, lower memory usage)")
	themeManager.GetTextColor().Println("  mthread - Use multi-threaded parallel I/O (faster for large files >= 50MB)")
}

func getCurrentDirectory() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	return dir
}

func listDirectory(path string) {
	if path == "" {
		path = "."
	}

	fmt.Printf("\nDirectory of %s\n\n", path)

	entries, err := os.ReadDir(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		name := entry.Name()
		if entry.IsDir() {
			fmt.Printf("%s\t<DIR>\n", name)
		} else {
			fmt.Printf("%s\t%d bytes\n", name, info.Size())
		}
	}
}

func changeDirectory(path string) {
	err := os.Chdir(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
}

func createDirectory(dirName string) {
	err := os.Mkdir(dirName, 0755)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Directory '%s' created successfully\n", dirName)
}

func removeDirectory(dirName string) {
	err := os.RemoveAll(dirName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Directory '%s' removed successfully\n", dirName)
}

func connectToFTP(server, username, password string, port int) {
	if atomic.LoadInt32(&ftpConn.isConnected) != 0 {
		fmt.Println("Already connected to FTP server. Disconnect first.")
		return
	}

	// Handle localhost/local server connections
	isLocalServer := server == "localhost" || server == "127.0.0.1" || strings.HasPrefix(server, "192.168.")
	if isLocalServer {
		fmt.Println("Connecting to local server...")
	}

	addr := fmt.Sprintf("%s:%d", server, port)

	// Establish raw connection for MODE Z commands
	rawConn, rawErr := net.Dial("tcp", addr)
	if rawErr != nil {
		fmt.Printf("Warning: Raw connection failed (MODE Z commands unavailable): %v\n", rawErr)
	}

	client, err := ftp.Dial(addr, ftp.DialWithTimeout(60*time.Second))
	if err != nil {
		if rawConn != nil {
			rawConn.Close()
		}
		fmt.Printf("Failed to connect to FTP server: %v\n", err)
		return
	}

	// Login
	err = client.Login(username, password)
	if err != nil {
		fmt.Printf("Failed to login: %v\n", err)
		client.Quit()
		return
	}

	ftpConn.client = client
	atomic.StoreInt32(&ftpConn.isConnected, 1)
	ftpConn.isSecure = false
	ftpConn.server = server
	ftpConn.username = username
	ftpConn.password = password
	ftpConn.port = port
	ftpConn.currentRemoteDir = "/"
	ftpConn.rawConn = rawConn
	ftpConn.compressionLevel = 6 // Default compression level

	// Initialize and register FTP connection wrapper for real-time metrics
	connWrapper := &FTPConnectionWrapper{
		conn:           &ftpConn,
		transferWindow: &transferWindow,
	}
	transfer.SetMainFTPConnection(connWrapper)

	// Check if server supports MODE Z compression
	fmt.Printf("Connected to FTP server: %s on port %d\n", server, port)

	// Initialize raw connection authentication
	if rawConn != nil {
		// Skip server greeting
		reader := bufio.NewReader(rawConn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Warning: Failed to read server greeting: %v\n", err)
				break
			}
			if len(line) > 3 && line[3] == ' ' {
				break
			}
		}

		// Send USER command
		fmt.Fprintf(rawConn, "USER %s\r\n", username)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Warning: Failed to read USER response: %v\n", err)
				break
			}
			if len(line) > 3 && line[3] == ' ' {
				if !strings.HasPrefix(line, "331") {
					fmt.Printf("Warning: Unexpected USER response: %s\n", line)
				}
				break
			}
		}

		// Send PASS command
		fmt.Fprintf(rawConn, "PASS %s\r\n", password)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Warning: Failed to read PASS response: %v\n", err)
				break
			}
			if len(line) > 3 && line[3] == ' ' {
				if !strings.HasPrefix(line, "230") {
					fmt.Printf("Warning: Authentication for raw connection failed: %s\n", line)
				} else {
					fmt.Println("Raw connection authenticated successfully")
				}
				break
			}
		}

		// Check for MODE Z support but OFF by default
		ftpConn.usingCompression = false // Ensure compression is off by default
		ftpConn.supportsCompression = checkModeZSupport(&ftpConn)
		if ftpConn.supportsCompression {
			fmt.Println("Server supports MODE Z compression")
			fmt.Println("Use 'MODEZ ON' to enable compression or 'MODEZ OFF' to disable")
		} else {
			fmt.Println("Note: Server does not support MODE Z compression")
		}
	} else {
		ftpConn.supportsCompression = false
		if transferWindow.useCompression {
			fmt.Println("Note: MODE Z compression not available (no raw connection)")
		}
	}
}

func disconnectFTP() {
	// Disable compression if it's enabled
	if ftpConn.usingCompression {
		// Try multiple times to ensure compression is disabled
		var disableErr error
		for retries := 0; retries < 3; retries++ {
			disableErr = disableModeZ(&ftpConn)
			if disableErr == nil {
				break
			}
			fmt.Printf("Attempt %d: Failed to disable compression: %v. Retrying...\n", retries+1, disableErr)
			time.Sleep(time.Second)
		}

		if disableErr != nil {
			fmt.Printf("Warning: Failed to disable compression during disconnect: %v\n", disableErr)
		}
	}

	// Close raw connection
	if ftpConn.rawConn != nil {
		ftpConn.rawConn.Close()
		ftpConn.rawConn = nil
	}

	// Close client connection
	if ftpConn.client != nil {
		ftpConn.client.Quit()
		ftpConn.client = nil
	}

	atomic.StoreInt32(&ftpConn.isConnected, 0)
	ftpConn.supportsCompression = false
	ftpConn.usingCompression = false
	fmt.Println("Disconnected from FTP server.")
}

// sendRawCommand sends a raw FTP command and reads the response
func sendRawCommand(fc *FTPConnection, command string) (string, error) {
	if fc.rawConn == nil {
		// Try to restore the raw connection if it's missing
		err := restoreRawConnection(fc)
		if err != nil {
			return "", fmt.Errorf("no raw connection available and restoration failed: %v", err)
		}
	}

	// Send command
	_, err := fmt.Fprintf(fc.rawConn, "%s\r\n", command)
	if err != nil {
		// Connection might be dead, clear it and try to restore
		fc.rawConn = nil
		err = restoreRawConnection(fc)
		if err != nil {
			return "", fmt.Errorf("failed to send command and restore connection: %v", err)
		}

		// Retry sending command
		_, err = fmt.Fprintf(fc.rawConn, "%s\r\n", command)
		if err != nil {
			fc.rawConn = nil
			return "", fmt.Errorf("failed to send command after connection restoration: %v", err)
		}
	}

	// Read response with timeout
	reader := bufio.NewReader(fc.rawConn)
	var response strings.Builder

	// Set read deadline
	err = fc.rawConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		fmt.Printf("Warning: Failed to set read deadline: %v\n", err)
	}
	defer fc.rawConn.SetReadDeadline(time.Time{}) // Clear deadline

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			// Connection might be dead
			fc.rawConn = nil
			return "", fmt.Errorf("failed to read response: %v", err)
		}

		response.WriteString(line)

		// Check if this is the last line of the response
		if len(line) > 3 && line[3] == ' ' {
			break
		}

		// Also check for known multiline endings
		if strings.TrimSpace(line) == "211 End" ||
			strings.TrimSpace(line) == "226 Transfer complete" {
			break
		}
	}

	return response.String(), nil
}

// checkModeZSupport checks if the server supports MODE Z compression
func checkModeZSupport(fc *FTPConnection) bool {
	resp, err := sendRawCommand(fc, "FEAT")
	if err != nil {
		fmt.Printf("Warning: Failed to check MODE Z support: %v\n", err)
		return false
	}

	return strings.Contains(resp, "MODE Z")
}

// enableModeZ enables MODE Z compression with improved error handling and state management
func enableModeZ(fc *FTPConnection) error {
	// Verify connection state first
	if err := verifyConnectionState(fc); err != nil {
		// Try to restore the raw connection
		if restoreErr := restoreRawConnection(fc); restoreErr != nil {
			return fmt.Errorf("connection state verification failed and restoration failed: %v, %v", err, restoreErr)
		}
	}

	if !fc.supportsCompression {
		return fmt.Errorf("server does not support MODE Z compression")
	}

	if fc.usingCompression {
		// Already in MODE Z, just sync state to be sure
		if err := syncModeZState(fc); err != nil {
			fmt.Printf("Warning: Failed to sync compression state: %v\n", err)
		}
		return nil
	}

	// Send MODE Z command
	resp, err := sendRawCommand(fc, "MODE Z")
	if err != nil {
		return fmt.Errorf("failed to enable MODE Z: %v", err)
	}

	if !strings.HasPrefix(resp, "200") {
		return fmt.Errorf("server rejected MODE Z: %s", resp)
	}

	// Update state
	fc.usingCompression = true

	// Set compression level
	levelCmd := fmt.Sprintf("OPTS MODE Z LEVEL %d", fc.compressionLevel)
	levelResp, levelErr := sendRawCommand(fc, levelCmd)
	if levelErr != nil || !strings.HasPrefix(levelResp, "200") {
		// Continue even if setting level fails, just log warning
		fmt.Printf("Warning: Failed to set compression level: %v, %s\n", levelErr, levelResp)
	} else {
		fmt.Printf("Compression level set to %d\n", fc.compressionLevel)
	}

	return nil
}

// disableModeZ disables MODE Z compression with  error handling
func disableModeZ(fc *FTPConnection) error {
	if fc.rawConn == nil {
		// Try to restore the raw connection
		if err := restoreRawConnection(fc); err != nil {
			fc.usingCompression = false // Reset state since we can't verify
			return fmt.Errorf("raw connection unavailable and restoration failed: %v", err)
		}
	}

	if !fc.usingCompression {
		// Already in MODE S, just sync state to be sure
		if err := syncModeZState(fc); err != nil {
			fmt.Printf("Warning: Failed to sync compression state: %v\n", err)
		}
		return nil
	}

	// Send MODE S command
	resp, err := sendRawCommand(fc, "MODE S")
	if err != nil {
		// Even if command fails, reset our state since we can't maintain compression
		fc.usingCompression = false
		return fmt.Errorf("failed to disable MODE Z: %v", err)
	}

	if !strings.HasPrefix(resp, "200") {
		// Even if server rejects, reset our state to avoid inconsistency
		fc.usingCompression = false
		return fmt.Errorf("server rejected MODE S: %s", resp)
	}

	fc.usingCompression = false
	return nil
}

// verifyConnectionState ensures both raw and main connections are in sync
func verifyConnectionState(fc *FTPConnection) error {
	if fc.client == nil {
		return fmt.Errorf("main FTP connection is not established")
	}

	if fc.rawConn == nil {
		return fmt.Errorf("raw connection is not established")
	}

	// Send NOOP on both connections to verify they're alive
	// Check main connection
	if err := fc.client.NoOp(); err != nil {
		return fmt.Errorf("main connection error: %v", err)
	}

	// Check raw connection
	resp, err := sendRawCommand(fc, "NOOP")
	if err != nil || !strings.HasPrefix(resp, "200") {
		return fmt.Errorf("raw connection error: %v, response: %s", err, resp)
	}

	return nil
}

// syncModeZState ensures the MODE Z state is consistent
func syncModeZState(fc *FTPConnection) error {
	if fc.rawConn == nil {
		fc.usingCompression = false
		return fmt.Errorf("no raw connection available")
	}

	// Check current mode
	resp, err := sendRawCommand(fc, "STAT")
	if err != nil {
		return fmt.Errorf("failed to check transfer mode: %v", err)
	}

	currentlyCompressed := strings.Contains(resp, "MODE Z")

	// Synchronize state if mismatch
	if currentlyCompressed != fc.usingCompression {
		if currentlyCompressed {
			fc.usingCompression = true
			fmt.Println("Notice: MODE Z compression state synchronized (enabled)")
		} else {
			fc.usingCompression = false
			fmt.Println("Notice: MODE Z compression state synchronized (disabled)")
		}
	}

	return nil
}

// restoreRawConnection attempts to restore the raw connection if it's lost
func restoreRawConnection(fc *FTPConnection) error {
	if fc.rawConn != nil {
		return nil // Connection is already established
	}

	addr := fmt.Sprintf("%s:%d", fc.server, fc.port)
	rawConn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to restore raw connection: %v", err)
	}

	// Skip server greeting
	reader := bufio.NewReader(rawConn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			rawConn.Close()
			return fmt.Errorf("failed to read server greeting: %v", err)
		}
		if len(line) > 3 && line[3] == ' ' {
			break
		}
	}

	// Re-authenticate
	fmt.Fprintf(rawConn, "USER %s\r\n", fc.username)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			rawConn.Close()
			return fmt.Errorf("failed to read USER response: %v", err)
		}
		if len(line) > 3 && line[3] == ' ' {
			if !strings.HasPrefix(line, "331") {
				rawConn.Close()
				return fmt.Errorf("unexpected USER response: %s", line)
			}
			break
		}
	}

	fmt.Fprintf(rawConn, "PASS %s\r\n", fc.password)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			rawConn.Close()
			return fmt.Errorf("failed to read PASS response: %v", err)
		}
		if len(line) > 3 && line[3] == ' ' {
			if !strings.HasPrefix(line, "230") {
				rawConn.Close()
				return fmt.Errorf("login failed: %s", line)
			}
			break
		}
	}

	fc.rawConn = rawConn

	// Check if previous mode was Z and restore it if needed
	if fc.usingCompression {
		fmt.Println("Attempting to restore MODE Z compression...")
		resp, err := sendRawCommand(fc, "MODE Z")
		if err != nil || !strings.HasPrefix(resp, "200") {
			fc.usingCompression = false
			fmt.Printf("Failed to restore compression: %v, %s\n", err, resp)
		}
	}

	return nil
}

// MeasureRTT adds a new RTT measurement and updates statistics
func (nm *NetworkMetrics) MeasureRTT(rtt time.Duration) {
	nm.metricsMutex.Lock()
	defer nm.metricsMutex.Unlock()

	// Add to history
	nm.RTTHistory = append(nm.RTTHistory, rtt)
	if len(nm.RTTHistory) > nm.MaxRTTHistory {
		// Remove oldest entry
		nm.RTTHistory = nm.RTTHistory[1:]
	}

	// Update current RTT
	nm.RTT = rtt

	// Update min/max
	if rtt < nm.MinRTT {
		nm.MinRTT = rtt
	}
	if rtt > nm.MaxRTT {
		nm.MaxRTT = rtt
	}

	// Compute average
	var sum time.Duration
	for _, r := range nm.RTTHistory {
		sum += r
	}
	nm.AvgRTT = sum / time.Duration(len(nm.RTTHistory))

	// Update last measurement time
	nm.LastMeasurement = time.Now()

	// Assess network condition based on RTT
	nm.assessNetworkCondition()
}

// MeasureThroughput adds a new throughput measurement and updates statistics
func (nm *NetworkMetrics) MeasureThroughput(bytesPerSecond float64) {
	nm.metricsMutex.Lock()
	defer nm.metricsMutex.Unlock()

	// Add to history
	nm.ThroughputHistory = append(nm.ThroughputHistory, bytesPerSecond)
	if len(nm.ThroughputHistory) > nm.MaxThroughputHistory {
		// Remove oldest entry
		nm.ThroughputHistory = nm.ThroughputHistory[1:]
	}

	// Update current throughput
	nm.Throughput = bytesPerSecond

	// Update min/max
	if bytesPerSecond < nm.MinThroughput || nm.MinThroughput == 0 {
		nm.MinThroughput = bytesPerSecond
	}
	if bytesPerSecond > nm.MaxThroughput {
		nm.MaxThroughput = bytesPerSecond
	}

	// Compute average
	var sum float64
	for _, t := range nm.ThroughputHistory {
		sum += t
	}
	nm.AvgThroughput = sum / float64(len(nm.ThroughputHistory))

	// Update last measurement time
	nm.LastMeasurement = time.Now()

	// Assess network condition based on throughput
	nm.assessNetworkCondition()
}

// assessNetworkCondition evaluates network conditions based on RTT and throughput
func (nm *NetworkMetrics) assessNetworkCondition() {
	// Simplified condition assessment

	// Start with assumption of fair condition
	condition := NetworkFair

	// Assess based on RTT if we have measurements
	if len(nm.RTTHistory) > 0 {
		avgRTT := nm.AvgRTT.Milliseconds()

		if avgRTT < 50 {
			condition = NetworkExcellent
		} else if avgRTT < 150 {
			condition = NetworkGood
		} else if avgRTT < 300 {
			condition = NetworkFair
		} else {
			condition = NetworkPoor
		}
	}

	// Consider throughput to potentially downgrade assessment
	if len(nm.ThroughputHistory) > 0 {
		// Convert to MB/s for easier thresholds
		avgThroughputMBps := nm.AvgThroughput / (1024 * 1024)

		// Downgrade based on throughput if appropriate
		if avgThroughputMBps < 0.2 && condition != NetworkPoor {
			condition = NetworkPoor
		} else if avgThroughputMBps < 1.0 && condition == NetworkExcellent {
			condition = NetworkGood
		} else if avgThroughputMBps < 0.5 && condition == NetworkGood {
			condition = NetworkFair
		}
	}

	nm.NetworkCondition = condition
}

// GetOptimalWindowSize returns the optimal window size based on network conditions
func (tw *TransferWindow) GetOptimalWindowSize() int {
	tw.metrics.metricsMutex.Lock()
	defer tw.metrics.metricsMutex.Unlock()

	// Default to current window size if not enough data
	if len(tw.metrics.RTTHistory) < 3 || len(tw.metrics.ThroughputHistory) < 3 {
		return tw.windowSize
	}

	// Calculate optimal window size based on bandwidth-delay product (BDP)
	// BDP = bandwidth (bytes/sec) * RTT (sec)
	rttSec := tw.metrics.AvgRTT.Seconds()
	bdp := tw.metrics.AvgThroughput * rttSec

	// Convert BDP to number of connections needed
	// Each connection can handle approximately bufferSize bytes effectively
	optimalConnections := int(math.Ceil(bdp / float64(tw.bufferSize)))

	// Apply condition-based adjustments
	switch tw.metrics.NetworkCondition {
	case NetworkExcellent:
		// Can use more connections when network is excellent
		optimalConnections = int(float64(optimalConnections) * 1.25)
	case NetworkGood:
		// No adjustment for good network
	case NetworkFair:
		// Reduce connections slightly for fair network
		optimalConnections = int(float64(optimalConnections) * 0.8)
	case NetworkPoor:
		// Reduce connections significantly for poor network
		optimalConnections = int(float64(optimalConnections) * 0.5)
	}

	// Ensure within min/max bounds
	if optimalConnections < tw.minWindowSize {
		optimalConnections = tw.minWindowSize
	} else if optimalConnections > tw.maxWindowSize {
		optimalConnections = tw.maxWindowSize
	}

	return optimalConnections
}

// AdjustWindowSize updates the window size based on network metrics
func (tw *TransferWindow) AdjustWindowSize() {
	// Only adjust if adaptive mode is enabled and enough time has passed
	if !tw.adaptiveEnabled || time.Since(tw.lastAdjustment) < tw.adjustInterval {
		return
	}

	tw.transferMutex.Lock()
	defer tw.transferMutex.Unlock()

	// Get optimal window size based on network conditions
	optimalSize := tw.GetOptimalWindowSize()

	// Only adjust if there's a significant difference
	if optimalSize != tw.windowSize {
		oldSize := tw.windowSize
		tw.windowSize = optimalSize
		tw.lastAdjustment = time.Now()

		// Log the adjustment if significant
		if float64(optimalSize)/float64(oldSize) < 0.8 || float64(optimalSize)/float64(oldSize) > 1.2 {
			fmt.Printf("\nAdjusted transfer window: %d → %d connections (Network: %s)\n",
				oldSize, optimalSize, tw.metrics.NetworkCondition)
		}
	}
}

// acquireTransfer attempts to acquire a transfer slot from the window
// Returns true if successful, false if window is full

// MonitorTransferPerformance updates metrics based on the latest transfer performance
func (tw *TransferWindow) MonitorTransferPerformance(bytesSent int64, duration time.Duration) {
	// Skip if duration is too small to avoid division by zero or inaccurate measurements
	if duration < 100*time.Millisecond {
		return
	}

	// Calculate throughput in bytes per second
	throughput := float64(bytesSent) / duration.Seconds()

	// Update metrics
	tw.metrics.MeasureThroughput(throughput)

	// Update total bytes transferred and transfer time statistics
	atomic.AddInt64(&tw.totalBytesTransferred, bytesSent)
	tw.totalTransferTime += duration
}

// CalculateRetryDelay computes exponential backoff with jitter for retries
func (tw *TransferWindow) CalculateRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	// Calculate delay with exponential backoff: baseDelay * 2^attempt
	delay := tw.baseRetryDelay * time.Duration(1<<uint(attempt-1))

	// Add jitter (±20%)
	jitter := float64(delay) * (0.8 + rand.Float64()*0.4)

	// Cap at maximum delay
	if delay > tw.maxRetryDelay {
		delay = tw.maxRetryDelay
	}

	return time.Duration(jitter)
}

// UpdateConnectionHealth updates the health score for a connection
func (tw *TransferWindow) UpdateConnectionHealth(connID string, success bool, responseTime time.Duration) {
	tw.healthMutex.Lock()
	defer tw.healthMutex.Unlock()

	currentHealth, exists := tw.connectionHealth[connID]
	if !exists {
		// Initialize new connections with moderate health
		currentHealth = 0.7
	}

	// Update health based on success/failure and response time
	if success {
		// Increase health on success, with faster improvement for quick responses
		responseScore := 0.0
		if responseTime < 100*time.Millisecond {
			responseScore = 0.15
		} else if responseTime < 500*time.Millisecond {
			responseScore = 0.1
		} else if responseTime < 1*time.Second {
			responseScore = 0.05
		} else {
			responseScore = 0.03
		}

		// Success improves health
		currentHealth = math.Min(1.0, currentHealth+responseScore)
	} else {
		// Failure significantly decreases health
		currentHealth = math.Max(0.0, currentHealth-0.3)
	}

	tw.connectionHealth[connID] = currentHealth
}

// GetConnectionHealth returns the health score for a connection
func (tw *TransferWindow) GetConnectionHealth(connID string) float64 {
	tw.healthMutex.Lock()
	defer tw.healthMutex.Unlock()

	health, exists := tw.connectionHealth[connID]
	if !exists {
		return 0.7 // Default health for unknown connections
	}
	return health
}

// IsConnectionHealthy checks if a connection is healthy enough to use
func (tw *TransferWindow) IsConnectionHealthy(connID string) bool {
	return tw.GetConnectionHealth(connID) >= 0.3
}

// SaveSessionState stores information about the current session for restoration
func (tw *TransferWindow) SaveSessionState(key string, value interface{}) {
	tw.sessionMutex.Lock()
	defer tw.sessionMutex.Unlock()

	tw.sessionInfo[key] = value
}

// GetSessionState retrieves stored session information
func (tw *TransferWindow) GetSessionState(key string) (interface{}, bool) {
	tw.sessionMutex.Lock()
	defer tw.sessionMutex.Unlock()

	value, exists := tw.sessionInfo[key]
	return value, exists
}

// GenerateConnID creates a unique identifier for a connection
func GenerateConnID(server, username string, port int) string {
	return fmt.Sprintf("%s@%s:%d", username, server, port)
}

// CalculateChecksum computes a checksum for a block of data
func (tw *TransferWindow) CalculateChecksum(data []byte) string {
	switch tw.checksumAlgorithm {
	case "md5":
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:])
	case "crc32":
		fallthrough
	default:
		checksum := crc32.ChecksumIEEE(data)
		return fmt.Sprintf("%08x", checksum)
	}
}

// VerifyChecksum checks if the provided data matches the expected checksum
func (tw *TransferWindow) VerifyChecksum(data []byte, expectedChecksum string) bool {
	actualChecksum := tw.CalculateChecksum(data)
	return actualChecksum == expectedChecksum
}

// uploadFile handles the file upload process using the transfer package
func uploadFile(localFile, remotePath string) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		fmt.Println("Not connected to FTP server")
		return
	}

	targetRemotePath := remotePath
	if targetRemotePath == "" {
		targetRemotePath = filepath.Base(localFile)
	}

	// Pre-check for lock using SITE CHECKLOCK (TEMPORARILY DISABLED)
	if !disableSiteCHECKLOCKCommand {
		for {
			resp, err := sendRawCommand(&ftpConn, fmt.Sprintf("SITE CHECKLOCK %s", targetRemotePath))
			if err != nil {
				fmt.Printf("Error checking file lock: %v\n", err)
				return
			}
			if strings.HasPrefix(resp, "200") {
				break // Not locked, proceed
			}
			if strings.HasPrefix(resp, "450") {
				fmt.Printf("\r[WAIT] %s\n", strings.TrimSpace(resp))
				time.Sleep(1 * time.Second)
				continue
			}
			fmt.Printf("Unexpected response during lock check: %s\n", resp)
			return
		}
		fmt.Print("\r                \r") // Clear wait message
	} else {
		fmt.Println("[DEBUG] SITE CHECKLOCK command disabled - skipping file lock check")
	}

	// Retrieve file size
	fileInfo, err := os.Stat(localFile)
	if err != nil {
		fmt.Printf("Error retrieving file size: %v\n", err)
		return
	}

	// Send ALLO command with file size to server
	allocCmd := fmt.Sprintf("ALLO %d", fileInfo.Size())
	response, err := sendRawCommand(&ftpConn, allocCmd)
	if err != nil {
		fmt.Printf("Error sending ALLO command: %v\n", err)
		return
	}
	// Check if server accepted the command (200 or 202 are acceptable)
	if !strings.HasPrefix(response, "200") && !strings.HasPrefix(response, "202") {
		fmt.Printf("Warning: Server response to ALLO: %s\n", strings.TrimSpace(response))
	}

	// Sync server mode based on client ioMode setting
	if err := syncTransferModeWithServer(ioMode); err != nil {
		fmt.Printf("⚠️  Warning: Failed to sync transfer mode with server: %v\n", err)
		fmt.Println("Server may not support the selected mode. Upload will continue with client-side mode only.")
	} else {
		fmt.Printf("✅ Server synchronized to %s mode\n", getIOModeString())
	}

	// Attempt file upload using mode-based selection
	err = transfer.UploadFileWithMode(ftpConn.client, localFile, remotePath, ioMode)
	if err != nil {
		fmt.Printf("Error uploading file: %v\n", err)
		return
	}

}

// downloadFile handles the file download process
func downloadFile(remoteFile, localPath string) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		fmt.Println("Not connected to FTP server")
		return
	}

	// For parallel download mode, create FTP config and use parallel-specific function
	if ioMode == "mthread" {
		// Create FTP connection configuration for parallel downloads
		cfg := &config.FTPLoginConfig{
			Address:  fmt.Sprintf("%s:%d", ftpConn.server, ftpConn.port),
			Username: ftpConn.username,
			Password: ftpConn.password,
			Timeout:  60 * time.Second,
		}
		
		// Use parallel download with proper configuration
		err := transfer.DownloadFileParallelWithConfig(ftpConn.client, remoteFile, localPath, cfg)
		if err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
			return
		}
	} else {
		// Use regular single-connection download
		err := transfer.DownloadFileWithMode(ftpConn.client, remoteFile, localPath, ioMode)
		if err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
			return
		}
	}
}

// RetryWithExponentialBackoff executes a function with exponential backoff retries
func (tw *TransferWindow) RetryWithExponentialBackoff(operation string, fn func() error) error {
	var err error

	for attempt := 1; attempt <= tw.maxRetries; attempt++ {
		err = fn()
		if err == nil {
			// Success
			return nil
		}

		// Check if we should retry
		if attempt < tw.maxRetries {
			delay := tw.CalculateRetryDelay(attempt)

			if attempt > 1 {
				fmt.Printf("\nRetry %d/%d for %s after %v: %v\n",
					attempt, tw.maxRetries, operation, delay, err)
			}

			time.Sleep(delay)
		}
	}

	return fmt.Errorf("operation %s failed after %d attempts: %v",
		operation, tw.maxRetries, err)
}

// getIOModeString returns a string representation of the current I/O mode
func getIOModeString() string {
	switch ioMode {
	case "mmap":
		return "Memory-mapped"
	case "mthread":
		return "Multi-threaded parallel"
	case "chunked":
		fallthrough
	default:
		return "Chunked"
	}
}

// listFTPDirectory formats and displays FTP directory contents
func listFTPDirectory(path string) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		fmt.Println("Not connected to FTP server.")
		return
	}

	// Use current directory if path is empty
	if path == "" {
		path = ftpConn.currentRemoteDir
	}

	// List directory contents
	entries, err := ftpConn.client.List(path)
	if err != nil {
		fmt.Printf("Error listing directory: %v\n", err)
		return
	}

	// Update autocomplete cache
	var files []string
	var dirs []string
	for _, entry := range entries {
		if entry.Type == ftp.EntryTypeFolder {
			dirs = append(dirs, entry.Name)
		} else {
			files = append(files, entry.Name)
		}
	}
	commandCompleter.UpdateRemoteFiles(files, dirs)

	// Format and display the listing
	if err := tableFormatter.FormatFTPDirectory(entries); err != nil {
		fmt.Printf("Error formatting directory listing: %v\n", err)
	}
}

// changeFTPDirectory changes the current FTP directory
func changeFTPDirectory(path string) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		themeManager.GetErrorColor().Println("Not connected to FTP server.")
		return
	}

	var newPath string
	if path == ".." {
		// Go up one directory
		lastSlash := strings.LastIndex(ftpConn.currentRemoteDir, "/")
		if lastSlash > 0 {
			newPath = ftpConn.currentRemoteDir[:lastSlash]
		} else {
			newPath = "/"
		}
	} else if strings.HasPrefix(path, "/") {
		// Absolute path
		newPath = path
	} else {
		// Relative path
		newPath = ftpConn.currentRemoteDir
		if !strings.HasSuffix(newPath, "/") {
			newPath += "/"
		}
		newPath += path
	}

	err := ftpConn.client.ChangeDir(newPath)
	if err != nil {
		themeManager.GetErrorColor().Printf("Failed to change directory: %v\n", err)
		return
	}

	ftpConn.currentRemoteDir = newPath
	themeManager.GetSuccessColor().Printf("FTP directory changed to: %s\n", newPath)
}

// readPasswordWithAsterisks reads password input and displays asterisks
func readPasswordWithAsterisks() (string, error) {
	var password []byte
	for {
		char, _, err := getChar()
		if err != nil {
			return "", err
		}

		// Handle different key presses
		switch char {
		case '\r', '\n': // Enter key
			return string(password), nil
		case '\b', 127: // Backspace key
			if len(password) > 0 {
				password = password[:len(password)-1]
				fmt.Print("\b \b") // Move back, print space, move back again
			}
		case 3: // Ctrl+C
			fmt.Println()
			return "", fmt.Errorf("interrupted")
		default:
			if char >= 32 && char <= 126 { // Printable characters
				password = append(password, char)
				fmt.Print("*")
			}
		}
	}
}

// getChar reads a single character from stdin (Windows-specific implementation)

// getCharWindows reads a character on Windows without echo using _getch
func getChar() (byte, byte, error) {
	// Use msvcrt _getch which doesn't echo
	msvcrt := syscall.NewLazyDLL("msvcrt.dll")
	getch := msvcrt.NewProc("_getch")

	ret, _, err := getch.Call()
	if ret == 0 {
		return 0, 0, err
	}

	return byte(ret), 0, nil
}

// Global variables for console echo control
var (
	consoleHandle  uintptr
	originalMode   uint32
	echoDisabled   bool
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	getStdHandle   = kernel32.NewProc("GetStdHandle")
	getConsoleMode = kernel32.NewProc("GetConsoleMode")
	setConsoleMode = kernel32.NewProc("SetConsoleMode")
)

// disableConsoleEcho disables console echo on Windows
func disableConsoleEcho() {
	if runtime.GOOS != "windows" {
		return
	}

	if echoDisabled {
		return // Already disabled
	}

	const STD_INPUT_HANDLE = ^uintptr(10 - 1) // -10 as uintptr
	handle, _, _ := getStdHandle.Call(STD_INPUT_HANDLE)
	consoleHandle = handle

	// Get current console mode
	getConsoleMode.Call(handle, uintptr(unsafe.Pointer(&originalMode)))

	// Disable echo and line input mode
	const ENABLE_ECHO_INPUT = 0x0004
	const ENABLE_LINE_INPUT = 0x0002
	newMode := originalMode &^ (ENABLE_ECHO_INPUT | ENABLE_LINE_INPUT)

	// Set new console mode
	setConsoleMode.Call(handle, uintptr(newMode))
	echoDisabled = true
}

// enableConsoleEcho restores console echo on Windows
func enableConsoleEcho() {
	if runtime.GOOS != "windows" {
		return
	}

	if !echoDisabled {
		return // Already enabled
	}

	// Restore original console mode
	setConsoleMode.Call(consoleHandle, uintptr(originalMode))
	echoDisabled = false
}

// sendSiteCommand sends a SITE command to the FTP server and displays the response
func sendSiteCommand(siteArgs string) {
	if atomic.LoadInt32(&ftpConn.isConnected) == 0 {
		fmt.Println("Not connected to FTP server.")
		return
	}

	if ftpConn.rawConn == nil {
		fmt.Println("Raw connection not available for SITE commands.")
		return
	}

	// Send the complete SITE command
	siteCommand := fmt.Sprintf("SITE %s", siteArgs)
	fmt.Printf("Sending: %s\n", siteCommand)

	// Send command and get response
	response, err := sendRawCommand(&ftpConn, siteCommand)
	if err != nil {
		fmt.Printf("Error sending SITE command: %v\n", err)
		return
	}

	// Display the server response
	fmt.Print(response)
}

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

// syncTransferModeWithServer synchronizes the transfer mode between client and server
func syncTransferModeWithServer(mode string) error {
	if ftpConn.rawConn == nil {
		return fmt.Errorf("raw connection not available")
	}

	var siteCommand string
	var modeDescription string

	// Map client modes to server SITE commands
	switch mode {
	case "mmap":
		siteCommand = "SITE MODE MMAP"
		modeDescription = "Memory-mapped"
	case "mthread":
		siteCommand = "SITE MODE PARALLEL" 
		modeDescription = "Multi-threaded parallel"
	case "chunked":
		fallthrough
	default:
		siteCommand = "SITE MODE CHUNKED"
		modeDescription = "Chunked"
	}

	// Send the appropriate SITE MODE command to server
	response, err := sendRawCommand(&ftpConn, siteCommand)
	if err != nil {
		return fmt.Errorf("failed to send %s command: %v", siteCommand, err)
	}

	// Display the server's sync message
	if strings.HasPrefix(response, "200") {
		// Extract and display the server message
		lines := strings.Split(strings.TrimSpace(response), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "200 ") {
				serverMessage := strings.TrimPrefix(line, "200 ")
				fmt.Printf("🔄 Server: %s\n", serverMessage)
				break
			}
		}
		return nil
	} else if strings.HasPrefix(response, "502") {
		return fmt.Errorf("server does not support %s mode", modeDescription)
	} else {
		return fmt.Errorf("server rejected %s mode: %s", modeDescription, strings.TrimSpace(response))
	}
}
