package terminal

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// Config holds the server configuration parsed from command line
type Config struct {
	ListenPort    int
	DataPortStart int
	DataPortEnd   int
	RootDir       string
}

// DefaultConfig returns the default server configuration
func DefaultConfig() *Config {
	return &Config{
		ListenPort:    2121,
		DataPortStart: 20000,
		DataPortEnd:   21000,
		RootDir:       ".",
	}
}

// ParseCommandLineArgs parses command line arguments and returns server configuration
func ParseCommandLineArgs() (*Config, error) {
	config := DefaultConfig()

	// Parse command line arguments if provided
	if len(os.Args) > 1 {
		if port, err := strconv.Atoi(os.Args[1]); err == nil {
			config.ListenPort = port
		} else {
			return nil, fmt.Errorf("invalid port number: %s", os.Args[1])
		}
	}

	if len(os.Args) > 2 {
		config.RootDir = os.Args[2]
	}

	return config, nil
}

// ValidateConfig validates the parsed configuration
func ValidateConfig(config *Config) error {
	// Ensure root directory exists
	if _, err := os.Stat(config.RootDir); os.IsNotExist(err) {
		return fmt.Errorf("root directory does not exist: %s", config.RootDir)
	}

	// Validate port range
	if config.ListenPort <= 0 || config.ListenPort > 65535 {
		return fmt.Errorf("invalid listen port: %d (must be 1-65535)", config.ListenPort)
	}

	if config.DataPortStart <= 0 || config.DataPortStart > 65535 {
		return fmt.Errorf("invalid data port start: %d (must be 1-65535)", config.DataPortStart)
	}

	if config.DataPortEnd <= 0 || config.DataPortEnd > 65535 {
		return fmt.Errorf("invalid data port end: %d (must be 1-65535)", config.DataPortEnd)
	}

	if config.DataPortStart >= config.DataPortEnd {
		return fmt.Errorf("data port start (%d) must be less than data port end (%d)", 
			config.DataPortStart, config.DataPortEnd)
	}

	return nil
}

// PrintStartupInfo prints server startup information
func PrintStartupInfo(config *Config, users []string) {
	log.Printf("Starting High-Performance FTP Server...")
	log.Printf("Listening on port: %d", config.ListenPort)
	log.Printf("Data port range: %d-%d", config.DataPortStart, config.DataPortEnd)
	log.Printf("Root directory: %s", config.RootDir)
	log.Printf("Configured users: %v", users)
}

// PrintUsage prints usage information
func PrintUsage() {
	fmt.Printf("Usage: %s [port] [root_directory]\n", os.Args[0])
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  port             Listen port for FTP server (default: 2121)")
	fmt.Println("  root_directory   Root directory for FTP server (default: current directory)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Printf("  %s                    # Default: port 2121, current directory\n", os.Args[0])
	fmt.Printf("  %s 8021               # Custom port, current directory\n", os.Args[0])
	fmt.Printf("  %s 8021 /home/ftp     # Custom port and directory\n", os.Args[0])
}

// HandleStartupError handles startup errors with appropriate logging and exit
func HandleStartupError(err error, context string) {
	log.Fatalf("Failed to %s: %v", context, err)
}

// ShowVersion displays version information
func ShowVersion() {
	fmt.Println("High-Performance FTP Server v1.0")
	fmt.Println("Built with Go")
}

// ParseFlags parses advanced command line flags (for future expansion)
func ParseFlags() (*Config, bool, error) {
	config := DefaultConfig()
	showHelp := false
	showVersion := false

	// Simple flag parsing (can be enhanced with flag package later)
	for i, arg := range os.Args[1:] {
		switch arg {
		case "-h", "--help":
			showHelp = true
		case "-v", "--version":
			showVersion = true
		default:
			// Handle positional arguments
			if i == 0 {
				if port, err := strconv.Atoi(arg); err == nil {
					config.ListenPort = port
				} else {
					return nil, false, fmt.Errorf("invalid port number: %s", arg)
				}
			} else if i == 1 {
				config.RootDir = arg
			}
		}
	}

	if showHelp {
		PrintUsage()
		return nil, true, nil
	}

	if showVersion {
		ShowVersion()
		return nil, true, nil
	}

	return config, false, nil
}
