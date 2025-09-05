package config

import "time"

// FTPLoginConfig holds FTP connection credentials and settings.
type FTPLoginConfig struct {
	Address  string // Example: "ftp.gnu.org:21"
	Username string
	Password string
	Timeout  time.Duration
}
