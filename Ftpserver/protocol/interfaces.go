package protocol

import (
	"net"
	"os"
	"ftpserver/auth"
	"ftpserver/transfer"
)

type SessionInterface interface {
	// Core session operations
	SendResponse(code int, message string)
	LogPrintf(format string, args ...interface{})

	// Session state
	GetCurrentDir() string
	SetCurrentDir(dir string)
	IsAuthenticated() bool
	GetUsername() string
	GetUserProfile() *auth.UserProfile

	// Server context
	GetServerRootDir() string
	ResolvePath(path string) string
	GetFullSystemPath(path string) string

	// Transfer state
	GetRestartPos() int64
	SetRestartPos(pos int64)
	GetMode() string
	SetMode(mode string)
	GetTransferType() string
	SetTransferType(transferType string)
	GetTransferMode() string
	SetTransferMode(mode string)
	GetExpectedUploadSize() int64
	SetExpectedUploadSize(size int64)
	GetUtf8Enabled() bool
	SetUtf8Enabled(enabled bool)
	GetRenameFrom() string
	SetRenameFrom(path string)

	// Data connections
	OpenDataConnection() (net.Conn, error)
	CloseDataConnection()

	// File locking (for your advanced features)
	LockFileRangeWithNotification(file *os.File, exclusive bool) (*transfer.LockResult, *transfer.TimedFileLocker, error)

	// Advanced transfer features (for your optimizations)
	GetClientIP() string
	GetServer() *FTPServer
}

