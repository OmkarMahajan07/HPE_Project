package auth

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// SiteCommandHandler handles SITE commands for user management
type SiteCommandHandler struct {
	userManager *UserManager
	rootDir     string
}

// NewSiteCommandHandler creates a new site command handler
func NewSiteCommandHandler(userManager *UserManager, rootDir string) *SiteCommandHandler {
	return &SiteCommandHandler{
		userManager: userManager,
		rootDir:     rootDir,
	}
}

// HandleAddUser handles SITE ADDUSER command
func (h *SiteCommandHandler) HandleAddUser(requestingUser string, args string) (string, error) {
	if requestingUser != "admin" {
		return "", fmt.Errorf("access denied. Administrator privileges required")
	}

	parts := strings.Fields(args)
	if len(parts) < 3 {
		return "", fmt.Errorf("syntax: SITE ADDUSER <username> <password> <homeDir> [permissions]")
	}

	username := parts[0]
	password := parts[1]
	homeDir := parts[2]

	// Validate inputs
	if !IsValidUsername(username) {
		return "", fmt.Errorf("invalid username. Use alphanumeric characters and underscores only")
	}

	if !IsValidPassword(password) {
		return "", fmt.Errorf("password too weak. Must be at least 6 characters with letters and numbers")
	}

	if !IsValidDirectory(homeDir) {
		return "", fmt.Errorf("invalid home directory path")
	}

	// Parse permissions if provided
	permissions := GetDefaultPermissions("user")
	if len(parts) >= 4 {
		permissions = ParsePermissions(parts[3])
	}

	// Add user
	_, err := h.userManager.AddUser(username, password, homeDir, permissions)
	if err != nil {
		return "", err
	}

	// Create home directory
	fullHomeDir := filepath.Join(h.rootDir, homeDir)
	fullHomeDir = filepath.FromSlash(fullHomeDir)
	if err := os.MkdirAll(fullHomeDir, 0755); err != nil {
		log.Printf("Warning: failed to create home directory for user %s: %v", username, err)
	}

	return fmt.Sprintf("User '%s' created successfully with home directory '%s' and saved to persistent storage", username, homeDir), nil
}

// HandleDelUser handles SITE DELUSER command
func (h *SiteCommandHandler) HandleDelUser(requestingUser string, username string) (string, error) {
	if requestingUser != "admin" {
		return "", fmt.Errorf("access denied. Administrator privileges required")
	}

	if username == "admin" {
		return "", fmt.Errorf("cannot delete admin user")
	}

	if !IsValidUsername(username) {
		return "", fmt.Errorf("invalid username")
	}

	userProfile, exists := h.userManager.GetUser(username)
	if !exists {
		return "", fmt.Errorf("user not found")
	}

	// Check if user has active connections
	if atomic.LoadInt32(&userProfile.ActiveConnections) > 0 {
		return "", fmt.Errorf("cannot delete user with active connections")
	}

	if err := h.userManager.DeleteUser(username); err != nil {
		return "", err
	}

	return fmt.Sprintf("User '%s' deleted successfully and removed from persistent storage", username), nil
}

// HandleListUsers handles SITE LISTUSERS command
func (h *SiteCommandHandler) HandleListUsers(requestingUser string) (string, error) {
	if requestingUser != "admin" {
		return "", fmt.Errorf("access denied. Administrator privileges required")
	}

	users := h.userManager.ListUsers()
	
	userList := "211-User List:\r\n"
	for _, profile := range users {
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
			profile.Username, statusStr, activeConns, profile.MaxConnections, profile.HomeDir, lastLogin)
	}
	userList += "211 End of user list\r\n"

	return userList, nil
}

// HandleUserInfo handles SITE USERINFO command
func (h *SiteCommandHandler) HandleUserInfo(requestingUser string, username string) (string, error) {
	if requestingUser != "admin" {
		return "", fmt.Errorf("access denied. Administrator privileges required")
	}

	userProfile, exists := h.userManager.GetUser(username)
	if !exists {
		return "", fmt.Errorf("user not found")
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

	return userInfo, nil
}
