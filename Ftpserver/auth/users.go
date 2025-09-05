package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// UserManager manages user profiles and persistence
type UserManager struct {
	users    map[string]*UserProfile
	mutex    sync.RWMutex
	filePath string
}

// NewUserManager creates a new user manager
func NewUserManager(filePath string) *UserManager {
	return &UserManager{
		users:    make(map[string]*UserProfile),
		filePath: filePath,
	}
}

// GetUser retrieves a user profile by username
func (um *UserManager) GetUser(username string) (*UserProfile, bool) {
	um.mutex.RLock()
	defer um.mutex.RUnlock()

	user, exists := um.users[username]
	return user, exists
}

// AddUser adds a new user
func (um *UserManager) AddUser(username, password, homeDir string, perms UserPermissions) (*UserProfile, error) {
	um.mutex.Lock()
	defer um.mutex.Unlock()

	if _, exists := um.users[username]; exists {
		return nil, fmt.Errorf("user '%s' already exists", username)
	}

	passwordHash, err := HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %v", err)
	}

	newUser := &UserProfile{
		Username:          username,
		PasswordHash:      passwordHash,
		HomeDir:           homeDir,
		Permissions:       perms,
		MaxConnections:    10,
		BandwidthLimit:    0,
		TransferStats:     &TransferStats{},
	}

	um.users[username] = newUser

	// Save users without additional locking since we already hold the lock
	if err := um.saveUsersToFileNoLock(); err != nil {
		log.Printf("Warning: failed to save users after adding user '%s': %v", username, err)
	}

	return newUser, nil
}

// DeleteUser deletes a user
func (um *UserManager) DeleteUser(username string) error {
	um.mutex.Lock()
	defer um.mutex.Unlock()

	if _, exists := um.users[username]; !exists {
		return fmt.Errorf("user '%s' not found", username)
	}

	delete(um.users, username)

	if err := um.saveUsersToFileNoLock(); err != nil {
		return fmt.Errorf("failed to save users after deleting user '%s': %v", username, err)
	}

	return nil
}

// ListUsers lists all users
func (um *UserManager) ListUsers() []*UserProfile {
	um.mutex.RLock()
	defer um.mutex.RUnlock()

	userList := make([]*UserProfile, 0, len(um.users))
	for _, user := range um.users {
		userList = append(userList, user)
	}
	return userList
}

// saveUsersToFileNoLock saves all users to a JSON file without acquiring the lock
// This method assumes the caller already holds the appropriate lock
func (um *UserManager) saveUsersToFileNoLock() error {
	// No locking - caller must hold the lock

	userData := make(map[string]*UserProfile)
	for username, profile := range um.users {
		userCopy := &UserProfile{
			Username:       username,
			PasswordHash:   profile.PasswordHash,
			HomeDir:        profile.HomeDir,
			Permissions:    profile.Permissions,
			MaxConnections: profile.MaxConnections,
			BandwidthLimit: profile.BandwidthLimit,
		}
		userData[username] = userCopy
	}

	data, err := json.MarshalIndent(userData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal users to JSON: %v", err)
	}

	if err := os.WriteFile(um.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write users file: %v", err)
	}

	return nil
}

// SaveUsersToFile saves all users to a JSON file
func (um *UserManager) SaveUsersToFile() error {
	um.mutex.RLock()
	defer um.mutex.RUnlock()
	return um.saveUsersToFileNoLock()
}

// LoadUsersFromFile loads users from a JSON file
func (um *UserManager) LoadUsersFromFile(rootDir string) error {
	if _, err := os.Stat(um.filePath); os.IsNotExist(err) {
		log.Printf("Users file '%s' not found, will be created on first save.", um.filePath)
		return nil
	}

	data, err := os.ReadFile(um.filePath)
	if err != nil {
		return fmt.Errorf("failed to read users file: %v", err)
	}

	var userData map[string]*UserProfile
	if err := json.Unmarshal(data, &userData); err != nil {
		return fmt.Errorf("failed to parse users JSON: %v", err)
	}

um.mutex.Lock()
	defer um.mutex.Unlock()

	for username, profile := range userData {
		profile.Username = username
		profile.ActiveConnections = 0
		profile.TransferStats = &TransferStats{}
		profile.LastLogin = time.Time{}
		profile.LoginAttempts = 0
		profile.Locked = false

		um.users[username] = profile

		fullHomeDir := filepath.Join(rootDir, profile.HomeDir)
		if err := os.MkdirAll(fullHomeDir, 0755); err != nil {
			log.Printf("Warning: failed to create home directory for user '%s': %v", username, err)
		}
	}

	log.Printf("Loaded %d users from '%s'", len(um.users), um.filePath)
	return nil
}

// UserProfile contains enhanced user information
type UserProfile struct {
	Username          string          `json:"username"`
	PasswordHash      string          `json:"password"`
	HomeDir           string          `json:"homeDir"`
	Permissions       UserPermissions `json:"permissions"`
	MaxConnections    int             `json:"maxConnections"`
	ActiveConnections int32           `json:"-"`
	BandwidthLimit    int64           `json:"bandwidthLimit"`
	TransferStats     *TransferStats  `json:"-"`
	LastLogin         time.Time       `json:"-"`
	LoginAttempts     int32           `json:"-"`
	Locked            bool            `json:"-"`
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
