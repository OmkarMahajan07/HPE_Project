package auth

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// AuthService handles authentication operations
type AuthService struct {
	userManager *UserManager
}

// NewAuthService creates a new authentication service
func NewAuthService(userManager *UserManager) *AuthService {
	return &AuthService{
		userManager: userManager,
	}
}

// AuthenticateUser authenticates a user with username and password
func (auth *AuthService) AuthenticateUser(username, password string) (*AuthResult, error) {
	userProfile, exists := auth.userManager.GetUser(username)
	if !exists {
		return &AuthResult{
			Success: false,
			Message: "User not found",
		}, nil
	}

	// Check if user is locked
	if userProfile.Locked {
		return &AuthResult{
			Success: false,
			Message: "Account is locked",
		}, nil
	}

	// Check connection limit
	if int(atomic.LoadInt32(&userProfile.ActiveConnections)) >= userProfile.MaxConnections {
		return &AuthResult{
			Success: false,
			Message: "Too many connections for this user",
		}, nil
	}

	// Verify password using bcrypt
	var passwordValid bool
	if username == "anonymous" {
		// Anonymous user - allow any password or empty password
		passwordValid = (userProfile.PasswordHash == "" || password != "")
	} else {
		// Regular user - compare bcrypt hash
		err := bcrypt.CompareHashAndPassword([]byte(userProfile.PasswordHash), []byte(password))
		passwordValid = (err == nil)
	}

	if passwordValid {
		// Increment active connections
		atomic.AddInt32(&userProfile.ActiveConnections, 1)

		// Update last login
		userProfile.LastLogin = time.Now()

		// Reset failed login attempts
		atomic.StoreInt32(&userProfile.LoginAttempts, 0)

		return &AuthResult{
			Success:     true,
			Message:     "Authentication successful",
			UserProfile: userProfile,
		}, nil
	} else {
		// Increment failed attempts
		attempts := atomic.AddInt32(&userProfile.LoginAttempts, 1)

		// Lock account after 5 failed attempts
		if attempts >= 5 {
			userProfile.Locked = true
		}

		return &AuthResult{
			Success: false,
			Message: "Login incorrect",
			FailedAttempts: int(attempts),
			AccountLocked: userProfile.Locked,
		}, nil
	}
}

// ValidateUser checks if a username exists
func (auth *AuthService) ValidateUser(username string) bool {
	_, exists := auth.userManager.GetUser(username)
	return exists
}

// DecrementUserConnection decrements the active connection count for a user
func (auth *AuthService) DecrementUserConnection(userProfile *UserProfile) {
	if userProfile != nil {
		atomic.AddInt32(&userProfile.ActiveConnections, -1)
	}
}

// InitializeUserSession initializes session-specific settings for a user
func (auth *AuthService) InitializeUserSession(userProfile *UserProfile, sessionCtx context.Context) *SessionConfig {
	config := &SessionConfig{
		BandwidthLimit: userProfile.BandwidthLimit,
		MaxParallel:    1, // Default
		Permissions:    userProfile.Permissions,
	}

	// Set parallel transfer limits based on user permissions
	if userProfile.Permissions.Parallel {
		config.MaxParallel = 16 // MaxConcurrentTransfers
	}

	return config
}

// AuthResult represents the result of an authentication attempt
type AuthResult struct {
	Success        bool
	Message        string
	UserProfile    *UserProfile
	FailedAttempts int
	AccountLocked  bool
}

// SessionConfig holds session-specific configuration
type SessionConfig struct {
	BandwidthLimit int64
	MaxParallel    int
	Permissions    UserPermissions
}

// HashPassword creates a bcrypt hash of a password
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// ValidatePassword verifies a password against its hash
func ValidatePassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}
