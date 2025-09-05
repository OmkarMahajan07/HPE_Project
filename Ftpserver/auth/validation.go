package auth

import "strings"

// IsValidUsername checks if username contains only allowed characters
func IsValidUsername(username string) bool {
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

// IsValidPassword checks password strength
func IsValidPassword(password string) bool {
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

// IsValidDirectory checks if directory path is valid and safe
func IsValidDirectory(dir string) bool {
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

// GetDefaultPermissions returns default permissions for different user types
func GetDefaultPermissions(userType string) UserPermissions {
	switch strings.ToLower(userType) {
	case "admin":
		return UserPermissions{
			Read: true, Write: true, Delete: true, Create: true,
			List: true, Rename: true, Resume: true, Compress: true, Parallel: true,
		}
	case "user":
		return UserPermissions{
			Read: true, Write: true, Delete: false, Create: true,
			List: true, Rename: false, Resume: true, Compress: true, Parallel: true,
		}
	case "anonymous":
		return UserPermissions{
			Read: true, Write: false, Delete: false, Create: false,
			List: true, Rename: false, Resume: true, Compress: true, Parallel: false,
		}
	default:
		return UserPermissions{
			Read: true, Write: false, Delete: false, Create: false,
			List: true, Rename: false, Resume: true, Compress: false, Parallel: false,
		}
	}
}
