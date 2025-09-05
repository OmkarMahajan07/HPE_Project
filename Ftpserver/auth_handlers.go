package main

import (
	"fmt"
)

// Authentication commands using the new auth module
func (session *ClientSession) handleUSER(username string) {
	session.username = username
	if session.server.authService.ValidateUser(username) {
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

	authResult, err := session.server.authService.AuthenticateUser(session.username, password)
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
}

func (session *ClientSession) handleQUIT() {
	// Decrement user connection count before disconnecting
	if session.authenticated && session.userProfile != nil {
		session.server.authService.DecrementUserConnection(session.userProfile)
		session.logger.Printf("User %s disconnected, active connections: %d",
			session.username, session.userProfile.ActiveConnections)
	}

	session.sendResponse(221, "Goodbye")
	session.controlConn.Close()
}
