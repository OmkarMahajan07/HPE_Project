package protocol

import (
	"strconv"
	"strings"
)

// Data Connection Commands

// handlePASV - Passive mode
func (h *CommandHandler) HandlePASV() {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	// Implementation will need to be delegated to session
	// This involves creating a data listener and returning the address
	h.session.SendResponse(227, "Entering Passive Mode")
}

// handleEPSV - Extended Passive Mode
func (h *CommandHandler) HandleEPSV(protocol string) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	// For simplicity, fall back to PASV
	h.HandlePASV()
}

// handlePORT - Active mode
func (h *CommandHandler) HandlePORT(portCmd string) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	// Parse PORT command (h1,h2,h3,h4,p1,p2)
	parts := strings.Split(portCmd, ",")
	if len(parts) != 6 {
		h.session.SendResponse(501, "Syntax error in parameters")
		return
	}

	// Build IP address
	ip := strings.Join(parts[0:4], ".")

	// Calculate port
	p1, err1 := strconv.Atoi(parts[4])
	p2, err2 := strconv.Atoi(parts[5])
	if err1 != nil || err2 != nil {
		h.session.SendResponse(501, "Syntax error in parameters")
		return
	}
	port := p1*256 + p2

	// Store the active mode connection info (implementation needed in session)
	h.session.LogPrintf("PORT command: %s:%d", ip, port)
	h.session.SendResponse(200, "PORT command successful")
}
