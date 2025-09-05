package protocol

import (
	"fmt"
	"strings"
)

// Basic System Commands

// handleSYST - System type
func (h *CommandHandler) HandleSYST() {
	h.session.SendResponse(215, "UNIX Type: L8")
}

// handleTYPE - Set transfer type (ASCII/Binary)
func (h *CommandHandler) HandleTYPE(typeStr string) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	switch strings.ToUpper(typeStr) {
	case "A":
		// Set ASCII mode (implementation needed in session)
		h.session.SendResponse(200, "Switching to ASCII mode")
	case "I":
		// Set Binary mode (implementation needed in session)
		h.session.SendResponse(200, "Switching to Binary mode")
	default:
		h.session.SendResponse(504, "Command not implemented for that parameter")
	}
}

// handleMODE - Set transfer mode (Stream/Compressed/Memory-mapped)
func (h *CommandHandler) HandleMODE(modeStr string) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	switch strings.ToUpper(modeStr) {
	case "S":
		// Set Stream mode (implementation needed in session)
		h.session.SendResponse(200, "Mode set to Stream")
	case "Z":
		// Set Compressed mode (implementation needed in session)
		h.session.SendResponse(200, "Mode set to Compressed")
	case "M":
		// Set Memory-mapped mode (implementation needed in session)
		h.session.SendResponse(200, "Mode set to Memory-mapped")
	default:
		h.session.SendResponse(504, "Command not implemented for that parameter")
	}
}

// Directory Commands

// handlePWD - Print working directory
func (h *CommandHandler) HandlePWD() {
	h.withAuth(func() {
		h.session.SendResponse(257, fmt.Sprintf(`"%s" is the current directory`, h.session.GetCurrentDir()))
	})
}

// handleCWD - Change working directory
func (h *CommandHandler) HandleCWD(path string) {
	h.withAuth(func() {
		h.withValidParam(path, func() {
			// Directory change logic needs session implementation
			// This will need to be implemented in the session
			h.session.SendResponse(250, fmt.Sprintf(`CWD command successful. "%s" is current directory`, path))
		})
	})
}

// handleCDUP - Change to parent directory
func (h *CommandHandler) HandleCDUP() {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	// Navigate to parent directory
	currentDir := h.session.GetCurrentDir()
	if currentDir == "/" {
		h.session.SendResponse(250, "Already in root directory")
		return
	}

	// Go up one level (implementation needed in session)
	h.session.SendResponse(250, fmt.Sprintf(`CDUP command successful. "%s" is current directory`, currentDir))
}
