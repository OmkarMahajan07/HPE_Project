package protocol

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Advanced & Miscellaneous Commands

func (h *CommandHandler) HandleFEAT() {
	response := "211-Features:\r\n"
	response += " SIZE\r\n"
	response += " MDTM\r\n"
	response += " REST STREAM\r\n"
	response += "211 End\r\n"
	h.session.SendResponse(211, response)
}

func (h *CommandHandler) HandleOPTS(args string) {
	if args == "" {
		h.session.SendResponse(501, "OPTS command requires arguments")
		return
	}
	switch args {
	case "UTF8 ON":
		// Enable UTF8 (implementation needed in session)
		h.session.SendResponse(200, "UTF8 set to on")
	case "UTF8 OFF":
		// Disable UTF8 (implementation needed in session)
		h.session.SendResponse(200, "UTF8 set to off")
	default:
		h.session.SendResponse(501, "Unsupported option")
	}
}

func (h *CommandHandler) HandleREST(position string) {
	if position == "" {
		h.session.SendResponse(501, "Syntax error in parameters")
		return
	}
	// Parse and set restart position (implementation needed in session)
	// This will need to be stored in the session
	h.session.SendResponse(350, "Restarting at position ")
}

func (h *CommandHandler) HandleSTAT(args string) {
	if args == "" {
		h.session.SendResponse(211, "FTP Server Status OK")
	} else {
		// Check file status
		// Needs implementation in session
		h.session.SendResponse(211, "File status OK")
	}
}

func (h *CommandHandler) HandleHELP(args string) {
	response := "214-The following commands are recognized:\r\n"
	response += " USER PASS QUIT SYST TYPE MODE PWD CWD LIST\r\n"
	response += " PASV PORT RETR STOR APPE DELE RNFR RNTO\r\n"
	response += " MKD RMD CDUP SIZE MDTM REST STAT FEAT\r\n"
	response += " OPTS STRU ALLO ABOR NOOP HELP\r\n"
	response += "214 Help OK\r\n"
	h.session.SendResponse(214, response)
}

func (h *CommandHandler) HandleABOR() {
	// Abort operation (implementation needed in session)
	// Close any ongoing transfer
	h.session.SendResponse(226, "Abort successful")
}

func (h *CommandHandler) HandleSTRU(structure string) {
	if structure == "F" {
		h.session.SendResponse(200, "Structure set to File")
	} else {
		h.session.SendResponse(501, "Unsupported structure type")
	}
}

func (h *CommandHandler) HandleALLO(args string) {
	// Allocate storage command is typically ignored
	h.session.SendResponse(202, "ALLO command ignored")
}

func (h *CommandHandler) HandleAPPE(filename string) {
	h.withAuth(func() {
		if filename == "" {
			h.session.SendResponse(501, "Syntax error in parameters")
			return
		}
		filePath := h.session.ResolvePath(filename)
		fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)
		dataConn, err := h.session.OpenDataConnection()
		if err != nil {
			h.session.SendResponse(425, "Can't open data connection")
			return
		}
		defer h.session.CloseDataConnection()
		
		h.session.SendResponse(150, fmt.Sprintf("Opening data connection for append to %s", filename))
		
		file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			h.session.SendResponse(550, "Could not open file")
			return
		}
		defer file.Close()
		
		_, err = io.Copy(file, dataConn)
		if err != nil {
			h.session.SendResponse(426, "Connection closed; transfer aborted")
			return
		}
		
		h.session.SendResponse(226, "Append complete")
	})
}

// handleSIZE - Get file size
func (h *CommandHandler) HandleSIZE(filename string) {
	h.withAuth(func() {
		if filename == "" {
			h.session.SendResponse(501, "Syntax error in parameters")
			return
		}
		
		filePath := h.session.ResolvePath(filename)
		fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)
		
		fileInfo, err := os.Stat(fullPath)
		if err != nil {
			h.session.SendResponse(550, "File not found")
			return
		}
		
		if fileInfo.IsDir() {
			h.session.SendResponse(550, "Is a directory")
			return
		}
		
		h.session.SendResponse(213, fmt.Sprintf("%d", fileInfo.Size()))
	})
}

// handleMDTM - Get file modification time
func (h *CommandHandler) HandleMDTM(filename string) {
	h.withAuth(func() {
		if filename == "" {
			h.session.SendResponse(501, "Syntax error in parameters")
			return
		}
		
		filePath := h.session.ResolvePath(filename)
		fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)
		
		fileInfo, err := os.Stat(fullPath)
		if err != nil {
			h.session.SendResponse(550, "File not found")
			return
		}
		
		if fileInfo.IsDir() {
			h.session.SendResponse(550, "Is a directory")
			return
		}
		
		// Format: YYYYMMDDHHMMSS
		modTime := fileInfo.ModTime().UTC().Format("20060102150405")
		h.session.SendResponse(213, modTime)
	})
}

// handleAUTH - Authentication command (typically for TLS)
func (h *CommandHandler) HandleAUTH(args string) {
	h.session.SendResponse(502, "AUTH command not supported")
}
