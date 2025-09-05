package protocol

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// handleLIST - Directory listing (YOUR ACTUAL IMPLEMENTATION)
func (h *CommandHandler) HandleLIST(path string) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}

	dataConn, err := h.session.OpenDataConnection()
	if err != nil {
		h.session.SendResponse(425, "Can't open data connection")
		return
	}
	defer h.session.CloseDataConnection()

	h.session.SendResponse(150, "Here comes the directory listing")

	// Resolve the path for listing
	listPath := h.session.GetCurrentDir()
	if path != "" {
		listPath = h.session.ResolvePath(path)
	}

	// Use the enhanced path resolution method
	fullPath := h.session.GetFullSystemPath(listPath)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		h.session.SendResponse(550, "Failed to list directory")
		return
	}

	var listing bytes.Buffer
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Format: permissions, links, owner, group, size, date, name
		perms := "-rw-r--r--"
		if info.IsDir() {
			perms = "drwxr-xr-x"
		}

		line := fmt.Sprintf("%s %3d %-8s %-8s %8d %s %s\r\n",
			perms, 1, "owner", "group", info.Size(),
			info.ModTime().Format("Jan 02 15:04"), info.Name())
		listing.WriteString(line)
	}

	// Send listing through data connection
	var writer io.Writer = dataConn
	if h.session.GetMode() == "Z" {
		zWriter := zlib.NewWriter(dataConn)
		defer zWriter.Close()
		writer = zWriter
	}

	_, err = writer.Write(listing.Bytes())
	if err != nil {
		h.session.SendResponse(426, "Connection closed; transfer aborted")
		return
	}

	h.session.SendResponse(226, "Directory send OK")
	h.session.LogPrintf("📜 [LIST] Directory listing sent for: %s", listPath)
}

// handleRETR - Retrieve file
func (h *CommandHandler) HandleRETR(filename string) {
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

		dataConn, err := h.session.OpenDataConnection()
		if err != nil {
			h.session.SendResponse(425, "Can't open data connection")
			return
		}
		defer h.session.CloseDataConnection()

		h.session.SendResponse(150, fmt.Sprintf("Opening data connection for %s (%d bytes)", filename, fileInfo.Size()))

		file, err := os.Open(fullPath)
		if err != nil {
			h.session.SendResponse(550, fmt.Sprintf("Failed to open file: %v", err))
			return
		}
		defer file.Close()

		_, err = io.Copy(dataConn, file)
		if err != nil {
			h.session.SendResponse(426, "Connection closed; transfer aborted")
			return
		}

		h.session.SendResponse(226, "Transfer complete")
	})
}

// handleSTOR - Store file
func (h *CommandHandler) HandleSTOR(filename string) {
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

		h.session.SendResponse(150, fmt.Sprintf("Opening data connection for %s", filename))

		file, err := os.Create(fullPath)
		if err != nil {
			h.session.SendResponse(550, fmt.Sprintf("Failed to open file: %v", err))
			return
		}
		defer file.Close()

		_, err = io.Copy(file, dataConn)
		if err != nil {
			h.session.SendResponse(426, "Connection closed; transfer aborted")
			return
		}

		h.session.SendResponse(226, "Transfer complete")
	})
}
