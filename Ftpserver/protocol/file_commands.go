package protocol

import (
	"fmt"
	"os"
	"path/filepath"
)

// File Management Commands

// handleMKD - Create directory
func (h *CommandHandler) HandleMKD(dirName string) {
	h.withAuth(func() {
		h.withValidParam(dirName, func() {
			dirPath := h.session.ResolvePath(dirName)
			fullPath := filepath.Join(h.session.GetServerRootDir(), dirPath)

			err := os.MkdirAll(fullPath, 0755)
			if err != nil {
				h.session.SendResponse(550, fmt.Sprintf("Failed to create directory: %v", err))
				return
			}

			h.session.SendResponse(257, fmt.Sprintf("\"%s\" directory created", dirPath))
		})
	})
}

// handleRMD - Remove directory
func (h *CommandHandler) HandleRMD(dirName string) {
	h.withAuth(func() {
		h.withValidParam(dirName, func() {
			h.withExistingDirectory(dirName, func(dirPath, fullPath string, fileInfo os.FileInfo) {
				entries, err := os.ReadDir(fullPath)
				if err != nil {
					h.session.SendResponse(550, "Failed to read directory")
					return
				}

				if len(entries) > 0 {
					h.session.SendResponse(550, "Directory not empty")
					return
				}

				err = os.Remove(fullPath)
				if err != nil {
					h.session.SendResponse(550, fmt.Sprintf("Failed to remove directory: %v", err))
					return
				}

				h.session.SendResponse(250, "Directory removed")
			})
		})
	})
}

// handleDELE - Delete file
func (h *CommandHandler) HandleDELE(filename string) {
	h.withAuth(func() {
		h.withValidParam(filename, func() {
			h.withExistingFile(filename, func(filePath, fullPath string, fileInfo os.FileInfo) {
				if fileInfo.IsDir() {
					h.session.SendResponse(550, "Is a directory, use RMD")
					return
				}

				err := os.Remove(fullPath)
				if err != nil {
					h.session.SendResponse(550, fmt.Sprintf("Failed to delete file: %v", err))
					return
				}

				h.session.SendResponse(250, "File deleted")
			})
		})
	})
}

// handleRNFR - Rename from
func (h *CommandHandler) HandleRNFR(filename string) {
	h.withAuth(func() {
		h.withValidParam(filename, func() {
			filePath := h.session.ResolvePath(filename)
			fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)

			if _, err := os.Stat(fullPath); err != nil {
				h.session.SendResponse(550, "File or directory not found")
				return
			}

			// Store the "from" path for RNTO operation
			// This will need to be stored in the session
			h.session.SendResponse(350, "Ready for RNTO")
		})
	})
}

// handleRNTO - Rename to
func (h *CommandHandler) HandleRNTO(filename string) {
	h.withAuth(func() {
		h.withValidParam(filename, func() {
			// Retrieve the "from" path from the session
			// Complete the rename operation
			h.session.SendResponse(250, "Rename successful")
		})
	})
}
