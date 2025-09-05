package protocol

import (
	"os"
	"path/filepath"
)

func (h *CommandHandler) withAuth(handler func()) {
	if !h.session.IsAuthenticated() {
		h.session.SendResponse(530, "Not logged in")
		return
	}
	handler()
}

func (h *CommandHandler) withValidParam(param string, handler func()) {
	if param == "" {
		h.session.SendResponse(501, "Syntax error in parameters")
		return
	}
	handler()
}

func (h *CommandHandler) withExistingFile(filename string, handler func(string, string, os.FileInfo)) {
	filePath := h.session.ResolvePath(filename)
	fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)
	fullPath = filepath.FromSlash(fullPath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		h.session.SendResponse(550, "File not found")
		return
	}

	handler(filePath, fullPath, fileInfo)
}

func (h *CommandHandler) withExistingDirectory(dirname string, handler func(string, string, os.FileInfo)) {
	dirPath := h.session.ResolvePath(dirname)
	fullPath := filepath.Join(h.session.GetServerRootDir(), dirPath)
	fullPath = filepath.FromSlash(fullPath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		h.session.SendResponse(550, "Directory not found")
		return
	}

	if !fileInfo.IsDir() {
		h.session.SendResponse(550, "Not a directory")
		return
	}

	handler(dirPath, fullPath, fileInfo)
}

func (h *CommandHandler) withResolvedPath(filename string, handler func(string, string)) {
	filePath := h.session.ResolvePath(filename)
	fullPath := filepath.Join(h.session.GetServerRootDir(), filePath)
	fullPath = filepath.FromSlash(fullPath)

	handler(filePath, fullPath)
}
