package terminal

import (
	"os"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/jlaffaye/ftp"
)

// FTPClient interface to avoid circular dependency
type FTPClient interface {
	List(path string) ([]*ftp.Entry, error)
	IsConnected() bool
	GetCurrentDir() string
}

// CommandCompleter handles command and argument completion
type CommandCompleter struct {
	ftpCommands       []prompt.Suggest
	systemCommands    map[string]bool
	currentDir        string
	remoteFiles       []string
	remoteDirs        []string
	lastUpdate        time.Time
	ftpClient         FTPClient
	cacheTimeout      time.Duration
	localFileCache    map[string][]string // Cache for local files by directory
	localFileCacheAge map[string]time.Time
}

// NewCommandCompleter creates a new command completer
func NewCommandCompleter() *CommandCompleter {
	return &CommandCompleter{
		ftpCommands: []prompt.Suggest{
			{Text: "HOST", Description: "Connect to FTP server"},
			{Text: "QUIT", Description: "Disconnect from FTP server"},
			{Text: "LIST", Description: "List files on FTP server"},
			{Text: "CWD", Description: "Change directory on FTP server"},
			{Text: "PWD", Description: "Show current FTP directory"},
			{Text: "RETR", Description: "Download file from FTP"},
			{Text: "STOR", Description: "Upload file to FTP"},
			{Text: "MKD", Description: "Create directory on FTP server"},
			{Text: "RMD", Description: "Remove directory on FTP server"},
			{Text: "DEL", Description: "Delete file on FTP server"},
			{Text: "SITE", Description: "Server commands (USER management)"},
			{Text: "MODEZ", Description: "Toggle MODE Z compression (ON/OFF)"},
			{Text: "HELP", Description: "Show help information"},
			{Text: "setio", Description: "Set I/O mode (mmap/chunked)"},
			{Text: "theme", Description: "Change terminal theme"},
			{Text: "clear", Description: "Clear terminal screen"},
		},
		systemCommands:    make(map[string]bool),
		lastUpdate:        time.Now(),
		cacheTimeout:      15 * time.Second, // 15 second cache
		localFileCache:    make(map[string][]string),
		localFileCacheAge: make(map[string]time.Time),
	}
}

// SetFTPClient sets the FTP client for remote file operations
func (c *CommandCompleter) SetFTPClient(client FTPClient) {
	c.ftpClient = client
}

// UpdateRemoteFiles updates the cached remote files and directories
func (c *CommandCompleter) UpdateRemoteFiles(files, dirs []string) {
	c.remoteFiles = files
	c.remoteDirs = dirs
	c.lastUpdate = time.Now()
}

// Completer returns suggestions for the current input
func (c *CommandCompleter) Completer(d prompt.Document) []prompt.Suggest {
	text := d.TextBeforeCursor()
	words := strings.Fields(text)

	// If we're at the start of a new command
	if len(words) == 0 || (len(words) == 1 && !strings.HasSuffix(text, " ")) {
		return c.suggestCommands(words)
	}

	// If we're suggesting arguments for a command
	return c.suggestArguments(words)
}

// suggestCommands returns suggestions for commands
func (c *CommandCompleter) suggestCommands(words []string) []prompt.Suggest {
	var suggestions []prompt.Suggest

	// Add FTP commands
	suggestions = append(suggestions, c.ftpCommands...)

	// Add system commands
	for cmd := range c.systemCommands {
		suggestions = append(suggestions, prompt.Suggest{
			Text:        cmd,
			Description: "System command",
		})
	}

	// Filter suggestions based on current input
	if len(words) > 0 {
		prefix := strings.ToUpper(words[0])
		var filtered []prompt.Suggest
		for _, s := range suggestions {
			if strings.HasPrefix(strings.ToUpper(s.Text), prefix) {
				filtered = append(filtered, s)
			}
		}
		return filtered
	}

	return suggestions
}

// suggestArguments returns suggestions for command arguments
func (c *CommandCompleter) suggestArguments(words []string) []prompt.Suggest {
	if len(words) == 0 {
		return nil
	}

	cmd := strings.ToUpper(words[0])
	lastWord := words[len(words)-1]

	// Only suggest if we have at least one character typed
	if len(lastWord) == 0 {
		return nil
	}

	// Skip suggestions for hidden files unless explicitly typed
	if !strings.HasPrefix(lastWord, ".") && len(lastWord) == 1 {
		// Allow suggestions after first character
	}

	switch cmd {
	case "CD", "CWD":
		return c.suggestRemoteDirectories(lastWord)
	case "RETR", "DEL":
		return c.suggestRemoteFiles(lastWord)
	case "STOR":
		return c.suggestLocalFiles(lastWord)
	case "LIST":
		// Suggest both files and directories for LIST
		suggestions := c.suggestRemoteDirectories(lastWord)
		suggestions = append(suggestions, c.suggestRemoteFiles(lastWord)...)
		return suggestions
	default:
		return nil
	}
}

// suggestRemoteDirectories returns remote directory suggestions
func (c *CommandCompleter) suggestRemoteDirectories(prefix string) []prompt.Suggest {
	var suggestions []prompt.Suggest

	// Try to refresh cache if stale
	if time.Since(c.lastUpdate) > c.cacheTimeout {
		c.refreshRemoteCache()
	}

	// Filter suggestions
	for _, dir := range c.remoteDirs {
		// Skip hidden directories unless explicitly requested
		if strings.HasPrefix(dir, ".") && !strings.HasPrefix(prefix, ".") {
			continue
		}

		if strings.HasPrefix(strings.ToLower(dir), strings.ToLower(prefix)) {
			suggestions = append(suggestions, prompt.Suggest{
				Text:        dir,
				Description: "Remote directory",
			})
		}
	}

	return suggestions
}

// suggestRemoteFiles returns remote file suggestions
func (c *CommandCompleter) suggestRemoteFiles(prefix string) []prompt.Suggest {
	var suggestions []prompt.Suggest

	// Try to refresh cache if stale
	if time.Since(c.lastUpdate) > c.cacheTimeout {
		c.refreshRemoteCache()
	}

	// Filter suggestions
	for _, file := range c.remoteFiles {
		// Skip hidden files unless explicitly requested
		if strings.HasPrefix(file, ".") && !strings.HasPrefix(prefix, ".") {
			continue
		}

		if strings.HasPrefix(strings.ToLower(file), strings.ToLower(prefix)) {
			suggestions = append(suggestions, prompt.Suggest{
				Text:        file,
				Description: "Remote file",
			})
		}
	}

	return suggestions
}

// suggestLocalFiles returns local file suggestions for STOR command
func (c *CommandCompleter) suggestLocalFiles(prefix string) []prompt.Suggest {
	var suggestions []prompt.Suggest

	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return nil
	}

	// Check if we have cached files for this directory
	if cachedFiles, exists := c.localFileCache[cwd]; exists {
		if time.Since(c.localFileCacheAge[cwd]) < 10*time.Second {
			// Use cached files
			for _, file := range cachedFiles {
				// Skip hidden files unless explicitly requested
				if strings.HasPrefix(file, ".") && !strings.HasPrefix(prefix, ".") {
					continue
				}

				if strings.HasPrefix(strings.ToLower(file), strings.ToLower(prefix)) {
					suggestions = append(suggestions, prompt.Suggest{
						Text:        file,
						Description: "Local file",
					})
				}
			}
			return suggestions
		}
	}

	// Read directory and cache files
	entries, err := os.ReadDir(cwd)
	if err != nil {
		return nil
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())

			// Skip hidden files unless explicitly requested
			if strings.HasPrefix(entry.Name(), ".") && !strings.HasPrefix(prefix, ".") {
				continue
			}

			if strings.HasPrefix(strings.ToLower(entry.Name()), strings.ToLower(prefix)) {
				suggestions = append(suggestions, prompt.Suggest{
					Text:        entry.Name(),
					Description: "Local file",
				})
			}
		}
	}

	// Cache the files
	c.localFileCache[cwd] = files
	c.localFileCacheAge[cwd] = time.Now()

	return suggestions
}

// refreshRemoteCache attempts to refresh the remote file cache
func (c *CommandCompleter) refreshRemoteCache() {
	if c.ftpClient == nil || !c.ftpClient.IsConnected() {
		return
	}

	// Get current remote directory
	currentDir := c.ftpClient.GetCurrentDir()
	if currentDir == "" {
		currentDir = "/"
	}

	// List directory contents
	entries, err := c.ftpClient.List(currentDir)
	if err != nil {
		return // Silent failure, keep using old cache
	}

	// Update cache
	var files []string
	var dirs []string
	for _, entry := range entries {
		if entry.Type == 1 { // Directory type (ftp.EntryTypeFolder)
			dirs = append(dirs, entry.Name)
		} else {
			files = append(files, entry.Name)
		}
	}

	c.UpdateRemoteFiles(files, dirs)
}

// UpdateCurrentDir updates the current directory for suggestions
func (c *CommandCompleter) UpdateCurrentDir(dir string) {
	c.currentDir = dir
	// Clear local file cache when directory changes
	c.localFileCache = make(map[string][]string)
	c.localFileCacheAge = make(map[string]time.Time)
}

// AddSystemCommand adds a system command to the suggestions
func (c *CommandCompleter) AddSystemCommand(cmd string) {
	c.systemCommands[cmd] = true
}

// RemoveSystemCommand removes a system command from suggestions
func (c *CommandCompleter) RemoveSystemCommand(cmd string) {
	delete(c.systemCommands, cmd)
}

// ClearCache clears all cached suggestions
func (c *CommandCompleter) ClearCache() {
	c.remoteFiles = nil
	c.remoteDirs = nil
	c.localFileCache = make(map[string][]string)
	c.localFileCacheAge = make(map[string]time.Time)
	c.lastUpdate = time.Time{}
}
