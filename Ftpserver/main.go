package main

import (
	"ftpserver/terminal"
)

func main() {
	// Parse command line arguments
	config, shouldExit, err := terminal.ParseFlags()
	if err != nil {
		terminal.HandleStartupError(err, "parse command line arguments")
		return
	}

	// Exit if help or version was shown
	if shouldExit {
		return
	}

	// Validate configuration
	if err := terminal.ValidateConfig(config); err != nil {
		terminal.HandleStartupError(err, "validate configuration")
		return
	}

	// Create and start server
	server := NewFTPServer(config.ListenPort, config.DataPortStart, config.DataPortEnd, config.RootDir)

	// Print startup information
	terminal.PrintStartupInfo(config, getKeys(server.users))

	// Start the server
	if err := server.Start(); err != nil {
		terminal.HandleStartupError(err, "start server")
	}
}
