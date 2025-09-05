package protocol

// No imports needed for the constructor file

// CommandHandler for all FTP commands
// Takes a SessionInterface for dependency injection

type CommandHandler struct {
	session SessionInterface
}

// NewCommandHandler creates a new command handler with session dependency injection
func NewCommandHandler(session SessionInterface) *CommandHandler {
	return &CommandHandler{session: session}
}

// This file only contains the constructor
// Individual command handlers are in separate files

