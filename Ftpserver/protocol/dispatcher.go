package protocol

import "strings"

// Main command dispatcher that routes commands to appropriate handlers
func (h *CommandHandler) HandleCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToUpper(parts[0])
	args := ""
	if len(parts) > 1 {
		args = strings.Join(parts[1:], " ")
	}

	switch cmd {
	// Basic system commands
	case "SYST":
		h.HandleSYST()
	case "TYPE":
		h.HandleTYPE(args)
	case "MODE":
		h.HandleMODE(args)

	// Directory commands
	case "PWD":
		h.HandlePWD()
	case "CWD":
		h.HandleCWD(args)
	case "CDUP":
		h.HandleCDUP()

	// Data connection commands
	case "PASV":
		h.HandlePASV()
	case "EPSV":
		h.HandleEPSV(args)
	case "PORT":
		h.HandlePORT(args)

	// File transfer commands
	case "LIST":
		h.HandleLIST(args)
	case "RETR":
		h.HandleRETR(args)
	case "STOR":
		h.HandleSTOR(args)

	// File management commands
	case "MKD", "XMKD":
		h.HandleMKD(args)
	case "RMD", "XRMD":
		h.HandleRMD(args)
	case "DELE":
		h.HandleDELE(args)
	case "RNFR":
		h.HandleRNFR(args)
	case "RNTO":
		h.HandleRNTO(args)

	// Advanced commands
	case "FEAT":
		h.HandleFEAT()
	case "OPTS":
		h.HandleOPTS(args)
	case "SIZE":
		h.HandleSIZE(args)
	case "MDTM":
		h.HandleMDTM(args)
	case "REST":
		h.HandleREST(args)
	case "STAT":
		h.HandleSTAT(args)
	case "HELP":
		h.HandleHELP(args)
	case "ABOR":
		h.HandleABOR()
	case "STRU":
		h.HandleSTRU(args)
	case "ALLO":
		h.HandleALLO(args)
	case "APPE":
		h.HandleAPPE(args)
	case "AUTH":
		h.HandleAUTH(args)

	// Simple response commands
	case "NOOP":
		h.session.SendResponse(200, "NOOP command successful")

	default:
		h.session.SendResponse(502, "Command not implemented")
	}
}
