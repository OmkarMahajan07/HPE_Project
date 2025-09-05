package terminal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
)

// Theme represents a terminal theme configuration
type Theme struct {
	Name         string `json:"name"`
	PromptColor  string `json:"promptColor"`
	TextColor    string `json:"textColor"`
	ErrorColor   string `json:"errorColor"`
	SuccessColor string `json:"successColor"`
	InfoColor    string `json:"infoColor"`
}

// ThemeManager handles theme operations
type ThemeManager struct {
	currentTheme Theme
	configPath   string
}

// NewThemeManager creates a new theme manager
func NewThemeManager() (*ThemeManager, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %v", err)
	}

	configPath := filepath.Join(homeDir, ".ftpconfig.json")
	tm := &ThemeManager{
		configPath: configPath,
	}

	// Load default theme
	tm.currentTheme = Theme{
		Name:         "dark",
		PromptColor:  "green",
		TextColor:    "white",
		ErrorColor:   "red",
		SuccessColor: "green",
		InfoColor:    "blue",
	}

	// Try to load saved theme
	if err := tm.LoadTheme(); err != nil {
		// If config doesn't exist, save default theme
		if os.IsNotExist(err) {
			if err := tm.SaveTheme(); err != nil {
				return nil, fmt.Errorf("failed to save default theme: %v", err)
			}
		} else {
			return nil, fmt.Errorf("failed to load theme: %v", err)
		}
	}

	return tm, nil
}

// LoadTheme loads the theme from config file
func (tm *ThemeManager) LoadTheme() error {
	data, err := os.ReadFile(tm.configPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &tm.currentTheme)
}

// SaveTheme saves the current theme to config file
func (tm *ThemeManager) SaveTheme() error {
	data, err := json.MarshalIndent(tm.currentTheme, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(tm.configPath, data, 0644)
}

// SetTheme sets a new theme
func (tm *ThemeManager) SetTheme(name string) error {
	switch name {
	case "light":
		tm.currentTheme = Theme{
			Name:         "light",
			PromptColor:  "black",
			TextColor:    "black",
			ErrorColor:   "red",
			SuccessColor: "green",
			InfoColor:    "blue",
		}
	case "dark":
		tm.currentTheme = Theme{
			Name:         "dark",
			PromptColor:  "green",
			TextColor:    "green",
			ErrorColor:   "red",
			SuccessColor: "green",
			InfoColor:    "green",
		}
	default:
		return fmt.Errorf("unknown theme: %s", name)
	}

	return tm.SaveTheme()
}

// GetPromptColor returns the color function for prompts
func (tm *ThemeManager) GetPromptColor() *color.Color {
	return getColorFromName(tm.currentTheme.PromptColor)
}

// GetTextColor returns the color function for normal text
func (tm *ThemeManager) GetTextColor() *color.Color {
	return getColorFromName(tm.currentTheme.TextColor)
}

// GetErrorColor returns the color function for error messages
func (tm *ThemeManager) GetErrorColor() *color.Color {
	return getColorFromName(tm.currentTheme.ErrorColor)
}

// GetSuccessColor returns the color function for success messages
func (tm *ThemeManager) GetSuccessColor() *color.Color {
	return getColorFromName(tm.currentTheme.SuccessColor)
}

// GetInfoColor returns the color function for info messages
func (tm *ThemeManager) GetInfoColor() *color.Color {
	return getColorFromName(tm.currentTheme.InfoColor)
}

// getColorFromName returns a color.Color based on the color name
func getColorFromName(name string) *color.Color {
	switch name {
	case "black":
		return color.New(color.FgBlack)
	case "red":
		return color.New(color.FgRed)
	case "green":
		return color.New(color.FgGreen)
	case "yellow":
		return color.New(color.FgYellow)
	case "blue":
		return color.New(color.FgBlue)
	case "magenta":
		return color.New(color.FgMagenta)
	case "cyan":
		return color.New(color.FgCyan)
	case "white":
		return color.New(color.FgWhite)
	default:
		return color.New(color.FgWhite)
	}
}

// GetThemeName returns the name of the current theme
func (tm *ThemeManager) GetThemeName() string {
	return tm.currentTheme.Name
}
