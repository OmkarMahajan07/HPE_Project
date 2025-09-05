package terminal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// FileInfo represents a file or directory entry
type FileInfo struct {
	Name      string
	Type      string
	Size      uint64
	Modified  time.Time
	IsDir     bool
	IsSymlink bool
}

// TableFormatter handles formatted table output
type TableFormatter struct {
	table *tablewriter.Table
}

// NewTableFormatter creates a new table formatter
func NewTableFormatter() *TableFormatter {
	table := tablewriter.NewWriter(os.Stdout)
	table.Header("Name", "Type", "Size", "Modified")
	table.Options(
		tablewriter.WithRendition(tw.Rendition{Borders: tw.Border{Left: tw.Pending, Right: tw.Pending, Top: tw.Pending, Bottom: tw.Pending}}),
		tablewriter.WithPadding(tw.Padding{Left: "\t", Right: "\t"}),
	)
	table.Configure(func(cfg *tablewriter.Config) {
		cfg.MaxWidth = 0 // No max width
		cfg.Header = tw.CellConfig{
			Alignment: tw.CellAlignment{
				Global: tw.AlignLeft,
			},
		}
		cfg.Row = tw.CellConfig{
			Alignment: tw.CellAlignment{
				Global: tw.AlignLeft,
			},
		}
		cfg.Behavior = tw.Behavior{}
	})

	return &TableFormatter{
		table: table,
	}
}

// FormatLocalDirectory formats a local directory listing
func (tf *TableFormatter) FormatLocalDirectory(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	var files []FileInfo
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		fileType := "file"
		if entry.IsDir() {
			fileType = "dir"
		} else if info.Mode()&os.ModeSymlink != 0 {
			fileType = "link"
		}

		files = append(files, FileInfo{
			Name:      entry.Name(),
			Type:      fileType,
			Size:      uint64(info.Size()),
			Modified:  info.ModTime(),
			IsDir:     entry.IsDir(),
			IsSymlink: info.Mode()&os.ModeSymlink != 0,
		})
	}

	return tf.renderTable(files)
}

// FormatFTPDirectory formats an FTP directory listing
func (tf *TableFormatter) FormatFTPDirectory(entries []*ftp.Entry) error {
	var files []FileInfo
	for _, entry := range entries {
		files = append(files, FileInfo{
			Name:     entry.Name,
			Type:     entry.Type.String(),
			Size:     entry.Size,
			Modified: entry.Time,
			IsDir:    entry.Type == ftp.EntryTypeFolder,
		})
	}

	return tf.renderTable(files)
}

// renderTable renders the table with the given file information
func (tf *TableFormatter) renderTable(files []FileInfo) error {
	if len(files) == 0 {
		fmt.Println("Directory is empty")
		return nil
	}

	// Reset table
	tf.table.Reset()

	// Set headers again after reset
	tf.table.Header("Name", "Type", "Size", "Modified")

	// Add rows
	for _, file := range files {
		// Format size
		size := formatSize(file.Size)
		if file.IsDir {
			size = "-"
		}

		// Format modified time
		modified := file.Modified.Format("Jan 02 15:04")

		// Format name with type indicator
		name := file.Name
		if file.IsDir {
			name = name + "/"
		} else if file.IsSymlink {
			name = name + "@"
		}

		// Truncate long names
		if len(name) > 50 {
			name = name[:47] + "..."
		}

		// Format type to show extension in caps
		fileType := file.Type
		if !file.IsDir && !file.IsSymlink {
			ext := filepath.Ext(name)
			if ext != "" {
				fileType = strings.ToUpper(strings.TrimPrefix(ext, "."))
			}
		}

		tf.table.Append([]string{
			name,
			fileType,
			size,
			modified,
		})
	}

	// Render table
	return tf.table.Render()
}

// formatSize formats a file size in human-readable format
func formatSize(size uint64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := uint64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
