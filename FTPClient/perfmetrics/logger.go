package perfmetrics

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// CsvHeader defines the CSV header for performance logging
const CsvHeader = "Timestamp,Client,FileName,FileSizeMB,Compression,RTTms,WindowSize,ThroughputMBps,TimeSec,Retries\n"

// LogPerformanceToCSV logs performance metrics to a CSV file
func LogPerformanceToCSV(fileName string, metrics map[string]interface{}) error {
	// Ensure the perfmetrics directory exists
	dir := "perfmetrics"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	// Full path to the CSV file
	filePath := filepath.Join(dir, fileName)

	// Check if file exists to determine if we need to write header
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	// Open file for appending (create if doesn't exist)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Write header if file is new
	if !fileExists {
		if _, err := file.WriteString(CsvHeader); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Extract values from metrics map with type assertions and validation
	fileName, ok := metrics["FileName"].(string)
	if !ok {
		return fmt.Errorf("FileName must be a string")
	}

	fileSizeMB, ok := metrics["FileSizeMB"].(float64)
	if !ok {
		return fmt.Errorf("FileSizeMB must be a float64")
	}

	compression, ok := metrics["Compression"].(bool)
	if !ok {
		return fmt.Errorf("Compression must be a bool")
	}

	rttMs, ok := metrics["RTTms"].(int)
	if !ok {
		return fmt.Errorf("RTTms must be an int")
	}

	windowSize, ok := metrics["WindowSize"].(int)
	if !ok {
		return fmt.Errorf("WindowSize must be an int")
	}

	throughputMBps, ok := metrics["ThroughputMBps"].(float64)
	if !ok {
		return fmt.Errorf("ThroughputMBps must be a float64")
	}

	timeSec, ok := metrics["TimeSec"].(float64)
	if !ok {
		return fmt.Errorf("TimeSec must be a float64")
	}

	retries, ok := metrics["Retries"].(int)
	if !ok {
		return fmt.Errorf("Retries must be an int")
	}

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Get current timestamp in RFC3339 format
	timestamp := time.Now().Format(time.RFC3339)

	// Get custom client name or use fallback
	clientName := "FTP_Client"
	if val, ok := metrics["Client"].(string); ok {
		clientName = val
	}

	// Create CSV record
	record := []string{
		timestamp,
		clientName, // now customizable!
		fileName,
		strconv.FormatFloat(fileSizeMB, 'f', 2, 64),
		strconv.FormatBool(compression),
		strconv.Itoa(rttMs),
		strconv.Itoa(windowSize),
		strconv.FormatFloat(throughputMBps, 'f', 2, 64),
		strconv.FormatFloat(timeSec, 'f', 2, 64),
		strconv.Itoa(retries),
	}

	// Write the record
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %v", err)
	}

	// Ensure data is written to disk
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %v", err)
	}

	return nil
}

