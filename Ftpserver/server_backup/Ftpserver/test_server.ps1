# High-Performance FTP Server Test Script
Write-Host "High-Performance FTP Server Test Script" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green
Write-Host ""

# Check if server executable exists
if (-not (Test-Path ".\ftp_server.exe")) {
    Write-Host "Error: ftp_server.exe not found. Please build the server first." -ForegroundColor Red
    Write-Host "Run: go build -o ftp_server.exe ftp_server.go" -ForegroundColor Yellow
    exit 1
}

# Check if test client exists
if (-not (Test-Path "..\test_client.exe")) {
    Write-Host "Error: test_client.exe not found in parent directory. Please build the test client first." -ForegroundColor Red
    Write-Host "Run: go build -o test_client.exe test_client.go" -ForegroundColor Yellow
    exit 1
}

Write-Host "Starting FTP Server in background..." -ForegroundColor Cyan

# Start the FTP server in background
$serverJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    .\ftp_server.exe
}

# Wait a moment for server to start
Start-Sleep -Seconds 2

Write-Host "Server started (Job ID: $($serverJob.Id))" -ForegroundColor Green
Write-Host "Testing connection..." -ForegroundColor Cyan
Write-Host ""

try {
    # Run the test client
    ..\test_client.exe localhost:2121
    
    Write-Host ""
    Write-Host "Test completed successfully!" -ForegroundColor Green
} catch {
    Write-Host "Test failed: $_" -ForegroundColor Red
} finally {
    # Stop the server
    Write-Host ""
    Write-Host "Stopping FTP Server..." -ForegroundColor Cyan
    Stop-Job -Job $serverJob
    Remove-Job -Job $serverJob
    Write-Host "Server stopped." -ForegroundColor Green
}

Write-Host ""
Write-Host "To manually test the server:" -ForegroundColor Yellow
Write-Host "1. Run: .\ftp_server.exe" -ForegroundColor White
Write-Host "2. Connect with any FTP client to localhost:2121" -ForegroundColor White
Write-Host "3. Use credentials: admin/password123" -ForegroundColor White
