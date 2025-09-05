@echo off
echo Building High-Performance FTP Server...
echo.

REM Build the FTP server
go build -o ftp_server.exe ftp_server.go

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Server built successfully!
    echo Executable: ftp_server.exe
    echo.
    echo To start the server, run: start_server.bat
    echo Or directly: ftp_server.exe
) else (
    echo.
    echo Build failed! Check for errors above.
)

pause
