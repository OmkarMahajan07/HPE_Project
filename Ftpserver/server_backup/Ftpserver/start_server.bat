@echo off
echo Starting High-Performance FTP Server...
echo.
echo Server will start on port 2121
echo Data port range: 20000-20100
echo Root directory: Parent directory (..)
echo.
echo Available users:
echo   admin / password123
echo   user / userpass
echo   anonymous / (any password)
echo.
echo Press Ctrl+C to stop the server
echo.
.\ftp_server.exe
pause
