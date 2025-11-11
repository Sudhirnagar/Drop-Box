@echo off
REM ==============================
REM Mini Dropbox - Auto Run Script
REM ==============================

echo Starting Distributed Mini Dropbox System...

REM Step 1: Start Storage Nodes
echo Starting Node 1...
start "Node 1" cmd /k python distributed_storage.py node --id 1 --host localhost --port 5001 --storage node1_storage

timeout /t 2 >nul

echo Starting Node 2...
start "Node 2" cmd /k python distributed_storage.py node --id 2 --host localhost --port 5002 --storage node2_storage

timeout /t 2 >nul

REM Step 2: Start Web Server
echo Starting Web Interface...
start "Web Server" cmd /k python web_server.py

REM Step 3: Open in Browser
timeout /t 3 >nul
echo Opening Web Interface...
start http://localhost:8000

echo.
echo ==========================================
echo ✅ All systems running successfully!
echo Web: http://localhost:8000
echo Node 1: localhost:5001
echo Node 2: localhost:5002
echo ==========================================
pause
