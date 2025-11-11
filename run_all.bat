@echo off
REM ==============================
REM Mini Dropbox Automation Script
REM ==============================

echo Starting Mini Dropbox System...

REM Step 1: Start storage nodes
start "Node 1" cmd /k python distributed_storage.py node --id 1 --host localhost --port 5001 --storage node1_storage
timeout /t 2 >nul
start "Node 2" cmd /k python distributed_storage.py node --id 2 --host localhost --port 5002 --storage node2_storage
timeout /t 2 >nul

REM Step 2: Start web server
start "Web Server" cmd /k python web_server.py
timeout /t 2 >nul

REM Step 3: Git upload
echo Uploading project to GitHub...
git add .
git commit -m "Auto update"
git branch -M main
git remote set-url origin https://github.com/Sudhirnagar/Drop-Box.git
git push -u origin main

echo All systems started and uploaded successfully!
pause
