@echo off
title Hybrid Big Data Pipeline Dashboard
cd /d "%~dp0"

call .venv\Scripts\activate.bat

start "" powershell -WindowStyle Hidden -Command "Start-Sleep -Seconds 2; Start-Process 'http://127.0.0.1:5000'"

python web\app.py

pause
