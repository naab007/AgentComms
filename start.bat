@echo off
cd /d "%~dp0"
echo Starting AgentComms Hub...
.venv\Scripts\python server.py
pause
