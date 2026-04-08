@echo off
setlocal
cd /d "%~dp0\.."
call run_simulation.cmd --scenario set-corpus --rounds 1000
