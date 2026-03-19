@echo off
setlocal
cd /d "%~dp0\.."
call run_simulation.cmd --scenario forking-string-corpus --rounds 10000
