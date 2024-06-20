@echo off
setlocal
cd /d "%~dp0\.."
call run_simulation.cmd --scenario string-corpus --rounds 1000
