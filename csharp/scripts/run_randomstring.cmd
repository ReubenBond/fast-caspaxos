@echo off
setlocal
cd /d "%~dp0\.."
call run_simulation.cmd --scenario random-string-corpus --rounds 10000
