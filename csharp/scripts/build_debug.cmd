@echo off
setlocal
cd /d "%~dp0\.."
dotnet build -c Debug src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj
