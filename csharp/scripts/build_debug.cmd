@echo off
setlocal
cd /d "%~dp0\.."
dotnet build -c Debug src\Orleans.Consensus.CasPaxos.Simulation.Runner\Orleans.Consensus.CasPaxos.Simulation.Runner.csproj
