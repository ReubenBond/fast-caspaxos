@echo off
setlocal
dotnet run --project src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj -- %*
