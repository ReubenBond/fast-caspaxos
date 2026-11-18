@echo off
setlocal
dotnet run --project src\Orleans.Consensus.CasPaxos.Simulation.Runner\Orleans.Consensus.CasPaxos.Simulation.Runner.csproj -- %*
