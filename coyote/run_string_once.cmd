dotnet build -c Release -f net6.0
coyote rewrite .\bin\Release\net6.0\FastCASPaxos.dll
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String --fail-on-maxsteps