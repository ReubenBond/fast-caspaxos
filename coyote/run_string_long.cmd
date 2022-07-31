dotnet build -c Release -f net6.0
coyote rewrite .\bin\Release\net6.0\FastCASPaxos.dll
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String -s portfolio -i 1000
@if %ERRORLEVEL% NEQ 0 exit
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String -s random -i 1000
@if %ERRORLEVEL% NEQ 0 exit
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String -s fair-prioritization -i 1000
@if %ERRORLEVEL% NEQ 0 exit
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String -s rl -i 1000
@if %ERRORLEVEL% NEQ 0 exit
coyote test .\bin\Release\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.String -s probabilistic -i 1000
@if %ERRORLEVEL% NEQ 0 exit
