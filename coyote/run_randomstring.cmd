dotnet build -c Debug -f net6.0
coyote rewrite .\bin\Debug\net6.0\FastCASPaxos.dll
coyote test .\bin\Debug\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.RandomString -i 10000 --strategy portfolio
