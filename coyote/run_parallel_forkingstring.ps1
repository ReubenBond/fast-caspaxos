dotnet build -c Debug -f net6.0
coyote rewrite .\bin\Debug\net6.0\FastCASPaxos.dll

0..[System.Environment]::ProcessorCount | % { Start-Job { param($num) coyote test .\bin\Debug\net6.0\FastCASPaxos.dll -m FastCASPaxos.Program.ForkingString -i 100000 --strategy portfolio -o tmp/$num/ } -ArgumentList $_ }
