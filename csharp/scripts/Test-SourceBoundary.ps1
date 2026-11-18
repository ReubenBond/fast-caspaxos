[CmdletBinding()]
param([switch] $PublishAot)

$ErrorActionPreference = 'Stop'
$upstream = Split-Path $PSScriptRoot -Parent
$root = Join-Path ([IO.Path]::GetTempPath()) ("fastcaspaxos-source-" + [Guid]::NewGuid().ToString('N'))
$source = Join-Path $root 'dependency/csharp'
$consumer = Join-Path $root 'Consumer'
$clientSmoke = Join-Path $root 'ClientSmoke'

try {
    New-Item -ItemType Directory -Path $source, $consumer, $clientSmoke -Force | Out-Null
    # An embedding repository pins its own SDK; this one uses the upstream pin.
    Copy-Item (Join-Path (Split-Path $upstream -Parent) 'global.json') (Join-Path $root 'global.json')
    foreach ($file in @('.editorconfig', 'Directory.Build.props', 'Directory.Build.targets', 'Directory.Packages.props')) {
        Copy-Item (Join-Path $upstream $file) (Join-Path $source $file)
    }
    foreach ($project in @('Orleans.Consensus.CasPaxos', 'Orleans.Consensus.CasPaxos.Server', 'Orleans.Consensus.CasPaxos.Grpc',
        'Clockwork', 'Orleans.Consensus.CasPaxos.Simulation', 'Orleans.Consensus.CasPaxos.Testing')) {
        $projectSource = Join-Path $upstream "src/$project"
        foreach ($file in Get-ChildItem $projectSource -Recurse -File) {
            $relative = [IO.Path]::GetRelativePath($projectSource, $file.FullName)
            if ($relative -match '(^|[\\/])(bin|obj)[\\/]') { continue }
            $target = Join-Path $source "src/$project/$relative"
            New-Item -ItemType Directory -Path (Split-Path $target -Parent) -Force | Out-Null
            Copy-Item $file.FullName $target
        }
    }

    @'
<Project>
  <PropertyGroup>
    <TargetFramework>net11.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <DefineConstants>$(DefineConstants);EMBEDDING_REPOSITORY</DefineConstants>
  </PropertyGroup>
  <Target Name="CheckPropsIsolation" BeforeTargets="PrepareForBuild"
    Condition="'$(MSBuildProjectName)' != 'Consumer' And '$(MSBuildProjectName)' != 'ClientSmoke'">
    <Error Text="Upstream project inherited the embedding repository's Directory.Build.props." />
  </Target>
</Project>
'@ | Set-Content (Join-Path $root 'Directory.Build.props')
    @'
<Project>
  <Target Name="CheckBuildIsolation" BeforeTargets="PrepareForBuild"
    Condition="'$(MSBuildProjectName)' != 'Consumer' And '$(MSBuildProjectName)' != 'ClientSmoke'">
    <Error Text="Upstream project inherited the embedding repository's Directory.Build.targets." />
  </Target>
</Project>
'@ | Set-Content (Join-Path $root 'Directory.Build.targets')
    @'
<Project>
  <ItemGroup>
    <PackageVersion Include="HostOnly.Package" Version="1.0.0" />
  </ItemGroup>
  <Target Name="CheckPackageIsolation" BeforeTargets="PrepareForBuild"
    Condition="'$(MSBuildProjectName)' != 'Consumer' And '$(MSBuildProjectName)' != 'ClientSmoke'">
    <Error Text="Upstream project inherited the embedding repository's Directory.Packages.props." />
  </Target>
</Project>
'@ | Set-Content (Join-Path $root 'Directory.Packages.props')
    @'
root = true

[*.cs]
dotnet_diagnostic.CA1050.severity = error
dotnet_diagnostic.CA2000.severity = error
dotnet_diagnostic.CA2016.severity = error
dotnet_diagnostic.CA2213.severity = error
'@ | Set-Content (Join-Path $root '.editorconfig')
    @'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType></PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../dependency/csharp/src/Orleans.Consensus.CasPaxos.Grpc/Orleans.Consensus.CasPaxos.Grpc.csproj" />
    <ProjectReference Include="../dependency/csharp/src/Orleans.Consensus.CasPaxos.Testing/Orleans.Consensus.CasPaxos.Testing.csproj" />
  </ItemGroup>
</Project>
'@ | Set-Content (Join-Path $consumer 'Consumer.csproj')
    @'
#if !EMBEDDING_REPOSITORY
#error The consuming project must retain its own build policy.
#endif
using Orleans.Consensus.CasPaxos.Grpc;
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Testing;

using var client = new GrpcRegisterClient([new Uri("https://register.example")]);
await using var simulation = new RegisterSimulationCluster(seed: 42);
simulation.Run(async cluster =>
{
    var registers = cluster.CreateClient();
    var result = await registers.CompareExchangeAsync("embedded", null, ByteString.Empty);
    if (result.Status != CompareExchangeStatus.Applied || !RegisterVersion.IsValid(result.Current!.Version!.Value))
        throw new InvalidOperationException("The embedded consumer must observe a native register write.");
    cluster.CrashNode(4);
    cluster.CrashNode(5);
    if (await registers.ReadAsync("embedded") != result.Current)
        throw new InvalidOperationException("Fast recovery must preserve the embedded consumer's value.");
});
Console.WriteLine("Embedded production and framework-neutral testing closures executed.");
'@ | Set-Content (Join-Path $consumer 'Program.cs')

    # Prove the consumer retains its parent analyzer policy while the source closure uses its own.
    $probe = Join-Path $consumer 'PolicyProbe.cs'
    'public sealed class ConsumerPolicyProbe { }' | Set-Content $probe
    $probeOutput = dotnet build (Join-Path $consumer 'Consumer.csproj') --configuration Release --verbosity minimal 2>&1 | Out-String
    if ($LASTEXITCODE -eq 0 -or $probeOutput -notmatch 'error CA1050:') {
        throw "The consumer did not enforce its parent .editorconfig: $probeOutput"
    }
    Remove-Item -LiteralPath $probe

    dotnet run --project (Join-Path $consumer 'Consumer.csproj') --configuration Release --verbosity minimal
    if ($LASTEXITCODE -ne 0) { throw "Source-boundary consumer failed: $LASTEXITCODE" }

    if ($PublishAot) {
        @'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <PublishAot>true</PublishAot>
    <TrimmerSingleWarn>false</TrimmerSingleWarn>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../dependency/csharp/src/Orleans.Consensus.CasPaxos.Grpc/Orleans.Consensus.CasPaxos.Grpc.csproj" />
  </ItemGroup>
</Project>
'@ | Set-Content (Join-Path $clientSmoke 'ClientSmoke.csproj')
        @'
#if !EMBEDDING_REPOSITORY
#error The consuming project must retain its own build policy.
#endif
using Orleans.Consensus.CasPaxos.Grpc;
using Orleans.Consensus.CasPaxos.Server;
using Grpc.Core;

using var client = new GrpcRegisterClient([new Uri("http://127.0.0.1:1")],
    new() { AttemptTimeout = TimeSpan.FromSeconds(1), CallTimeout = TimeSpan.FromSeconds(1) });
try
{
    await client.ReadAsync("aot");
    throw new InvalidOperationException("An offline endpoint must fail.");
}
catch (RemoteCallException error) when (error.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded) { }
var result = await client.CompareExchangeAsync("aot", null, ByteString.Empty);
if (result.Status != CompareExchangeStatus.Unknown || result.Error is not RemoteCallException)
    throw new InvalidOperationException("An ambiguous transport failure must retain mutation uncertainty.");
try
{
    await client.ListKeysAsync();
    throw new InvalidOperationException("An offline endpoint must fail.");
}
catch (RemoteCallException error) when (error.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded) { }
Console.WriteLine("Native AOT register client exercised all RPC methods.");
'@ | Set-Content (Join-Path $clientSmoke 'Program.cs')
        $output = Join-Path $clientSmoke 'published'
        dotnet publish (Join-Path $clientSmoke 'ClientSmoke.csproj') --configuration Release --output $output --verbosity minimal
        if ($LASTEXITCODE -ne 0) { throw "Native AOT client publish failed: $LASTEXITCODE" }
        $executable = Join-Path $output $(if ($IsWindows) { 'ClientSmoke.exe' } else { 'ClientSmoke' })
        & $executable
        if ($LASTEXITCODE -ne 0) { throw "Native AOT client smoke failed: $LASTEXITCODE" }
    }
}
finally {
    if (Test-Path $root) { Remove-Item -LiteralPath $root -Recurse -Force }
}
