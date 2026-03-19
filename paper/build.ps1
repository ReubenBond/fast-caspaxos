<#
.SYNOPSIS
    Build script for the Fast CASPaxos paper (replaces Makefile on Windows).

.DESCRIPTION
    Builds the paper PDF using latexmk. Supports ACM (default), IEEE, and USENIX targets.
    Auto-detects TeX Live at C:\texlive if not already in PATH.

.PARAMETER Target
    Build target: acm (default), ieee, usenix, clean, zip.

.EXAMPLE
    .\build.ps1              # Build ACM (default)
    .\build.ps1 ieee         # Build IEEE
    .\build.ps1 clean        # Clean build artifacts
    .\build.ps1 zip          # Create submission archive
#>
param(
    [ValidateSet('acm', 'ieee', 'usenix', 'clean', 'zip')]
    [string]$Target = 'acm'
)

$ErrorActionPreference = 'Stop'
$Filename = 'main'

# Auto-detect TeX Live if not in PATH
if (-not (Get-Command latexmk -ErrorAction SilentlyContinue)) {
    $texBin = Get-ChildItem 'C:\texlive\*\bin\windows' -Directory -ErrorAction SilentlyContinue |
              Sort-Object Name -Descending | Select-Object -First 1
    if ($texBin) {
        $env:PATH = "$($texBin.FullName);$env:PATH"
        Write-Host "Added TeX Live to PATH: $($texBin.FullName)" -ForegroundColor Yellow
    }
}

$preambleMap = @{
    'acm'    = '\def\ForceACM{1}'
    'ieee'   = '\def\ForceIEEE{1}'
    'usenix' = '\def\ForceUSENIX{1}'
}

$jobNameMap = @{
    'acm'    = "${Filename}_acm"
    'ieee'   = "${Filename}_ieee"
    'usenix' = "${Filename}_usenix"
}

function Test-Tool($name) {
    $null -ne (Get-Command $name -ErrorAction SilentlyContinue)
}

function Invoke-Build($target) {
    if (-not (Test-Tool 'latexmk')) {
        Write-Error "latexmk not found. Install TeX Live: choco install texlive -y (admin shell), then: tlmgr install latexmk"
        return
    }

    $jobName = $jobNameMap[$target]
    $preamble = $preambleMap[$target]

    Write-Host "Building $target -> $jobName.pdf" -ForegroundColor Cyan
    $pdflatexCmd = "pdflatex -interaction=nonstopmode %O `"$preamble\input{%S}`""
    & latexmk -pdf "-jobname=$jobName" "-pdflatex=$pdflatexCmd" $Filename

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Check $jobName.log for details."
        return
    }

    # Font embedding via Ghostscript (optional)
    if (Test-Tool 'ps2pdf') {
        Write-Host "Embedding fonts..." -ForegroundColor Cyan
        ps2pdf -dColorConversionStrategy=/LeaveColorUnchanged -dPDFSETTINGS=/prepress "$jobName.pdf" "${jobName}_font_embedded.pdf"
    }

    Write-Host "Done: $jobName.pdf" -ForegroundColor Green
}

function Invoke-Clean {
    Write-Host "Cleaning build artifacts..." -ForegroundColor Cyan
    $extensions = @('*.aux','*.bbl','*.blg','*.bcf','*.fdb_latexmk','*.fls','*.log','*.out',
                    '*.run.xml','*.synctex.gz','*.synctex(busy)','*.toc','*.lof','*.lot','*.xdv',
                    '*_font_embedded.pdf','spell_check','source_*.zip',
                    'main_acm.*','main_ieee.*','main_usenix.*')
    foreach ($ext in $extensions) {
        Get-ChildItem -Path . -Filter $ext -ErrorAction SilentlyContinue | Remove-Item -Force
    }
    # Also remove generated PDFs
    @('main_acm.pdf','main_ieee.pdf','main_usenix.pdf') | ForEach-Object {
        if (Test-Path $_) { Remove-Item $_ -Force }
    }
    Write-Host "Clean." -ForegroundColor Green
}

function Invoke-Zip {
    $target = 'acm'
    $jobName = $jobNameMap[$target]
    $zipName = "source_$jobName.zip"

    Write-Host "Creating submission archive: $zipName" -ForegroundColor Cyan
    if (Test-Path $zipName) { Remove-Item $zipName -Force }

    Compress-Archive -Path @(
        "$Filename.tex",
        'sections',
        'figures',
        'utils',
        '*.bib',
        '*.cls',
        '*.sty',
        '*.bst',
        'Makefile',
        'build.ps1'
    ) -DestinationPath $zipName -Force

    Write-Host "Created: $zipName" -ForegroundColor Green
}

# Execute
Push-Location $PSScriptRoot
try {
    switch ($Target) {
        'clean' { Invoke-Clean }
        'zip'   { Invoke-Zip }
        default { Invoke-Build $Target }
    }
} finally {
    Pop-Location
}
