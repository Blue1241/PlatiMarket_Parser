$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

$iscc = Get-Command ISCC.exe -ErrorAction SilentlyContinue
if (-not $iscc) {
    $candidate = Join-Path ${env:ProgramFiles(x86)} 'Inno Setup 6\ISCC.exe'
    if (Test-Path $candidate) { $iscc = [pscustomobject]@{ Source = $candidate } }
}
if (-not $iscc) {
    throw 'Inno Setup 6 is required. Install it, then rerun this script.'
}

Push-Location $repoRoot
try {
    cargo build --release
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    & $iscc.Source (Join-Path $repoRoot 'installer\PlatiRadar.iss')
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Write-Host 'Installer created in dist\'
}
finally {
    Pop-Location
}
