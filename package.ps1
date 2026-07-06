$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$dist = Join-Path $root "dist"

New-Item -ItemType Directory -Force -Path $dist | Out-Null

$version = ""
try {
	$versionOutput = & git -C $root describe --long 2>$null
	if ($LASTEXITCODE -eq 0) {
		$version = "$versionOutput".Trim()
	}
} catch {
	$version = ""
}

$ldflags = "-s -w"
if ($version -ne "") {
	$ldflags = "$ldflags -X github.com/charmbracelet/crush/internal/version.Version=$version"
}

$targets = @(
	@{ GOOS = "darwin"; GOARCH = "arm64"; Extension = "" },
	@{ GOOS = "windows"; GOARCH = "amd64"; Extension = ".exe" },
	@{ GOOS = "linux"; GOARCH = "amd64"; Extension = "" }
)

$envNames = @("CGO_ENABLED", "GOEXPERIMENT", "GOOS", "GOARCH")
$previousEnv = @{}
foreach ($name in $envNames) {
	$previousEnv[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

try {
	foreach ($target in $targets) {
		$goos = $target["GOOS"]
		$goarch = $target["GOARCH"]
		$extension = $target["Extension"]
		$output = Join-Path $dist ("crush_{0}_{1}{2}" -f $goos, $goarch, $extension)

		Write-Host "Building $output"
		$env:CGO_ENABLED = "0"
		$env:GOEXPERIMENT = "greenteagc"
		$env:GOOS = $goos
		$env:GOARCH = $goarch

		& go build -trimpath -ldflags $ldflags -o $output $root
		if ($LASTEXITCODE -ne 0) {
			throw "go build failed for $goos/$goarch"
		}
	}
} finally {
	foreach ($name in $envNames) {
		if ($null -eq $previousEnv[$name]) {
			Remove-Item "Env:$name" -ErrorAction SilentlyContinue
			continue
		}
		Set-Item "Env:$name" $previousEnv[$name]
	}
}
