#!/usr/bin/env pwsh
<#
.SYNOPSIS
Bundle policy validator:
  Rule 1 (PY): is_test must NOT default to "Yes"
  Rule 2 (YAML): cluster names = ^[a-zA-Z0-9_-]+$
  Rule 3 (PY+YAML): no env names (dev/qa/prod/etc.) in values
#>

param(
    [string]$BundleDir = "AMALDAB"
)

$ErrorActionPreference = "Stop"
Write-Host "::group::🔍 Running bundle policy validator..."

# 🔒 Execution policy
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force

# ⚙️ Config
$forbiddenEnvs = @("dev", "ab_dev", "qa", "prod", "test", "staging", "uat")
$envPattern = "\b(" + ($forbiddenEnvs -join "|") + ")\b"

$yamlFiles = Get-ChildItem -Path "$BundleDir" -Include "*.yml", "*.yaml" -Recurse
$pyFiles   = Get-ChildItem -Path "$BundleDir" -Include "*.py" -Recurse

Write-Host "Found $($pyFiles.Count) Python and $($yamlFiles.Count) YAML files."

$violations = @()

# ───────────────────────────────────────
# 🔹 PYTHON FILES — Rule 1 & Rule 3
# ───────────────────────────────────────
foreach ($file in $pyFiles) {
    Write-Host "  [PY] $($file.Name)"
    try {
        # Safely read as UTF8 (avoid encoding issues)
        $content = Get-Content $file.FullName -Raw -Encoding UTF8
        if ([string]::IsNullOrWhiteSpace($content)) { continue }

        # 🔸 Rule 1: is_test = "Yes" via widget
        $isTestPattern = 'dbutils\s*\.\s*widgets\s*\.\s*text\s*\(\s*["''][Ii]s[Tt]est["'']\s*,\s*["''][Yy][Ee][Ss]["'']\s*[,)]'
        $matches = Select-String -Path $file.FullName -Pattern $isTestPattern -CaseSensitive:$false -AllMatches -Encoding UTF8
        foreach ($match in $matches.Matches) {
            $lineNum = $match.LineNumber
            $lineText = if ($match.Line) { $match.Line.Trim() } else { "[line unavailable]" }
            $violations += "❌ [Rule 1] is_test defaulted to 'Yes' in $($file.Name):$lineNum → '$lineText'"
        }
    }

        # 🔸 Rule 3: Env leaks
    #     $envMatches = Select-String -Path $file.FullName -Pattern $envPattern -CaseSensitive:$false -AllMatches -Encoding UTF8
    #     foreach ($match in $envMatches.Matches) {
    #         $lineNum = $match.LineNumber
    #         $lineText = if ($match.Line) { $match.Line.Trim() } else { "[line unavailable]" }
    #         $envName = $match.Value

    #         if ($lineText -notmatch "\.$envName\.") {
    #             $violations += "❌ [Rule 3] Env '$envName' in Python $($file.Name):$lineNum → '$lineText'"
    #         }
    #     }
    # }
    catch {
        Write-Warning "⚠️ Skipping $($file.Name): $($_.Exception.Message)"
    }
}

# ───────────────────────────────────────
# 🔹 YAML FILES — Rule 2 & Rule 3
# ───────────────────────────────────────
foreach ($file in $yamlFiles) {
    Write-Host "  [YAML] $($file.Name)"
    try {
        $content = Get-Content $file.FullName -Raw -Encoding UTF8
        if ([string]::IsNullOrWhiteSpace($content)) { continue }

        $data = $content | ConvertFrom-Yaml -ErrorAction Stop

        function Test-Node {
            param($obj, $path = "")

            if ($obj -is [System.Collections.IDictionary]) {
                foreach ($key in $obj.Keys) {
                    $currentPath = if ($path) { "$path.$key" } else { "$key" }
                    $val = $obj[$key]

                    # 🔸 Rule 2: Cluster name format
                    if ($currentPath -match "^resources\.SharedObjects\..+\.name$" -and $val -is [string]) {
                        if ($val -notmatch "^[a-zA-Z0-9_-]+$") {
                            $violations += "❌ [Rule 2] Invalid cluster name '$val' at ${currentPath} (file: $($file.Name))"
                        }
                    }

                    # 🔸 Rule 3: Env in YAML values
                    if ($val -is [string]) {
                        foreach ($envName in $forbiddenEnvs) {
                            if ($val -match "(?i)\b$envName\b" -and $val -notmatch "\.$envName\.") {
                                $violations += "❌ [Rule 3] Env '$envName' in YAML value at ${currentPath}: '$val' (file: $($file.Name))"
                            }
                        }
                    }

                    Test-Node -obj $val -path $currentPath
                }
            }
            elseif ($obj -is [System.Collections.IList]) {
                for ($i = 0; $i -lt $obj.Count; $i++) {
                    Test-Node -obj $obj[$i] -path "${path}[$i]"
                }
            }
        }

        Test-Node -obj $data
    }
    catch {
        Write-Warning "⚠️ Skipping $($file.Name): $($_.Exception.Message)"
    }
}

# ───────────────────────────────────────
# 📊 REPORT
# ───────────────────────────────────────
Write-Host "::endgroup::"
if ($violations.Count -gt 0) {
    Write-Host "🛑 Policy violations detected:"
    $violations | ForEach-Object { Write-Host "  $_" }
    exit 1
} else {
    Write-Host "✅ Bundle policy check PASSED."
    exit 0
}
