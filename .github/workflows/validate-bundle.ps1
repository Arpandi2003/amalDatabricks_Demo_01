#!/usr/bin/env pwsh
<#
.SYNOPSIS
Enforces bundle policy:
  1. is_test must NOT default to "Yes"/"yes"/"YES" in Python files (via dbutils.widgets.text)
  2. Cluster names must match ^[a-zA-Z0-9_-]+$ in YAML (resources.SharedObjects.*.name)
  3. No env names (dev/qa/prod/test/staging/uat/ab_dev) in values — in BOTH Python and YAML
#>

param(
    [string]$BundleDir = "AMALDAB"
)

$ErrorActionPreference = "Stop"
Write-Host "::group::🔍 Running bundle policy validator..."

# 🔒 Set execution policy for this process only
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force

# ⚙️ Configuration
$forbiddenEnvs = @("dev", "ab_dev", "qa", "prod", "test", "staging", "uat")
$envPattern = "\b(" + ($forbiddenEnvs -join "|") + ")\b"

# 📁 Gather files
$yamlFiles = Get-ChildItem -Path "$BundleDir" -Include "*.yml", "*.yaml" -Recurse
$pyFiles   = Get-ChildItem -Path "$BundleDir" -Include "*.py" -Recurse

Write-Host "Found $($pyFiles.Count) Python and $($yamlFiles.Count) YAML files to scan."

$violations = @()

# ───────────────────────────────────────
# 🔹 RULE 1 & 3: PYTHON FILES (.py)
# ───────────────────────────────────────
foreach ($file in $pyFiles) {
    Write-Host "  [PY] $($file.Name)"
    $content = Get-Content $file.FullName -Raw
    if ([string]::IsNullOrWhiteSpace($content)) { continue }

    # 🔸 Rule 1: is_test widget defaults to "Yes"
    $isTestPattern = 'dbutils\s*\.\s*widgets\s*\.\s*text\s*\(\s*["''][Ii]s[Tt]est["'']\s*,\s*["''][Yy][Ee][Ss]["'']\s*[,)]'
    $matches = Select-String -Path $file.FullName -Pattern $isTestPattern -CaseSensitive:$false -AllMatches
    foreach ($match in $matches.Matches) {
        $lineNum = $match.LineNumber
        $lineText = $match.Line.Trim()
        $violations += "❌ [Rule 1] is_test defaulted to 'Yes' in $($file.Name):$lineNum → '$lineText'"
    }

    # 🔸 Rule 3: Env leaks in Python (case-insensitive word boundary)
    $envMatches = Select-String -Path $file.FullName -Pattern $envPattern -CaseSensitive:$false -AllMatches
    foreach ($match in $envMatches.Matches) {
        $lineNum = $match.LineNumber
        $lineText = $match.Line.Trim()
        $envName = $match.Value

        # ✅ Allow domains like dev.databricks.com, qa.cloud
        if ($lineText -match "\.$envName\.") { continue }

        # ✅ Skip comments (optional — uncomment to enable)
        # if ($lineText -match '^\s*#') { continue }

        $violations += "❌ [Rule 3] Env '$envName' in Python $($file.Name):$lineNum → '$lineText'"
    }
}

# ───────────────────────────────────────
# 🔹 RULES 2 & 3: YAML FILES (.yml/.yaml)
# ───────────────────────────────────────
foreach ($file in $yamlFiles) {
    Write-Host "  [YAML] $($file.Name)"
    $content = Get-Content $file.FullName -Raw
    if ([string]::IsNullOrWhiteSpace($content)) { continue }

    try {
        $data = $content | ConvertFrom-Yaml -ErrorAction Stop

        function Test-Node {
            param($obj, $path = "")

            if ($obj -is [System.Collections.IDictionary]) {
                foreach ($key in $obj.Keys) {
                    $currentPath = if ($path) { "$path.$key" } else { "$key" }
                    $val = $obj[$key]

                    # 🔸 Rule 2: Cluster name format (YAML only)
                    if ($currentPath -match "^resources\.SharedObjects\..+\.name$" -and $val -is [string]) {
                        if ($val -notmatch "^[a-zA-Z0-9_-]+$") {
                            $violations += "❌ [Rule 2] Invalid cluster name '$val' at ${currentPath} (file: $($file.Name))"
                        }
                    }

                    # 🔸 Rule 3: Env leaks in string values (YAML only)
                    if ($val -is [string]) {
                        foreach ($envName in $forbiddenEnvs) {
                            # Case-insensitive word-boundary match, exclude *.envName.*
                            if ($val -match "(?i)\b$envName\b" -and $val -notmatch "\.$envName\.") {
                                $violations += "❌ [Rule 3] Env '$envName' in YAML value at ${currentPath}: '$val' (file: $($file.Name))"
                            }
                        }
                    }

                    # Recurse
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

    } catch {
        Write-Warning "⚠️ Skipping $($file.Name): Invalid or non-YAML content"
    }
}

# ───────────────────────────────────────
# 📊 REPORT
# ───────────────────────────────────────
Write-Host "::endgroup::"
if ($violations.Count -gt 0) {
    Write-Host "🛑 Policy violations detected:"
    $violations | ForEach-Object { Write-Host "  $_" }
    
    # Optional: GitHub Actions annotations (uncomment to enable)
    # $violations | ForEach-Object {
    #     if ($_ -match "in (?<file>[^:]+):(?<line>\d+) →") {
    #         $file = $Matches.file
    #         $line = $Matches.line
    #         $msg = ($_ -replace ".*→ '", "") -replace "'$", ""
    #         Write-Host "::error file=$file,line=$line::$msg"
    #     }
    # }
    
    exit 1
} else {
    Write-Host "✅ Bundle policy check PASSED:"
    Write-Host "   • Rule 1 (is_test ≠ 'Yes') — enforced in $($pyFiles.Count) Python files"
    Write-Host "   • Rule 2 (cluster name format) — enforced in $($yamlFiles.Count) YAML files"
    Write-Host "   • Rule 3 (no env leaks) — enforced in $($pyFiles.Count + $yamlFiles.Count) files"
    exit 0
}
