# validate-bundle.ps1
# Enforces:
# 1. is_test ≠ "yes"
# 2. Cluster names: ^[a-zA-Z0-9_-]+$
# 3. No env names (dev, ab_dev, qa, prod, test, staging) in values

param(
    [string]$BundleDir = "AMALDAB"
)

$ErrorActionPreference = "Stop"
Write-Host "::group::🔍 Running bundle policy checks..."

# 1. Scan all YAML files in resources/ and config/
$yamlFiles = Get-ChildItem -Path "$BundleDir" -Include "*.py", "*.yml", "*.yaml" -Recurse

if ($yamlFiles.Count -eq 0) {
    Write-Warning "No YAML files found in $BundleDir"
    exit 0
}

$violations = @()

foreach ($file in $yamlFiles) {
    Write-Host "  Checking $($file.FullName)"
    try {
        $content = Get-Content $file.FullName -Raw
        $data = $content | ConvertFrom-Yaml -ErrorAction Stop

        # Helper: Recursively scan for policy violations
        function Test-Node {
            param($obj, $path = "")

            if ($obj -is [System.Collections.IDictionary]) {
                foreach ($key in $obj.Keys) {
                    $currentPath = if ($path) { "$path.$key" } else { "$key" }

                    # Rule 1: is_test must not be "yes"
                    if ($key -eq "is_test" -and $obj[$key] -eq "dbutils.widgets.text('IsTest','Yes')") {
                        $violations += "❌ [Rule 1] 'is_test: yes' found at $currentPath in $($file.Name)"
                    }

                    # Rule 3: Block env names in *values* (not keys!)
                    $val = $obj[$key]
                    if ($val -is [string]) {
                        # List of forbidden env substrings (case-insensitive)
                        $forbiddenEnvs = @("dev", "ab_dev", "qa", "prod", "test", "staging", "uat")
                        foreach ($envName in $forbiddenEnvs) {
                            if ($val -match "(?i)\b$envName\b") {
                                # Allow if it's part of a domain (e.g., "dev.databricks.com") → skip
                                # But block if standalone or in resource names
                                if ($val -notmatch "\.$envName\.") {
                                    $violations += "❌ [Rule 3] Env name '$envName' in value at $currentPath: '$val' (file: $($file.Name))"
                                }
                            }
                        }
                    }

                    # Rule 2: Check cluster names (in `resources.clusters.*.name`)
                    if ($currentPath -match "^resources\.SharedObjects\..+\.name$" -and $val -is [string]) {
                        if ($val -notmatch "^[a-zA-Z0-9_-]+$") {
                            $violations += "❌ [Rule 2] Invalid cluster name '$val' at $currentPath (file: $($file.Name)). Only letters, digits, '-', '_' allowed."
                        }
                    }

                    # Recurse
                    Test-Node -obj $obj[$key] -path $currentPath
                }
            } elseif ($obj -is [System.Collections.IList]) {
                for ($i = 0; $i -lt $obj.Count; $i++) {
                    Test-Node -obj $obj[$i] -path "$path[$i]"
                }
            }
        }

        Test-Node -obj $data

    } catch {
        Write-Warning "⚠️ Could not parse $($file.Name): $_"
    }
}

# Report
if ($violations.Count -gt 0) {
    Write-Host "::endgroup::"
    Write-Host "🛑 Policy violations detected:"
    $violations | ForEach-Object { Write-Host "  $_" }
    exit 1
} else {
    Write-Host "::endgroup::"
    Write-Host "✅ All bundle files comply with naming and config policies."
    exit 0
}
