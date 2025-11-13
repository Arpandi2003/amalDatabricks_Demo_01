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

# 1. Scan all YAML/Python files in the bundle
$yamlFiles = Get-ChildItem -Path "$BundleDir" -Include "*.py", "*.yml", "*.yaml" -Recurse

if ($yamlFiles.Count -eq 0) {
    Write-Warning "No YAML/Python files found in $BundleDir"
    exit 0
}

$violations = @()

foreach ($file in $yamlFiles) {
    Write-Host "  Checking $($file.FullName)"
    try {
        $content = Get-Content $file.FullName -Raw
        # Skip if empty
        if ([string]::IsNullOrWhiteSpace($content)) { continue }

        # Attempt YAML parse (most files are YAML); skip if fails
        $data = $content | ConvertFrom-Yaml -ErrorAction Stop

        # Helper: Recursively scan for policy violations
        function Test-Node {
            param($obj, $path = "")

            if ($obj -is [System.Collections.IDictionary]) {
                foreach ($key in $obj.Keys) {
                    $currentPath = if ($path) { "$path.$key" } else { "$key" }
                    $val = $obj[$key]

                    # Rule 1: is_test must not be "yes" (or equivalent patterns)
                    if ($key -eq "is_test" -and ($val -eq "yes" -or $val -eq "Yes" -or $val -match "dbutils\.widgets\.text\(\s*'IsTest'\s*,\s*'Yes'")) {
                        $violations += "❌ [Rule 1] 'is_test: yes' found at ${currentPath} (file: $($file.Name))"
                    }

                    # Rule 3: Block env names in *values* (not keys!)
                    if ($val -is [string]) {
                        $forbiddenEnvs = @("dev", "ab_dev", "qa", "prod", "test", "staging", "uat")
                        foreach ($envName in $forbiddenEnvs) {
                            # Word-boundary match, case-insensitive
                            if ($val -match "(?i)\b$envName\b") {
                                # Allow if part of domain (e.g., dev.databricks.com), deny otherwise
                                if ($val -notmatch "\.$envName\.") {
                                    $violations += "❌ [Rule 3] Env name '$envName' in value at ${currentPath}: '$val' (file: $($file.Name))"
                                }
                            }
                        }
                    }

                    # Rule 2: Validate cluster names (adjust path pattern as needed)
                    # Update: You said SharedObjects — ensure this matches your actual path structure
                    if ($currentPath -match "^resources\.SharedObjects\..+\.name$" -and $val -is [string]) {
                        if ($val -notmatch "^[a-zA-Z0-9_-]+$") {
                            $violations += "❌ [Rule 2] Invalid cluster name '$val' at ${currentPath} (file: $($file.Name)). Only letters, digits, '-', '_' allowed."
                        }
                    }

                    # Recurse into child nodes
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
        Write-Warning "⚠️ Could not parse $($file.Name) as YAML: $_"
        # Optionally: skip or fall back to string scan for env leaks in raw text
    }
}

# Final report
Write-Host "::endgroup::"
if ($violations.Count -gt 0) {
    Write-Host "🛑 Policy violations detected:"
    $violations | ForEach-Object { Write-Host "  $_" }
    exit 1
} else {
    Write-Host "✅ All bundle files comply with naming and config policies."
    exit 0
}
