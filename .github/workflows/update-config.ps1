# update-config.ps1
# Script to update NB_Configuration.py with environment-specific values from databricks.yml

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('DEV', 'QA', 'PROD')]
    [string]$Environment,
    
    [Parameter(Mandatory=$false)]
    [string]$BundleDir = "AMALDAB",
    
    [Parameter(Mandatory=$false)]
    [string]$ConfigFile = "AMALDAB/DataPlatform/General/NB_Configuration.py"
)

Write-Host "🔧 Updating NB_Configuration.py for $Environment environment..."

# Import PowerShell YAML module
Import-Module powershell-yaml -ErrorAction Stop

# Read databricks.yml
$bundleYamlPath = Join-Path $BundleDir "databricks.yml"
if (-not (Test-Path $bundleYamlPath)) {
    Write-Error "❌ databricks.yml not found at: $bundleYamlPath"
    exit 1
}

Write-Host "📖 Reading bundle configuration from: $bundleYamlPath"
$bundleContent = Get-Content $bundleYamlPath -Raw
$bundleConfig = ConvertFrom-Yaml $bundleContent

# Extract variables for the target environment
if (-not $bundleConfig.targets.ContainsKey($Environment)) {
    Write-Error "❌ Environment '$Environment' not found in databricks.yml"
    exit 1
}

$targetConfig = $bundleConfig.targets[$Environment]
$variables = $targetConfig.variables

# Extract variable values (handle both direct values and default values)
$keyVaultScope = if ($variables.key_vault_scope.default) { $variables.key_vault_scope.default } else { $variables.key_vault_scope }
$catalogName = if ($variables.catalog_name.default) { $variables.catalog_name.default } else { $variables.catalog_name }
$loggerDirectory = if ($variables.logger_directory.default) { $variables.logger_directory.default } else { $variables.logger_directory }
$axiomArchive = if ($variables.axiom_archive.default) { $variables.axiom_archive.default } else { $variables.axiom_archive }
$axiomInput = if ($variables.axiom_input.default) { $variables.axiom_input.default } else { $variables.axiom_input }
$axiomVolume = if ($variables.axiom_volume.default) { $variables.axiom_volume.default } else { $variables.axiom_volume }
$employeeFlagPath = if ($variables.employee_flag_path.default) { $variables.employee_flag_path.default } else { $variables.employee_flag_path }
$marketingSourceSystem = if ($variables.marketing_source_system.default) { $variables.marketing_source_system.default } else { $variables.marketing_source_system }

Write-Host "✅ Extracted variables from databricks.yml:"
Write-Host "   - Key Vault Scope: $keyVaultScope"
Write-Host "   - Catalog Name: $catalogName"
Write-Host "   - Logger Directory: $loggerDirectory"
Write-Host "   - Axiom Archive: $axiomArchive"
Write-Host "   - Axiom Input: $axiomInput"
Write-Host "   - Axiom Volume: $axiomVolume"
Write-Host "   - Employee Flag Path: $employeeFlagPath"
Write-Host "   - Marketing Source System: $marketingSourceSystem"

# Check if config file exists
if (-not (Test-Path $ConfigFile)) {
    Write-Error "❌ Configuration file not found: $ConfigFile"
    exit 1
}

Write-Host "📝 Updating configuration file: $ConfigFile"

# Read the configuration file
$content = Get-Content $ConfigFile -Raw

# Define regex replacements
$replacements = @{
    "scope='[^']*'" = "scope='$keyVaultScope'"
    "catalog='[^']*'" = "catalog='$catalogName'"
    "logger_directory='[^']*'" = "logger_directory='$loggerDirectory'"
    'axiom_archive = ''[^'']*''' = "axiom_archive = '$axiomArchive'"
    'axiom_input = ''[^'']*''' = "axiom_input = '$axiomInput'"
    'axiom_volume="[^"]*"' = "axiom_volume=`"$axiomVolume`""
    "employee_flag_path='[^']*'" = "employee_flag_path='$employeeFlagPath'"
    "marketing_source_system = '[^']*'" = "marketing_source_system = '$marketingSourceSystem'"
}

# Apply replacements
foreach ($pattern in $replacements.Keys) {
    $replacement = $replacements[$pattern]
    $content = $content -replace $pattern, $replacement
}

# Write updated content back to file
$content | Out-File -FilePath $ConfigFile -Encoding utf8 -NoNewline

Write-Host "✅ Configuration file updated successfully!"
Write-Host ""
Write-Host "📋 Updated values:"
Write-Host "   scope='$keyVaultScope'"
Write-Host "   catalog='$catalogName'"
Write-Host "   logger_directory='$loggerDirectory'"
Write-Host "   axiom_archive = '$axiomArchive'"
Write-Host "   axiom_input = '$axiomInput'"
Write-Host "   axiom_volume=`"$axiomVolume`""
Write-Host "   employee_flag_path='$employeeFlagPath'"
Write-Host "   marketing_source_system = '$marketingSourceSystem'"
Write-Host ""
Write-Host "🎉 Configuration update complete!"

