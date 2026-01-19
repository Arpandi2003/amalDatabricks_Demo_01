# sync-with-deletion.ps1
# Script to sync notebooks to Databricks workspace and remove deleted files

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('DEV', 'QA', 'PROD')]
    [string]$Environment,
    
    [Parameter(Mandatory=$false)]
    [string]$SourceDir = "AMALDAB/DataPlatform",
    
    [Parameter(Mandatory=$false)]
    [string]$WorkspacePath = "/Workspace/DataPlatform"
)

Write-Host "🔄 Starting sync with deletion for $Environment environment..."
Write-Host "📂 Source Directory: $SourceDir"
Write-Host "🎯 Workspace Path: $WorkspacePath"

# Function to get all files in source directory
function Get-SourceFiles {
    param([string]$Path)
    
    Write-Host "📋 Scanning source files..."
    
    $files = Get-ChildItem -Path $Path -Recurse -File -Include "*.py", "*.ipynb" | 
        Where-Object { $_.FullName -notmatch '\\\.git\\' -and $_.FullName -notmatch '\\__pycache__\\' }
    
    $relativePaths = @()
    foreach ($file in $files) {
        $relativePath = $file.FullName.Substring($Path.Length).Replace('\', '/')
        if ($relativePath.StartsWith('/')) {
            $relativePath = $relativePath.Substring(1)
        }
        $relativePaths += $relativePath
    }
    
    Write-Host "✅ Found $($relativePaths.Count) source files"
    return $relativePaths
}

# Function to get all files in workspace
function Get-WorkspaceFiles {
    param([string]$Path)
    
    Write-Host "📋 Scanning workspace files..."
    
    try {
        # List workspace directory recursively
        $output = databricks workspace list $Path --recursive --output json 2>&1
        
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "⚠️  Workspace path does not exist or is empty: $Path"
            return @()
        }
        
        $items = $output | ConvertFrom-Json
        
        # Filter for notebooks and Python files
        $files = $items | Where-Object { 
            $_.object_type -eq 'NOTEBOOK' -or 
            $_.object_type -eq 'FILE' -or
            $_.path -match '\.(py|ipynb)$'
        }
        
        $relativePaths = @()
        foreach ($file in $files) {
            $relativePath = $file.path.Substring($Path.Length)
            if ($relativePath.StartsWith('/')) {
                $relativePath = $relativePath.Substring(1)
            }
            $relativePaths += $relativePath
        }
        
        Write-Host "✅ Found $($relativePaths.Count) workspace files"
        return $relativePaths
    }
    catch {
        Write-Warning "⚠️  Error listing workspace files: $_"
        return @()
    }
}

# Function to remove file from workspace
function Remove-WorkspaceFile {
    param([string]$Path)
    
    Write-Host "🗑️  Removing: $Path"
    
    try {
        databricks workspace delete $Path --recursive 2>&1 | Out-Null
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Removed successfully"
            return $true
        }
        else {
            Write-Warning "   ⚠️  Failed to remove (may not exist)"
            return $false
        }
    }
    catch {
        Write-Warning "   ⚠️  Error removing file: $_"
        return $false
    }
}

# Main sync logic
try {
    # Get source files
    $sourceFiles = Get-SourceFiles -Path $SourceDir
    
    # Get workspace files
    $workspaceFiles = Get-WorkspaceFiles -Path $WorkspacePath
    
    # Find files to delete (in workspace but not in source)
    $filesToDelete = @()
    foreach ($workspaceFile in $workspaceFiles) {
        # Normalize paths for comparison
        $normalizedWorkspaceFile = $workspaceFile.Replace('\', '/')
        
        # Check if file exists in source
        $existsInSource = $false
        foreach ($sourceFile in $sourceFiles) {
            $normalizedSourceFile = $sourceFile.Replace('\', '/')
            
            # Match with or without extension (Databricks may strip .py extension)
            if ($normalizedWorkspaceFile -eq $normalizedSourceFile -or
                $normalizedWorkspaceFile -eq $normalizedSourceFile.Replace('.py', '') -or
                $normalizedWorkspaceFile -eq $normalizedSourceFile.Replace('.ipynb', '')) {
                $existsInSource = $true
                break
            }
        }
        
        if (-not $existsInSource) {
            $filesToDelete += $normalizedWorkspaceFile
        }
    }
    
    # Report findings
    Write-Host ""
    Write-Host "=" * 70
    Write-Host "📊 SYNC ANALYSIS"
    Write-Host "=" * 70
    Write-Host "📁 Source files:    $($sourceFiles.Count)"
    Write-Host "🎯 Workspace files: $($workspaceFiles.Count)"
    Write-Host "🗑️  Files to delete: $($filesToDelete.Count)"
    Write-Host "=" * 70
    Write-Host ""
    
    # Delete files if any
    if ($filesToDelete.Count -gt 0) {
        Write-Host "🗑️  Removing deleted files from workspace..."
        Write-Host ""
        
        $deletedCount = 0
        foreach ($file in $filesToDelete) {
            $fullPath = "$WorkspacePath/$file"
            if (Remove-WorkspaceFile -Path $fullPath) {
                $deletedCount++
            }
        }
        
        Write-Host ""
        Write-Host "✅ Removed $deletedCount of $($filesToDelete.Count) files"
    }
    else {
        Write-Host "✅ No files to delete - workspace is in sync!"
    }
    
    Write-Host ""
    Write-Host "🎉 Sync with deletion completed successfully!"
    exit 0
}
catch {
    Write-Error "❌ Sync failed: $_"
    exit 1
}

