# Databricks CI/CD Pipeline Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Workflows](#workflows)
4. [Authentication & Security](#authentication--security)
5. [Environment Configuration](#environment-configuration)
6. [Deployment Process](#deployment-process)
7. [Disaster Recovery Backup](#disaster-recovery-backup)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

---

## Overview

### Purpose
This CI/CD system automates the deployment of Databricks Asset Bundles (DAB) and provides comprehensive disaster recovery capabilities for Databricks workspaces across multiple environments.

### Key Features
- ✅ **Automated DAB Deployment** - Deploy Databricks resources via Infrastructure-as-Code
- ✅ **Multi-Environment Support** - DEV, UAT, and PROD environments
- ✅ **Service Principal Authentication** - Secure, token-free authentication
- ✅ **Disaster Recovery Backups** - Automated full workspace backups
- ✅ **Sync with Deletion** - Remove deleted files from workspace
- ✅ **Bundle Policy Validation** - Enforce coding standards
- ✅ **Dynamic Configuration Management** - Environment-specific configurations

### Technology Stack
- **CI/CD Platform**: GitHub Actions
- **IaC Framework**: Databricks Asset Bundles (DAB)
- **CLI**: Databricks CLI (v0.200.0+)
- **Scripting**: PowerShell, Python, Bash
- **Authentication**: Azure Service Principal (OAuth 2.0)
- **Version Control**: Git with orphan branches for backups

---

## Architecture

### Repository Structure
```
amalDatabricks_Demo_01/
├── .github/
│   └── workflows/
│       ├── main.yml                    # DAB Deployment Pipeline
│       ├── dr-backup.yml               # DR Backup Pipeline
│       ├── prerequisites.py            # Backup script
│       ├── sync-with-deletion.ps1      # Workspace sync script
│       └── validate-bundle.ps1         # Bundle policy validator
├── AMALDAB/
│   ├── databricks.yml                  # DAB configuration
│   ├── DataPlatform/
│   │   └── General/
│   │       └── NB_Configuration.py     # Environment config
│   └── resources/
│       ├── jobs/                       # Job definitions
│       ├── SharedObjects/              # Clusters, warehouses, policies
│       └── uc_ddl/                     # Unity Catalog DDL
└── docs/
    ├── CICD-Documentation.md           # This file
    ├── DR-Runbook.md                   # Disaster recovery guide
    └── Workflow-Diagram.md             # Visual workflow diagram
```

### Environment Mapping
| Git Branch | Environment | Mode | Workspace URL |
|------------|-------------|------|---------------|
| `dev` | DEV | development | https://adb-201068313543333.13.azuredatabricks.net |
| `qa` / `uat` | UAT | production | https://adb-201068313543333.13.azuredatabricks.net |
| `main` | PROD | production | https://adb-201068313543333.13.azuredatabricks.net |

---

## Workflows

### 1. DAB Deployment Pipeline (`main.yml`)

#### Workflow File
`.github/workflows/main.yml`

#### Triggers
```yaml
# Manual trigger with environment selection
workflow_dispatch:
  inputs:
    environment:
      description: 'Target environment'
      required: true
      type: choice
      options:
        - DEV
        - UAT
        - PROD

# Automatic trigger on push
push:
  branches:
    - dev      # Triggers DEV deployment
    - qa       # Triggers UAT deployment
    - uat      # Triggers UAT deployment
    - main     # Triggers PROD deployment
  paths:
    - 'AMALDAB/**'
    - '.github/workflows/main.yml'
```

#### Jobs Overview

##### Job 1: Unit Testing
**Purpose**: Validate bundle configuration before deployment

**Steps**:
1. Checkout code
2. Setup Python environment
3. Install dependencies
4. Run bundle policy validation (`validate-bundle.ps1`)

**Validation Rules**:
- ✅ Rule 1: `is_test` parameter must NOT default to "Yes"
- ✅ Rule 2: Cluster names must match pattern `^[a-zA-Z0-9_-]+$`
- ✅ Rule 3: No environment names (dev/qa/prod) in configuration values

**Exit Criteria**: All validation rules pass

##### Job 2: Configuration Management
**Purpose**: Update environment-specific configuration file

**Steps**:
1. Detect environment from branch or manual input
2. Read `NB_Configuration.py`
3. Apply regex replacements for environment-specific values:
   - Key Vault scope
   - Catalog name
   - Logger directory
   - Axiom storage paths
   - Axiom volume paths
4. Commit updated configuration (if changes detected)

**Configuration Mapping**:

| Variable | DEV | UAT | PROD |
|----------|-----|-----|------|
| Key Vault Scope | `kv-az-dbricks-dev-001` | `kv-az-dbricks-uat-001` | `kv-az-dbricks-prod-001` |
| Catalog | `ab_dev_catalog` | `ab_uat_catalog` | `ab_prod_catalog` |
| Logger Directory | `/Volumes/ab_dev_catalog/config/logger` | `/Volumes/ab_uat_catalog/config/logger` | `/Volumes/ab_prod_catalog/config/logger` |
| Axiom Archive | `abfss://dev-external-location@...` | `abfss://uat-external-location@...` | `abfss://prod-external-location@...` |
| Axiom Input | `abfss://dev-external-location@...` | `abfss://uat-external-location@...` | `abfss://prod-external-location@...` |
| Axiom Volume | `/Volumes/ab_dev_catalog/config/axiom/` | `/Volumes/ab_uat_catalog/config/axiom/` | `/Volumes/ab_prod_catalog/config/axiom/` |

##### Job 3: DAB Operations
**Purpose**: Deploy Databricks Asset Bundle to target environment

**Steps**:

1. **Environment Detection**
   ```powershell
   $env:TARGET_ENV = switch ($env:GITHUB_REF_NAME) {
     'dev' { 'DEV' }
     { $_ -in 'qa', 'uat' } { 'UAT' }
     'main' { 'PROD' }
     default { $env:MANUAL_ENV }
   }
   ```

2. **Service Principal Authentication**
   ```powershell
   # Create Databricks config file
   $configContent = @"
   [DEFAULT]
   host = $env:DATABRICKS_HOST
   auth_type = oauth-m2m
   client_id = $env:DATABRICKS_CLIENT_ID
   client_secret = $env:DATABRICKS_CLIENT_SECRET
   "@

   Set-Content -Path "$env:USERPROFILE\.databrickscfg" -Value $configContent
   ```

3. **Install Databricks CLI**
   ```powershell
   # Download latest CLI from GitHub releases
   $cliUrl = "https://github.com/databricks/cli/releases/download/v0.234.0/databricks_cli_0.234.0_windows_amd64.zip"
   Invoke-WebRequest -Uri $cliUrl -OutFile "databricks_cli.zip"
   Expand-Archive -Path "databricks_cli.zip" -DestinationPath "C:\databricks-cli"
   $env:PATH = "C:\databricks-cli;$env:PATH"
   ```

4. **Dynamic Workspace Path Resolution**
   ```powershell
   # Load databricks.yml to get workspace path configuration
   $yamlContent = Get-Content "AMALDAB/databricks.yml" -Raw | ConvertFrom-Yaml
   $targetConfig = $yamlContent.targets[$env:TARGET_ENV]
   $rootPathTemplate = $targetConfig.workspace.root_path

   # Replace template variables
   $workspacePath = $rootPathTemplate `
     -replace '\$\{workspace\.current_user\.userName\}', $env:DATABRICKS_CLIENT_ID `
     -replace '\$\{bundle\.name\}', $yamlContent.bundle.name `
     -replace '\$\{bundle\.target\}', $env:TARGET_ENV
   ```

5. **Bundle Validation**
   ```bash
   cd AMALDAB
   databricks bundle validate -t $TARGET_ENV
   ```

6. **Bundle Deployment**
   ```bash
   databricks bundle deploy -t $TARGET_ENV --force-lock
   ```

**Validation Checks**:
- ✅ Bundle-level variable declarations exist
- ✅ Development mode uses username in paths
- ✅ Production mode uses static paths
- ✅ All referenced variables are defined

##### Job 4: Sync with Deletion
**Purpose**: Remove files from workspace that were deleted from source control

**Steps**:

1. **Resolve Workspace Path** (from databricks.yml)
2. **Run Sync Script**
   ```powershell
   .\.github\workflows\sync-with-deletion.ps1 `
     -Environment $env:TARGET_ENV `
     -SourceDir "AMALDAB/DataPlatform" `
     -WorkspacePath $workspacePath
   ```

3. **Sync Logic**:
   - List all files in workspace directory
   - Compare with source directory
   - Delete files that exist in workspace but not in source
   - Preserve files not managed by source control

**Safety Features**:
- ✅ Dry-run mode available
- ✅ Detailed logging of deletions
- ✅ Excludes system directories
- ✅ Confirmation prompts (in manual mode)

---

### 2. DR Backup Pipeline (`dr-backup.yml`)

#### Workflow File
`.github/workflows/dr-backup.yml`

#### Triggers
```yaml
# Manual trigger with environment selection
workflow_dispatch:
  inputs:
    environment:
      description: 'Environment to backup'
      required: true
      type: choice
      options:
        - DEV
        - QA
        - PROD
        - ALL

# Scheduled backup (daily at 2 AM UTC)
schedule:
  - cron: '0 2 * * *'  # All environments
```

#### Jobs Overview

The DR backup workflow runs **three parallel jobs** (one per environment):

##### Job 1: Backup DEV Environment
##### Job 2: Backup QA Environment
##### Job 3: Backup PROD Environment

Each job follows the same process:

**Step 1: Setup Authentication**
```bash
# Service Principal credentials from GitHub secrets
export DATABRICKS_HOST="${{ vars.DEV_DATABRICKS_HOST }}"
export CLIENT_ID="${{ vars.DEV_CLIENT_ID }}"
export CLIENT_SECRET="${{ secrets.DEV_CLIENT_SECRET }}"
```

**Step 2: Create Backup Directory**
```bash
TIMESTAMP=$(date -u +"%Y-%m-%d-%H-%M")
BACKUP_DIR="dr-backups/DEV"
mkdir -p "${BACKUP_DIR}"
```

**Step 3: Run Full Backup Script**
```bash
python .github/workflows/prerequisites.py \
  --DATABRICKS_HOST "${DATABRICKS_HOST}" \
  --CLIENT_ID "${CLIENT_ID}" \
  --CLIENT_SECRET "${CLIENT_SECRET}" \
  --full-backup \
  --output-dir "${BACKUP_DIR}"
```

**Backup Components**:

1. **Infrastructure** (`SharedObjects/`):
   - All-purpose clusters (`all_purpose_clusters.yml`)
   - SQL warehouses (`sql_warehouses.yml`)
   - Cluster policies (`cluster_policies.yml`)
   - Storage credentials (`storage_credentials.yml`)
   - External locations (`external_locations.yml`)
   - Connections (`connections.yml`)

2. **Jobs** (`jobs/`):
   - Each job as separate file: `{job_name}.job.yml`
   - Includes task definitions, schedules, notifications
   - Git source configurations
   - Cluster configurations

3. **Unity Catalog** (`uc_ddl/`):
   - Catalogs, schemas, tables, volumes DDL (`00_catalogs_schemas_tables_volumes.ddl.sql`)
   - All grants and permissions (`99_grants.ddl.sql`)

4. **Metadata**:
   - `backup-metadata.json` - Backup timestamp, environment, triggered by
   - `README.md` - Restoration instructions

**Step 4: Create Backup Metadata**
```json
{
  "environment": "DEV",
  "timestamp": "2026-01-19-14-30",
  "backup_date": "2026-01-19 14:30:00 UTC",
  "workspace_url": "https://adb-201068313543333.13.azuredatabricks.net",
  "triggered_by": "github-actions[bot]",
  "workflow_run_id": "12345678",
  "git_commit": "abc123def456",
  "backup_type": "full",
  "branch_name": "dr-backups/DEV"
}
```

**Step 5: Commit to Backup Branch**
```bash
# Check if branch exists
BRANCH_NAME="dr-backups/DEV"
if git ls-remote --heads origin "${BRANCH_NAME}" | grep -q "${BRANCH_NAME}"; then
  # Branch exists - checkout and update
  git fetch origin "${BRANCH_NAME}"
  git checkout "${BRANCH_NAME}"
  git rm -rf . 2>/dev/null || true
else
  # Create new orphan branch
  git checkout --orphan "${BRANCH_NAME}"
  git rm -rf . 2>/dev/null || true
fi

# Copy backup files to root
cp -r "${BACKUP_DIR}"/* .
git add -A

# Commit with detailed message
git commit -m "DR Backup DEV ${TIMESTAMP}" \
  -m "Environment: DEV" \
  -m "Timestamp: ${TIMESTAMP}" \
  -m "[skip ci]"

# Push to remote
git push origin "${BRANCH_NAME}" --force
```

**Step 6: Cleanup**
```bash
# Return to original branch
git checkout ${{ github.ref_name }}

# Remove all backup artifacts
rm -rf dr-backups/
rm -rf jobs/ uc_ddl/ SharedObjects/ backup-metadata.json README.md
```

**Backup Branch Structure**:
```
Repository Branches:
├── main                    # Production code
├── dev                     # Development code
├── qa                      # QA code
├── dr-backups/DEV          # DEV backups (orphan branch)
├── dr-backups/QA           # QA backups (orphan branch)
└── dr-backups/PROD         # PROD backups (orphan branch)
```

**Backup History**:
- Each backup run creates a **new commit** on the same branch
- Git history preserves all previous backups
- Access older backups via `git log` and `git checkout <commit-hash>`

##### Job 4: Backup Summary
**Purpose**: Aggregate results from all backup jobs

**Steps**:
1. Check status of all backup jobs
2. Generate summary report
3. Send notifications (if configured)

---

## Authentication & Security

### Service Principal Authentication

#### Why Service Principal?
- ✅ **No PAT Token Expiration** - OAuth tokens refresh automatically
- ✅ **Consistent Identity** - Same identity across all deployments
- ✅ **Audit Trail** - All actions attributed to Service Principal
- ✅ **Separation of Concerns** - Deployment identity ≠ Runtime identity
- ✅ **CI/CD Best Practice** - Recommended by Databricks

#### Authentication Flow
```
GitHub Actions
    ↓
Service Principal (Deployment Identity)
    ↓ OAuth 2.0 M2M
Databricks Workspace
    ↓
Resources run as Service Principal (Runtime Identity)
```

#### Two Service Principals

1. **Deployment Identity** (`8f58ed47-fbbe-4c2d-ba65-973b82ab6371`)
   - **Purpose**: Deploys DAB bundles to workspace
   - **Configured in**: GitHub Actions secrets
   - **Permissions**: Workspace admin, can create/update resources

2. **Runtime Identity** (`a7923e63-e14c-4c32-87cc-e89e3b4a165a`)
   - **Purpose**: Runs jobs and notebooks
   - **Configured in**: `databricks.yml` (`run_as` section)
   - **Permissions**: Data access, job execution

#### GitHub Secrets Configuration

**Required Secrets per Environment**:

| Secret Name | Type | Description | Example |
|-------------|------|-------------|---------|
| `DEV_CLIENT_ID` | Variable | Service Principal Application ID | `8f58ed47-fbbe-4c2d-ba65-973b82ab6371` |
| `DEV_CLIENT_SECRET` | Secret | Service Principal Secret | `***` (encrypted) |
| `DEV_DATABRICKS_HOST` | Variable | Workspace URL | `https://adb-xxx.azuredatabricks.net` |
| `UAT_CLIENT_ID` | Variable | Service Principal Application ID | `...` |
| `UAT_CLIENT_SECRET` | Secret | Service Principal Secret | `***` |
| `UAT_DATABRICKS_HOST` | Variable | Workspace URL | `...` |
| `PROD_CLIENT_ID` | Variable | Service Principal Application ID | `...` |
| `PROD_CLIENT_SECRET` | Secret | Service Principal Secret | `***` |
| `PROD_DATABRICKS_HOST` | Variable | Workspace URL | `...` |

**Setting Secrets in GitHub**:
1. Navigate to repository → Settings → Secrets and variables → Actions
2. Click "New repository secret" or "New repository variable"
3. Enter name and value
4. Click "Add secret" or "Add variable"

#### Databricks Configuration File

The workflow creates `.databrickscfg` dynamically:

```ini
[DEFAULT]
host = https://adb-201068313543333.13.azuredatabricks.net
auth_type = oauth-m2m
client_id = 8f58ed47-fbbe-4c2d-ba65-973b82ab6371
client_secret = ***
```

**Location**: `$env:USERPROFILE\.databrickscfg` (Windows) or `~/.databrickscfg` (Linux/Mac)

**Security Notes**:
- ✅ File created at runtime, never committed to Git
- ✅ Deleted after workflow completes
- ✅ Secrets masked in logs
- ✅ Only accessible to workflow runner

---

## Environment Configuration

### databricks.yml Structure

The `AMALDAB/databricks.yml` file is the central configuration for all environments.

#### Bundle-Level Configuration

```yaml
bundle:
  name: amaldatabricks
  uuid: 98bee946-7642-4fd2-beb9-d89c27ed0c2c

include:
  - resources/**/*.yml
  - resources/*.yml

artifacts:
  notebooks:
    files:
      - source: DataPlatform/**/*.py

# Bundle-level variable declarations (required)
variables:
  catalog_name:
    description: "Unity Catalog name"
  key_vault_scope:
    description: "Azure Key Vault scope name"
  logger_directory:
    description: "Logger directory path in Unity Catalog volumes"
  axiom_archive:
    description: "Axiom archive storage path (ABFSS)"
  axiom_input:
    description: "Axiom input storage path (ABFSS)"
  axiom_volume:
    description: "Axiom volume path in Unity Catalog"
  email_notifications:
    description: "Email address for job notifications"
  git_branch:
    description: "Git branch name"
```

#### Target-Specific Configuration

##### DEV Environment
```yaml
targets:
  DEV:
    mode: development  # Requires username in paths
    default: true

    run_as:
      service_principal_name: a7923e63-e14c-4c32-87cc-e89e3b4a165a

    presets:
      name_prefix: '[${workspace.current_user.short_name}]'
      tags:
        amal_owner: DataEngineering
        Environment: Dev

    variables:
      catalog_name:
        default: ab_dev_catalog
      key_vault_scope:
        default: kv-az-dbricks-dev-001
      logger_directory:
        default: /Volumes/ab_dev_catalog/config/logger
      git_branch:
        default: dev
      axiom_archive:
        default: abfss://dev-external-location@stgazdbricksdev001.dfs.core.windows.net/archive/axiom/
      axiom_input:
        default: abfss://dev-external-location@stgazdbricksdev001.dfs.core.windows.net/input/axiom/
      axiom_volume:
        default: /Volumes/ab_dev_catalog/config/axiom/
      email_notifications:
        default: pandi.anbu@zeb.co

    workspace:
      host: https://adb-201068313543333.13.azuredatabricks.net
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

    permissions:
      - group_name: DataEngineering
        level: CAN_MANAGE
```

### Workspace Path Resolution

The workflow dynamically resolves workspace paths from `databricks.yml`:

```powershell
# Load YAML configuration
$yamlContent = Get-Content "AMALDAB/databricks.yml" -Raw | ConvertFrom-Yaml
$targetConfig = $yamlContent.targets[$env:TARGET_ENV]
$rootPathTemplate = $targetConfig.workspace.root_path

# Replace template variables
$workspacePath = $rootPathTemplate `
  -replace '\$\{workspace\.current_user\.userName\}', $currentUser `
  -replace '\$\{bundle\.name\}', $bundleName `
  -replace '\$\{bundle\.target\}', $env:TARGET_ENV
```

**Resolved Paths**:
- **DEV**: `/Workspace/Users/8f58ed47-fbbe-4c2d-ba65-973b82ab6371/.bundle/amaldatabricks/DEV`
- **UAT**: `/Workspace/uat`
- **PROD**: `/Workspace/PROD`

---

## Troubleshooting

### Common Issues & Solutions

#### 1. Bundle Validation Fails
**Error**: `variable axiom_volume is not defined but is assigned a value`

**Solution**: Add variable declaration in `databricks.yml`:
```yaml
variables:
  axiom_volume:
    description: "Axiom volume path"
```

#### 2. Authentication Failure
**Error**: `failed to authenticate: invalid client credentials`

**Solution**: Verify GitHub secrets and Service Principal credentials

#### 3. DR Backup Branch Conflict
**Error**: `cannot lock ref 'refs/heads/dr-backups/DEV'`

**Solution**: Delete old timestamp-based branches:
```bash
git push origin --delete dr-backups/DEV/2026-01-17-13-08
git fetch --prune
```

---

## Best Practices

### Development Workflow
1. ✅ Always work in feature branches
2. ✅ Test in DEV before promoting to UAT/PROD
3. ✅ Use pull requests for code review
4. ✅ Run bundle validation locally before pushing
5. ✅ Keep databricks.yml in sync across environments

### Security
1. ✅ Never commit secrets to Git
2. ✅ Rotate Service Principal secrets regularly
3. ✅ Use least-privilege access for Service Principals
4. ✅ Enable audit logging in Databricks
5. ✅ Review permissions regularly

### Backup & Recovery
1. ✅ Test DR procedures quarterly
2. ✅ Verify backups after each run
3. ✅ Document custom restoration procedures
4. ✅ Keep backup branches clean (no manual edits)
5. ✅ Monitor backup workflow failures

---

## Appendix

### Useful Commands

#### Databricks CLI
```bash
# Validate bundle
databricks bundle validate -t DEV

# Deploy bundle
databricks bundle deploy -t DEV

# List workspace files
databricks workspace list /Workspace/DEV

# Export notebook
databricks workspace export /path/to/notebook.py
```

#### Git Operations
```bash
# View backup branches
git branch -r | grep dr-backups

# Checkout backup
git checkout dr-backups/DEV

# View backup history
git log --oneline --graph

# Compare backups
git diff <commit1> <commit2>
```

### Support & Contacts

| Role | Contact | Responsibility |
|------|---------|----------------|
| DevOps Lead | devops@company.com | CI/CD pipeline issues |
| Data Engineering Lead | data-eng@company.com | DAB configuration, jobs |
| Platform Admin | platform@company.com | Databricks workspace access |
| Security Team | security@company.com | Service Principal management |

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Maintained By**: Data Engineering Team

##### UAT Environment
```yaml
  uat:
    mode: production  # Static paths, no username required

    run_as:
      service_principal_name: a7923e63-e14c-4c32-87cc-e89e3b4a165a

    presets:
      name_prefix: '[UAT]'
      tags:
        amal_owner: DataEngineering
        Environment: UAT

    variables:
      catalog_name:
        default: ab_uat_catalog
      key_vault_scope:
        default: kv-az-dbricks-uat-001
      logger_directory:
        default: /Volumes/ab_uat_catalog/config/logger
      git_branch:
        default: uat
      axiom_archive:
        default: abfss://uat-external-location@stgazdbricksdev001.dfs.core.windows.net/archive/axiom/
      axiom_input:
        default: abfss://uat-external-location@stgazdbricksdev001.dfs.core.windows.net/input/axiom/
      axiom_volume:
        default: /Volumes/ab_uat_catalog/config/axiom/
      email_notifications:
        default: pandi.anbu@zeb.co

    workspace:
      host: https://adb-201068313543333.13.azuredatabricks.net
      root_path: /Workspace/${bundle.target}
```

##### PROD Environment
```yaml
  PROD:
    mode: production

    run_as:
      service_principal_name: a7923e63-e14c-4c32-87cc-e89e3b4a165a

    presets:
      name_prefix: '[PROD]'
      tags:
        amal_owner: DataEngineering
        Environment: Production

    variables:
      catalog_name:
        default: ab_prod_catalog
      key_vault_scope:
        default: kv-az-dbricks-prod-001
      logger_directory:
        default: /Volumes/ab_prod_catalog/config/logger
      git_branch:
        default: main
      axiom_archive:
        default: abfss://prod-external-location@stgazdbricksdev001.dfs.core.windows.net/archive/axiom/
      axiom_input:
        default: abfss://prod-external-location@stgazdbricksdev001.dfs.core.windows.net/input/axiom/
      axiom_volume:
        default: /Volumes/ab_prod_catalog/config/axiom/
      email_notifications:
        default: pandi.anbu@zeb.co

    workspace:
      host: https://adb-201068313543333.13.azuredatabricks.net
      root_path: /Workspace/${bundle.target}
```

#### Key Differences: Development vs Production Mode

| Aspect | Development Mode | Production Mode |
|--------|------------------|-----------------|
| **Path Format** | `/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}` | `/Workspace/${bundle.target}` |
| **Name Prefix** | `[${workspace.current_user.short_name}]` | `[UAT]` or `[PROD]` |
| **Uniqueness** | Per-user isolation | Shared workspace |
| **Use Case** | Individual development | Team collaboration, production |
| **Permissions** | User-specific | Group-based |

---

## Deployment Process

### Manual Deployment

#### Step 1: Trigger Workflow
1. Navigate to GitHub repository
2. Click **Actions** tab
3. Select **"Databricks DAB Deployment"** workflow
4. Click **"Run workflow"**
5. Select environment (DEV/UAT/PROD)
6. Click **"Run workflow"** button

#### Step 2: Monitor Progress
1. Click on the running workflow
2. Expand each job to see detailed logs
3. Monitor for errors or warnings

#### Step 3: Verify Deployment
1. Log into Databricks workspace
2. Navigate to workspace path (from databricks.yml)
3. Verify resources are deployed:
   - Jobs in **Workflows** section
   - Clusters in **Compute** section
   - SQL Warehouses in **SQL Warehouses** section
   - Unity Catalog objects in **Data** section

### Automatic Deployment (Push-based)

#### Workflow
```
Developer pushes to branch
    ↓
GitHub detects push event
    ↓
Determines environment from branch name
    ↓
Triggers deployment workflow
    ↓
Deploys to corresponding environment
```

#### Branch Mapping
- Push to `dev` → Deploys to **DEV**
- Push to `qa` or `uat` → Deploys to **UAT**
- Push to `main` → Deploys to **PROD**

#### Safety Measures
- ✅ Bundle validation before deployment
- ✅ Policy validation (coding standards)
- ✅ Configuration validation
- ✅ Automatic rollback on failure (via Git revert)

---

## Disaster Recovery Backup

### Backup Schedule

**Automated Backups**:
- **Frequency**: Daily at 2:00 AM UTC
- **Environments**: All (DEV, QA, PROD)
- **Retention**: Unlimited (Git history)

**Manual Backups**:
- Trigger via GitHub Actions
- Select specific environment or all

### Backup Storage

**Git Branches**:
- `dr-backups/DEV` - DEV environment backups
- `dr-backups/QA` - QA environment backups
- `dr-backups/PROD` - PROD environment backups

**Branch Type**: Orphan branches (no parent commits, isolated history)

**Backup Structure**:
```
dr-backups/DEV/
├── SharedObjects/
│   ├── all_purpose_clusters.yml
│   ├── sql_warehouses.yml
│   ├── cluster_policies.yml
│   ├── storage_credentials.yml
│   ├── external_locations.yml
│   └── connections.yml
├── jobs/
│   ├── job_name_1.job.yml
│   ├── job_name_2.job.yml
│   └── job_name_3.job.yml
├── uc_ddl/
│   ├── 00_catalogs_schemas_tables_volumes.ddl.sql
│   └── 99_grants.ddl.sql
├── backup-metadata.json
└── README.md
```

### Accessing Backups

#### View Latest Backup
```bash
# Checkout backup branch
git checkout dr-backups/DEV

# View files
ls -la
```

#### View Backup History
```bash
# Checkout backup branch
git checkout dr-backups/DEV

# View all backup commits
git log --oneline

# Example output:
# abc1234 DR Backup DEV 2026-01-19-14-30
# def5678 DR Backup DEV 2026-01-18-02-00
# ghi9012 DR Backup DEV 2026-01-17-02-00
```

#### Restore from Specific Backup
```bash
# Checkout backup branch
git checkout dr-backups/DEV

# View commit history
git log --oneline

# Checkout specific backup
git checkout <commit-hash>

# Example:
git checkout def5678

# View files from that backup
ls -la
```

### Backup Verification

**Automated Checks**:
- ✅ Backup script exit code
- ✅ File count validation
- ✅ Metadata file creation
- ✅ Git commit success
- ✅ Git push success

**Manual Verification**:
1. Checkout backup branch
2. Verify file structure
3. Check backup-metadata.json
4. Review README.md
5. Validate DDL syntax (optional)

---

## Troubleshooting

### Common Issues

#### Issue 1: Bundle Validation Fails

**Error**:
```
Error: variable axiom_volume is not defined but is assigned a value
```

**Cause**: Variable used in resources but not declared at bundle level

**Solution**:
1. Open `AMALDAB/databricks.yml`
2. Add variable declaration in `variables:` section:
   ```yaml
   variables:
     axiom_volume:
       description: "Axiom volume path in Unity Catalog"
   ```
3. Commit and push changes

---

#### Issue 2: Authentication Failure

**Error**:
```
Error: failed to authenticate: invalid client credentials
```

**Cause**: Invalid or expired Service Principal credentials

**Solution**:
1. Verify GitHub secrets are set correctly:
   - `DEV_CLIENT_ID` / `UAT_CLIENT_ID` / `PROD_CLIENT_ID`
   - `DEV_CLIENT_SECRET` / `UAT_CLIENT_SECRET` / `PROD_CLIENT_SECRET`
2. Verify Service Principal exists in Azure AD
3. Verify Service Principal has workspace access
4. Regenerate client secret if expired

---

#### Issue 3: Deployment Path Conflict

**Error**:
```
Error: prefix should contain the current username or ${workspace.current_user.short_name}
```

**Cause**: Development mode requires username in paths for uniqueness

**Solution**:
1. For DEV environment, use:
   ```yaml
   presets:
     name_prefix: '[${workspace.current_user.short_name}]'
   workspace:
     root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
   ```
2. For UAT/PROD, use production mode with static paths

