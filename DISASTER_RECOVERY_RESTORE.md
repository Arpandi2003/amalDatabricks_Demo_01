# 🔄 Disaster Recovery Restore Guide

## 🚨 Quick Start - Emergency Restore

If you need to restore your Databricks workspace **RIGHT NOW**, follow these steps:

### 1️⃣ Find Latest Backup (30 seconds)

```bash
# Clone the repository (if not already)
git clone <your-repo-url>
cd <repo-directory>

# List all available backups
git branch -r | grep dr-backups/PROD | sort -r | head -5

# Example output:
# origin/dr-backups/PROD/2026-01-17-02-00  ← Latest
# origin/dr-backups/PROD/2026-01-16-02-00
# origin/dr-backups/PROD/2026-01-15-02-00
```

### 2️⃣ Checkout the Backup (10 seconds)

```bash
# Checkout the latest backup
git checkout dr-backups/PROD/2026-01-17-02-00

# Verify backup metadata
cat backup-metadata.json
```

### 3️⃣ Restore Unity Catalog (5 minutes)

```bash
# Install Databricks CLI (if not installed)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configure authentication
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi123..."

# Restore Unity Catalog objects in order
databricks sql execute -f uc_ddl/catalogs.sql
databricks sql execute -f uc_ddl/schemas.sql
databricks sql execute -f uc_ddl/tables.sql
databricks sql execute -f uc_ddl/volumes.sql
databricks sql execute -f uc_ddl/functions.sql
databricks sql execute -f uc_ddl/grants.sql
```

### 4️⃣ Restore Infrastructure (10 minutes)

```bash
# Option A: Using Databricks Asset Bundles (Recommended)
# Copy backup to your bundle directory
cp -r SharedObjects/* ./AMALDAB/resources/SharedObjects/
cp -r jobs/* ./AMALDAB/resources/jobs/

# Deploy using bundle
databricks bundle deploy -t prod

# Option B: Using REST API (see detailed script below)
```

---

## 📋 Detailed Restore Procedures

### Restore Unity Catalog Objects

Unity Catalog objects are backed up as SQL DDL statements. Restore them in this order:

#### Order of Restoration

```
1. Catalogs       (uc_ddl/catalogs.sql)
2. Schemas        (uc_ddl/schemas.sql)
3. Tables         (uc_ddl/tables.sql)
4. Volumes        (uc_ddl/volumes.sql)
5. Functions      (uc_ddl/functions.sql)
6. Grants         (uc_ddl/grants.sql)
```

**Why this order?** Each level depends on the previous one:
- Schemas need catalogs to exist
- Tables need schemas to exist
- Grants need objects to exist

#### Restore Script

```bash
#!/bin/bash
# restore_unity_catalog.sh

set -e  # Exit on error

BACKUP_BRANCH="dr-backups/PROD/2026-01-17-02-00"
DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
DATABRICKS_TOKEN="dapi123..."

echo "🔄 Starting Unity Catalog restore from ${BACKUP_BRANCH}"

# Checkout backup
git checkout "${BACKUP_BRANCH}"

# Restore in order
echo "📦 Restoring catalogs..."
databricks sql execute -f uc_ddl/catalogs.sql

echo "📦 Restoring schemas..."
databricks sql execute -f uc_ddl/schemas.sql

echo "📦 Restoring tables..."
databricks sql execute -f uc_ddl/tables.sql

echo "📦 Restoring volumes..."
databricks sql execute -f uc_ddl/volumes.sql

echo "📦 Restoring functions..."
databricks sql execute -f uc_ddl/functions.sql

echo "📦 Restoring grants..."
databricks sql execute -f uc_ddl/grants.sql

echo "✅ Unity Catalog restore completed!"
```

### Restore Clusters

Clusters are backed up as YAML configurations. Restore using Databricks CLI or API.

#### Using Databricks Asset Bundles

```bash
# 1. Copy cluster configs to bundle
cp SharedObjects/all_purpose_clusters.yml ./AMALDAB/resources/SharedObjects/

# 2. Deploy using bundle
databricks bundle deploy -t prod

# 3. Verify clusters
databricks clusters list
```

#### Using REST API (Python)

```python
#!/usr/bin/env python3
# restore_clusters.py

import requests
import yaml
import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def restore_clusters(backup_file):
    """Restore clusters from backup YAML file."""
    
    # Load backup
    with open(backup_file) as f:
        data = yaml.safe_load(f)
    
    clusters = data.get('resources', {}).get('clusters', {})
    
    print(f"🔄 Restoring {len(clusters)} clusters...")
    
    for name, config in clusters.items():
        print(f"  • Creating cluster: {name}")
        
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/clusters/create",
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            json=config
        )
        
        if response.status_code == 200:
            cluster_id = response.json().get('cluster_id')
            print(f"    ✅ Created: {cluster_id}")
        else:
            print(f"    ❌ Failed: {response.text}")
    
    print("✅ Cluster restore completed!")

if __name__ == "__main__":
    restore_clusters("SharedObjects/all_purpose_clusters.yml")
```

### Restore SQL Warehouses

```python
#!/usr/bin/env python3
# restore_warehouses.py

import requests
import yaml
import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def restore_warehouses(backup_file):
    """Restore SQL warehouses from backup YAML file."""
    
    with open(backup_file) as f:
        data = yaml.safe_load(f)
    
    warehouses = data.get('resources', {}).get('sql_warehouses', {})
    
    print(f"🔄 Restoring {len(warehouses)} SQL warehouses...")
    
    for name, config in warehouses.items():
        print(f"  • Creating warehouse: {name}")
        
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/warehouses",
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            json=config
        )
        
        if response.status_code == 200:
            warehouse_id = response.json().get('id')
            print(f"    ✅ Created: {warehouse_id}")
        else:
            print(f"    ❌ Failed: {response.text}")
    
    print("✅ Warehouse restore completed!")

if __name__ == "__main__":
    restore_warehouses("SharedObjects/sql_warehouses.yml")
```

### Restore Jobs

```python
#!/usr/bin/env python3
# restore_jobs.py

import requests
import yaml
import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def restore_jobs(backup_file):
    """Restore jobs from backup YAML file."""
    
    with open(backup_file) as f:
        data = yaml.safe_load(f)
    
    jobs = data.get('resources', {}).get('jobs', {})
    
    print(f"🔄 Restoring {len(jobs)} jobs...")
    
    for name, config in jobs.items():
        print(f"  • Creating job: {name}")
        
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/create",
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            json=config
        )
        
        if response.status_code == 200:
            job_id = response.json().get('job_id')
            print(f"    ✅ Created: {job_id}")
        else:
            print(f"    ❌ Failed: {response.text}")
    
    print("✅ Job restore completed!")

if __name__ == "__main__":
    restore_jobs("jobs/all_jobs.yml")
```

---

## 🎯 Complete Restore Script

Here's a complete script that restores everything:

```bash
#!/bin/bash
# complete_restore.sh - Restore entire Databricks workspace from backup

set -e  # Exit on error

# ============================================================================
# CONFIGURATION
# ============================================================================
BACKUP_BRANCH="${1:-dr-backups/PROD/2026-01-17-02-00}"
DATABRICKS_HOST="${DATABRICKS_HOST:-https://adb-xxx.azuredatabricks.net}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN}"

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "❌ Error: DATABRICKS_TOKEN environment variable not set"
    exit 1
fi

echo "============================================================"
echo "🔄 DATABRICKS DISASTER RECOVERY RESTORE"
echo "============================================================"
echo "📍 Workspace: ${DATABRICKS_HOST}"
echo "📦 Backup: ${BACKUP_BRANCH}"
echo "============================================================"

# ============================================================================
# STEP 1: Checkout Backup
# ============================================================================
echo ""
echo "📥 Step 1: Checking out backup..."
git checkout "${BACKUP_BRANCH}"

# Verify backup metadata
if [ -f "backup-metadata.json" ]; then
    echo "✅ Backup metadata found:"
    cat backup-metadata.json | python3 -m json.tool
else
    echo "⚠️  Warning: No backup metadata found"
fi

# ============================================================================
# STEP 2: Restore Unity Catalog
# ============================================================================
echo ""
echo "📦 Step 2: Restoring Unity Catalog..."

if [ -d "uc_ddl" ]; then
    for ddl_file in uc_ddl/catalogs.sql uc_ddl/schemas.sql uc_ddl/tables.sql uc_ddl/volumes.sql uc_ddl/functions.sql uc_ddl/grants.sql; do
        if [ -f "$ddl_file" ]; then
            echo "  • Executing: $ddl_file"
            databricks sql execute -f "$ddl_file" || echo "    ⚠️  Warning: Some statements may have failed"
        fi
    done
    echo "✅ Unity Catalog restored"
else
    echo "⚠️  No Unity Catalog DDL found"
fi

# ============================================================================
# STEP 3: Restore Infrastructure
# ============================================================================
echo ""
echo "🏗️  Step 3: Restoring infrastructure..."

# Option: Use Databricks Asset Bundles
if command -v databricks &> /dev/null; then
    echo "  • Using Databricks Asset Bundles..."
    
    # Copy backup files to bundle directory
    cp -r SharedObjects/* ./AMALDAB/resources/SharedObjects/ 2>/dev/null || true
    cp -r jobs/* ./AMALDAB/resources/jobs/ 2>/dev/null || true
    
    # Deploy bundle
    databricks bundle deploy -t prod
    
    echo "✅ Infrastructure restored via bundle"
else
    echo "⚠️  Databricks CLI not found. Use Python scripts to restore manually."
fi

# ============================================================================
# SUMMARY
# ============================================================================
echo ""
echo "============================================================"
echo "✅ RESTORE COMPLETED"
echo "============================================================"
echo "📋 Next Steps:"
echo "  1. Verify Unity Catalog objects in Databricks UI"
echo "  2. Check clusters are created and running"
echo "  3. Verify jobs are configured correctly"
echo "  4. Test SQL warehouses"
echo "  5. Review grants and permissions"
echo "============================================================"
```

---

## 🔍 Verification After Restore

### 1. Verify Unity Catalog

```sql
-- Check catalogs
SHOW CATALOGS;

-- Check schemas
SHOW SCHEMAS IN ab_prod_catalog;

-- Check tables
SHOW TABLES IN ab_prod_catalog.bronze;

-- Check volumes
SHOW VOLUMES IN ab_prod_catalog.bronze;
```

### 2. Verify Clusters

```bash
databricks clusters list
```

### 3. Verify Jobs

```bash
databricks jobs list
```

### 4. Verify SQL Warehouses

```bash
databricks sql warehouses list
```

---

## 📞 Support

If restore fails:
1. Check backup metadata for compatibility
2. Verify service principal has WRITE permissions
3. Review error logs for specific failures
4. Try restoring objects individually

---

**Last Updated**: 2026-01-17  
**Version**: 1.0.0

