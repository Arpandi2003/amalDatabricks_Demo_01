# Disaster Recovery Runbook

## 🚨 Emergency Contact Information

| Role | Contact | Phone | Email | Availability |
|------|---------|-------|-------|--------------|
| **Primary On-Call** | Data Engineering Lead | +1-XXX-XXX-XXXX | data-eng-oncall@company.com | 24/7 |
| **Secondary On-Call** | DevOps Lead | +1-XXX-XXX-XXXX | devops-oncall@company.com | 24/7 |
| **Databricks Support** | Databricks TAM | +1-XXX-XXX-XXXX | support@databricks.com | 24/7 (Premium) |
| **Azure Support** | Microsoft Support | +1-XXX-XXX-XXXX | azure-support@microsoft.com | 24/7 |
| **Management Escalation** | VP Engineering | +1-XXX-XXX-XXXX | vp-eng@company.com | Business Hours |

---

## 📋 Table of Contents

1. [Disaster Scenarios](#disaster-scenarios)
2. [Pre-Recovery Checklist](#pre-recovery-checklist)
3. [Accessing Backups](#accessing-backups)
4. [Recovery Procedures](#recovery-procedures)
5. [Verification Steps](#verification-steps)
6. [Rollback Procedures](#rollback-procedures)
7. [Post-Recovery Tasks](#post-recovery-tasks)
8. [Testing DR Procedures](#testing-dr-procedures)

---

## 🔥 Disaster Scenarios

### When to Initiate Disaster Recovery

#### Scenario 1: Complete Workspace Loss
**Symptoms**:
- Workspace inaccessible (404 or 500 errors)
- All jobs stopped
- Data unavailable

**Severity**: **CRITICAL** 🔴  
**Recovery Time Objective (RTO)**: 4 hours  
**Recovery Point Objective (RPO)**: 24 hours (last backup)

**Action**: Proceed to [Full Workspace Recovery](#full-workspace-recovery)

---

#### Scenario 2: Accidental Deletion of Critical Resources
**Symptoms**:
- Jobs deleted or misconfigured
- Clusters removed
- Unity Catalog objects dropped

**Severity**: **HIGH** 🟠  
**RTO**: 2 hours  
**RPO**: 24 hours

**Action**: Proceed to [Selective Resource Recovery](#selective-resource-recovery)

---

#### Scenario 3: Data Corruption
**Symptoms**:
- Tables contain incorrect data
- Schemas modified incorrectly
- Permissions revoked

**Severity**: **MEDIUM** 🟡  
**RTO**: 1 hour  
**RPO**: 24 hours

**Action**: Proceed to [Unity Catalog Recovery](#unity-catalog-recovery)

---

#### Scenario 4: Failed Deployment
**Symptoms**:
- DAB deployment broke existing jobs
- Configuration errors
- Jobs failing after deployment

**Severity**: **MEDIUM** 🟡  
**RTO**: 30 minutes  
**RPO**: Last known good state

**Action**: Proceed to [Rollback Deployment](#rollback-deployment)

---

## ✅ Pre-Recovery Checklist

Before starting recovery, complete this checklist:

- [ ] **Incident Declared**: Incident ticket created (JIRA/ServiceNow)
- [ ] **Stakeholders Notified**: Management, affected teams informed
- [ ] **Backup Identified**: Correct backup branch and commit identified
- [ ] **Access Verified**: GitHub access, Databricks workspace access confirmed
- [ ] **Tools Ready**: Git, Databricks CLI, Python installed
- [ ] **Service Principal Credentials**: Available and valid
- [ ] **Communication Channel**: Slack/Teams war room established
- [ ] **Change Approval**: Emergency change request approved (if required)
- [ ] **Backup Workspace** (Optional): Consider restoring to test workspace first

**Incident Commander**: ___________________________  
**Start Time**: ___________________________  
**Target Completion**: ___________________________

---

## 📦 Accessing Backups

### Step 1: Identify the Correct Backup

#### Option A: Use Latest Backup (Most Common)
```bash
# Clone repository (if not already cloned)
git clone https://github.com/Arpandi2003/amalDatabricks_Demo_01.git
cd amalDatabricks_Demo_01

# Checkout the backup branch for your environment
git checkout dr-backups/DEV    # For DEV
git checkout dr-backups/QA     # For QA
git checkout dr-backups/PROD   # For PROD

# Verify you're on the latest backup
git log --oneline -1
```

**Expected Output**:
```
abc1234 DR Backup DEV 2026-01-19-14-30
```

#### Option B: Use Specific Historical Backup
```bash
# Checkout backup branch
git checkout dr-backups/PROD

# View all available backups
git log --oneline --graph

# Example output:
# abc1234 DR Backup PROD 2026-01-19-02-00
# def5678 DR Backup PROD 2026-01-18-02-00
# ghi9012 DR Backup PROD 2026-01-17-02-00

# Checkout specific backup by commit hash
git checkout def5678

# Verify backup metadata
cat backup-metadata.json
```

### Step 2: Review Backup Contents

```bash
# List all backup files
ls -la

# Expected structure:
# SharedObjects/       - Infrastructure configs
# jobs/                - Job definitions
# uc_ddl/              - Unity Catalog DDL
# backup-metadata.json - Backup information
# README.md            - Restoration guide

# Review backup metadata
cat backup-metadata.json
```

**Sample Metadata**:
```json
{
  "environment": "PROD",
  "timestamp": "2026-01-19-02-00",
  "backup_date": "2026-01-19 02:00:00 UTC",
  "workspace_url": "https://adb-201068313543333.13.azuredatabricks.net",
  "triggered_by": "github-actions[bot]",
  "workflow_run_id": "12345678",
  "git_commit": "abc123def456",
  "backup_type": "full",
  "branch_name": "dr-backups/PROD"
}
```

### Step 3: Verify Backup Integrity

```bash
# Check file counts
echo "Jobs: $(ls jobs/*.yml 2>/dev/null | wc -l)"
echo "Shared Objects: $(ls SharedObjects/*.yml 2>/dev/null | wc -l)"
echo "UC DDL Files: $(ls uc_ddl/*.sql 2>/dev/null | wc -l)"

# Validate YAML syntax (optional)
for file in jobs/*.yml SharedObjects/*.yml; do
  python -c "import yaml; yaml.safe_load(open('$file'))" && echo "✅ $file" || echo "❌ $file"
done

# Validate SQL syntax (optional)
for file in uc_ddl/*.sql; do
  echo "Checking $file..."
  head -n 5 "$file"
done
```

---

## 🔧 Recovery Procedures

### Full Workspace Recovery

**Use Case**: Complete workspace loss or corruption

**Estimated Time**: 3-4 hours

**Prerequisites**:
- [ ] Backup identified and verified
- [ ] Service Principal credentials available
- [ ] Databricks CLI installed
- [ ] New or cleaned workspace available

#### Step 1: Setup Environment

```bash
# Set environment variables
export DATABRICKS_HOST="https://adb-201068313543333.13.azuredatabricks.net"
export DATABRICKS_CLIENT_ID="8f58ed47-fbbe-4c2d-ba65-973b82ab6371"
export DATABRICKS_CLIENT_SECRET="<your-secret>"

# Create Databricks config
cat > ~/.databrickscfg << EOF
[DEFAULT]
host = ${DATABRICKS_HOST}
auth_type = oauth-m2m
client_id = ${DATABRICKS_CLIENT_ID}
client_secret = ${DATABRICKS_CLIENT_SECRET}
EOF

# Verify authentication
databricks workspace list /
```

#### Step 2: Restore Unity Catalog Objects

```bash
# Navigate to backup directory
cd dr-backups/PROD  # or DEV/QA

# Review Unity Catalog DDL
cat uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql

# Execute DDL statements
databricks sql execute \
  --warehouse-id <your-warehouse-id> \
  --file uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql

# Restore grants
databricks sql execute \
  --warehouse-id <your-warehouse-id> \
  --file uc_ddl/99_grants.ddl.sql
```

**⚠️ Important Notes**:
- Review DDL before execution
- Some objects may already exist (handle errors gracefully)
- External tables require external locations to exist first
- Volumes require storage credentials

**Verification**:
```bash
# List catalogs
databricks catalogs list

# List schemas in catalog
databricks schemas list --catalog ab_prod_catalog

# List tables in schema
databricks tables list --catalog ab_prod_catalog --schema bronze
```

#### Step 3: Restore Infrastructure (Clusters, Warehouses, Policies)

```bash
# Copy backup to DAB resources directory
cp -r SharedObjects/* ../AMALDAB/resources/SharedObjects/

# Navigate to DAB directory
cd ../AMALDAB

# Validate bundle
databricks bundle validate -t PROD

# Deploy infrastructure
databricks bundle deploy -t PROD --force-lock
```

**Components Restored**:
- ✅ All-purpose clusters
- ✅ SQL warehouses
- ✅ Cluster policies
- ✅ Storage credentials
- ✅ External locations
- ✅ Connections

**Verification**:
```bash
# List clusters
databricks clusters list

# List SQL warehouses
databricks sql warehouses list

# List cluster policies
databricks cluster-policies list
```

#### Step 4: Restore Jobs

```bash
# Copy job definitions to DAB resources
cp -r jobs/* ../AMALDAB/resources/jobs/

# Navigate to DAB directory
cd ../AMALDAB

# Validate jobs
databricks bundle validate -t PROD

# Deploy jobs
databricks bundle deploy -t PROD --force-lock
```

**Verification**:
```bash
# List all jobs
databricks jobs list

# Get specific job details
databricks jobs get --job-id <job-id>

# Test run a job (optional)
databricks jobs run-now --job-id <job-id>
```

#### Step 5: Restore Notebooks and Code

```bash
# The DAB deployment already synced notebooks from DataPlatform/
# Verify notebooks are present

databricks workspace list /Workspace/PROD/

# If manual upload needed:
databricks workspace import-dir \
  ../AMALDAB/DataPlatform \
  /Workspace/PROD/DataPlatform \
  --overwrite
```

**Verification**:
```bash
# List workspace files
databricks workspace list /Workspace/PROD/DataPlatform

# Export a notebook to verify content
databricks workspace export /Workspace/PROD/DataPlatform/General/NB_Configuration.py
```

---

### Selective Resource Recovery

**Use Case**: Restore specific resources without full recovery

#### Restore Single Job

```bash
# Checkout backup branch
git checkout dr-backups/PROD

# Identify job file
ls jobs/

# Copy specific job to DAB resources
cp jobs/my_critical_job.job.yml ../AMALDAB/resources/jobs/

# Deploy only that job
cd ../AMALDAB
databricks bundle deploy -t PROD --force-lock

# Verify job
databricks jobs list | grep "my_critical_job"
```

#### Restore Single Cluster

```bash
# Checkout backup branch
git checkout dr-backups/PROD

# Copy cluster definition
cp SharedObjects/all_purpose_clusters.yml ../AMALDAB/resources/SharedObjects/

# Edit to keep only the needed cluster (optional)
# Then deploy
cd ../AMALDAB
databricks bundle deploy -t PROD --force-lock
```

#### Restore Specific Unity Catalog Objects

```bash
# Checkout backup branch
git checkout dr-backups/PROD

# Extract specific DDL from backup
grep -A 20 "CREATE SCHEMA my_schema" uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql > restore_schema.sql

# Execute DDL
databricks sql execute \
  --warehouse-id <warehouse-id> \
  --file restore_schema.sql

# Restore grants for that schema
grep "GRANT.*ON SCHEMA.*my_schema" uc_ddl/99_grants.ddl.sql > restore_grants.sql
databricks sql execute \
  --warehouse-id <warehouse-id> \
  --file restore_grants.sql
```

---

### Unity Catalog Recovery

**Use Case**: Restore Unity Catalog metadata after accidental deletion or corruption

#### Full Unity Catalog Restore

```bash
# Checkout backup
git checkout dr-backups/PROD

# Review DDL files
cat uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql
cat uc_ddl/99_grants.ddl.sql

# Execute in order
databricks sql execute \
  --warehouse-id <warehouse-id> \
  --file uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql

databricks sql execute \
  --warehouse-id <warehouse-id> \
  --file uc_ddl/99_grants.ddl.sql
```

#### Restore Specific Catalog

```bash
# Extract catalog DDL
grep -A 100 "CREATE CATALOG ab_prod_catalog" uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql > catalog.sql

# Execute
databricks sql execute --warehouse-id <warehouse-id> --file catalog.sql

# Restore grants
grep "GRANT.*ON CATALOG ab_prod_catalog" uc_ddl/99_grants.ddl.sql > catalog_grants.sql
databricks sql execute --warehouse-id <warehouse-id> --file catalog_grants.sql
```

#### Restore Specific Schema

```bash
# Extract schema DDL
grep -A 50 "CREATE SCHEMA.*bronze" uc_ddl/00_catalogs_schemas_tables_volumes.ddl.sql > schema.sql

# Execute
databricks sql execute --warehouse-id <warehouse-id> --file schema.sql

# Restore grants
grep "GRANT.*ON SCHEMA.*bronze" uc_ddl/99_grants.ddl.sql > schema_grants.sql
databricks sql execute --warehouse-id <warehouse-id> --file schema_grants.sql
```

---

### Rollback Deployment

**Use Case**: Revert failed DAB deployment

#### Option 1: Git Revert (Recommended)

```bash
# Identify the bad commit
git log --oneline -10

# Revert the commit
git revert <bad-commit-hash>

# Push revert
git push origin <branch-name>

# Trigger deployment workflow
# The workflow will automatically deploy the reverted state
```

#### Option 2: Manual Rollback to Previous Backup

```bash
# Checkout backup from before the failed deployment
git checkout dr-backups/PROD
git log --oneline

# Find backup before the deployment
git checkout <previous-commit-hash>

# Copy resources to main branch
git checkout main
cp -r ../dr-backups-temp/jobs/* AMALDAB/resources/jobs/
cp -r ../dr-backups-temp/SharedObjects/* AMALDAB/resources/SharedObjects/

# Deploy
cd AMALDAB
databricks bundle deploy -t PROD --force-lock
```

---

## ✅ Verification Steps

### Post-Recovery Verification Checklist

#### Infrastructure Verification
```bash
# Verify clusters
databricks clusters list
# Expected: All clusters from backup present

# Verify SQL warehouses
databricks sql warehouses list
# Expected: All warehouses from backup present

# Verify cluster policies
databricks cluster-policies list
# Expected: All policies from backup present
```

#### Jobs Verification
```bash
# List all jobs
databricks jobs list

# Count jobs
databricks jobs list | wc -l

# Compare with backup
ls dr-backups/PROD/jobs/*.yml | wc -l

# Test critical jobs
databricks jobs run-now --job-id <critical-job-id>
```

#### Unity Catalog Verification
```bash
# List catalogs
databricks catalogs list

# List schemas
databricks schemas list --catalog ab_prod_catalog

# List tables
databricks tables list --catalog ab_prod_catalog --schema bronze

# Verify grants
databricks grants list --securable-type catalog --full-name ab_prod_catalog
```

#### Data Verification
```sql
-- Run in Databricks SQL
SELECT COUNT(*) FROM ab_prod_catalog.bronze.my_table;

-- Verify recent data
SELECT MAX(updated_at) FROM ab_prod_catalog.bronze.my_table;

-- Compare row counts with expected values
```

#### End-to-End Test
```bash
# Run a test job that exercises the full pipeline
databricks jobs run-now --job-id <test-job-id>

# Monitor job run
databricks runs get --run-id <run-id>

# Verify output data
```

---

## 🔄 Rollback Procedures

### If Recovery Fails

#### Step 1: Stop Recovery
```bash
# Document current state
databricks jobs list > recovery_state_jobs.txt
databricks clusters list > recovery_state_clusters.txt

# Stop any running jobs
databricks jobs cancel-all-runs --job-id <job-id>
```

#### Step 2: Assess Damage
- [ ] What was restored successfully?
- [ ] What failed to restore?
- [ ] Are there any data inconsistencies?
- [ ] Is the workspace in a worse state than before?

#### Step 3: Decide Next Steps

**Option A**: Continue with manual fixes
**Option B**: Restore to a different backup
**Option C**: Escalate to Databricks support

#### Step 4: Document Issues
```markdown
## Recovery Failure Report

**Date**: 2026-01-19
**Environment**: PROD
**Backup Used**: dr-backups/PROD @ abc1234
**Failure Point**: Unity Catalog restoration
**Error Message**: [paste error]
**Impact**: [describe impact]
**Next Steps**: [describe plan]
```

---

## 📝 Post-Recovery Tasks

### Immediate Tasks (Within 1 Hour)

- [ ] **Verify All Critical Jobs Running**
  ```bash
  databricks jobs list --filter "name LIKE '%critical%'"
  ```

- [ ] **Check Data Freshness**
  ```sql
  SELECT table_name, MAX(updated_at) as last_update
  FROM ab_prod_catalog.information_schema.tables
  GROUP BY table_name;
  ```

- [ ] **Notify Stakeholders**
  - Send "Recovery Complete" email
  - Update incident ticket
  - Post in Slack/Teams

- [ ] **Monitor for Errors**
  - Check job run history
  - Review Databricks logs
  - Monitor alerts

### Short-Term Tasks (Within 24 Hours)

- [ ] **Complete Incident Report**
  - Root cause analysis
  - Timeline of events
  - Lessons learned

- [ ] **Update Documentation**
  - Document any deviations from runbook
  - Update recovery procedures if needed

- [ ] **Review Backup Strategy**
  - Verify backups are running
  - Check backup retention
  - Test backup integrity

- [ ] **Schedule Post-Mortem**
  - Invite all stakeholders
  - Review incident timeline
  - Identify improvements

### Long-Term Tasks (Within 1 Week)

- [ ] **Implement Preventive Measures**
  - Add monitoring/alerts
  - Improve access controls
  - Enhance testing procedures

- [ ] **Update DR Plan**
  - Incorporate lessons learned
  - Update RTO/RPO targets
  - Revise escalation procedures

- [ ] **Conduct DR Drill**
  - Test recovery in non-prod
  - Train team members
  - Validate runbook accuracy

---

## 🧪 Testing DR Procedures

### Quarterly DR Drill

**Objective**: Validate recovery procedures without impacting production

#### Test Plan

**Environment**: DEV (non-production)
**Frequency**: Quarterly
**Duration**: 2-3 hours
**Participants**: Data Engineering team, DevOps, Platform Admin

#### Test Scenario 1: Full Workspace Recovery
1. Backup current DEV workspace
2. Delete all jobs from DEV
3. Restore from latest DR backup
4. Verify all jobs restored
5. Run test job to validate functionality

#### Test Scenario 2: Selective Resource Recovery
1. Delete specific job from DEV
2. Restore only that job from backup
3. Verify job configuration
4. Run job to validate

#### Test Scenario 3: Unity Catalog Recovery
1. Drop a test schema
2. Restore schema from backup DDL
3. Verify tables and grants
4. Query data to validate

#### Success Criteria
- [ ] All resources restored within RTO
- [ ] No data loss (within RPO)
- [ ] All verification steps pass
- [ ] Team comfortable with procedures
- [ ] Documentation accurate

#### Post-Test Actions
- [ ] Document any issues encountered
- [ ] Update runbook with improvements
- [ ] Share results with stakeholders
- [ ] Schedule next drill

---

## 📞 Escalation Procedures

### Level 1: Team Lead (0-30 minutes)
- Initial assessment
- Start recovery procedures
- Notify stakeholders

### Level 2: Engineering Manager (30-60 minutes)
- If recovery not progressing
- If impact is wider than expected
- If additional resources needed

### Level 3: Databricks Support (60+ minutes)
- If workspace-level issues
- If recovery procedures failing
- If data corruption suspected

### Level 4: Executive Management (2+ hours)
- If RTO will be exceeded
- If customer impact
- If regulatory implications

---

## 📚 Appendix

### Useful Commands Reference

```bash
# Databricks CLI - Jobs
databricks jobs list
databricks jobs get --job-id <id>
databricks jobs run-now --job-id <id>
databricks runs get --run-id <id>

# Databricks CLI - Clusters
databricks clusters list
databricks clusters get --cluster-id <id>
databricks clusters start --cluster-id <id>

# Databricks CLI - Unity Catalog
databricks catalogs list
databricks schemas list --catalog <name>
databricks tables list --catalog <name> --schema <name>

# Git - Backup Operations
git checkout dr-backups/PROD
git log --oneline
git show <commit>:<file>
git diff <commit1> <commit2>
```

### Recovery Time Estimates

| Component | Estimated Time | Complexity |
|-----------|----------------|------------|
| Unity Catalog (full) | 30-60 min | Medium |
| Infrastructure | 15-30 min | Low |
| Jobs (all) | 20-40 min | Low |
| Notebooks | 10-20 min | Low |
| Verification | 30-60 min | Medium |
| **Total (Full Recovery)** | **2-4 hours** | **High** |

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Last Tested**: [Date of last DR drill]
**Next Review**: [Date + 3 months]
**Maintained By**: Data Engineering Team
