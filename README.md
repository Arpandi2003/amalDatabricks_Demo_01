# 🚀 Databricks Asset Bundle (DAB) - Enterprise Data Platform

[![CI/CD Status](https://img.shields.io/badge/CI%2FCD-Active-success)](https://github.com/Arpandi2003/amalDatabricks_Demo_01/actions)
[![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundle-FF3621?logo=databricks)](https://docs.databricks.com/dev-tools/bundles/)
[![DR Backup](https://img.shields.io/badge/DR%20Backup-Automated-blue)](https://github.com/Arpandi2003/amalDatabricks_Demo_01/tree/dr-backups)

> **Enterprise-grade Databricks deployment with automated CI/CD, disaster recovery, and multi-environment management**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Environments](#environments)
- [CI/CD Pipeline](#cicd-pipeline)
- [Disaster Recovery](#disaster-recovery)
- [Documentation](#documentation)
- [Development Workflow](#development-workflow)
- [Security](#security)
- [Support](#support)

---

## 🎯 Overview

This repository contains a **production-ready Databricks Asset Bundle (DAB)** implementation with:

- ✅ **Automated CI/CD** - GitHub Actions workflows for deployment
- ✅ **Multi-Environment** - DEV, UAT, and PROD environments
- ✅ **Disaster Recovery** - Automated daily backups with Git-based versioning
- ✅ **Service Principal Authentication** - Secure OAuth 2.0 M2M authentication
- ✅ **Unity Catalog** - Complete data governance and metadata management
- ✅ **Infrastructure as Code** - All resources defined in YAML
- ✅ **Policy Validation** - Automated coding standards enforcement
- ✅ **Comprehensive Documentation** - Detailed guides for all stakeholders

### What is Databricks Asset Bundle (DAB)?

DAB is Databricks' **Infrastructure-as-Code** framework that allows you to:
- Define all Databricks resources (jobs, clusters, notebooks, etc.) in YAML files
- Version control your entire Databricks workspace
- Deploy consistently across multiple environments
- Automate testing and validation
- Enable team collaboration with Git workflows

---

## ✨ Key Features

### 🔄 Automated CI/CD Pipeline

- **Push-based deployment** - Automatic deployment on Git push
- **Manual deployment** - On-demand deployment via GitHub Actions
- **Environment detection** - Automatic environment selection based on branch
- **Configuration management** - Dynamic environment-specific configuration
- **Bundle validation** - Pre-deployment validation and policy checks
- **Workspace sync** - Automatic cleanup of deleted files

### 🛡️ Disaster Recovery System

- **Daily automated backups** - Scheduled at 2 AM UTC
- **Full workspace backup** - All clusters, jobs, warehouses, Unity Catalog objects
- **Git-based storage** - Unlimited retention with full version history
- **Point-in-time recovery** - Restore from any previous backup
- **Selective restoration** - Restore individual resources or entire workspace
- **Backup verification** - Automated integrity checks

### 🔐 Enterprise Security

- **Service Principal authentication** - No personal access tokens
- **OAuth 2.0 M2M** - Industry-standard authentication
- **Separate identities** - Deployment vs runtime service principals
- **GitHub Secrets** - Secure credential management
- **Audit logging** - Complete deployment history
- **Access control** - Role-based permissions

### 🌍 Multi-Environment Support

| Environment | Branch | Mode | Purpose |
|-------------|--------|------|---------|
| **DEV** | `dev` | Development | Individual development, testing |
| **UAT** | `qa`/`uat` | Production | User acceptance testing, staging |
| **PROD** | `main` | Production | Production workloads |

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        GitHub Repository                         │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐              │
│  │   dev    │ ───> │    qa    │ ───> │   main   │              │
│  │  (DEV)   │      │  (UAT)   │      │  (PROD)  │              │
│  └──────────┘      └──────────┘      └──────────┘              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GitHub Actions Workflows                      │
│  ┌────────────────────────┐    ┌────────────────────────┐      │
│  │  DAB Deployment        │    │  DR Backup             │      │
│  │  ├─ Unit Testing       │    │  ├─ Backup DEV         │      │
│  │  ├─ Config Management  │    │  ├─ Backup QA          │      │
│  │  ├─ Bundle Deploy      │    │  ├─ Backup PROD        │      │
│  │  └─ Workspace Sync     │    │  └─ Git Commit         │      │
│  └────────────────────────┘    └────────────────────────┘      │
└─────────────────────────┬───────────────────┬───────────────────┘
                          │                   │
                          ▼                   ▼
┌─────────────────────────────────┐  ┌──────────────────────────┐
│   Databricks Workspace          │  │  Backup Branches         │
│   ├─ Jobs & Workflows           │  │  ├─ dr-backups/DEV       │
│   ├─ Clusters & Warehouses      │  │  ├─ dr-backups/QA        │
│   ├─ Unity Catalog               │  │  └─ dr-backups/PROD      │
│   ├─ Notebooks & Code            │  │                          │
│   └─ Policies & Permissions     │  │  (Daily backups)         │
└─────────────────────────────────┘  └──────────────────────────┘
```

### Component Architecture

```
Repository Root
├── .github/workflows/          # CI/CD automation
│   ├── main.yml               # DAB deployment pipeline
│   ├── dr-backup.yml          # Disaster recovery backup
│   ├── prerequisites.py       # Backup script
│   ├── sync-with-deletion.ps1 # Workspace cleanup
│   └── validate-bundle.ps1    # Policy validation
│
├── AMALDAB/                   # Databricks Asset Bundle
│   ├── databricks.yml         # Bundle configuration
│   ├── DataPlatform/          # Notebooks and code
│   │   ├── General/           # Shared utilities
│   │   ├── CRM/               # CRM data pipelines
│   │   ├── Extraction/        # Data extraction
│   │   └── Transformation/    # Data transformation
│   └── resources/             # DAB resources
│       ├── jobs/              # Job definitions
│       ├── SharedObjects/     # Infrastructure configs
│       └── uc_ddl/            # Unity Catalog DDL
│
├── docs/                      # Documentation
│   ├── README.md              # Documentation index
│   ├── CICD-Documentation.md  # CI/CD technical guide
│   └── DR-Runbook.md          # Disaster recovery procedures
│
└── src/                       # Source notebooks (legacy)
```

---

## 🚀 Quick Start

### Prerequisites

- **GitHub Account** with repository access
- **Databricks Workspace** (Azure Databricks)
- **Service Principal** credentials (Client ID & Secret)
- **Git** installed locally (for development)
- **Databricks CLI** (optional, for local testing)

### For Developers: Deploy Your First Change

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Arpandi2003/amalDatabricks_Demo_01.git
   cd amalDatabricks_Demo_01
   ```

2. **Create a feature branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```

3. **Make your changes**:
   - Edit notebooks in `AMALDAB/DataPlatform/`
   - Update job definitions in `AMALDAB/resources/jobs/`
   - Modify infrastructure in `AMALDAB/resources/SharedObjects/`

4. **Test locally** (optional):
   ```bash
   cd AMALDAB
   databricks bundle validate -t DEV
   ```

5. **Commit and push**:
   ```bash
   git add .
   git commit -m "Add new feature"
   git push origin feature/my-new-feature
   ```

6. **Create Pull Request** to `dev` branch

7. **Merge to deploy**:
   - Merge PR to `dev` → Deploys to **DEV**
   - Merge `dev` to `qa` → Deploys to **UAT**
   - Merge `qa` to `main` → Deploys to **PROD**

### For Operations: Run Manual Backup

1. Go to **GitHub Actions**
2. Select **"Disaster Recovery Backup"** workflow
3. Click **"Run workflow"**
4. Select environment (DEV/QA/PROD/ALL)
5. Click **"Run workflow"** button

### For Incident Response: Emergency Recovery

1. **Identify the issue** - See [DR Runbook](docs/DR-Runbook.md#disaster-scenarios)
2. **Access backup**:
   ```bash
   git checkout dr-backups/PROD
   git log --oneline  # View backup history
   ```
3. **Follow recovery procedure** - See [DR Runbook](docs/DR-Runbook.md#recovery-procedures)
4. **Verify restoration** - See [DR Runbook](docs/DR-Runbook.md#verification-steps)

---

## 📁 Project Structure

### Key Directories

#### `.github/workflows/` - CI/CD Automation
Contains all GitHub Actions workflows and scripts:
- **main.yml** - Main deployment pipeline (4 jobs: testing, config, deploy, sync)
- **dr-backup.yml** - Disaster recovery backup (3 parallel jobs for DEV/QA/PROD)
- **prerequisites.py** - Python script for full workspace backup
- **sync-with-deletion.ps1** - PowerShell script for workspace cleanup
- **validate-bundle.ps1** - Policy validation script

#### `AMALDAB/` - Databricks Asset Bundle
The core DAB implementation:
- **databricks.yml** - Bundle configuration with environment-specific variables
- **DataPlatform/** - All notebooks and Python code
- **resources/** - DAB resource definitions (jobs, clusters, warehouses, etc.)

#### `docs/` - Documentation
Comprehensive documentation suite:
- **README.md** - Documentation index and quick start guides
- **CICD-Documentation.md** - Complete CI/CD technical reference (700+ lines)
- **DR-Runbook.md** - Disaster recovery procedures (800+ lines)

#### `src/` - Source Notebooks (Legacy)
Original notebook structure (being migrated to `AMALDAB/DataPlatform/`)

---

## 🌍 Environments

### Environment Configuration

| Aspect | DEV | UAT | PROD |
|--------|-----|-----|------|
| **Branch** | `dev` | `qa`/`uat` | `main` |
| **Mode** | Development | Production | Production |
| **Workspace Path** | `/Workspace/Users/{user}/.bundle/...` | `/Workspace/uat` | `/Workspace/PROD` |
| **Name Prefix** | `[{username}]` | `[UAT]` | `[PROD]` |
| **Catalog** | `ab_dev_catalog` | `ab_uat_catalog` | `ab_prod_catalog` |
| **Key Vault** | `kv-az-dbricks-dev-001` | `kv-az-dbricks-uat-001` | `kv-az-dbricks-prod-001` |
| **Deployment** | Automatic on push | Automatic on push | Automatic on push |
| **Backup Schedule** | Daily 2 AM UTC | Daily 2 AM UTC | Daily 2 AM UTC |

### Environment-Specific Features

#### DEV Environment
- **Purpose**: Individual developer testing and experimentation
- **Isolation**: Each developer gets their own workspace path with username
- **Flexibility**: Can break things without affecting others
- **Testing**: Full integration testing before promoting to UAT

#### UAT Environment
- **Purpose**: User acceptance testing and staging
- **Stability**: Production-like configuration
- **Testing**: Final validation before production deployment
- **Access**: Limited to QA team and stakeholders

#### PROD Environment
- **Purpose**: Production workloads and live data processing
- **Stability**: Highest stability requirements
- **Monitoring**: Enhanced monitoring and alerting
- **Access**: Restricted to authorized personnel only

---

## 🔄 CI/CD Pipeline

### Deployment Workflow

The deployment pipeline consists of **4 sequential jobs**:

#### Job 1: Unit Testing & Bundle Policy Validation
- Runs Python unit tests
- Validates bundle structure
- Enforces coding standards:
  - ✅ No default "Yes" for `is_test` parameter
  - ✅ Cluster names must match pattern `^[a-zA-Z0-9_-]+$`
  - ✅ No environment names in configuration values

#### Job 2: Configuration Management
- Determines target environment from branch
- Reads environment-specific variables from `databricks.yml`
- Updates `NB_Configuration.py` with correct values
- Validates configuration file syntax

#### Job 3: DAB Operations (Validate & Deploy)
- Authenticates with Service Principal (OAuth 2.0)
- Validates bundle configuration
- Deploys to Databricks workspace
- Creates/updates:
  - Jobs and workflows
  - Clusters and SQL warehouses
  - Cluster policies
  - Unity Catalog objects
  - Storage credentials and external locations

#### Job 4: Sync with Deletion
- Compares Git repository with Databricks workspace
- Removes files deleted from Git
- Ensures workspace matches source control
- Prevents orphaned files

### Workflow Triggers

| Trigger Type | When | Environments |
|--------------|------|--------------|
| **Push** | Code pushed to `dev`, `qa`, or `main` | Automatic |
| **Manual** | Via GitHub Actions UI | User-selected |
| **Schedule** | Not configured (can be added) | N/A |

### Branch → Environment Mapping

```
dev branch    → DEV environment
qa/uat branch → UAT environment
main branch   → PROD environment
```

---

## 🛡️ Disaster Recovery

### Backup System

#### What Gets Backed Up

- ✅ **All-Purpose Clusters** - Shared compute resources
- ✅ **Job Clusters** - Job-specific compute configurations
- ✅ **SQL Warehouses** - SQL endpoint configurations
- ✅ **Jobs** - Complete job definitions with schedules
- ✅ **Cluster Policies** - Compute governance policies
- ✅ **Unity Catalog** - Catalogs, schemas, tables, volumes, functions
- ✅ **Storage Credentials** - Cloud storage authentication
- ✅ **External Locations** - External data source configurations
- ✅ **Connections** - External system connections (sanitized)
- ✅ **Grants & Permissions** - Complete access control

#### Backup Schedule

- **Frequency**: Daily at 2:00 AM UTC
- **Retention**: Unlimited (Git history)
- **Storage**: Git orphan branches (`dr-backups/DEV`, `dr-backups/QA`, `dr-backups/PROD`)
- **Format**: YAML (infrastructure) + SQL (Unity Catalog DDL)

#### Backup Branches

```bash
# View available backups
git branch -r | grep dr-backups

# Output:
# origin/dr-backups/DEV
# origin/dr-backups/QA
# origin/dr-backups/PROD

# Access latest backup
git checkout dr-backups/PROD

# View backup history
git log --oneline

# Restore from specific backup
git checkout <commit-hash>
```

### Recovery Procedures

#### Quick Recovery (Single Resource)

```bash
# Checkout backup branch
git checkout dr-backups/PROD

# Copy specific resource
cp jobs/my_critical_job.job.yml ../AMALDAB/resources/jobs/

# Deploy
cd ../AMALDAB
databricks bundle deploy -t PROD
```

#### Full Workspace Recovery

See detailed procedures in [DR Runbook](docs/DR-Runbook.md)

**Estimated Recovery Times**:
- Unity Catalog: 30-60 minutes
- Infrastructure: 15-30 minutes
- Jobs: 20-40 minutes
- **Total**: 2-4 hours

---

## 📚 Documentation

### Available Documentation

| Document | Purpose | Audience | Lines |
|----------|---------|----------|-------|
| **[README.md](README.md)** | Project overview | All | This file |
| **[docs/README.md](docs/README.md)** | Documentation index | All | 737 |
| **[docs/CICD-Documentation.md](docs/CICD-Documentation.md)** | CI/CD technical guide | DevOps, Engineers | 704 |
| **[docs/DR-Runbook.md](docs/DR-Runbook.md)** | Disaster recovery procedures | On-call, Ops | 823 |
| **[SERVICE_PRINCIPAL_SETUP.md](SERVICE_PRINCIPAL_SETUP.md)** | Service Principal setup | Admins | - |
| **[QUICK_START_SERVICE_PRINCIPAL.md](QUICK_START_SERVICE_PRINCIPAL.md)** | Quick start guide | Developers | - |

### Documentation by Role

#### For Developers
- [Quick Start](#quick-start) (this file)
- [Development Workflow](#development-workflow) (this file)
- [CI/CD Documentation](docs/CICD-Documentation.md)

#### For DevOps Engineers
- [CI/CD Documentation](docs/CICD-Documentation.md)
- [Architecture](#architecture) (this file)
- [Service Principal Setup](SERVICE_PRINCIPAL_SETUP.md)

#### For Operations/On-Call
- [DR Runbook](docs/DR-Runbook.md)
- [Disaster Recovery](#disaster-recovery) (this file)
- [Documentation Index](docs/README.md)

#### For Management
- [Overview](#overview) (this file)
- [Key Features](#key-features) (this file)
- [Documentation Index](docs/README.md)

---

## 👨‍💻 Development Workflow

### Standard Development Flow

```
1. Create feature branch from dev
   ↓
2. Make changes (notebooks, jobs, infrastructure)
   ↓
3. Test locally (optional: databricks bundle validate)
   ↓
4. Commit and push to feature branch
   ↓
5. Create Pull Request to dev
   ↓
6. Code review and approval
   ↓
7. Merge to dev → Auto-deploy to DEV
   ↓
8. Test in DEV environment
   ↓
9. Merge dev to qa → Auto-deploy to UAT
   ↓
10. UAT testing and validation
    ↓
11. Merge qa to main → Auto-deploy to PROD
    ↓
12. Production monitoring
```

### Best Practices

#### Code Organization
- ✅ Keep notebooks in `AMALDAB/DataPlatform/`
- ✅ Define jobs in `AMALDAB/resources/jobs/`
- ✅ Store shared utilities in `DataPlatform/General/`
- ✅ Use meaningful file and job names
- ✅ Add comments and documentation

#### Git Workflow
- ✅ Always create feature branches
- ✅ Write descriptive commit messages
- ✅ Keep commits small and focused
- ✅ Use pull requests for code review
- ✅ Never commit secrets or credentials

#### Testing
- ✅ Test in DEV before promoting to UAT
- ✅ Run bundle validation locally
- ✅ Verify job runs in Databricks UI
- ✅ Check data quality and outputs
- ✅ Monitor for errors and warnings

#### Deployment
- ✅ Deploy during business hours (DEV/UAT)
- ✅ Deploy during maintenance windows (PROD)
- ✅ Monitor deployment logs
- ✅ Verify resources in Databricks workspace
- ✅ Have rollback plan ready

---

## 🔐 Security

### Authentication

#### Service Principals

Two Service Principals are used:

1. **Deployment Service Principal** (`8f58ed47-fbbe-4c2d-ba65-973b82ab6371`)
   - **Purpose**: Deploy bundles to Databricks workspace
   - **Permissions**: Workspace Admin
   - **Used By**: GitHub Actions workflows

2. **Runtime Service Principal** (`a7923e63-e14c-4c32-87cc-e89e3b4a165a`)
   - **Purpose**: Execute jobs and access data
   - **Permissions**: Job execution, data access
   - **Used By**: Databricks jobs (defined in `run_as`)

#### GitHub Secrets

Required secrets per environment:

| Secret Name | Purpose | Example |
|-------------|---------|---------|
| `DEV_CLIENT_ID` | Deployment SP Client ID | `8f58ed47-...` |
| `DEV_CLIENT_SECRET` | Deployment SP Secret | `***` |
| `UAT_CLIENT_ID` | Deployment SP Client ID | `8f58ed47-...` |
| `UAT_CLIENT_SECRET` | Deployment SP Secret | `***` |
| `PROD_CLIENT_ID` | Deployment SP Client ID | `8f58ed47-...` |
| `PROD_CLIENT_SECRET` | Deployment SP Secret | `***` |
| `GIT_TOKEN` | GitHub PAT for DR backups | `ghp_***` |

### Access Control

#### Repository Access
- **Developers**: Write access to `dev` branch
- **QA Team**: Write access to `qa` branch
- **Release Managers**: Write access to `main` branch
- **All**: Read access to documentation and backup branches

#### Databricks Workspace
- **DEV**: All developers have access
- **UAT**: QA team and stakeholders
- **PROD**: Production support team only

### Security Best Practices

- ✅ Never commit secrets to Git
- ✅ Rotate Service Principal secrets quarterly
- ✅ Use least-privilege access
- ✅ Enable audit logging
- ✅ Review permissions regularly
- ✅ Use Service Principals instead of PAT tokens
- ✅ Sanitize sensitive data in backups

---

## 📞 Support

### Getting Help

| Issue Type | Contact | Response Time |
|------------|---------|---------------|
| **Deployment Issues** | DevOps Team | 1 hour (business hours) |
| **DR/Incidents** | On-Call Engineer | 15 minutes (24/7) |
| **Access Issues** | Platform Admin | 4 hours (business hours) |
| **Databricks Issues** | Databricks Support | Per SLA |

### Escalation Path

1. **L1**: Team Lead / On-Call Engineer
2. **L2**: Engineering Manager
3. **L3**: Databricks Support / Azure Support
4. **L4**: VP Engineering / Executive Team

### Useful Links

- **GitHub Repository**: [amalDatabricks_Demo_01](https://github.com/Arpandi2003/amalDatabricks_Demo_01)
- **GitHub Actions**: [Workflows](https://github.com/Arpandi2003/amalDatabricks_Demo_01/actions)
- **Databricks Workspace**: [Azure Databricks](https://adb-201068313543333.13.azuredatabricks.net)
- **Databricks Documentation**: [Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- **Unity Catalog Docs**: [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

---

## 🎓 Training & Onboarding

### New Team Member Checklist

- [ ] Read this README
- [ ] Review [CI/CD Documentation](docs/CICD-Documentation.md)
- [ ] Review [DR Runbook](docs/DR-Runbook.md)
- [ ] Get GitHub repository access
- [ ] Get Databricks workspace access (DEV)
- [ ] Clone repository locally
- [ ] Deploy a test change to DEV
- [ ] Participate in DR drill
- [ ] Shadow on-call engineer

### Training Resources

- **Week 1**: Documentation review and system understanding
- **Week 2**: Hands-on practice with DEV deployments
- **Week 3**: Advanced topics and DR procedures
- **Week 4**: Certification and independent work

---

## 📊 Metrics & Monitoring

### Key Metrics

- **Deployment Success Rate**: Track via GitHub Actions
- **Deployment Frequency**: Daily/weekly deployment counts
- **Backup Success Rate**: Monitor DR backup workflow
- **Recovery Time**: Measure during DR drills
- **Job Success Rate**: Monitor in Databricks workspace

### Monitoring

- **GitHub Actions**: Workflow notifications
- **Databricks**: Job alerts and monitoring
- **Unity Catalog**: Audit logs
- **Backup Health**: Daily backup verification

---

## 🔄 Maintenance

### Regular Tasks

#### Daily
- ✅ Monitor workflow executions
- ✅ Review failed jobs
- ✅ Check backup completion

#### Weekly
- ✅ Review backup integrity
- ✅ Check for outdated dependencies
- ✅ Review access logs

#### Monthly
- ✅ Update documentation
- ✅ Review and rotate secrets
- ✅ Analyze deployment metrics

#### Quarterly
- ✅ Conduct DR drill
- ✅ Review and update RTO/RPO
- ✅ Security audit
- ✅ Team training session

---

## 🚀 Future Enhancements

### Planned Features

- [ ] Automated testing framework
- [ ] Data quality validation
- [ ] Performance monitoring
- [ ] Cost optimization alerts
- [ ] Multi-region deployment
- [ ] Blue-green deployments
- [ ] Canary releases
- [ ] Automated rollback

---

## 📝 License

This project is proprietary and confidential.

---

## 🙏 Acknowledgments

- **Databricks** - For the Asset Bundle framework
- **GitHub** - For Actions and version control
- **Data Engineering Team** - For implementation and maintenance

---

## 📅 Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0.0 | 2026-01-19 | Initial comprehensive README | Data Engineering Team |
| 0.9.0 | 2026-01-17 | Added DR backup system | Data Engineering Team |
| 0.8.0 | 2026-01-15 | Service Principal authentication | Data Engineering Team |
| 0.7.0 | 2026-01-10 | Multi-environment support | Data Engineering Team |

---

**Last Updated**: 2026-01-20
**Maintained By**: Data Engineering Team
**Repository**: [amalDatabricks_Demo_01](https://github.com/Arpandi2003/amalDatabricks_Demo_01)