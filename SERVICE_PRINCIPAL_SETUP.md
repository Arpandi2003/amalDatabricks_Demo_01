# 🔐 Service Principal Authentication Setup Guide

This guide explains how to configure Service Principal authentication for your Databricks workflows, replacing PAT tokens with production-ready OAuth authentication.

---

## 📋 **Prerequisites**

### **1. Azure Databricks Service Principal**

You need a Service Principal (also called App Registration) in Azure Active Directory with access to your Databricks workspace.

**If you already have a Service Principal:**
- ✅ You're using `a7923e63-e14c-4c32-87cc-e89e3b4a165a` in your `databricks.yml`
- ✅ You just need to get the Client Secret

**If you need to create a new Service Principal:**
1. Go to Azure Portal → Azure Active Directory → App Registrations
2. Click "New registration"
3. Name it (e.g., "Databricks-CI-CD-SP")
4. Click "Register"
5. Note the **Application (client) ID** - this is your `CLIENT_ID`
6. Go to "Certificates & secrets" → "New client secret"
7. Create a secret and **copy the value immediately** - this is your `CLIENT_SECRET`

### **2. Grant Service Principal Access to Databricks**

The Service Principal needs permissions in your Databricks workspace:

**Option A: Using Databricks CLI**
```bash
databricks workspace-access grant \
  --principal-id a7923e63-e14c-4c32-87cc-e89e3b4a165a \
  --permission-level USER
```

**Option B: Using Databricks UI**
1. Go to Databricks workspace → Settings → Identity and access
2. Click "Service principals" tab
3. Click "Add service principal"
4. Enter the Application ID: `a7923e63-e14c-4c32-87cc-e89e3b4a165a`
5. Assign appropriate permissions (Workspace access, Cluster create, etc.)

### **3. Required Permissions**

The Service Principal needs these permissions for the backup/deployment workflows:

| Permission | Required For | How to Grant |
|------------|--------------|--------------|
| **Workspace access** | Reading/writing notebooks | Workspace Admin → Add SP as User |
| **Cluster create** | Creating/managing clusters | Workspace Admin → Grant "Allow cluster creation" |
| **SQL Warehouse access** | Managing SQL warehouses | Workspace Admin → Grant warehouse permissions |
| **Unity Catalog access** | Reading UC metadata | Account Admin → Grant catalog permissions |
| **Job management** | Creating/managing jobs | Workspace Admin → Grant job permissions |

---

## 🔧 **GitHub Configuration**

### **Step 1: Configure GitHub Repository Variables**

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **Variables** tab

Add these **Repository Variables**:

| Variable Name | Value | Example |
|---------------|-------|---------|
| `DEV_DATABRICKS_HOST` | DEV workspace URL | `https://adb-201068313543333.13.azuredatabricks.net` |
| `DEV_CLIENT_ID` | Service Principal Client ID for DEV | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |
| `QA_DATABRICKS_HOST` | QA workspace URL | `https://adb-201068313543333.13.azuredatabricks.net` |
| `QA_CLIENT_ID` | Service Principal Client ID for QA | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |
| `PROD_DATABRICKS_HOST` | PROD workspace URL | `https://adb-201068313543333.13.azuredatabricks.net` |
| `PROD_CLIENT_ID` | Service Principal Client ID for PROD | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |

**Note**: You can use the same Service Principal for all environments or create separate ones for better isolation.

### **Step 2: Configure GitHub Repository Secrets**

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **Secrets** tab

Add these **Repository Secrets**:

| Secret Name | Value | Example |
|-------------|-------|---------|
| `DEV_CLIENT_SECRET` | Service Principal Client Secret for DEV | `abc123...xyz789` |
| `QA_CLIENT_SECRET` | Service Principal Client Secret for QA | `abc123...xyz789` |
| `PROD_CLIENT_SECRET` | Service Principal Client Secret for PROD | `abc123...xyz789` |
| `GIT_TOKEN` | GitHub Personal Access Token (for DR backups) | `ghp_...` |

**Important**: 
- Client secrets are sensitive! Never commit them to Git
- Client secrets expire (default 90 days) - set a reminder to rotate them
- You can use the same secret for all environments if using the same SP

### **Step 3: (Optional) Keep PAT Tokens as Fallback**

For backward compatibility, you can keep your existing PAT tokens:

| Secret Name | Value | Purpose |
|-------------|-------|---------|
| `DEV_DATABRICKS_TOKEN` | PAT token for DEV | Fallback if SP not configured |
| `QA_DATABRICKS_TOKEN` | PAT token for QA | Fallback if SP not configured |
| `PROD_DATABRICKS_TOKEN` | PAT token for PROD | Fallback if SP not configured |

The workflows will automatically use Service Principal if configured, otherwise fall back to PAT tokens.

---

## 🧪 **Testing the Configuration**

### **Test 1: Verify Service Principal Access**

```bash
# Install Databricks CLI
pip install databricks-cli

# Test authentication
databricks auth login \
  --host https://adb-201068313543333.13.azuredatabricks.net \
  --client-id a7923e63-e14c-4c32-87cc-e89e3b4a165a \
  --client-secret <your-secret>

# Test API access
databricks clusters list
databricks warehouses list
databricks catalogs list
```

If these commands work, your Service Principal is configured correctly!

### **Test 2: Test prerequisites.py Locally**

```bash
# Clone your repository
git clone <your-repo-url>
cd amalDatabricks_Demo_01

# Install dependencies
pip install requests PyYAML

# Test with Service Principal
python .github/workflows/prerequisites.py \
  --DATABRICKS_HOST https://adb-201068313543333.13.azuredatabricks.net \
  --CLIENT_ID a7923e63-e14c-4c32-87cc-e89e3b4a165a \
  --CLIENT_SECRET <your-secret>

# Check output
ls -la AMALDAB/resources/SharedObjects/
ls -la AMALDAB/resources/uc_ddl/
```

Expected output:
```
🔐 Authenticating with Service Principal...
   Client ID: a7923e63...165a
   Token URL: https://adb-xxx.net/oidc/v1/token
✅ OAuth token obtained successfully
   Token expires in: 3600 seconds
✅ Using Service Principal authentication (token valid until 2026-01-19 15:30:00)

 Starting Databricks DR Configuration Backup
 Fetching infrastructure...
...
```

### **Test 3: Test GitHub Workflow**

1. Go to your GitHub repository → **Actions**
2. Select **"Databricks Notebooks Deployment"** workflow
3. Click **"Run workflow"**
4. Select environment: **DEV**
5. Click **"Run workflow"**

Watch the logs for:
```
🔐 Using Service Principal authentication (recommended)
   CLIENT_ID: a7923e63...
🔧 Running prerequisites script...
🔐 Authenticating with Service Principal...
✅ OAuth token obtained successfully
```

If you see this, Service Principal authentication is working! 🎉

---

## 🔄 **Migration Path: PAT → Service Principal**

### **Phase 1: Add Service Principal (Parallel)**
1. ✅ Configure Service Principal in Azure AD
2. ✅ Add `{ENV}_CLIENT_ID` variables to GitHub
3. ✅ Add `{ENV}_CLIENT_SECRET` secrets to GitHub
4. ✅ Keep existing `{ENV}_DATABRICKS_TOKEN` secrets
5. ✅ Test workflows - they should use SP automatically

### **Phase 2: Verify (1-2 weeks)**
1. ✅ Monitor workflow runs
2. ✅ Verify all environments work with SP
3. ✅ Check for any authentication errors

### **Phase 3: Remove PAT Tokens (Optional)**
1. ✅ Delete `{ENV}_DATABRICKS_TOKEN` secrets from GitHub
2. ✅ Revoke PAT tokens in Databricks UI
3. ✅ Update documentation

---

## 🔒 **Security Best Practices**

### **1. Client Secret Rotation**

Client secrets expire! Set up rotation:

```bash
# Every 90 days (or your organization's policy):
1. Go to Azure Portal → App Registrations → Your SP
2. Go to "Certificates & secrets"
3. Create a new client secret
4. Update GitHub secret: {ENV}_CLIENT_SECRET
5. Delete old secret after verifying new one works
```

### **2. Least Privilege Principle**

Grant only the permissions needed:

| Environment | Recommended Permissions |
|-------------|------------------------|
| **DEV** | Full access (for testing) |
| **QA** | Read/Write (for validation) |
| **PROD** | Read-only for backups, Write for deployments |

### **3. Separate Service Principals per Environment**

For maximum security:

```
DEV_CLIENT_ID = sp-dev-12345
QA_CLIENT_ID = sp-qa-67890
PROD_CLIENT_ID = sp-prod-abcde
```

Benefits:
- ✅ Blast radius containment (compromised DEV SP doesn't affect PROD)
- ✅ Easier audit trails
- ✅ Different permission levels per environment

### **4. Monitor Service Principal Usage**

Check Azure AD sign-in logs:
1. Azure Portal → Azure Active Directory → Sign-in logs
2. Filter by Application ID
3. Review for suspicious activity

---

## 🐛 **Troubleshooting**

### **Error: "OAuth token exchange failed"**

**Cause**: Invalid CLIENT_ID or CLIENT_SECRET

**Solution**:
1. Verify CLIENT_ID matches your Service Principal Application ID
2. Verify CLIENT_SECRET is correct (regenerate if needed)
3. Check if secret has expired

### **Error: "Service Principal authentication failed"**

**Cause**: Service Principal not added to Databricks workspace

**Solution**:
1. Go to Databricks UI → Settings → Identity and access
2. Add Service Principal with Application ID
3. Grant appropriate permissions

### **Error: "Permission denied" when accessing Unity Catalog**

**Cause**: Service Principal lacks Unity Catalog permissions

**Solution**:
```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG ab_dev_catalog TO `a7923e63-e14c-4c32-87cc-e89e3b4a165a`;
GRANT USE SCHEMA ON SCHEMA ab_dev_catalog.default TO `a7923e63-e14c-4c32-87cc-e89e3b4a165a`;
```

### **Error: "Token expired" during long-running operations**

**Cause**: OAuth token expired (1 hour default)

**Solution**: The script automatically refreshes tokens! If you still see this error, check:
1. System clock is synchronized
2. Network connectivity is stable
3. Token cache is working (`_OAUTH_TOKEN_CACHE` in prerequisites.py)

---

## 📊 **Comparison: PAT vs Service Principal**

| Aspect | PAT Token | Service Principal |
|--------|-----------|-------------------|
| **Expiration** | 90 days max | Token auto-refreshes (1 hour) |
| **Rotation** | Manual | Automatic (token), Manual (secret) |
| **Tied to User** | Yes (breaks if user leaves) | No (organization-owned) |
| **Audit Trail** | Shows as user | Shows as SP (clearer) |
| **Production Ready** | ❌ Not recommended | ✅ Recommended |
| **Setup Complexity** | Simple | Moderate |
| **Security** | Medium | High |
| **Cost** | Free | Free |

---

## ✅ **Verification Checklist**

Before going to production, verify:

- [ ] Service Principal created in Azure AD
- [ ] Service Principal added to Databricks workspace
- [ ] Service Principal has required permissions (clusters, warehouses, UC)
- [ ] `{ENV}_CLIENT_ID` variables configured in GitHub
- [ ] `{ENV}_CLIENT_SECRET` secrets configured in GitHub
- [ ] Local test successful (`prerequisites.py` runs without errors)
- [ ] GitHub workflow test successful (DEV environment)
- [ ] Authentication logs show "Using Service Principal authentication"
- [ ] Backup files generated correctly
- [ ] Client secret expiration reminder set (90 days)
- [ ] Documentation updated for team

---

## 📚 **Additional Resources**

- [Databricks Service Principal Documentation](https://docs.databricks.com/en/dev-tools/service-principals.html)
- [Azure AD App Registration Guide](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
- [Databricks OAuth Authentication](https://docs.databricks.com/en/dev-tools/auth.html#oauth-2-0)
- [GitHub Actions Secrets Management](https://docs.github.com/en/actions/security-guides/encrypted-secrets)

---

## 🆘 **Need Help?**

If you encounter issues:

1. **Check the workflow logs** in GitHub Actions for detailed error messages
2. **Test locally** using the commands in the "Testing" section
3. **Verify permissions** in Databricks UI
4. **Check Azure AD** for Service Principal status
5. **Review this documentation** for common issues

---

**Last Updated**: 2026-01-19
**Version**: 1.0
**Maintained by**: Data Engineering Team



