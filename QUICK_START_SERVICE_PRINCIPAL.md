# 🚀 Quick Start: Service Principal Authentication

**Time to complete**: 15-20 minutes

---

## 📋 **What You Need**

1. ✅ Service Principal Application ID: `a7923e63-e14c-4c32-87cc-e89e3b4a165a` (you already have this)
2. ❓ Service Principal Client Secret (you need to get/create this)
3. ✅ Access to GitHub repository settings
4. ✅ Access to Databricks workspace admin

---

## ⚡ **5-Step Setup**

### **Step 1: Get Service Principal Client Secret** (5 min)

**Option A: If you have the secret already**
- ✅ Skip to Step 2

**Option B: Create a new secret**
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to: **Azure Active Directory** → **App Registrations**
3. Find your app with ID: `a7923e63-e14c-4c32-87cc-e89e3b4a165a`
4. Click **Certificates & secrets** → **New client secret**
5. Description: `Databricks-CI-CD-2026`
6. Expires: `90 days` (or your org's policy)
7. Click **Add**
8. **⚠️ COPY THE SECRET VALUE IMMEDIATELY** (you can't see it again!)

---

### **Step 2: Add Service Principal to Databricks** (3 min)

1. Go to your Databricks workspace: `https://adb-201068313543333.13.azuredatabricks.net`
2. Click **Settings** (gear icon) → **Identity and access**
3. Click **Service principals** tab
4. Click **Add service principal**
5. Enter Application ID: `a7923e63-e14c-4c32-87cc-e89e3b4a165a`
6. Click **Add**
7. Grant permissions:
   - ✅ Workspace access
   - ✅ Allow cluster creation
   - ✅ SQL Warehouse access

---

### **Step 3: Configure GitHub Variables** (2 min)

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions** → **Variables** tab
3. Click **New repository variable** and add these:

| Name | Value |
|------|-------|
| `DEV_CLIENT_ID` | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |
| `QA_CLIENT_ID` | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |
| `PROD_CLIENT_ID` | `a7923e63-e14c-4c32-87cc-e89e3b4a165a` |

**Note**: Using the same Service Principal for all environments. You can use different ones if needed.

---

### **Step 4: Configure GitHub Secrets** (2 min)

1. Still in GitHub repository settings
2. Click **Secrets** tab (next to Variables)
3. Click **New repository secret** and add these:

| Name | Value |
|------|-------|
| `DEV_CLIENT_SECRET` | `<paste the secret from Step 1>` |
| `QA_CLIENT_SECRET` | `<paste the secret from Step 1>` |
| `PROD_CLIENT_SECRET` | `<paste the secret from Step 1>` |

**Note**: Using the same secret for all environments. You can use different ones if needed.

---

### **Step 5: Test It!** (3 min)

1. Go to **Actions** tab in GitHub
2. Click **Databricks Notebooks Deployment** workflow
3. Click **Run workflow** button
4. Select **Environment**: `DEV`
5. Click **Run workflow**
6. Watch the logs - you should see:

```
🔐 Using Service Principal authentication (recommended)
   CLIENT_ID: a7923e63...
🔧 Running prerequisites script...
🔐 Authenticating with Service Principal...
   Client ID: a7923e63...165a
   Token URL: https://adb-xxx.net/oidc/v1/token
✅ OAuth token obtained successfully
   Token expires in: 3600 seconds
✅ Using Service Principal authentication (token valid until 2026-01-19 15:30:00)
```

**If you see this** → ✅ **SUCCESS! You're done!**

**If you see errors** → Check the Troubleshooting section below

---

## 🎉 **What You Just Accomplished**

✅ **Replaced PAT tokens** with production-ready Service Principal authentication  
✅ **Automatic token refresh** - no more manual token renewal every 90 days  
✅ **Better security** - tokens auto-expire after 1 hour  
✅ **Better audit trail** - actions show as Service Principal, not a user  
✅ **Organization-owned** - not tied to any individual user  

---

## 🔄 **What Happens to Your PAT Tokens?**

**Good news**: They still work as fallback!

The workflows now check:
1. **First**: Is `CLIENT_ID` + `CLIENT_SECRET` configured?
   - ✅ Yes → Use Service Principal (recommended)
   - ❌ No → Fall back to `DATABRICKS_TOKEN` (PAT)

**You can remove PAT tokens later** (after 1-2 weeks of successful SP usage):
1. Delete `{ENV}_DATABRICKS_TOKEN` secrets from GitHub
2. Revoke PAT tokens in Databricks UI

---

## 🐛 **Troubleshooting**

### **Error: "OAuth token exchange failed"**

**Check**:
- ✅ CLIENT_ID is correct: `a7923e63-e14c-4c32-87cc-e89e3b4a165a`
- ✅ CLIENT_SECRET is correct (copy-paste from Azure Portal)
- ✅ Secret hasn't expired

**Fix**: Regenerate secret in Azure Portal and update GitHub secret

---

### **Error: "Service Principal not found in workspace"**

**Check**:
- ✅ Service Principal added to Databricks workspace (Step 2)
- ✅ Application ID matches

**Fix**: Add Service Principal to Databricks workspace

---

### **Error: "Permission denied"**

**Check**:
- ✅ Service Principal has workspace access
- ✅ Service Principal can create clusters
- ✅ Service Principal has Unity Catalog permissions

**Fix**: Grant permissions in Databricks UI (Settings → Identity and access)

---

### **Still using PAT tokens?**

**Check**:
- ✅ GitHub variables configured: `{ENV}_CLIENT_ID`
- ✅ GitHub secrets configured: `{ENV}_CLIENT_SECRET`
- ✅ Variable/secret names match exactly (case-sensitive)

**Fix**: Double-check variable/secret names in GitHub settings

---

## 📚 **Next Steps**

### **Immediate**
- ✅ Test DEV environment (you just did this!)
- ✅ Test QA environment (run workflow with QA selected)
- ✅ Test PROD environment (run workflow with PROD selected)

### **This Week**
- ✅ Monitor workflow runs for any issues
- ✅ Verify all team members can trigger workflows
- ✅ Set calendar reminder for secret rotation (90 days)

### **Optional (After 1-2 Weeks)**
- ✅ Remove PAT token fallback (delete `{ENV}_DATABRICKS_TOKEN` secrets)
- ✅ Revoke PAT tokens in Databricks UI
- ✅ Update team documentation

---

## 📖 **Full Documentation**

For detailed information, see:
- **`SERVICE_PRINCIPAL_SETUP.md`** - Complete setup guide with all details
- **`SERVICE_PRINCIPAL_IMPLEMENTATION_SUMMARY.md`** - Technical implementation details

---

## ✅ **Verification Checklist**

Before marking this as complete:

- [ ] Service Principal client secret obtained from Azure Portal
- [ ] Service Principal added to Databricks workspace
- [ ] Service Principal has required permissions (workspace, clusters, UC)
- [ ] GitHub variables configured: `DEV_CLIENT_ID`, `QA_CLIENT_ID`, `PROD_CLIENT_ID`
- [ ] GitHub secrets configured: `DEV_CLIENT_SECRET`, `QA_CLIENT_SECRET`, `PROD_CLIENT_SECRET`
- [ ] Test workflow run successful in DEV
- [ ] Logs show "Using Service Principal authentication"
- [ ] Backup files generated correctly
- [ ] Calendar reminder set for secret rotation (90 days from now)

---

**Setup Time**: 15-20 minutes  
**Difficulty**: Easy  
**Status**: ✅ Production Ready  
**Last Updated**: 2026-01-19

