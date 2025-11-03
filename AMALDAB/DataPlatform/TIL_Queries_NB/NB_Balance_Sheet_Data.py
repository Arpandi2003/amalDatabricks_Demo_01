# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Balance_Sheet_Data <br>
# MAGIC **Created By:** Naveena <br>
# MAGIC **Created Date:** 12/08/2025<br>
# MAGIC **Modified By:** Naveena<br>
# MAGIC **Modified Date** 12/08/2025<br>
# MAGIC **Modification** :

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query that creates tab_bs_data view in ab_dev_catalog.finance
# MAGIC --Take the pivoted GL data and create groupings to match those on the Balance Sheet, also joins axiom_acct and axiom_dept tables
# MAGIC
# MAGIC CREATE OR REPLACE VIEW finance.tab_bs_data AS
# MAGIC
# MAGIC --start with pivoted GL data from (tab_gl_data_pivoted) other View in dev > finance
# MAGIC WITH gl_data AS (
# MAGIC     SELECT *
# MAGIC     FROM finance.tab_gl_data_pivoted
# MAGIC ),
# MAGIC
# MAGIC --Take ABReport, ABSummary, and ABReport_Det fields from axiom_acct table and create grouping fields to match lines of the balance sheet
# MAGIC acct AS (
# MAGIC     SELECT 
# MAGIC     ACCT,
# MAGIC     Description,
# MAGIC     ABSummary,
# MAGIC     ABReport,
# MAGIC     ABReport_Det,
# MAGIC     ABReport_Det2,
# MAGIC     CASE
# MAGIC     --categorizing assets, liabilities, and equity
# MAGIC   WHEN ABSummary IN (
# MAGIC     'Allowance for credit losses - Loans',
# MAGIC     'Cash & equivalents',
# MAGIC     'Commercial Loans',
# MAGIC     'Consumer Loans',
# MAGIC     'Deferred fees, net',
# MAGIC     'Other assets',
# MAGIC     'Securities',
# MAGIC     'Commercial Banking/C&I',
# MAGIC     'Allowance for credit losses - Securities'
# MAGIC     ) THEN 'Assets'
# MAGIC   WHEN ABSummary = 'Equity' THEN 'Equity'
# MAGIC   ELSE 'Liabilities'
# MAGIC     END AS BS_Category,
# MAGIC
# MAGIC     --categorizing next level of balance sheet (analogous to ABSummary but with more aggregated grouping)
# MAGIC     CASE
# MAGIC     WHEN ABSummary IN ('Commercial Loans', 'Consumer Loans') THEN 'Loans'
# MAGIC     WHEN ABReport = 'C&I- Commercial Banking' THEN 'Loans'
# MAGIC     WHEN ABSummary IN ('Allowance for credit losses - Loans', 'Deferred fees, net') THEN 'Loans'
# MAGIC     WHEN ABSummary IN (
# MAGIC         'Other Deposits', 'Non-Interest Bearing Demand', 'NOW', 'MMA', 'Savings',
# MAGIC         'Consumer CDs', 'CDARS / Non Broker Listing Service CDs'
# MAGIC     ) THEN 'Deposits'
# MAGIC     WHEN ABSummary = 'Borrowed funds' THEN 'Borrowed Funds'
# MAGIC     WHEN ABSummary = 'Equity' THEN 'Stockholder Equity'
# MAGIC     WHEN ABReport = 'Resell Agreements' THEN 'Cash & equivalents'
# MAGIC     WHEN ABSummary = 'Allowance for credit losses - Securities' THEN 'Securities'
# MAGIC     ELSE ABSummary
# MAGIC     END AS BS_Grouping_Summary,
# MAGIC
# MAGIC     --separate out pace and solar loans
# MAGIC     CASE
# MAGIC     WHEN ABReport IN ('1-4 Family First Mortgages', '1-4 Family Second Mortgages') THEN 'Residential Real Estate'
# MAGIC     WHEN BS_Grouping_Summary = 'Securities' AND UPPER(ABReport_Det) LIKE '%PACE%' THEN 'PACE'
# MAGIC     WHEN ABSummary = 'Consumer Loans' AND UPPER(ABReport_Det) LIKE '%SOLAR%' THEN 'Consumer Solar Loans'
# MAGIC     WHEN ABReport_Det = 'AFS - Unrealized Gain/(Loss)' THEN 'Unrealized Gain (Loss) on AFS Securities'
# MAGIC     WHEN ABReport = 'Investment Secty - HTM' THEN 'HTM Securities - Traditional'
# MAGIC     WHEN ABReport = 'Investment Secty- AFS' THEN 'AFS Securities - Traditional'
# MAGIC     ELSE ABReport
# MAGIC     END AS BS_Grouping
# MAGIC
# MAGIC     FROM bronze.axiom_acct
# MAGIC ),
# MAGIC
# MAGIC --Join in axiom_dept table and create separate grouping so that we can only show relevant depts in filter on balance sheet dashboard.
# MAGIC dept AS (
# MAGIC     SELECT
# MAGIC     DEPT,
# MAGIC     Description,
# MAGIC     CASE 
# MAGIC       WHEN DEPT IN (
# MAGIC         1001100004, 
# MAGIC         1001100004,
# MAGIC         1001100099,
# MAGIC         1001300002,
# MAGIC         1001300004,
# MAGIC         1001300008,
# MAGIC         1001300011,
# MAGIC         1001300013,
# MAGIC         1001300015,
# MAGIC         1001300020,
# MAGIC         1001300051,
# MAGIC         1001300052,
# MAGIC         1001300500,
# MAGIC         1001300508,
# MAGIC         1001300514,
# MAGIC         1001300520,
# MAGIC         1001300527,
# MAGIC         1001300530,
# MAGIC         1001320001,
# MAGIC         1001320004,
# MAGIC         1001321001,
# MAGIC         1001321005,
# MAGIC         1001340001)
# MAGIC         THEN Description
# MAGIC         ELSE 'All Other Depts'
# MAGIC         END AS BS_Dept_Filter
# MAGIC     FROM bronze.axiom_dept
# MAGIC ),
# MAGIC
# MAGIC --Join acct and dept fields with pivoted GL data to form one main table
# MAGIC joined AS (
# MAGIC SELECT 
# MAGIC g.*,
# MAGIC acct.BS_Category,
# MAGIC acct.BS_Grouping_Summary,
# MAGIC acct.BS_Grouping,
# MAGIC acct.Description AS Acct_Description,
# MAGIC acct.ABSummary,
# MAGIC acct.ABReport,
# MAGIC acct.ABReport_Det,
# MAGIC acct.ABReport_Det2,
# MAGIC dept.Description AS Dept_Description,
# MAGIC dept.BS_Dept_Filter
# MAGIC FROM gl_data AS g
# MAGIC LEFT JOIN acct ON g.ACCT = acct.ACCT
# MAGIC LEFT JOIN dept ON g.DEPT = dept.DEPT
# MAGIC ),
# MAGIC
# MAGIC --Bring budget data in and group to prevent duplication and format month columns to date format
# MAGIC budget AS (
# MAGIC SELECT 
# MAGIC ACCT,
# MAGIC DEPT,
# MAGIC SUM(M1) AS M1,
# MAGIC SUM(M2) AS M2,
# MAGIC SUM(M3) AS M3,
# MAGIC SUM(M4) AS M4,
# MAGIC SUM(M5) AS M5,
# MAGIC SUM(M6) AS M6,
# MAGIC SUM(M7) AS M7,
# MAGIC SUM(M8) AS M8,
# MAGIC SUM(M9) AS M9,
# MAGIC SUM(M10) AS M10,
# MAGIC SUM(M11) AS M11,
# MAGIC SUM(M12) AS M12
# MAGIC FROM bronze.axiom_bgt2025
# MAGIC WHERE DTYPE = 'EOM'
# MAGIC GROUP BY ACCT, DEPT
# MAGIC ),
# MAGIC
# MAGIC budget_pivoted AS (
# MAGIC SELECT
# MAGIC ACCT,
# MAGIC DEPT,
# MAGIC CASE Month
# MAGIC   WHEN 'M1' THEN DATE '2025-01-01'
# MAGIC   WHEN 'M2' THEN DATE '2025-02-01'
# MAGIC   WHEN 'M3' THEN DATE '2025-03-01'
# MAGIC   WHEN 'M4' THEN DATE '2025-04-01'
# MAGIC   WHEN 'M5' THEN DATE '2025-05-01'
# MAGIC   WHEN 'M6' THEN DATE '2025-06-01'
# MAGIC   WHEN 'M7' THEN DATE '2025-07-01'
# MAGIC   WHEN 'M8' THEN DATE '2025-08-01'
# MAGIC   WHEN 'M9' THEN DATE '2025-09-01'
# MAGIC   WHEN 'M10' THEN DATE '2025-10-01'
# MAGIC   WHEN 'M11' THEN DATE '2025-11-01'
# MAGIC   WHEN 'M12' THEN DATE '2025-12-01'
# MAGIC END AS Date,
# MAGIC Budget
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC   ACCT,
# MAGIC   DEPT,
# MAGIC   STACK(
# MAGIC     12,
# MAGIC     'M1', M1,
# MAGIC     'M2', M2,
# MAGIC     'M3', M3,
# MAGIC     'M4', M4,
# MAGIC     'M5', M5,
# MAGIC     'M6', M6,
# MAGIC     'M7', M7,
# MAGIC     'M8', M8,
# MAGIC     'M9', M9,
# MAGIC     'M10', M10,
# MAGIC     'M11', M11,
# MAGIC     'M12', M12
# MAGIC   ) AS (Month, Budget)
# MAGIC   FROM budget
# MAGIC )
# MAGIC )
# MAGIC
# MAGIC --final table to join budget data to previous CTE that includes ACCT and DEPT tables
# MAGIC SELECT 
# MAGIC j.*,
# MAGIC bp.Budget
# MAGIC FROM joined as j
# MAGIC LEFT JOIN budget_pivoted AS bp
# MAGIC ON j.ACCT = bp.ACCT
# MAGIC AND j.DEPT = bp.DEPT
# MAGIC AND DATE_TRUNC('month', j.Date) = DATE_TRUNC('month', bp.Date);
# MAGIC
