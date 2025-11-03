# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Balance_Sheet_Ratios <br>
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
# MAGIC --Query that creates finance.tab_bs_ratios view in ab_dev_catalog.finance
# MAGIC --Used for the KPIs on the Balance Sheet Tableau Dashboard that come from the GL data
# MAGIC
# MAGIC CREATE OR REPLACE VIEW finance.tab_bs_ratios AS
# MAGIC
# MAGIC --start with tab_bs_data view (dataset with grouping calcs for balance sheet, acct and dept tables joined)
# MAGIC WITH main_table AS (
# MAGIC   SELECT *
# MAGIC FROM finance.tab_bs_data AS bs
# MAGIC ),
# MAGIC
# MAGIC --group by month to find the latest business day in each month
# MAGIC month_end_dates AS (
# MAGIC   SELECT 
# MAGIC     DATE(DATE_TRUNC('month', Date)) AS month,
# MAGIC     MAX(Date) AS last_bd
# MAGIC   FROM main_table
# MAGIC   WHERE IsBD !=0
# MAGIC   AND Value > 0
# MAGIC   GROUP BY DATE_TRUNC('month', Date)
# MAGIC ),
# MAGIC
# MAGIC --filter main table to just month end dates
# MAGIC month_end_balances AS (
# MAGIC   SELECT m.*,
# MAGIC   eom.month
# MAGIC   FROM main_table m
# MAGIC   JOIN month_end_dates eom
# MAGIC     ON DATE_TRUNC('month', m.Date) = eom.month
# MAGIC     AND m.Date = eom.last_bd
# MAGIC )
# MAGIC
# MAGIC --Calculate main metrics for each month end
# MAGIC SELECT
# MAGIC   month,
# MAGIC   SUM(CASE WHEN BS_Grouping_Summary = 'Loans' THEN Value ELSE 0 END) AS Loans,
# MAGIC   SUM(CASE WHEN BS_Grouping_Summary = 'Loans' THEN Budget ELSE 0 END) AS Loans_Budget,
# MAGIC   SUM(CASE WHEN BS_Grouping_Summary = 'Deposits' THEN Value ELSE 0 END) AS Deposits,
# MAGIC   SUM(CASE WHEN BS_Grouping_Summary = 'Deposits' THEN Budget ELSE 0 END) AS Deposits_Budget,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('HTM Securities - Traditional', 'AFS Securities - Traditional') THEN Value ELSE 0 END) AS Traditional_Securities,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('HTM Securities - Traditional', 'AFS Securities - Traditional') THEN Budget ELSE 0 END) AS Traditional_Securities_Budget,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('Certificates of Deposit', 'Money Market Accounts', 'NOW Accounts', 'Savings Accounts') THEN Value ELSE 0 END) AS Interest_Bearing_Deposits,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('Certificates of Deposit', 'Money Market Accounts', 'NOW Accounts', 'Savings Accounts') THEN Budget ELSE 0 END) AS Interest_Bearing_Deposits_Budget,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('Escrow', 'Transaction Deposits', 'Non-Interest Bearing Demand Deposits') THEN Value ELSE 0 END) AS Non_Interest_Bearing_Deposits,
# MAGIC   SUM(CASE WHEN BS_Grouping IN ('Escrow', 'Transaction Deposits', 'Non-Interest Bearing Demand Deposits') THEN Budget ELSE 0 END) AS Non_Interest_Bearing_Deposits_Budget
# MAGIC
# MAGIC   FROM month_end_balances
# MAGIC   GROUP BY month
# MAGIC   ORDER BY month DESC
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC --ratios (like Loans/Deposits) are done as calculated fields in Tableau based on the metrics above
# MAGIC
