# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC
# MAGIC ## Tracker Details
# MAGIC - Description: Daily_Instruments_Header
# MAGIC - Created Date: 08/12/2025
# MAGIC - Created By: Pandi Anbu
# MAGIC - Modified Date: 08/12/2025
# MAGIC - Modified By: Pandi Anbu
# MAGIC - Changes made:  
# MAGIC

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW finance.tab_instruments_header
# MAGIC AS
# MAGIC WITH instruments_control_import AS (
# MAGIC   SELECT *
# MAGIC   FROM finance.tableau_instruments_control
# MAGIC ),
# MAGIC
# MAGIC loans_fields_import AS (
# MAGIC   SELECT *
# MAGIC   FROM finance.tab_processed_loans
# MAGIC ),
# MAGIC
# MAGIC latest_loan_dates AS (
# MAGIC   SELECT 
# MAGIC     MAX(EntryDate) AS latest_date, 
# MAGIC     MIN(EntryDate) AS max_prior_date, 
# MAGIC     DATE(DATEADD(day, -1, DATE_TRUNC('month', MAX(EntryDate)))) AS prior_month, 
# MAGIC     DATE(DATEADD(day, -1, DATE_TRUNC('quarter', MAX(EntryDate)))) AS prior_quarter, 
# MAGIC     DATE(DATEADD(day, -1, DATE_TRUNC('year', MAX(EntryDate)))) AS prior_year
# MAGIC   FROM loans_fields_import
# MAGIC   WHERE lookup_YRMO = max_lookup_YRMO2
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   instruments_control_import.*, 
# MAGIC   latest_loan_dates.*
# MAGIC FROM instruments_control_import,
# MAGIC latest_loan_dates;
