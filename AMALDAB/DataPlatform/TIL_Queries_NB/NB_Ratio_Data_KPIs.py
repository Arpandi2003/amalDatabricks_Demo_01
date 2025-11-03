# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Ratio_Data_KPIs <br>
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
# MAGIC
# MAGIC CREATE OR REPLACE VIEW finance.tab_ratio2025_KPIs AS
# MAGIC --start with axiom_ratio 2025 table + join axiom_ratio lookup table
# MAGIC WITH joined AS (
# MAGIC SELECT 
# MAGIC m.RatioID,
# MAGIC m.M1,
# MAGIC m.M2,
# MAGIC m.M3,
# MAGIC m.M4,
# MAGIC m.M5,
# MAGIC m.M6,
# MAGIC m.M7,
# MAGIC m.M8,
# MAGIC m.M9,
# MAGIC m.M10,
# MAGIC m.M11,
# MAGIC m.M12,
# MAGIC l.Description
# MAGIC FROM bronze.axiom_ratio2025 as m
# MAGIC LEFT JOIN bronze.axiom_ratio as l
# MAGIC ON m.RatioID = l.RatioID
# MAGIC WHERE m.RatioID IN (9010338, 9010380, 9010000, 9100002, 9010326, 9010230)
# MAGIC ),
# MAGIC --filtered to the ratio IDs relevant to the balance sheet, can be modified to add more metrics
# MAGIC
# MAGIC --join in budget values from axiom_ratiobud2025
# MAGIC joined_target AS (
# MAGIC SELECT 
# MAGIC r.RatioID,
# MAGIC r.M1,
# MAGIC r.M2,
# MAGIC r.M3,
# MAGIC r.M4,
# MAGIC r.M5,
# MAGIC r.M6,
# MAGIC r.M7,
# MAGIC r.M8,
# MAGIC r.M9,
# MAGIC r.M10,
# MAGIC r.M11,
# MAGIC r.M12,
# MAGIC l.Description
# MAGIC FROM bronze.axiom_ratiobud2025 as r
# MAGIC LEFT JOIN bronze.axiom_ratio as l
# MAGIC ON r.RatioID = l.RatioID
# MAGIC WHERE r.RatioID IN (9010338, 9010380, 9010000, 9100002, 9010326, 9010230)
# MAGIC ),
# MAGIC
# MAGIC --pivot month columns to rows and format them as dates
# MAGIC month_pivot AS (
# MAGIC SELECT 
# MAGIC Description,
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
# MAGIC Value
# MAGIC FROM (
# MAGIC   SELECT Description,
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
# MAGIC   ) AS (Month, Value)
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     FROM joined
# MAGIC   ) t
# MAGIC )
# MAGIC ),
# MAGIC
# MAGIC --do the same pivot for budget values
# MAGIC month_pivot_target AS (
# MAGIC SELECT 
# MAGIC Description || ' target' AS Description,
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
# MAGIC Value
# MAGIC FROM (
# MAGIC   SELECT Description,
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
# MAGIC   ) AS (Month, Value)
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     FROM joined_target
# MAGIC   ) t
# MAGIC )
# MAGIC ),
# MAGIC
# MAGIC --union rows for metrics with metric targets
# MAGIC combined AS (
# MAGIC   SELECT * FROM month_pivot
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM month_pivot_target
# MAGIC )
# MAGIC
# MAGIC --pivot again to turn metrics (currently rows) into columns with intuitive names
# MAGIC SELECT * 
# MAGIC FROM combined
# MAGIC PIVOT (
# MAGIC   MAX(Value) FOR Description IN (
# MAGIC     'Tangible common equity to assets', 'Tangible common equity to assets target', 
# MAGIC     'Tangible book value per share', 'Tangible book value per share target',
# MAGIC     'Tier 1 leverage ratio', 'Tier 1 leverage ratio target',
# MAGIC     'Loans/ Deposits', 'Loans/ Deposits target',
# MAGIC     'Core ROAA, excl TC sol', 'Core ROAA, excl TC sol target',
# MAGIC     'Net interest margin', 'net interest margin target')
# MAGIC )
# MAGIC WHERE DATE_TRUNC('month', Date) < DATE_TRUNC('month', CURRENT_DATE)
# MAGIC --filtering to end of prior month (when we have reliable data from ratios table)
# MAGIC ORDER BY Date ASC
# MAGIC ;
