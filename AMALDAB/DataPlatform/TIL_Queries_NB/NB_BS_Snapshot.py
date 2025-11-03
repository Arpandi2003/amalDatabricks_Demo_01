# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_BS_Snapshot <br>
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
# MAGIC --Query that creates tab_bs_snapshot view in ab_dev_catalog.finance
# MAGIC --Takes the balance sheet data (GL data with some modification) and filters to only dates that are used for comparisons in the Balance Sheet
# MAGIC --This is used to create a smaller, more performant dataset for the dashboard, and will dynamically pull dates relative to the latest day we have data loaded for.
# MAGIC
# MAGIC CREATE OR REPLACE VIEW finance.tab_bs_snapshot AS
# MAGIC
# MAGIC --start with tab_bs_data view (pivoted GL data with groupings for balance sheet)
# MAGIC WITH original_table AS (
# MAGIC SELECT *
# MAGIC FROM finance.tab_bs_data
# MAGIC ),
# MAGIC
# MAGIC --Find the latest business day we have data for
# MAGIC max_date AS (
# MAGIC   SELECT MAX(Date) as ReportDate
# MAGIC   FROM original_table
# MAGIC   WHERE IsBD != 0
# MAGIC   AND value > 0
# MAGIC   AND Date <= CURRENT_DATE
# MAGIC ),
# MAGIC
# MAGIC --Grab balance sheet data for the max date
# MAGIC table_with_max AS (
# MAGIC   SELECT
# MAGIC     o.*,
# MAGIC     m.ReportDate
# MAGIC   FROM original_table as o
# MAGIC   JOIN max_date as m on 1=1
# MAGIC ),
# MAGIC
# MAGIC --Modify the previous CTE so that we have the value at the max date in every row (to calculate variance at a row level)
# MAGIC max_values AS (
# MAGIC   SELECT
# MAGIC     ACCT,
# MAGIC     DEPT,
# MAGIC     Value AS ReportValue
# MAGIC   FROM table_with_max
# MAGIC   WHERE Date = ReportDate
# MAGIC ),
# MAGIC
# MAGIC --Frind the preivous day relative to max date
# MAGIC prior_day AS (
# MAGIC   SELECT
# MAGIC   MAX(PriorDate) AS PriorDay
# MAGIC   FROM table_with_max
# MAGIC   WHERE Date = ReportDate
# MAGIC ),
# MAGIC
# MAGIC --find the prior month relative to max date
# MAGIC prior_month AS (
# MAGIC   SELECT MAX(DATE(DATE_TRUNC('MONTH', ReportDate) - INTERVAL 1 DAY)) as PriorMonth
# MAGIC   FROM table_with_max
# MAGIC   WHERE IsBD != 0
# MAGIC   AND value > 0
# MAGIC ),
# MAGIC
# MAGIC --find the prior quarter relative to max date
# MAGIC prior_quarter AS (
# MAGIC   SELECT MAX(DATE(DATE_TRUNC('QUARTER', ReportDate) - INTERVAL 1 DAY)) as PriorQuarter
# MAGIC   FROM table_with_max
# MAGIC   WHERE IsBD != 0
# MAGIC   AND value > 0
# MAGIC ),
# MAGIC
# MAGIC --find the prior year relative to max date
# MAGIC prior_year AS (
# MAGIC SELECT MAX(DATE(DATE_TRUNC('YEAR', ReportDate) - INTERVAL 1 DAY)) as PriorYear
# MAGIC FROM table_with_max
# MAGIC   WHERE IsBD != 0
# MAGIC   AND value > 0
# MAGIC ),
# MAGIC
# MAGIC --Join all comparison dates to every row (so that each is its own column)
# MAGIC comparison_table AS (
# MAGIC SELECT 
# MAGIC   table_with_max.*,
# MAGIC   prior_day.PriorDay,
# MAGIC   prior_month.PriorMonth,
# MAGIC   prior_quarter.PriorQuarter,
# MAGIC   prior_year.PriorYear,
# MAGIC   max_values.ReportValue
# MAGIC   FROM table_with_max
# MAGIC   JOIN max_values ON table_with_max.ACCT = max_values.ACCT
# MAGIC   AND table_with_max.DEPT = max_values.DEPT
# MAGIC   JOIN prior_day ON 1=1
# MAGIC   JOIN prior_month ON 1=1
# MAGIC   JOIN prior_quarter ON 1=1
# MAGIC   JOIN prior_year ON 1=1
# MAGIC ),
# MAGIC
# MAGIC --Filter main table to each date comparison
# MAGIC current_filter AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   'Current Day' AS DatePeriod
# MAGIC   FROM comparison_table
# MAGIC   WHERE Date = ReportDate
# MAGIC ),
# MAGIC
# MAGIC prior_day_filter AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   'Prior Day' AS DatePeriod
# MAGIC   FROM comparison_table
# MAGIC   WHERE Date = PriorDay
# MAGIC ),
# MAGIC
# MAGIC prior_month_filter AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   'Prior Month' AS DatePeriod
# MAGIC   FROM comparison_table
# MAGIC   WHERE Date = PriorMonth
# MAGIC ),
# MAGIC
# MAGIC prior_quarter_filter AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   'Prior Quarter' AS DatePeriod
# MAGIC   FROM comparison_table
# MAGIC   WHERE Date = PriorQuarter
# MAGIC ),
# MAGIC
# MAGIC prior_year_filter AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   'Prior Year' AS DatePeriod
# MAGIC   FROM comparison_table
# MAGIC   WHERE Date = PriorYear
# MAGIC )
# MAGIC
# MAGIC --union all of the above CTEs that filter to each date comparison
# MAGIC --Note that we want each comparison as its own CTE in case the comparison date is the same (ex: end of prior month could be the same as end of prior quarter)
# MAGIC SELECT * FROM current_filter
# MAGIC UNION ALL
# MAGIC SELECT * FROM prior_day_filter
# MAGIC UNION ALL
# MAGIC SELECT * FROM prior_month_filter
# MAGIC UNION ALL
# MAGIC SELECT * FROM prior_quarter_filter
# MAGIC UNION ALL
# MAGIC SELECT * FROM prior_year_filter
# MAGIC
# MAGIC ;
