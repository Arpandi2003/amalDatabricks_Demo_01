# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC ## Tracker Details
# MAGIC - Description: NB_CombinedDepositsGenerator
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
# MAGIC CREATE OR REPLACE VIEW finance.tab_processed_deposits
# MAGIC AS
# MAGIC with yrmodayimport AS (
# MAGIC     select 
# MAGIC         YRMODAY AS lookup_YRMODAY, 
# MAGIC         Description, 
# MAGIC         DayValue, 
# MAGIC         MonthValue, 
# MAGIC         QuarterValue, 
# MAGIC         YearValue, 
# MAGIC         YRMO AS lookup_YRMO, 
# MAGIC         to_date(DateValue, 'M/d/yyyy h:mm:ss a') as EntryDate, 
# MAGIC         to_date(PriorDataDate, 'M/d/yyyy h:mm:ss a') as PriorDate,
# MAGIC         IsBD
# MAGIC     from bronze.yrmoday
# MAGIC ),
# MAGIC
# MAGIC daily_deposits_import as (
# MAGIC   select *
# MAGIC   from bronze.axiom_daily_deposits),
# MAGIC
# MAGIC daily_cds_import as (
# MAGIC   select *
# MAGIC   from bronze.axiom_daily_cds
# MAGIC ),
# MAGIC /* ZIPs must be cast as string due to inconsistent table data types for this field*/
# MAGIC daily_deposits_raw AS(
# MAGIC   SELECT
# MAGIC       dd.YRMODAY,
# MAGIC       dd.YRMO,
# MAGIC       dd.InstrumentID,
# MAGIC       dd.ACCT,
# MAGIC       dd.DEPT,
# MAGIC       dd.CustName,
# MAGIC       dd.Officer1Name,
# MAGIC       dd.Officer2Name,
# MAGIC       dd.AX_CurBal AS CurrentBalance,
# MAGIC       dd.AX_AvgBal AS AverageBalance,
# MAGIC       dd.AX_OrigDate AS OriginDate,
# MAGIC       to_date(dd.LastActivityDate, 'M/d/yyyy h:mm:ss a') as LastActivityDate,
# MAGIC       to_date(dd.LastOverdraftDate, 'M/d/yyyy h:mm:ss a') as LastOverdraftDate,
# MAGIC       dd.OneWaySell,
# MAGIC             --adding OnewaySell column to determine on vs off balance sheet deposits
# MAGIC       dd.Status,
# MAGIC       dd.AcctType AS AccountType,
# MAGIC       dd.Branch,
# MAGIC       dd.Address1,
# MAGIC       dd.Address2,
# MAGIC       dd.Address3,
# MAGIC       dd.City,
# MAGIC       dd.State,
# MAGIC       CAST(dd.ZipCode AS STRING) AS ZipCode,
# MAGIC       dd.PoliticalFlag,
# MAGIC       dd.PoliticalRisk,
# MAGIC       dd.Segment,
# MAGIC       dd.RelationshipType
# MAGIC       from daily_deposits_import dd
# MAGIC       WHERE dd.YRMODAY >= 20241231
# MAGIC       and dd.instrumentid != 'DD-0001180522'
# MAGIC ),
# MAGIC
# MAGIC daily_cds_raw AS(
# MAGIC   SELECT
# MAGIC       YRMODAY,
# MAGIC       YRMO,
# MAGIC       InstrumentID,
# MAGIC       ACCT,
# MAGIC       DEPT,
# MAGIC       CustName,
# MAGIC       Off1Name AS Officer1Name,
# MAGIC       Off2Name AS Officer2Name,
# MAGIC       AX_CurBal AS CurrentBalance,
# MAGIC       AX_AvgBal AS AverageBalance,
# MAGIC       AX_OrigDate AS OriginDate,
# MAGIC       NULL as LastActivityDate,
# MAGIC       NULL as LastOverdraftDate,
# MAGIC       NULL as OneWaySell,
# MAGIC       Status,
# MAGIC       AcctType AS AccountType,
# MAGIC       Branch,
# MAGIC       Address1,
# MAGIC       Address2,
# MAGIC       Address3,
# MAGIC       City,
# MAGIC       State,
# MAGIC       CAST(ZipCode AS String) AS ZipCode,
# MAGIC       0 AS PoliticalFlag,
# MAGIC       NULL AS PoliticalRisk,
# MAGIC       Segment,
# MAGIC       RelationshipType
# MAGIC       from daily_cds_import
# MAGIC       WHERE YRMODAY >= 20241231
# MAGIC ),
# MAGIC
# MAGIC combined_deposits_cds AS(
# MAGIC   SELECT *
# MAGIC   FROM daily_deposits_raw
# MAGIC   UNION ALL
# MAGIC   SELECT *
# MAGIC   FROM daily_cds_raw
# MAGIC ),
# MAGIC /* Maximum date in data source */
# MAGIC max_date AS (
# MAGIC   SELECT MAX(YRMODAY) as latest_DepositEntry
# MAGIC   FROM combined_deposits_cds
# MAGIC ),
# MAGIC /* Latest business date for each month - end of month */
# MAGIC max_entry AS (
# MAGIC   SELECT lookup_YRMO as max_lookup_YRMO, MAX(EntryDate) AS max_EntryDate
# MAGIC   FROM yrmodayimport
# MAGIC   WHERE IsBD = 1
# MAGIC   GROUP BY max_lookup_YRMO 
# MAGIC ),
# MAGIC /* for current month, find maximum prior business date  - *not* for each month* */
# MAGIC max_prior AS (
# MAGIC   SELECT lookup_YRMO as max_lookup_YRMO2, MAX(PriorDate) as max_PriorDate
# MAGIC   FROM yrmodayimport y
# MAGIC   INNER JOIN max_date md ON y.lookup_YRMODAY = md.latest_DepositEntry
# MAGIC   GROUP BY max_lookup_YRMO2  
# MAGIC ),
# MAGIC  
# MAGIC /* This creates calendar of last date of all prior months, plus most recent date, plus most recent prior business date*/ 
# MAGIC filtered_yrmoimport AS (
# MAGIC   SELECT *, DATE_TRUNC('month', EntryDate) AS EntryMonth
# MAGIC   FROM yrmodayimport y
# MAGIC   LEFT JOIN max_date md ON y.lookup_YRMODAY = md.latest_DepositEntry
# MAGIC   LEFT JOIN max_entry my ON y.lookup_YRMO = my.max_lookup_YRMO
# MAGIC   LEFT JOIN max_prior mp ON y.lookup_YRMO = mp.max_lookup_YRMO2
# MAGIC   WHERE y.EntryDate = my.max_EntryDate -- latest date per month
# MAGIC   OR y.EntryDate = mp.max_PriorDate  -- latest prior date (previous day)
# MAGIC   OR y.lookup_YRMODAY = md.latest_DepositEntry -- latest date loaded
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC   from filtered_yrmoimport y
# MAGIC   INNER JOIN combined_deposits_cds d ON y.lookup_YRMODAY = d.YRMODAY
# MAGIC   WHERE lookup_YRMO >=202412

# COMMAND ----------



# COMMAND ----------


