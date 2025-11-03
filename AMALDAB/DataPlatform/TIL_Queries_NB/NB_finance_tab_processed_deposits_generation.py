# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC ## Tracker Details
# MAGIC - Description: NB_CombinedDepositsGenerator
# MAGIC - Created Date: 08/12/2025
# MAGIC - Created By: Pandi Anbu
# MAGIC - Modified Date: 09/16/2025
# MAGIC - Modified By: Aaron Potts
# MAGIC - Changes made: Refactoring to match AB internal coding practices, implemented type changes
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
import pandas as pd
import os

# COMMAND ----------

# DBTITLE 1,Initialize Logger Notebook
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,Initialize Utility Notebook
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,Notebook Configuration
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,Set Catalog
# Set overall catalog. If cloning to user space, this seems to pull from UAT preferentially
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.

ErrorLogger = ErrorLogs(f"finance_tab_processed_deposits_generation")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for tab_processed_deposits")

# COMMAND ----------

# This code reads data from the 'config.metadata' table, filtering for the specified table
DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 3002)
)

display(DFMetadata)

# COMMAND ----------

# DBTITLE 1,Metadata config

    # Use metadata from config table
    TableID = 3002
    metadata = GetMetaDataDetails(TableID)
    LoadType = metadata['LoadType']
    LastLoadColumnName = metadata['LastLoadDateColumn']
    DependencyTableIDs = metadata['DependencyTableIDs']
    SourceDBName = metadata['SourceDBName']
    LastLoadDate = metadata['LastLoadDateValue']
    DWHSchemaName = metadata['DWHSchemaName']
    DWHTableName = metadata['DWHTableName']
    MergeKey = metadata['MergeKey']
    MergeKeyColumn = metadata['MergeKeyColumn']
    SourceSelectQuery = metadata['SourceSelectQuery']
    SourcePath = metadata['SourcePath']
    LoadedDependencies = True
    SrcTableName = metadata['SourceTableName']
    sourcesystem = metadata['SourceSystem']
    schemanames = metadata['SourceSchema']

# COMMAND ----------

# DBTITLE 1,YRMODAY Date Scaffold Import
# Import yrmoday date lookup table
# This will be used to filter out dates that are not business days, and toward the end of the notebook we will use it to create a calendar scaffold with key dates needed for proper display in Tableau
try: 
    df_yrmodayimport_tpd = spark.sql("""
    select 
            YRMODAY AS lookup_YRMODAY, 
            Description, 
            DayValue, 
            MonthValue, 
            QuarterValue, 
            YearValue, 
            YRMO AS lookup_YRMO, 
            to_date(DateValue, 'M/d/yyyy h:mm:ss a') as EntryDate, 
            to_date(PriorDataDate, 'M/d/yyyy h:mm:ss a') as PriorDate,
            IsBD
        from bronze.yrmoday """)
    df_yrmodayimport_tpd.createOrReplaceTempView("vw_yrmodayimport_tpd")
    df_yrmodayimport_tpd.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error generating yrmoday date scaffold: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Daily Deposits Import

# Import axiom_daily_deposits table, only for business days defined in yrmoday table
try:
  df_daily_deposits_import_tpd = spark.sql("""
  select *
    from bronze.axiom_daily_deposits
    WHERE YRMODAY IN (SELECT lookup_YRMODAY FROM vw_yrmodayimport_tpd WHERE isbd = 1) """)
  df_daily_deposits_import_tpd.createOrReplaceTempView("vw_daily_deposits_import_tpd")
  df_daily_deposits_import_tpd.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error generating importing daily deposits table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Daily CDs Import
# Imports Axiom daily_cds table, only for business days defined in yrmoday table

try:
  df_daily_cds_import_tpd = spark.sql("""
  select *
    from bronze.axiom_daily_cds
    WHERE YRMODAY IN (SELECT lookup_YRMODAY FROM vw_yrmodayimport_tpd WHERE isbd = 1) """)
  df_daily_cds_import_tpd.createOrReplaceTempView("vw_daily_cds_import_tpd")
  df_daily_cds_import_tpd.display()
except Exception as e:
  # Log errors as failed
  logger.error(f"Error generating importing daily deposits table: {e}")
  UpdatePipelineStatusAndTime(TableID, "Failed")
  ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Daily Deposits Type and Select Fields
# We choose relevant columns from the daily deposit table and cast values to appropriate types where necessary. Current and Average Balance are cast to decimal(18,6)t. LastActivityDate and LastOverdraftDate are cast as necessary; acct, dept, and ZipCode are set to strings despite being integers in the source table.
# We restrict the size of the table by only loading values after end of 2024; we also filter InstrumentID DD-0001180522 per request from finance

try:
      df_daily_deposits_typed_tpd = spark.sql("""
      SELECT
            dd.YRMODAY,
            dd.YRMO,
            dd.InstrumentID,
            TRY_CAST(dd.ACCT AS string) AS ACCT,
            TRY_CAST(dd.DEPT AS string) AS DEPT,
            dd.CustName,
            dd.Officer1Name,
            dd.Officer2Name,
            TRY_CAST(dd.AX_CurBal AS decimal(18,6)) AS CurrentBalance,
            TRY_CAST(dd.AX_AvgBal AS decimal(18,6)) AS AverageBalance,
            dd.AX_OrigDate AS OriginDate,
            to_date(dd.LastActivityDate, 'M/d/yyyy h:mm:ss a') as LastActivityDate,
            to_date(dd.LastOverdraftDate, 'M/d/yyyy h:mm:ss a') as LastOverdraftDate,
            dd.OneWaySell,
            dd.Status,
            dd.AcctType AS AccountType,
            dd.Branch,
            dd.Address1,
            dd.Address2,
            dd.Address3,
            dd.City,
            dd.State,
            CAST(dd.ZipCode AS STRING) AS ZipCode,
            dd.PoliticalFlag,
            dd.PoliticalRisk,
            dd.Segment,
            dd.RelationshipType
            from vw_daily_deposits_import_tpd dd
            WHERE dd.YRMODAY >= 20241231
            and dd.instrumentid != 'DD-0001180522' """)
      df_daily_deposits_typed_tpd.createOrReplaceTempView("vw_daily_deposits_typed_tpd")
      df_daily_deposits_typed_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error typing and selecting fields after import in daily deposits table: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Daily CDs Type and Select Fields
# We choose relevant columns from the daily cds table and cast values to appropriate types where necessary. Current and Average Balance are cast to decimal(18,6). Acct, dept,and ZipCode are set to strings despite being integers in the source table.
# We restrict the size of the table by only loading values starting end of 2024
# We create the PoliticalFlag, PoliticalRisk, LastActivityDate, LastOverDraftDate, and OneWaySell columns as placeholders or with values defined by the business so we can union with deposits in the next stage
try:
      df_daily_cds_typed_tpd = spark.sql("""
      SELECT
            YRMODAY,
            YRMO,
            InstrumentID,
            TRY_CAST(ACCT AS STRING) AS ACCT,
            TRY_CAST(DEPT AS STRING) AS DEPT,
            CustName,
            Off1Name AS Officer1Name,
            Off2Name AS Officer2Name,
            TRY_CAST(AX_CurBal AS decimal(18,6)) AS CurrentBalance,
            TRY_CAST(AX_AvgBal AS decimal(18,6)) AS AverageBalance,
            AX_OrigDate AS OriginDate,
            TRY_CAST(NULL AS date) AS LastActivityDate,
            TRY_CAST(NULL AS date) AS LastOverdraftDate,
            TRY_CAST(NULL AS STRING) AS OneWaySell,
            Status,
            AcctType AS AccountType,
            Branch,
            Address1,
            Address2,
            Address3,
            City,
            State,
            CAST(ZipCode AS String) AS ZipCode,
            0 AS PoliticalFlag,
            TRY_CAST(NULL AS STRING) AS PoliticalRisk,
            Segment,
            RelationshipType
            from vw_daily_cds_import_tpd
            WHERE YRMODAY >= 20241231 """)
      df_daily_cds_typed_tpd.createOrReplaceTempView("vw_daily_cds_typed_tpd")
      df_daily_cds_typed_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error typing and selecting fields after import in daily cds table: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Combined deposits and CDs
# Union of both cds and deposits - errors here often come from field or type mismatches
try:
    df_combined_deposits_cds_tpd = spark.sql("""
        SELECT *
        FROM vw_daily_deposits_typed_tpd
        UNION ALL
        SELECT *
        FROM vw_daily_cds_typed_tpd """)
    df_combined_deposits_cds_tpd.createOrReplaceTempView("vw_combined_deposits_cds_tpd")
    df_combined_deposits_cds_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during UNION ALL of processed deposits and cds table: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Find Latest Entry
# We get the latest date present in the deposits and cds combined table, which excludes all non-business days.
try:
    df_max_date_tpd = spark.sql("""
        SELECT MAX(YRMODAY) as latest_DepositEntry
        FROM vw_combined_deposits_cds_tpd """)
    df_max_date_tpd.createOrReplaceTempView("vw_max_date_tpd")
    df_max_date_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during generation of latest entry from combined table: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,(max_entry) Latest Business Date per Month
# We get the latest business date per month in the calendar.

try:
    df_max_entry_tpd = spark.sql("""
        SELECT lookup_YRMO as max_lookup_YRMO, MAX(EntryDate) AS max_EntryDate
        FROM vw_yrmodayimport_tpd
        WHERE IsBD = 1
        GROUP BY max_lookup_YRMO """)
    df_max_entry_tpd.createOrReplaceTempView("vw_max_entry_tpd")
    df_max_entry_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during calculation of latest business date per month in view: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,(max_prior) Prior Business Date for current month
# We get the latest prior business date for the current month to permit DoD comparisons in Tableau.

try:
    df_max_prior_tpd = spark.sql("""
    SELECT
        lookup_YRMO as max_lookup_YRMO2, MAX(PriorDate) as max_PriorDate
        FROM vw_yrmodayimport_tpd y
        WHERE lookup_YRMODAY = (SELECT latest_DepositEntry FROM vw_max_date_tpd)
        GROUP BY max_lookup_YRMO2   """)
    df_max_prior_tpd.createOrReplaceTempView("vw_max_prior_tpd")
    df_max_prior_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during calculation of prior business date: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Calendar Generation
# We assemble a calendar where all values match the last business date of each month, the most recent business day with data, or the day prior to that, and those values are preserved as fields for analysis. We filter the dates to ensure that dates starting at the end of 2024 are included
try:
    df_filtered_yrmoimport_tpd = spark.sql("""
    SELECT
        *, DATE_TRUNC('month', y.EntryDate) AS EntryMonth
        FROM vw_yrmodayimport_tpd y
        LEFT JOIN vw_max_date_tpd md ON y.lookup_YRMODAY = md.latest_DepositEntry
        LEFT JOIN vw_max_entry_tpd my ON y.lookup_YRMO = my.max_lookup_YRMO
        LEFT JOIN vw_max_prior_tpd mp ON y.lookup_YRMO = mp.max_lookup_YRMO2
        WHERE (y.EntryDate = my.max_EntryDate -- latest date per month
        OR y.EntryDate = mp.max_PriorDate  -- latest prior date (previous day)
        OR y.lookup_YRMODAY = md.latest_DepositEntry -- latest date loaded
        ) AND y.lookup_YRMO >=202412
        """)
    df_filtered_yrmoimport_tpd.createOrReplaceTempView("vw_filtered_yrmoimport_tpd")
    df_filtered_yrmoimport_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during creation of final calendar with EOM/max date/max prior date fields: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Final CTE
# We join the calendar with the deposits and cds tables, rejecting values in the tables not in the calendar
try:
    df_final_tpd = spark.sql("""
    select
        *
        from vw_filtered_yrmoimport_tpd y
        INNER JOIN vw_combined_deposits_cds_tpd d ON y.lookup_YRMODAY = d.YRMODAY
        """)
    df_final_tpd.createOrReplaceTempView("vw_final_tpd")
    df_final_tpd.display()
except Exception as e:
      # Log errors as failed
      logger.error(f"Error during join of final calendar to combined deposits and cds table: {e}")
      UpdatePipelineStatusAndTime(TableID, "Failed")
      ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Create Table
# Creates a table because Databricks cannot create views from temporary views. Data governance will advise on how to proceed with finance package
try:
    if spark.catalog.tableExists("finance.tab_processed_deposits"):
        df_changed = DataTypeChange(df_final_tpd, "finance", "tab_processed_deposits")
    df_changed.write.mode("overwrite").saveAsTable(
                f"{catalog}.finance.tab_processed_deposits"
    )
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
    logger.info("Table tab_processed_deposits_v3 created successfully as a Delta table.")
except Exception as e:
    logger.error(f"Error creating tab_processed_deposits_v3: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = (
        str(e).split('java.lang.Exception: ')[0]
        if 'java.lang.Exception: ' in str(e)
        else str(e).split('at ')[0]
    )
