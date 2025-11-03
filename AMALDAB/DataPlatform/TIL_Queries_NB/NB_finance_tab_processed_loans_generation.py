# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC ## Tracker Details
# MAGIC - Description: NB_GatheringLoans
# MAGIC - Created Date: 08/12/2025
# MAGIC - Created By: Pandi Anbu
# MAGIC - Modified Date: 09/16/2025
# MAGIC - Modified By: Aaron Potts
# MAGIC - Changes made: Per W-001229 incorporated type changes from W-001188 (type changes) and refactored code to better match internal AB coding style
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import packages
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

# DBTITLE 1,Import Logger
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,Initialize Utilities Notebook
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,Set Catalog
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.

ErrorLogger = ErrorLogs(f"finance_tab_processed_loans_generation")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for tab_processed_loans")

# COMMAND ----------

from pyspark.sql.functions import col
# This code reads data from the 'config.metadata' table

DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 3001)
)
display(DFMetadata)

# COMMAND ----------

# Use metadata from config table
TableID = 3001
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

# DBTITLE 1,YRMODAY calendar import
# Import yrmoday date lookup table
# This will be used to filter out dates that are not business days, and toward the end of the workbook we will use it to create a calendar scaffold with key dates needed for proper display in Tableau
try: 
    df_yrmodayimport_tpl = spark.sql("""
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
    df_yrmodayimport_tpl.createOrReplaceTempView("vw_yrmodayimport_tpl")
    df_yrmodayimport_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error generating yrmoday date scaffold: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Generate last business day of each month
# We need the last business day of each month in order to:
# Push last reporting date of each month, as this is what we report in the dashboard
# Identify second to last reporting date of each month later in the query based on that last reporting date
# Have values to push forward the purchased loans portfolio to
try:
    df_max_entry_tpl = spark.sql("""
    SELECT lookup_YRMO as max_lookup_YRMO, MAX(EntryDate) AS max_EntryDate, MAX(lookup_YRMODAY) as max_YRMODAY
        FROM vw_yrmodayimport_tpl
        WHERE IsBD = 1
        GROUP BY max_lookup_YRMO """)
    df_max_entry_tpl.createOrReplaceTempView("vw_max_entry_tpl")
    df_max_entry_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error generating max entry date table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')


# COMMAND ----------

# DBTITLE 1,Daily Loans Import

# Import axiom_daily_loans table, only for business days defined in yrmoday table
try:
  df_daily_loans_import_tpl = spark.sql("""
  select *
    from bronze.axiom_daily_loans
    WHERE YRMODAY IN (SELECT lookup_YRMODAY FROM vw_yrmodayimport_tpl WHERE isbd = 1) """)
  df_daily_loans_import_tpl.createOrReplaceTempView("vw_daily_loans_import_tpl")
  df_daily_loans_import_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error importing daily loans table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Monthly Loans Import
# Import axiom_loans table
try:
  df_loans_import_tpl = spark.sql("""
  select *
    from bronze.axiom_loans
    """)
  df_loans_import_tpl.createOrReplaceTempView("vw_loans_import_tpl")
  df_loans_import_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error importing monthly loans table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Purchased Loans Import
# Import purchased loans table table
try:
    df_residential_loans_import_tpl = spark.sql("""
    select *
        from bronze.axiom_pm_consolidated
        """)
    df_residential_loans_import_tpl.createOrReplaceTempView("vw_residential_loans_import_tpl")
    df_residential_loans_import_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error importing purchased loans table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,GL Override Import
# We need a specific value from the pivoted GL data to account for charged-off loans, especially in the delinquent loans balances
try:
    df_gl_override_import_tpl = spark.sql("""
    SELECT *, 
        CAST(DATE_FORMAT(`Date`, 'yyyyMMdd') AS INT) AS YRMODAY
        FROM finance.tab_gl_data_pivoted
        WHERE ACCT = 126312 AND 
        CAST(DATE_FORMAT(`Date`, 'yyyyMMdd') AS INT) <= (SELECT MAX(YRMODAY) FROM vw_daily_loans_import_tpl)
        """)
    df_gl_override_import_tpl.createOrReplaceTempView("vw_gl_override_import_tpl")
    df_gl_override_import_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error importing GL override table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,GL Override Manual Typing
# Nonaccrual flag set to 1 for inclusion in Delinquent Loans analysis, with DaysPastDue over 90 for more complex analysis edge cases

try:
    df_gl_override_raw_tpl = spark.sql("""
        SELECT
            YRMODAY,
            TRY_CAST(YRQTR AS BIGINT) AS YRMO,
            NULL AS InstrumentID,
            NULL AS ITYPE,
            TRY_CAST(ACCT AS STRING) AS ACCT,
            TRY_CAST(DEPT AS STRING) AS DEPT,
            NULL AS CustomerName,
            NULL AS CommitmentDescription,
            TRY_CAST(NULL AS DATE) AS LastReviewDate,
            TRY_CAST(NULL AS DATE) AS NextReviewDate,
            NULL AS MasterTrancheFlag,
            TRY_CAST(NULL AS decimal(18,6)) AS CurrentOutstanding,
            TRY_CAST(Value AS decimal(18,6)) AS CurrentBalance,
            NULL AS Exposure,
            NULL as MaturityDate,
            '>=90' AS DaysUntilMaturity,
            NULL AS MaturityTerm,
            TRY_CAST(NULL AS decimal(18,6)) AS OriginalLoanAmount,
            NULL AS CurrentNoteDate,
            TRY_CAST(NULL AS decimal(18,6)) AS ScheduledPayment,
            TRY_CAST(NULL AS decimal(18,6)) AS AvailableBalance,
            NULL AS HoldAmount,
            NULL AS InterestIndex,
            NULL AS AX_IntRate,
            NULL AS InterestSpread,
            'Override' as Status,
            NULL as Address1,
            NULL as Address2,
            NULL as Address3,
            NULL as City,
            NULL as State,
            NULL as  ZipCode,
            NULL as Branch,
            NULL AS BranchID,
            NULL as Officer1Name,
            NULL as Officer2Name,
            NULL as CollateralType,
            NULL as CollateralTypeDesc,
            NULL AS PaymentType,
            NULL as RiskRating,
            1 AS NonAccrual,
            NULL as NonPerformingLoan,
            91 AS DaysPastDue,
            NULL as System_Type,
            'Daily' AS TableSource
            FROM vw_gl_override_import_tpl """)
    df_gl_override_raw_tpl.createOrReplaceTempView("vw_gl_override_raw_tpl")
    df_gl_override_raw_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error setting and retyping GL override table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]


# COMMAND ----------

# DBTITLE 1,Daily Loans Typing and Calculations
# Daily Loans and Monthly Loans table exclude accidental 0 account errors. Daily Loans only pulls from current month.
# MaturityDate based information for trial balance view
# ZipCode must be cast as string because some tables store it as string, others as int 
# We exclude account 125210 as it is a misclassified HTM security 
try:
    df_daily_loans_raw_tpl = spark.sql("""
        SELECT
            YRMODAY,
            TRY_CAST(YRMO AS bigint) AS YRMO,
            InstrumentID,
            ITYPE,
            TRY_CAST(ACCT AS STRING) AS ACCT,
            TRY_CAST(DEPT AS STRING) AS DEPT,
            CustName AS CustomerName,
            Commitment_Description AS CommitmentDescription,
            to_date(Commitment_Last_Review_Date, 'M/d/yyyy h:mm:ss a') AS LastReviewDate,
            to_date(Commitment_Next_Review_Date, 'M/d/yyyy h:mm:ss a') AS NextReviewDate,
            MasterTrancheFlag,
            TRY_CAST(Book_Balance AS decimal(18,6)) AS CurrentOutstanding,
            TRY_CAST(AX_CurBal AS decimal(18,6)) AS CurrentBalance,
            Exposure,
            to_date(AX_MatDate, 'M/d/yyyy hh:mm:ss a') as MaturityDate,
            (CASE 
            WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 30 THEN '<30'
            WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 90 THEN '<90'
            ELSE '>=90'
            END) AS DaysUntilMaturity,
            AX_MatTerm AS MaturityTerm,
            TRY_CAST(AX_OrigBal AS decimal(18,6)) AS OriginalLoanAmount,
            AX_OrigDate AS CurrentNoteDate,
            TRY_CAST(AX_Payment AS decimal(18,6)) AS ScheduledPayment,
            TRY_CAST(Available_Balance AS decimal(18,6) )AS AvailableBalance,
            Hold_Amount AS HoldAmount,
            AX_IntIndex AS InterestIndex,
            AX_IntRate,
            AX_IntSpread AS InterestSpread,
            LNSTATUS as Status,
            Address1,
            Address2,
            Address3,
            City,
            State,
            CAST(ZipCode AS String) AS ZipCode,
            Branch,
            BranchNumber AS BranchID,
            Officer1Name,
            Officer2Name,
            CollateralType,
            CollateralTypeDesc,
            AX_PmtType AS PaymentType,
            RiskRating,
            AX_NonAccrual AS NonAccrual,
            NonPerformingLoan,
            Days_Past_Due AS DaysPastDue,
            System_Type,
            'Daily' AS TableSource
        FROM
            vw_daily_loans_import_tpl
        WHERE YRMO = (SELECT MAX(YRMO) FROM vw_daily_loans_import_tpl) AND ACCT NOT IN(125210, 0) """)
    df_daily_loans_raw_tpl.createOrReplaceTempView("vw_daily_loans_raw_tpl")
    df_daily_loans_raw_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error setting fields and retyping fields in daily loans table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Daily Loans Max
# Used to find both latest month available in daily loans table and latest day within that month

try:
    df_daily_loans_max = spark.sql("""
         SELECT MAX(YRMO) AS max_daily_month, MAX(YRMODAY) as latest_LoanEntry
            FROM vw_daily_loans_raw_tpl""")
    df_daily_loans_max.createOrReplaceTempView("vw_daily_loans_max_tpl")
    df_daily_loans_max.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error generating max month for daily loans table: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Monthly Loans Typing and Calculations
try:
    df_loans_raw = spark.sql("""
        SELECT
            me.max_YRMODAY AS YRMODAY, /* Moves all monthly loan values to maximum valid reporting date for each month, taken from YRMODAY calendar*/
            TRY_CAST(ml.YRMO AS bigint) AS YRMO,
            ml.InstrumentID,
            ml.ITYPE,
            try_cast(ml.ACCT AS string) AS ACCT,
            try_cast(ml.DEPT AS string) as DEPT,
            ml.CustName AS CustomerName,
            ml.Commitment_Description AS CommitmentDescription,
            to_date(ml.Commitment_Last_Review_Date, 'M/d/yyyy h:mm:ss a') AS LastReviewDate,
            to_date(ml.Commitment_Next_Review_Date, 'M/d/yyyy h:mm:ss a') AS NextReviewDate,
            ml.MasterTrancheFlag,
            TRY_CAST(ml.Book_Balance AS decimal(18,6)) AS CurrentOutstanding,
            TRY_CAST(ml.AX_CurBal AS decimal(18,6)) AS CurrentBalance,
            ml.Exposure,
            to_date(ml.AX_MatDate, 'M/d/yyyy hh:mm:ss a') as MaturityDate,
            (CASE 
            WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 30 THEN '<30'
            WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 90 THEN '<90'
            ELSE '>=90'
            END) AS DaysUntilMaturity,
            ml.AX_MatTerm As MaturityTerm,
            TRY_CAST(ml.AX_OrigBal AS decimal(18,6)) AS OriginalLoanAmount,
            ml.AX_OrigDate AS CurrentNoteDate,
            TRY_CAST(ml.AX_Payment AS decimal(18,6)) as ScheduledPayment,
            TRY_CAST(ml.Available_Balance AS decimal(18,6)) AS AvailableBalance,
            ml.Hold_Amount AS HoldAmount,
            ml.AX_IntIndex AS InterestIndex,
            ml.AX_IntRate,
            ml.AX_IntSpread AS InterestSpread,
            ml.LNSTATUS as Status,
            ml.Address1,
            ml.Address2,
            ml.Address3,
            ml.City,
            ml.State,
            CAST(ml.ZipCode AS string) AS ZipCode,
            ml.Branch,
            ml.BranchNumber AS BranchID,
            ml.Officer1Name,
            ml.Officer2Name,
            ml.CollateralType,
            ml.CollateralTypeDesc,
            ml.AX_PmtType AS PaymentType,
            ml.RiskRating,
            ml.AX_NonAccrual AS NonAccrual,
            ml.NonPerformingLoan,
            ml.Days_Past_Due AS DaysPastDue,
            ml.System_Type,
            'monthly' AS TableSource
        FROM
            vw_loans_import_tpl ml
        JOIN vw_max_entry_tpl me ON ml.YRMO = me.max_lookup_YRMO
        WHERE ml.YRMO >= 202412 AND ml.YRMO < (SELECT max_daily_month FROM vw_daily_loans_max_tpl) AND ACCT NOT IN(125210, 0)
    """)
    df_loans_raw.createOrReplaceTempView("vw_loans_raw_tpl")
    df_loans_raw.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in monthly loans retyping : {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Purchased Loans Typing and Calculations
# Dept is provisionally cleared but assignment not fully validated
# This table combines information from multiple providers, which report different levels of detail
#'Anonymized Purchase' for loan providers that choose not to report CustomerName and to ensure entries are still reported in # Instruments view
# Finance policy is to flag purchased loans late by 90 days as Nonaccrual with Risk Rating 9
try:
    df_residential_raw = spark.sql("""
        SELECT
            me.max_YRMODAY AS YRMODAY,
            TRY_CAST(rl.YRMO AS BIGINT) AS YRMO,
            rl.InstrumentID,
            NULL AS ITYPE,
            try_cast(rl.ACCT AS string) AS ACCT,
            '1001321005' AS DEPT,
            IFNULL(rl.CustomerName, 'Anonymized Purchase') AS CustomerName,
            'Purchased' AS CommitmentDescription,
            TRY_CAST(NULL AS DATE) AS LastReviewDate,
            TRY_CAST(NULL AS DATE) AS NextReviewDate,
            NULL AS MasterTrancheFlag,
            TRY_CAST(rl.Book_Balance AS decimal(18,6)) AS CurrentOutstanding,
            TRY_CAST(rl.EndBookBal AS decimal(18,6)) AS CurrentBalance,
            0 AS Exposure,
            to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a') AS MaturityDate,
            (CASE 
            WHEN DATEDIFF(to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a'), CURRENT_DATE) < 30 THEN '<30'
            WHEN DATEDIFF(to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a'), CURRENT_DATE) < 90 THEN '<90'
            ELSE '>=90'
            END) AS DaysUntilMaturity,
            rl.OriginalTerm As MaturityTerm,
            TRY_CAST(rl.original_loan_amount AS decimal(18,6)) as OriginalLoanAmount,
            rl.Origination_Date AS CurrentNoteDate,
            TRY_CAST(rl.ScheduledPayment AS decimal(18,6)) AS ScheduledPayment,
            TRY_CAST(0 AS decimal(18,6)) AS AvailableBalance,
            0 AS HoldAmount,
            'Purchased' AS InterestIndex,
            rl.CouponRate AS AX_IntRate,
            0 AS InterestSpread,
            'Residential' as Status, /* Not a formal assignment in Axiom*/
            rl.Address AS Address1,
            NULL AS Address2,
            NULL AS Address3,
            rl.City,
            rl.State,
            rl.ZipCode,
            'Purchased Residential' AS Branch, /* For filtering only*/
            NULL AS BranchID,
            rl.PORTFOLIO AS Officer1Name,
            rl.Portfolio AS Officer2Name,
            NULL AS CollateralType,
            'Residential Home Value' AS CollateralTypeDesc,
            rl.PaymentType,
            (CASE WHEN rl.Days_Late >= 90 THEN 9 ELSE NULL END) AS RiskRating,
            (CASE WHEN rl.Days_Late >= 90 THEN 1 ELSE 0 END) AS NonAccrual,
            NULL AS NonPerformingLoan,
            rl.Days_Late AS DaysPastDue,
            'Mortgage' AS System_Type,
            'Purchased' AS TableSource
        FROM
            vw_residential_loans_import_tpl rl
    JOIN vw_max_entry_tpl me ON rl.YRMO = me.max_lookup_YRMO
    WHERE rl.YRMO >= 202412 AND rl.YRMO <= (SELECT max_daily_month-1 FROM vw_daily_loans_max_tpl) AND ACCT NOT IN(125210, 0)
    """)
    df_residential_raw.createOrReplaceTempView("vw_residential_raw_tpl")
    df_residential_raw.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in residential loans retyping and processing: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Base Processed Tables Union
# Union of the raw loans with minimal processing
try:
    df_combined_loans = spark.sql("""
    SELECT *
    FROM vw_daily_loans_raw_tpl
    UNION ALL
    SELECT * from vw_loans_raw_tpl
    UNION ALL
    SELECT * from vw_residential_raw_tpl
    UNION ALL
    SELECT * FROM vw_gl_override_raw_tpl""")
    df_combined_loans.createOrReplaceTempView("vw_combined_loans_tpl")
    df_combined_loans.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in combined loans union, possibly due to field mismatch: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Residential Loans Duplication for Later Imputation
# Duplicate residential loans for imputation to current and/or prior date, choosing only the latest (do not duplicate full purchased loans portfolio)
try:
    df_max_residential = spark.sql("""
      SELECT *
        FROM vw_residential_raw_tpl r
        WHERE r.YRMO = (
        SELECT MAX(YRMO) AS MaxResidentialData
        FROM vw_residential_raw_tpl)  """)
    df_max_residential.createOrReplaceTempView("vw_max_residential_tpl")
    df_max_residential.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in duplication of latest month residential loans for current and prior day: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Define maximum prior date
# Identifies most recent business day from daily_loans_max and uses that to find the maximum prior business date
try:
    df_max_prior_tpl = spark.sql("""
        SELECT
        lookup_YRMO as max_lookup_YRMO2, MAX(PriorDate) as max_PriorDate, TRY_CAST(DATE_FORMAT(max_PriorDate, 'yyyyMMdd') AS INT) AS prior_YRMODAY
        FROM vw_yrmodayimport_tpl y
        INNER JOIN vw_daily_loans_max_tpl md ON y.lookup_YRMODAY = md.latest_LoanEntry
        GROUP BY max_lookup_YRMO2                         
        """)
    df_max_prior_tpl.createOrReplaceTempView("vw_max_prior_tpl")
    df_max_prior_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in generation of prior business day calendar: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Create final calendar
# Generates final calendar from yrmodayimport and max prior date

try:
    df_filtered_yrmoimport_tpl = spark.sql("""
        SELECT
        *, DATE_TRUNC('month', EntryDate) AS EntryMonth
        FROM vw_yrmodayimport_tpl y
        LEFT JOIN vw_daily_loans_max_tpl md ON y.lookup_YRMODAY = md.latest_LoanEntry
        LEFT JOIN vw_max_entry_tpl my ON y.lookup_YRMO = my.max_lookup_YRMO
        LEFT JOIN vw_max_prior_tpl mp ON y.lookup_YRMO = mp.max_lookup_YRMO2
        WHERE y.EntryDate = my.max_EntryDate
        OR y.EntryDate = mp.max_PriorDate
        OR y.lookup_YRMODAY = md.latest_LoanEntry
        """)
    df_filtered_yrmoimport_tpl.createOrReplaceTempView("vw_filtered_yrmoimport_tpl")
    df_filtered_yrmoimport_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in generation of final calendar before imputation of residential loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Prior Date Purchased Loans Imputation
#  We need to impute values from the purchased residential portfolio, but *only* if the prior date isn't already accounted for as of the end of the last mont - we don't want to duplicate our data 
# Example case: If we are running this on July 2nd, capturing July 1st data, we already have June 30th (if a business day) from the pm_consolidated table, so this will skip and the next CTE will run
try:
    df_duplicate_residential_cross_prior_tpl = spark.sql("""
    SELECT
        (SELECT prior_YRMODAY FROM vw_max_prior_tpl) AS YRMODAY,
        (SELECT max_lookup_YRMO2 FROM vw_max_prior_tpl) AS YRMO,
        r.InstrumentID,
        r.ITYPE,
        r.ACCT,
        r.DEPT,
        r.CustomerName,
        r.CommitmentDescription,
        r.LastReviewDate,
        r.NextReviewDate,
        r.MasterTrancheFlag,
        r.CurrentOutstanding,
        r.CurrentBalance,
        r.Exposure,
        r.MaturityDate,
        r.DaysUntilMaturity,
        r.MaturityTerm,
        r.OriginalLoanAmount,
        r.CurrentNoteDate,
        r.ScheduledPayment,
        r.AvailableBalance,
        r.HoldAmount,
        r.InterestIndex,
        r.AX_IntRate,
        r.InterestSpread,
        r.Status,
        r.Address1,
        r.Address2,
        r.Address3,
        r.City,
        r.State,
        r.ZipCode,
        r.Branch,
        r.BranchID,
        r.Officer1Name,
        r.Officer2Name,
        r.CollateralType,
        r.CollateralTypeDesc,
        r.PaymentType,
        r.RiskRating,
        r.NonAccrual,
        r.NonPerformingLoan,
        r.DaysPastDue,
        r.System_Type,
        r.TableSource
        FROM vw_max_residential_tpl r
        WHERE r.YRMODAY <> (SELECT TRY_CAST(DATE_FORMAT(max_PriorDate, 'yyyyMMdd') AS INT) FROM vw_max_prior_tpl)
        """)
    df_duplicate_residential_cross_prior_tpl.createOrReplaceTempView("vw_duplicate_residential_cross_prior_tpl")
    df_duplicate_residential_cross_prior_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in generation of prior business day imputation of most recent month's purchased loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Most Recent Date Purchased Loans Imputation
#  If we do not have data from the current month, we need to impute last month's data to today's date. Example case: It is July 2nd, and we have data from pm_consolidated for June 30th but not July 1st.
# pm_consolidated June 30th data is imputed to July 1st; previous CTE does not run but this one does.
# If it is July 1st and we are reporting data as of June 30th, previous CTE will run but this one will not
# All other times this CTE runs
try:
    df_duplicate_residential_cross_current_tpl = spark.sql("""
        SELECT
            (SELECT latest_LoanEntry from vw_daily_loans_max_tpl) AS YRMODAY,
            (SELECT max_daily_month FROM vw_daily_loans_max_tpl) AS YRMO,
            r.InstrumentID,
            r.ITYPE,
            r.ACCT,
            r.DEPT,
            r.CustomerName,
            r.CommitmentDescription,
            r.LastReviewDate,
            r.NextReviewDate,
            r.MasterTrancheFlag,
            r.CurrentOutstanding,
            r.CurrentBalance,
            r.Exposure,
            r.MaturityDate,
            r.DaysUntilMaturity,
            r.MaturityTerm,
            r.OriginalLoanAmount,
            r.CurrentNoteDate,
            r.ScheduledPayment,
            r.AvailableBalance,
            r.HoldAmount,
            r.InterestIndex,
            r.AX_IntRate,
            r.InterestSpread,
            r.Status,
            r.Address1,
            r.Address2,
            r.Address3,
            r.City,
            r.State,
            r.ZipCode,
            r.Branch,
            r.BranchID,
            r.Officer1Name,
            r.Officer2Name,
            r.CollateralType,
            r.CollateralTypeDesc,
            r.PaymentType,
            r.RiskRating,
            r.NonAccrual,
            r.NonPerformingLoan,
            r.DaysPastDue,
            r.System_Type,
            r.TableSource
            FROM vw_max_residential_tpl r
            WHERE r.YRMODAY <> (SELECT latest_LoanEntry from vw_daily_loans_max_tpl)
        """)
    df_duplicate_residential_cross_current_tpl.createOrReplaceTempView("vw_duplicate_residential_cross_current_tpl")
    df_duplicate_residential_cross_current_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in generation of current business day imputation of most recent month's purchased loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

# COMMAND ----------

# DBTITLE 1,Attach Imputed Loans
# Combine imputed purchased loans with daily and monthly loans
try:
    df_imputed_and_combined_loans = spark.sql("""
        select *
            FROM vw_combined_loans_tpl
            UNION ALL 
            SELECT *
            FROM vw_duplicate_residential_cross_prior_tpl
            UNION ALL
            SELECT *
            FROM vw_duplicate_residential_cross_current_tpl
    """)
    df_imputed_and_combined_loans.createOrReplaceTempView("vw_imputed_and_combined_loans_tpl")
    df_imputed_and_combined_loans.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in union of imputed purchased loans for current and/or prior day with actual values from daily and monthly loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]


# COMMAND ----------

# DBTITLE 1,Join Calendar to Loans
# Attach final calendar and prepare for final data load
try:
    df_final_tpl= spark.sql("""
        select * 
        from vw_filtered_yrmoimport_tpl y
        INNER JOIN vw_imputed_and_combined_loans_tpl d ON y.lookup_YRMODAY = d.YRMODAY
        WHERE y.lookup_YRMO >= 202412
        AND d.InstrumentID IN (
            SELECT InstrumentID
            FROM vw_imputed_and_combined_loans_tpl
            WHERE Status <> 'InActive')
    """)
    df_final_tpl.createOrReplaceTempView("vw_final_loans_tpl")
    df_final_tpl.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error in joining combined and imputed loans to calendar data or filtering out of inactive loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]


# COMMAND ----------

# DBTITLE 1,Create Table
# Creates a table because Databricks cannot create views from temporary views.

try:
    if spark.catalog.tableExists("finance.tab_processed_loans"):
        df_changed = DataTypeChange(df_final_tpl, "finance", "tab_processed_loans")
    df_changed.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.finance.tab_processed_loans"
    )
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
    logger.info("Table tab_processed_loans created successfully.")
except Exception as e:
    logger.error(f"Error creating tab_processed_loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
    ErrorMessage = (
        str(e).split('java.lang.Exception: ')[0]
        if 'java.lang.Exception: ' in str(e)
        else str(e).split('at ')[0]
    )
