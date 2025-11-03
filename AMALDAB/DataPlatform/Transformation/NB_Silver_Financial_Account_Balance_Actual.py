# Databricks notebook source
# MAGIC %md
# MAGIC #### Importing Required Packages

# COMMAND ----------

#Importing the required packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp
import pytz
import pandas as pd
import os

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling Logger Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling Utilities Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling Configuration Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Catalog

# COMMAND ----------

spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# This code initializes an error logger specific to the current batch process.
# It then logs an informational message indicating the start of the pipeline for the given batch.
ErrorLogger = ErrorLogs(f"NB_RawToSTage")
logger = ErrorLogger[0]
logger.info("Starting the pipeline")

# COMMAND ----------

# This code reads data from the 'config.metadata' table, filtering for the specified batch, 'SQL' sourcesystem, and 'Bronze' zone.
DFMetadata = spark.read.table('config.metadata').filter(
    (col('Zone') == 'Silver') &
    (col('TableID') == 1018 )
)

display(DFMetadata)

# COMMAND ----------

TableID=1018
metadata = GetMetaDataDetails(TableID)
LoadType = metadata['LoadType']
LastLoadColumnName = metadata['LastLoadDateColumn']
DependencyTableID = metadata['DependencyTableIDs']
SourceDBName= metadata['SourceDBName']
LastLoadDate = metadata['LastLoadDateValue']
DWHSchemaName = metadata['DWHSchemaName']
DWHTableName = metadata['DWHTableName']
MergeKey = metadata['MergeKey']
MergeKeyColumn = metadata['MergeKeyColumn']
SelectQuery = metadata['SourceSelectQuery']
SourcePath = metadata['SourcePath']
LoadedDependencies = True
ListDeptable = DependencyTableID.split(',')
SrcTableName=metadata['SourceTableName']
sourcesystem=metadata['SourceSystem']
schemanames=metadata['SourceSchema']

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# DBTITLE 1,Getting current As_of_Date from Horizon Batch
df = spark.sql('''
SELECT max(Previous_Process_Date ) AS Previous_Process_Date
FROM bronze.ifs_batchcontrol 
''')
df.createOrReplaceTempView('vw_previous_process_date')
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Current Balance

# COMMAND ----------

# DBTITLE 1,ddabal
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.dbcurb Balance,
    'DD'  App_code, 
    to_timestamp(cast(DBCHNG as varchar(10)), 'yyyy-MM-dd-HH.mm.ss.SSSSSS') AS BalanceAsOfDate,
    '13-DD-'||Lpad(a.DBACCT, 20, 0) FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Current Balance' Type
    FROM bronze.ddabal a
    where a.DBCURB >= 0 and a.CurrentRecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Deposit_Current_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,SavBal
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.sbcurb Balance,
    'SV'  App_code, 
    to_timestamp(cast(SBCHNG as varchar(10)), 'yyyy-MM-dd-HH.mm.ss.SSSSSS') AS BalanceAsOfDate,
    '13-SV-'||Lpad(a.SBACCT, 20, 0) FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Current Balance' Type
    FROM bronze.savbal a
    where a.SBCURB >= 0 and currentrecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Savings_Current_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,td_master
# Revised 5/4 - changed data source to ods_td_master from tdmast

try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.tmcurb Balance,
    APPL  App_code, 
    case when a.TMLPDT > 0 then to_timestamp(cast(a.TMLPDT as varchar(10)), 'yyyyMMdd')
         when a.TMLPDT = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Current Balance' Type
    FROM bronze.ods_td_master a
    where a.tmcurb >= 0 and currentrecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("TimeDeposit_Current_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,ICS

try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = '''
    SELECT
    a.CurrentBalance Balance,
    ApplicationCode AS App_Code, 
    to_timestamp(cast(a.datekey as string), 'yyyyMMdd') AS BalanceAsOfDate,
    a.AccountLKey FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Current Balance' Type
    FROM bronze.v_trend_summary_da a
    where a.CurrentBalance >= 0 and a.CurrentRecord = 'Yes'
    and datekey in(select max(datekey) from bronze.v_trend_summary_da)
    and ApplicationCode = 'XC'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("ICS_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")

base_df.filter("FinancialAccountId ilike '%151028008%' ").display()


# COMMAND ----------

# DBTITLE 1,CDAR
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.CurrentBalance Balance,
    ApplicationCode App_code, 
    to_timestamp(cast(a.datekey as string), 'yyyyMMdd') AS BalanceAsOfDate,
    a.AccountLKey FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Current Balance' Type
    FROM bronze.v_trend_summary_da a
    where a.CurrentBalance >= 0 and a.CurrentRecord = 'Yes'
    and datekey in(select max(datekey) from bronze.v_trend_summary_da)
    and a.ApplicationCode = 'Z1'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("CDAR_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,Osaic
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
        SELECT
            a.Trade_Date_Value Balance,
            prod.ApplicationCode App_code,
            dt.previous_process_Date AS BalanceAsOfDate,
            "13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0") FinancialAccountID,
            'Osaic' SourceSystemIdentifier,
            'Current Balance' Type
            FROM bronze.account_osaic a
            left join silver.product_xref prod on prod.OsaicProductMapping = a.Account_Type
            inner join bronze.accountmapping_osaic mapping on mapping.Unique_Id = a.Account_Unique_ID 
            FULL Join vw_previous_process_date dt

            where a.Trade_Date_Value >= 0
            and a.CurrentRecord = 'Yes'
            and prod.CurrentRecord = 'Yes'
            AND  mapping.CurrentRecord='Yes'
        '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Osaic_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")

# COMMAND ----------

# DBTITLE 1,Final
df_final_current_balance=spark.sql("""SELECT * FROM Deposit_Current_Balance_vw
UNION ALL
SELECT * FROM Savings_Current_Balance_vw
UNION ALL
SELECT * FROM TimeDeposit_Current_Balance_vw
UNION ALL
SELECT * FROM ICS_Balance_vw
UNION ALL
SELECT * FROM CDAR_Balance_vw
UNION ALL 
SELECT * FROM Osaic_Balance_vw
""")

df_final_current_balance.createOrReplaceTempView("Vw_CurrentBalance")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountID, App_Code, COunt(1) from Vw_CurrentBalance Group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Original Loan Amount 

# COMMAND ----------

try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.LMTTLN Balance,
    substring(ACCT_SKEY,4,2) as App_code,
    dt.Previous_Process_Date AS BalanceAsOfDate,
    Acct_Skey FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Original Loan Amount' Type
    FROM bronze.ods_ln_master a
    FULL JOIN vw_previous_process_date dt
    where currentrecord = 'Yes' and a.LMPART=0 AND a.LMTTLO > 0 
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Vw_LoanOpeningBalance")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select FinancialAccountId, count(1) from Vw_LoanOpeningBalance 
# MAGIC group by FinancialAccountId
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Current Loan Amount

# COMMAND ----------

# DBTITLE 1,LN
#Need to update LMLSRN for LN
#MBORDT for ML
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ('''
                SELECT 
                lmttln AS Balance
                , substring(ACCT_SKEY,4,2) as App_code
                ,to_timestamp(cast(LMLSRN as string), 'yyyyMMdd') AS BalanceAsOfDate
                ,ACCT_SKEY AS FinancialAccountID
                ,'Horizon' as SourceSystemIdentifier
                ,'Current Loan Amount' as Type
                FROM bronze.ods_ln_master 
                where CurrentRecord = 'Yes' and LMPART=0
                ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_lm_amt_ln')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,ML
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ('''
                
        SELECT 
        MBORPR AS Balance
        ,substring(ACCT_SKEY,4,2) as App_code
        ,to_timestamp(cast(MBORDT as string), 'yyyyMMdd') AS BalanceAsOfDate
        ,ACCT_SKEY AS FinancialAccountID
        ,'Horizon' as SourceSystemIdentifier
        ,'Current Loan Amount' as Type
        FROM bronze.ods_ml_master
         where CurrentRecord = 'Yes'
        ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_lm_amt_ml')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI

try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery= ('''
        SELECT
        Original_Mortgage_Amount AS Balance 
        ,CASE when Investor_ID = '40H'and Category_Code = '002' then 'ML' 
          when Investor_ID = '40H'and Category_Code = '003' then 'LN' END as App_code
        ,to_timestamp(cast(Loan_Note_Date as Date), 'yyyy-MM-dd') as BalanceAsOfDate
        ,CASE when Investor_ID = '40H'and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
          when Investor_ID = '40H'and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0') END as FinancialAccountID
        ,'DMI' as SourceSystemIdentifier
        ,'Current Loan Amount' as Type
        from bronze.dmi_dcif 
        WHERE
        to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
            SELECT
            MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
            FROM
            bronze.dmi_dcif
        )
        AND CurrentRecord = 'Yes'
        AND Category_Code IN ('002', '003')
        AND Investor_ID != '40G' 
        ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_lm_amt_dmi')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum, col

df = spark.sql('''
               SELECT * FROM vw_lm_amt_ln
               UNION ALL
               SELECT * FROM vw_lm_amt_ml
               UNION ALL
               SELECT * FROM vw_lm_amt_dmi
               ''')

df.createOrReplaceTempView('Vw_CurrentLoanAmount')

aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
)
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter(col("Count") > 1)
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current Principal Balance 

# COMMAND ----------

# DBTITLE 1,LN
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery=('''
    SELECT 
    LMCBAL AS Balance
    , substring(mast.ACCT_SKEY,4,2) as App_code,
    case when mast.LMNADT > 0 then to_timestamp(cast(mast.LMNADT as string), 'yyyyMMdd')
         when mast.LMNADT = 0 then NULL
    end AS BalanceAsOfDate
    ,ACCT_SKEY AS FinancialAccountID
    ,'Horizon' as SourceSystemIdentifier
    ,'Current Principal Balance' as Type
    FROM bronze.ods_ln_master mast
    where CurrentRecord = 'Yes'
    AND mast.LMPART = 0
    ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_current_principal_balance_LN')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,ML
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery=('''
    SELECT 

    mast.MBCPBL AS Balance
    ,'ML' AS App_Code
    ,dt.previous_process_date BalanceAsOfDate
    ,Concat('13-ML-',lpad(cast(mbacct as string),20,'0') ) AS FinancialAccountID
    ,'Horizon' as SourceSystemIdentifier
    ,'Current Principal Balance' as Type
    FROM bronze.dmmlnbal mast
    FULL JOIN vw_previous_process_date dt
    where CurrentRecord = 'Yes'
    ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_current_principal_balance_ML')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT First_Principal_Balance AS Balance, 
    case when Investor_ID = '40H'and Category_Code = '002' then 'ML'  
      when Investor_ID = '40H'and Category_Code = '003' then 'LN' end App_code,
    to_timestamp(cast(As_of_Date as Date), 'yyyy-MM-dd') AS BalanceAsOfDate,
    case when Investor_ID = '40H'and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0') 
      when Investor_ID = '40H'and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0') 
      else Loan_Number end FinancialAccountID,
    'DMI' SourceSystemIdentifier,
    'Current Principal Balance' Type
    FROM bronze.dmi_dcif 
    WHERE
    to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
        SELECT
        MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
        FROM
        bronze.dmi_dcif
    )
    AND CurrentRecord = 'Yes'
    AND Category_Code IN ('002', '003')
    AND Investor_ID != '40G' 
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("DMI_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_current_principal_balance_LN
               UNION ALL
               SELECT * FROM vw_current_principal_balance_ML
               UNION ALL
               SELECT * FROM DMI_Balance_vw
               ''')

df.createOrReplaceTempView('Vw_CurrentPrincipalBalance')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bank Owned Book Balance

# COMMAND ----------

# DBTITLE 1,ML & LN
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =("""
    SELECT
    (MBCPBL - 0) AS Balance,
    substring(ACCT_SKEY,4,2) as App_code,
    dt.Previous_Process_Date AS BalanceAsOfDate,
    ACCT_SKEY AS FinancialAccountID,
    'Horizon' AS SourceSystemIdentifier,
    'Bank Owned Principal Balance' AS Type
    FROM
    bronze.ods_ml_master
    FULL JOIN vw_previous_process_date dt
    where
    CurrentRecord = 'Yes'

    UNION ALL

    Select 
        --W-001126 Add coalesce statement to avoid arithmetic issue
        (coalesce(a.LMCBAL,0) - coalesce(b.ParticipationCurrBal,0)) as Balance,
        substring(a.ACCT_SKEY,4,2) as App_code,
        dt.Previous_Process_Date AS BalanceAsOfDate,
        a.ACCT_SKEY as FinanciaAccountID,
        'Horizon' AS SourceSystemIdentifier,
        'Bank Owned Principal Balance' AS Type 
        from (
                select *
                from bronze.ods_ln_master 
                where LMPART=0 
                and CurrentRecord = 'Yes'
                ) a
        FULL JOIN vw_previous_process_date dt
        --W-001126 use left join to get all records
        left join ( 
                select ACCT_SKEY, LMACCT, sum(LMCBAL) as `ParticipationCurrBal`
                    from bronze.ods_ln_master 
                    where LMPART>0
                    and CurrentRecord = 'Yes'
                    group by ACCT_SKEY, LMACCT
                    ) b
        on  a.ACCT_SKEY=b.ACCT_SKEY and a.LMACCT=b.LMACCT 
    """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_horizon_Bank_Owned")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =("""
    SELECT
      First_Principal_Balance AS Balance,
      CASE
        WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'
        WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN'
      END AS App_code,
      to_timestamp(cast(As_of_Date AS Date), 'yyyyMMdd') AS BalanceAsOfDate,
      CASE
        WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || LPAD(CAST(Loan_Number AS STRING), 20, '0')
        WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || LPAD(CAST(Loan_Number AS STRING), 20, '0')
        ELSE Loan_Number
      END AS FinancialAccountId,
      'DMI' AS SourceSystemIdentifier,
      'Bank Owned Principal Balance' AS Type
    FROM
      bronze.dmi_dcif
    WHERE
      to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
        SELECT
          MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
        FROM
          bronze.dmi_dcif
      )
      AND CurrentRecord = 'Yes'
      AND Category_Code IN ('002', '003')
      AND Investor_ID != '40G' """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_dmi_bank_owned")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_horizon_Bank_Owned
               UNION ALL
               SELECT * FROM vw_dmi_bank_owned
               ''')

df.createOrReplaceTempView('Vw_BankOwnedBookBalance')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Book Balance

# COMMAND ----------

# DBTITLE 1,Horizon
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =(
      """
      SELECT 
          MSAMT7 AS Balance,
          substring(ACCT_SKEY,4,2) as App_code,
          dt.previous_process_Date AS BalanceAsOfDate,
          ACCT_SKEY AS FinancialAccountID, 
          'Horizon' AS SourceSystemIdentifier,
          'Book Balance' AS Type  
        FROM bronze.ods_ml_master 
        FULL Join vw_previous_process_date dt
        WHERE CurrentRecord = 'Yes'
        
        UNION ALL
        
        SELECT 
          --W-001126 Add coalesce statement to avoid arithmetic issue
          (coalesce(a.PrincipalAsset,0) - coalesce(b.ParPrincipalAsset,0)) AS Balance,
          substring(a.ACCT_SKEY,4,2) as App_code,
          dt.previous_process_date BalanceAsOfDate,
          a.ACCT_SKEY AS FinancialAccountID, 
          'Horizon' AS SourceSystemIdentifier,
          'Book Balance' AS Type
        FROM (
          SELECT 
            ACCT_SKEY, 
            LSACCT, 
            LSCBAL AS `PrincipalAsset`
          FROM bronze.ods_lnimpr 
          WHERE LSPART = 0 
            AND CurrentRecord = 'Yes'
        ) a
        FULL JOIN vw_previous_process_date dt
        --W-001126 use left join to get all record
        LEFT JOIN (
          SELECT 
            ACCT_SKEY, 
            LSACCT, 
            SUM(LSCBAL) AS `ParPrincipalAsset`
          FROM bronze.ods_lnimpr 
          WHERE LSPART > 0
            AND CurrentRecord = 'Yes'
          GROUP BY ACCT_SKEY, LSACCT
        ) b
        ON a.ACCT_SKEY = b.ACCT_SKEY 
        AND a.LSACCT = b.LSACCT;
        """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("VW_BookBalance_Horizon")
except Exception as e:
    logger.error("Issue while joining the base tables") 
    raise e

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =("""
    SELECT
      First_Principal_Balance - Non_Accrual_Balance AS Balance,
      CASE
        WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'
        WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN'
      END AS App_code,
      to_timestamp(cast(As_of_Date AS Date), 'yyyyMMdd') AS BalanceAsOfDate,
      CASE
        WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || LPAD(CAST(Loan_Number AS STRING), 20, '0')
        WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || LPAD(CAST(Loan_Number AS STRING), 20, '0')
        ELSE Loan_Number
      END AS FinancialAccountID,
      'DMI' AS SourceSystemIdentifier,
      'Book Balance' AS Type
    FROM
      bronze.dmi_dcif
    WHERE
      to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
        SELECT
          MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
        FROM
          bronze.dmi_dcif
      )
      AND CurrentRecord = 'Yes'
      AND Category_Code IN ('002', '003')
      AND Investor_ID != '40G'
       """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_book_balance_dmi")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM VW_BookBalance_Horizon
               UNION ALL
               SELECT * FROM vw_book_balance_dmi
               ''')

df.createOrReplaceTempView('Vw_BookBalance')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Available Credit

# COMMAND ----------

# DBTITLE 1,Horizon
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ('''
    SELECT 
    LMAVAL AS Balance, 
    substring(ACCT_SKEY,4,2) as App_code,
    case when lmnadt > 0 then to_timestamp(cast(lmnadt as varchar(10)), 'yyyyMMdd')
         when lmnadt = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY AS FinancialAccountId,  
    'Horizon' AS SourceSystemIdentifier, 
    'Available Credit' As Type
    FROM bronze.ods_ln_master where CurrentRecord = 'Yes' and LMPART = 0

    union all

    SELECT 
    0 AS Balance,
    substring(ACCT_SKEY,4,2) as App_code,
    dt.previous_process_date as BalanceAsOfDate, 
    ACCT_SKEY AS FinancialAccountId,
    'Horizon' AS SourceSystemIdentifier, 
    'Available Credit' As Type
    FROM bronze.ods_ml_master
    FULL JOIN vw_previous_process_date dt
    where CurrentRecord = 'Yes'
                ''')
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_ac_horizon')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ("""
     SELECT 
        HELOC_Available_Balance AS Balance, 
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'  
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN' 
        END AS App_code,
        to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') AS BalanceAsOfDate,
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || lpad(cast(Loan_Number AS string), 20, '0') 
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || lpad(cast(Loan_Number AS string), 20, '0') 
            ELSE Loan_Number 
        END AS FinancialAccountId,
        'DMI' AS SourceSystemIdentifier,
        'Available Credit' AS Type
    FROM 
        bronze.dmi_dcif 
    WHERE
    to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
        SELECT
        MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
        FROM
        bronze.dmi_dcif
    )
    AND CurrentRecord = 'Yes'
    AND Category_Code IN ('002', '003')
    AND Investor_ID != '40G' 
    """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_ac_dmi')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_ac_horizon
               UNION ALL
               SELECT * FROM vw_ac_dmi
               ''')
df.createOrReplaceTempView('Vw_AvailableCredit')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Available Credit for Participants

# COMMAND ----------

# DBTITLE 1,Horizon
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery =("""
    select
      lmttln-lmcbal as Balance,
      substring(ACCT_SKEY,4,2) as App_code,
      dt.Previous_Process_Date AS BalanceAsOfDate,
      ACCT_SKEY as FinancialAccountId,
      'Horizon' as SourceSystemIdentifier,
      'Available Credit For Participants' as Type
    from
      bronze.ods_ln_master
      FULL JOIN vw_previous_process_date dt
    where
      CurrentRecord = 'Yes'
      and LMPART = 0

    union all

    select
      0 as Balance,
      substring(ACCT_SKEY,4,2) as App_code,
      dt.Previous_Process_Date AS BalanceAsOfDate,
      ACCT_SKEY as FinanciaAccountId,
      'Horizon' as SourceSystemIdentifier,
      'Available Credit For Participants' as Type
    from
      bronze.ods_ml_master
      FULL JOIN vw_previous_process_date dt
    where
      CurrentRecord = 'Yes' 
    """)

    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_acfp_horizon')
    #base_df.display()
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,DMI
##  DMI NOT INCLUDED IN AVAILABLE CREDIT FOR PARTICIPANTS

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_acfp_horizon
               ''')
df.display()
df.createOrReplaceTempView('Vw_AvailableCreditForParticipants')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Book Balance for Participants

# COMMAND ----------

# DBTITLE 1,Horizon
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ("""
        Select 
        LSCBAL AS Balance,
        substring(ACCT_SKEY,4,2) As App_Code,
        dt.Previous_Process_Date AS BalanceAsOfDate,
        ACCT_SKEY AS FinancialAccountID,
        'Horizon' AS SourceSystemIdentifier,
        'Book Balance For Participants' AS Type
        From bronze.ods_lnimpr
        FULL JOIN vw_previous_process_date dt
        Where LSPART=0 and CurrentRecord='Yes'

        union all

        Select 
        0 AS Balance,
        substring(ACCT_SKEY,4,2) As App_Code,
        case when MBLPDT > 0 then to_timestamp(cast(MBLPDT as varchar(10)), 'yyyyMMdd')
         when MBLPDT = 0 then NULL
        end AS BalanceAsOfDate,
        ACCT_SKEY AS FinancialAccountID,
        'Horizon' AS SourceSystemIdentifier,
        'Book Balance For Participants' AS Type
        From bronze.ods_ml_master
        Where CurrentRecord='Yes'
        """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_bbfp_horizon')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,DMI
##  DMI NOT INCLUDED IN AVAILABLE CREDIT FOR PARTICIPANTS

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               select * from vw_bbfp_horizon
               ''')
#df.display()
df.createOrReplaceTempView('Vw_BookBalanceForParticipants')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exposure 

# COMMAND ----------

# DBTITLE 1,temp - bnkOwnPrnplAsst
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bnkOwnPrnplAsst AS 
# MAGIC       Select a.ACCT_SKEY, a.LMACCT, (a.LMCBAL- b.ParticipationCurrBal) AS `bnkOwnPrnplAsst`
# MAGIC       from (select ACCT_SKEY, LMACCT, LMCBAL
# MAGIC             from bronze.ods_ln_master 
# MAGIC             where LMPART=0 and CurrentRecord = 'Yes') a
# MAGIC
# MAGIC       inner join (select ACCT_SKEY, LMACCT, sum(LMCBAL) as `ParticipationCurrBal`
# MAGIC                   from bronze.ods_ln_master 
# MAGIC                   where LMPART>0 and CurrentRecord = 'Yes'
# MAGIC                   group by ACCT_SKEY, LMACCT) b
# MAGIC       on  a.ACCT_SKEY=b.ACCT_SKEY and a.LMACCT=b.LMACCT ;

# COMMAND ----------

# DBTITLE 1,temp - partPrnpAsst
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW partPrnpAsst AS 
# MAGIC   Select a.ACCT_SKEY, a.LSACCT, (a.PrincipalAsset - b.ParPrincipalAsset) AS `partPrnpAsst`
# MAGIC   from (select ACCT_SKEY, LSACCT, LSCBAL as `PrincipalAsset`
# MAGIC         from bronze.ods_lnimpr 
# MAGIC         where LSPART=0 
# MAGIC         and CurrentRecord = 'Yes') a
# MAGIC   inner join (select ACCT_SKEY, LSACCT, sum(LSCBAL) as `ParPrincipalAsset`
# MAGIC               from bronze.ods_lnimpr 
# MAGIC               where LSPART>0
# MAGIC               and CurrentRecord = 'Yes'
# MAGIC               group by ACCT_SKEY, LSACCT) b
# MAGIC   on  a.ACCT_SKEY=b.ACCT_SKEY and a.LSACCT=b.LSACCT;

# COMMAND ----------

# DBTITLE 1,temp - partAvailBal
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW partAvailBal AS 
# MAGIC   select ACCT_SKEY, LMACCT, sum(AVALO) as `partAvailBal` 
# MAGIC   from bronze.ods_ln_master 
# MAGIC   where CurrentRecord = 'Yes' and LMPART>0
# MAGIC   group by ACCT_SKEY, LMACCT

# COMMAND ----------

# DBTITLE 1,Horizon
try:
      logger.info("Joining base tables for silver Financial Account Balance table")
      basequery = ("""
      SELECT
      (MBCPBL-0) AS Balance,
      substring(ACCT_SKEY,4,2) as App_Code,
      dt.previous_process_date AS BalanceAsOfDate,
      ACCT_SKEY as FinancialAccountId,
      'Horizon' AS SourceSystemIdentifier,
      'Exposure' as Type
      FROM bronze.ods_ml_master
      FULL JOIN vw_previous_process_date dt 
      where CurrentRecord = 'Yes'

      union all
      select  
      lmttln as Balance,
      substring(lnmst.ACCT_SKEY,4,2) as App_Code,
      dt.previous_process_date AS BalanceAsOfDate,
      lnmst.ACCT_SKEY as FinancialAccountId,
      'Horizon' AS SourceSystemIdentifier,
      'Exposure' as Type
      from bronze.ods_ln_master lnmst
      FULL JOIN vw_previous_process_date dt
      where lnmst.CurrentRecord='Yes' and lnmst.LMPART = 0
      """)
      base_df = spark.sql(basequery)
      base_df.createOrReplaceTempView('vw_exposure_horizon')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = (
    """
    SELECT 
    Second_Principal_Balance AS Balance, 
    CASE 
                WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'  
                WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN' 
            END AS App_code,
            to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') AS BalanceAsOfDate,
            CASE 
                WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || lpad(cast(Loan_Number AS string), 20, '0') 
                WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || lpad(cast(Loan_Number AS string), 20, '0') 
                ELSE Loan_Number 
            END AS FinancialAccountId,
            'DMI' AS SourceSystemIdentifier,
            'Exposure' AS Type
        FROM 
            bronze.dmi_dcif 
                WHERE
        to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
            SELECT
            MAX(to_timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd'))
            FROM
            bronze.dmi_dcif
        )
        AND CurrentRecord = 'Yes'
        AND Category_Code IN ('002', '003')
        AND Investor_ID != '40G' 
        """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView('vw_exposure_dmi')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_exposure_horizon
               UNION ALL
               SELECT * FROM vw_exposure_dmi
               ''')
df.display()
df.createOrReplaceTempView('Vw_Exposure')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low Balance YTD

# COMMAND ----------

# DBTITLE 1,Horizon Low Balance YTD
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.LMLPRN Balance,
    substring(ACCT_SKEY, 4, 2) AS App_Code, 
    case when lmnadt > 0 then to_timestamp(cast(lmnadt as varchar(10)), 'yyyyMMdd')
         when lmnadt = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Low Balance YTD' Type
    FROM bronze.ods_ln_master a
    where currentrecord = 'Yes' and a.LMPART=0

    union all

    SELECT 
    0 AS Balance,
    substring(ACCT_SKEY, 4, 2) as App_Code,
    dt.previous_process_date AS BalanceAsOfDate,
    ACCT_SKEY as FinancialAccountId,
    "Horizon" AS SourceSystemIdentifier,
    "Low Balance YTD" AS Type 
    FROM bronze.ods_ml_master 
    FULL JOIN vw_previous_process_date dt
    where CurrentRecord = 'Yes'

    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Loan_Low_Balance_YTD_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise(e)


# COMMAND ----------

# DBTITLE 1,DMI
##  DMI NOT INCLUDED IN AVAILABLE CREDIT FOR PARTICIPANTS

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM Loan_Low_Balance_YTD_vw
               ''')
df.display()
df.createOrReplaceTempView('Vw_Loan_LowBalanceYTD')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Committment Limit 

# COMMAND ----------

# DBTITLE 1,Commitment Limit
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = """
    SELECT 
        CASE 
            WHEN C.ACCT_SKEY IS NULL THEN B.MCLIMT
            WHEN C.ACCT_SKEY IS NOT NULL AND C.LMX1C1 <> 'T' THEN B.MCLIMT
            WHEN C.ACCT_SKEY IS NOT NULL AND C.LMX1C1 = 'T' THEN E.MCLIMT
        END AS Balance,
        substring(C.ACCT_SKEY, 4, 2) AS App_Code,
        case when c.lmnadt > 0 then to_timestamp(cast(c.lmnadt as varchar(10)), 'yyyyMMdd')
         when c.lmnadt = 0 then NULL
        end AS BalanceAsOfDate,
        A.ACCT_SKEY AS FinancialAccountId, 
        'Horizon' AS SourceSystemIdentifier,
        'Commitment Limit' AS Type
    FROM bronze.ods_MCXREF A 
    INNER JOIN bronze.ods_mc_master B ON A.MC_SKEY = B.ACCT_SKEY
    LEFT JOIN bronze.ods_ln_master C ON A.ACCT_SKEY = C.ACCT_SKEY AND C.CurrentRecord = 'Yes'
    LEFT JOIN bronze.ods_LNTREL D ON C.LMACCT = D.LTSACT AND D.CurrentRecord = 'Yes'
    LEFT JOIN bronze.ods_mc_master E ON D.Acct_SKEY = E.ACCT_SKEY AND E.CurrentRecord = 'Yes'
    WHERE A.CurrentRecord = 'Yes' AND B.CurrentRecord = 'Yes' AND C.lmpart = 0
    """
    base_df = spark.sql(basequery)    
    base_df.createOrReplaceTempView("vw_commitmentLimit_Horizon")
except Exception as e:
    logger.error("Issue while joining the base table")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI-N/A
##  DMI NOT INCLUDED IN Hight Balance YTD

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               select * from vw_commitmentLimit_Horizon
               ''')
df.display()
df.createOrReplaceTempView('Vw_CommitmentLimit')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### High Balance YTD

# COMMAND ----------

# DBTITLE 1,LN and ML
try:
    logger.info("Joining base tables for silver Financial Account Balance table")
    basequery = ("""
    SELECT 
    LMHBYT AS Balance,
    substring(ACCT_SKEY,4,2) as App_Code,
    case when lmnadt > 0 then to_timestamp(cast(lmnadt as varchar(10)), 'yyyyMMdd')
         when lmnadt = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY as FinancialAccountId,
    "Horizon" AS SourceSystemIdentifier,
    "High Balance YTD" AS Type
    FROM bronze.ods_ln_master where CurrentRecord = 'Yes'
    and LMPART=0

    union all

    SELECT 
    0 AS Balance,
    substring(ACCT_SKEY,4,2) as App_Code,
    dt.previous_process_date AS BalanceAsOfDate,
    ACCT_SKEY as FinancialAccountId,
    "Horizon" AS SourceSystemIdentifier,
    "High Balance YTD" AS Type 
    FROM bronze.ods_ml_master
    FULL JOIN vw_previous_process_date dt
    where CurrentRecord='Yes'
    """)
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_HighBalance_YTD_Horizon")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,DMI-N/A
##  DMI NOT INCLUDED IN Hight Balance YTD

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               select * from vw_HighBalance_YTD_Horizon
               ''')
# df.display()
df.createOrReplaceTempView('Vw_Loan_HighBalanceYTD')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current Appraisal Amount

# COMMAND ----------

# DBTITLE 1,Appraisal
# per note to use Current Appraisal Amount instead of Date
# CPCAAT from CTPROP 
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =('''
    SELECT 
        prop.CPCAAT AS Balance,
        crel.CRAPPL2 as App_code,
        to_timestamp(cast(prop.CPCADT as string), 'yyyy-MM-dd') AS BalanceAsOfDate,
        crel.acct_skey AS FinancialAccountID,
        'Horizon' AS SourceSystemIdentifier,
        'Current Appraisal Amount' AS Type
    FROM 
        bronze.ods_ctprop prop
    LEFT JOIN 
        bronze.ods_ctactrel crel ON crel.CO_SKEY = prop.CO_SKEY
    FULL JOIN 
        vw_previous_process_date dt
    WHERE 
        prop.CurrentRecord = 'Yes' 
        AND crel.CRPRIM = 'Y' 
        AND crel.CurrentRecord = 'Yes'
        AND crel.CRRLDT != '0001-01-01'
        AND prop.CPCADT != '0001-01-01' 
        AND crel.acct_skey IS NOT NULL


''')
    basequery = spark.sql(basequery)
    basequery.createOrReplaceTempView("vw_Current_Appraisal_Amount_Horizon")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =('''
        SELECT
        Property_Value_Amount AS Balance,
        case when Investor_ID = '40H'and Category_Code = '002' then 'ML'  
        when Investor_ID = '40H'and Category_Code = '003' then 'LN' end App_code,
        to_timestamp(cast(Current_Appraisal_Date AS Date), 'yyyyMMdd') AS BalanceAsOfDate,
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || lpad(cast(Loan_Number AS string), 20, '0')
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || lpad(cast(Loan_Number AS string), 20, '0')
        END AS FinancialAccountID,
        'DMI' AS SourceSystemIdentifier,
        'Current Appraisal Amount' AS Type
    FROM 
        bronze.dmi_dcif dcifML
    WHERE 
        dcifML.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
        AND CurrentRecord = 'Yes'
        AND Category_Code IN ('002', '003')
        AND Investor_ID != '40G'
''')
    basequery = spark.sql(basequery)
    basequery.createOrReplaceTempView("vw_Current_Appraisal_Amount_dmi")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_Current_Appraisal_Amount_Horizon
               UNION ALL
               SELECT * FROM vw_Current_Appraisal_Amount_dmi
               ''')
df.createOrReplaceTempView("Vw_CurrentAppraisalAmount")
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID","App_Code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hold Amount

# COMMAND ----------

# DBTITLE 1,Horizon - Hold Amount
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =("""
    SELECT 
        Balance, App_Code, BalanceAsOfDate, FinancialAccountId, SourceSystemIdentifier, Type 
    FROM (
        SELECT 
            lh.LBHAMT AS Balance,
            substring(lh.ACCT_SKEY,4,2) As App_Code,
            dt.previous_process_date AS BalanceAsOfDate,
            lh.ACCT_SKEY AS FinancialAccountId,
            'Horizon' AS SourceSystemIdentifier,
            'Hold Amount' AS Type,
            lh.LBSINT,
            ROW_NUMBER() OVER (PARTITION BY ACCT_SKEY ORDER BY LBUDAT DESC) AS row_num
        FROM bronze.ods_lnbkhold lh
        FULL JOIN vw_previous_process_Date dt
        WHERE CurrentRecord = 'Yes'
    )
    WHERE row_num = 1

    UNION all

    Select  
    '0' AS Balance,
    substring(ml.ACCT_SKEY,4,2) As App_Code,
    dt.previous_process_date AS BalanceAsOfDate,
    ml.ACCT_SKEY AS FinancialAccountId,
    'Horizon' AS SourceSystemIdentifier,
    'Hold Amount' AS Type
    from bronze.ods_ml_master ml
    full join 
    vw_previous_process_date dt
    Where ml.CurrentRecord='Yes' 
    """)
    basequery = spark.sql(basequery)
    basequery.createOrReplaceTempView("vw_HoldAmount_Horizon")
    # df=spark.sql("select * from vw_HoldAmount_Horizon where BalanceAsOfDate >0")
    # df.createOrReplaceTempView("vw_HoldAmount_Horizon")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI-HoldAmount
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery =("""
     SELECT 
        (Original_Mortgage_Amount - First_Principal_Balance) AS Balance, 
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'  
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN' 
        END AS App_code,
        to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') AS BalanceAsOfDate,
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || lpad(cast(Loan_Number AS string), 20, '0') 
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || lpad(cast(Loan_Number AS string), 20, '0') 
            ELSE Loan_Number 
        END AS FinancialAccountId,
        'DMI' AS SourceSystemIdentifier,
        'Hold Amount' AS Type
    FROM 
        bronze.dmi_dcif 
        where As_of_Date in (select max(As_of_Date) FROM bronze.dmi_dcif)
        and to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') IS NOT NULL
        and Investor_ID = '40H' AND Category_Code IN ('003', '002')
    """)
    basequery = spark.sql(basequery)
    basequery.createOrReplaceTempView('vw_HoldAmount_dmi')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_HoldAmount_Horizon
               UNION ALL
               SELECT * FROM vw_HoldAmount_dmi
               ''')
df.createOrReplaceTempView("Vw_HoldAmount")
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID","App_Code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Second Principal Balance

# COMMAND ----------

# DBTITLE 1,DMI
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
        SELECT 
            Second_Principal_Balance AS Balance, 
            CASE 
                WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'  
                WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN' 
            END AS App_code,
            to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') AS BalanceAsOfDate,
            CASE 
                WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || lpad(cast(Loan_Number AS string), 20, '0') 
                WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || lpad(cast(Loan_Number AS string), 20, '0') 
                ELSE Loan_Number 
            END AS FinancialAccountId,
            'DMI' AS SourceSystemIdentifier,
            'Second Principal Balance' AS Type
        FROM 
            bronze.dmi_dcif 
        WHERE 
            to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd') IN (
                SELECT 
                    max(to_timestamp(cast(As_of_Date AS Date), 'yyyy-MM-dd')) 
                FROM 
                    bronze.dmi_dcif 
            )
            AND CurrentRecord = 'Yes'
            AND Category_Code IN ('002', '003')
            AND Investor_ID != '40G'
            AND Second_Principal_Balance > 0 
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Vw_SecondPrincipalBalance")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df= spark.sql('''
               SELECT * FROM Vw_SecondPrincipalBalance
               ''')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID","App_Code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Plus

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assets Under Management 

# COMMAND ----------

# DBTITLE 1,Horizon-Assets Under Management
try: 
    logger.info("Processing")
    basequery = '''
    select Balance, App_code, BalanceAsOfDate, FinancialAccountId, SourceSystemIdentifier, Type
    from
        (
        Select market_value_lcl_stl + accrued_inc_base as Balance
        ,'GP' as App_code
        ,to_timestamp(cast(date_as_of as date), 'yyyy-MM-dd') as BalanceAsOfDate
        ,"13-GP-" || lpad(cast(replace(portfolio, '.', '') as string), 20, '0') FinancialAccountId
        ,'Global Plus' SourceSystemIdentifier
        ,'Assets Under Management' Type
        , row_number() over(partition by portfolio order by date_as_of desc) as row_num
        from bronze.positions 
        WHERE date_as_of = (select max(date_as_of) from bronze.positions )
        AND upper(managed) = 'YES'
        )
        where row_num = 1
            '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Vw_AssetsUnderManagement")
except Exception as e:
        logger.error("Issue while joining the base tables")            
        raise(e)

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select FinancialAccountID, App_Code, count(1) from Vw_AssetsUnderManagement
# MAGIC group by all
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assets under Custody 

# COMMAND ----------

# DBTITLE 1,Horizon-Assests Under Custody
try: 
    logger.info("Processing")
    basequery = '''
        select Balance, App_code, BalanceAsOfDate, FinancialAccountId, SourceSystemIdentifier, Type
        from
            (
            Select market_value_lcl_stl + accrued_inc_lcl as Balance
            ,'GP' as App_code
            ,date_format(date_as_of, 'yyyy-MM-dd') AS BalanceAsOfDate
            ,"13-GP-" || lpad(cast(replace(portfolio, '.', '') as string), 20, '0') FinancialAccountId
            ,'Global Plus' SourceSystemIdentifier
            ,'Assets Under Custody' Type
            , row_number() over(partition by portfolio order by date_as_of desc) as row_num
            from bronze.positions 
            WHERE date_as_of = (select max(date_as_of) from bronze.positions )
            AND upper(managed) = 'NO'
            )
        where row_num = 1
            '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Vw_AssetsUnderCustody")
except Exception as e:
        logger.error("Issue while joining the base tables")
        raise e           

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select FinancialAccountID, App_Code, count(1) from Vw_AssetsUnderCustody
# MAGIC group by all
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Market Value

# COMMAND ----------

# DBTITLE 1,Horizon-Market Value
try: 
    logger.info("Processing")
    basequery = '''
    select Balance, App_code, BalanceAsOfDate, FinancialAccountId, SourceSystemIdentifier, Type
    from(
            Select market_value_lcl_trd + accrued_inc_lcl as Balance
            ,'GP' as App_code
            ,to_timestamp(cast(date_as_of as date), 'yyyy-MM-dd') as BalanceAsOfDate
            ,"13-GP-" || lpad(cast(replace(portfolio, '.', '') as string), 20, '0') FinancialAccountId
            ,'Global Plus' SourceSystemIdentifier
            ,'Market Value' Type
            , row_number() over(partition by portfolio order by date_as_of desc) as row_num
            from bronze.positions 
            WHERE date_as_of = (select max(date_as_of) from bronze.positions )
            )
            where row_num = 1
            '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Vw_MarketValue")
except Exception as e:
        logger.error("Issue while joining the base tables")
        raise e            

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select FinancialAccountID, App_Code, count(1) from Vw_MarketValue
# MAGIC group by all
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deposit

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Last Statement Balance

# COMMAND ----------

# DBTITLE 1,DEPOSIT - LAST STATEMENT BALANCE
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT a.DBLCSB Balance,
    'DD'  App_code, 
    to_timestamp(cast(DBCHNG as varchar(10)), 'yyyy-MM-dd-HH.mm.ss.SSSSSS') AS BalanceAsOfDate,
    '13-DD-'||Lpad(a.DBACCT, 20, 0) FinancialAccountID,
    'Horizon' SourceSystemIdentifier,
    'Last Statement Balance' Type
    FROM bronze.ddabal a
    where a.DBCURB >= 0 and a.CurrentRecord = 'Yes'

    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Deposit_Last_Statement_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,SAVINGS - LAST STATEMENT BALANCE
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.SBLCSB Balance,
    'SV'  App_code, 
    to_timestamp(cast(SBCHNG as varchar(10)), 'yyyy-MM-dd-HH.mm.ss.SSSSSS') AS BalanceAsOfDate,
    '13-SV-'||Lpad(a.SBACCT, 20, 0) FinancialAccountID,
    'Horizon' SourceSystemIdentifier,
    'Last Statement Balance' Type
    FROM bronze.savbal a
    where a.SBCURB >= 0 and currentrecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Savings_Last_Statement_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Final-Last Statement Balance
df_final_LSB = spark.sql("""
    SELECT * FROM Deposit_Last_Statement_Balance_vw 
    UNION ALL 
    SELECT * FROM Savings_Last_Statement_Balance_vw
""")

df_final_LSB.createOrReplaceTempView("Vw_LastStatementBalance")

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select FinancialAccountID,count(1) from Vw_LastStatementBalance group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Prev Month Avg Deposit Balance

# COMMAND ----------

# DBTITLE 1,Deposit - Prev Month AVG BALANCE
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
        a.RLFB01 AS Balance,
        'DD'  App_code, 
        dt.Previous_Process_Date AS BalanceAsOfDate,
        '13-DD-'||Lpad(a.RLACCT, 20, 0) FinancialAccountId,
        'Horizon' SourceSystemIdentifier,
        'Prev Month Avg Deposit Balance' Type
        FROM bronze.ddarol a
        FULL JOIN vw_previous_process_date dt
        where a.RLFB01 >= 0 and a.CurrentRecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Deposit_Prev_Month_Avg_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Savings - Prev Month AVG BALANCE
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.RLFB01 Balance,
    'SV'  App_code, 
    dt.Previous_Process_Date AS BalanceAsOfDate,
    '13-SV-'||Lpad(a.RLACCT, 20, 0) FinancialAccountId,
    'Horizon' SourceSystemIdentifier,
    'Prev Month Avg Deposit Balance' Type
    FROM bronze.savrol a
    FULL JOIN vw_previous_process_date dt
    where a.RLFB01 >= 0 and a.CurrentRecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Savings_Prev_Month_Avg_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Final- Prev Month AVG BALANCE
df_final_PMA = spark.sql("""
    SELECT * FROM Deposit_Prev_Month_Avg_Balance_vw 
    UNION ALL 
    SELECT * FROM Savings_Prev_Month_Avg_Balance_vw
""")
df_final_PMA.createOrReplaceTempView("Vw_PrevMonthAvgBalance")

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select  FinancialAccountID,App_Code, count(1) from Vw_PrevMonthAvgBalance group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Previous Day Balance  --need to validate the mapping

# COMMAND ----------

# DBTITLE 1,Deposit - Previous Day Balance
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    previous_working_day = spark.sql("SELECT date_format(max(previous_process_date), 'yyyyMMdd') AS datefield FROM bronze.ifs_batchcontrol").collect()[0]['datefield']
    basequery = f'''
    SELECT 
        CurrentBalance AS Balance,
        SUBSTRING(AccountLKey, 4, 2) AS App_Code,
        TO_TIMESTAMP(CAST(DateKey AS String), 'yyyyMMdd') AS BalanceAsOfDate,
        AccountLKey AS FinancialAccountID,
        'Horizon' AS SourceSystemIdentifier,
        'Previous Day Balance' AS Type
    FROM bronze.v_trend_summary_da 
    WHERE CurrentRecord = 'Yes'
    AND DateKey = '{previous_working_day}'
    AND ApplicationCode = 'DD'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Deposit_Previous_Day_Balance_vw")
    
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Savings - Previous Day Balance
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT
    a.SBYESB  AS Balance,
    'SV'  App_code, 
    to_timestamp(cast(SBCHNG as varchar(10)), 'yyyy-MM-dd-HH.mm.ss.SSSSSS') AS BalanceAsOfDate,
    '13-SV-'||Lpad(a.SBACCT, 20, 0) FinancialAccountID,
    'Horizon' SourceSystemIdentifier,
    'Previous Day Balance' Type
    FROM bronze.SAVBAL a
    where a.SBYESB  > 0 and a.CurrentRecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Savings_Previous_Day_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM Deposit_Previous_Day_Balance_vw
               UNION ALL
               SELECT * FROM Savings_Previous_Day_Balance_vw
               ''')
df.createOrReplaceTempView("Vw_PreviousDayBalance")
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID","App_Code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrow Balance

# COMMAND ----------

# DBTITLE 1,Horizon - Total Escrow Balance
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    df_TotalEscrowBalance_Horizon = spark.sql("""
    SELECT 
    0 AS Balance,
    substring(ACCT_SKEY, 4, 2) AS App_Code,
    case when lmnadt > 0 then to_timestamp(cast(lmnadt as varchar(10)), 'yyyyMMdd')
         when lmnadt = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY AS FinancialAccountId,
    'Horizon' AS SourceSystemIdentifier,
    'Horizon Total Escrow Balance' AS Type
    FROM bronze.ods_ln_master where CurrentRecord = 'Yes' and LMPART=0
    
    union all

    SELECT 
    MBESBL + MBUEBL AS Balance,
    substring(ACCT_SKEY, 4, 2) AS App_Code,
    case when MBLPDT > 0 then to_timestamp(cast(MBLPDT as varchar(10)), 'yyyyMMdd')
         when MBLPDT = 0 then NULL
    end AS BalanceAsOfDate,
    ACCT_SKEY AS FinancialAccountId, 
    'Horizon' AS SourceSystemIdentifier,
    'Horizon Total Escrow Balance' AS Type
    FROM bronze.ods_ml_master where CurrentRecord = 'Yes'
    """)

    df_TotalEscrowBalance_Horizon.createOrReplaceTempView('vw_TotalEscrowBalance_Horizon')
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,DMI Current Escrow Balance
try:
    logger.info("Joining base tables for silver Financial Account Balance table")

    basequery = '''
    SELECT 
        Escrow_Balance - Escrow_Advance_Balance AS Balance, 
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN 'ML'  
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN 'LN' 
        END AS App_code,
        TO_Timestamp(CAST(As_of_Date AS DATE), 'yyyy-MM-dd') AS BalanceAsOfDate,
        CASE 
            WHEN Investor_ID = '40H' AND Category_Code = '002' THEN '13-ML-' || LPAD(CAST(Loan_Number AS STRING), 20, '0') 
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN '13-LN-' || LPAD(CAST(Loan_Number AS STRING), 20, '0') 
            ELSE Loan_Number 
        END AS FinancialAccountId,
        'DMI' AS SourceSystemIdentifier,
        'DMI Net Escrow Balance' AS Type
    FROM bronze.dmi_dcif 
    WHERE 
        TO_Timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd') IN (
            SELECT MAX(TO_Timestamp(CAST(As_of_Date AS Date), 'yyyy-MM-dd')) 
            FROM bronze.dmi_dcif
        )
        AND CurrentRecord = 'Yes'
        AND Category_Code IN ('002', '003')
        AND Investor_ID != '40G'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("DMI_Current_Escrow_Balance_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e

# COMMAND ----------

# DBTITLE 1,Final
from pyspark.sql.functions import count, sum

df = spark.sql('''
               SELECT * FROM vw_TotalEscrowBalance_Horizon
               UNION ALL
               SELECT * FROM DMI_Current_Escrow_Balance_vw
               ''')

df.createOrReplaceTempView('Vw_EscrowBalance')
aggregation_df = df.groupBy("SourceSystemIdentifier", "Type").agg(
    count("*").alias("Count"),
    sum("Balance").alias("TotalBalance")
    )
display(aggregation_df)

duplicate_df = df.groupBy("FinancialAccountID", "App_code").agg(
    count("*").alias("Count")
).filter("Count > 1")
display(duplicate_df)

sample_data_df = df.filter(col("Balance") > 0).agg(count("*").alias("SampleCount"))
sample_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Big Union Query

# COMMAND ----------

# DBTITLE 1,Union-zeb
try:
    logger.info("Performing transformations for the bronze table")
    df_final_FA = spark.sql(
        """ SELECT tab.Balance, tab.BalanceAsOfDate, tab.App_Code, tab.FinancialAccountId, tab.SourceSystemIdentifier, tab.Type, tab.uid
    from
    (
    SELECT finance.Balance,finance.App_code,finance.BalanceAsOfDate,finance.FinancialAccountId, finance.SourceSystemIdentifier, finance.Type,
    CONCAT_WS('-', finance.FinancialAccountId, finance.Type) as uid
        from
        (
    -- ##############   CURRENT BALANCE ##############       
    SELECT * from Vw_CurrentBalance
    Union all

    -- ##############   ORIGINAL LOAN AMOUNT ##############
    SELECT * from Vw_LoanOpeningBalance

    Union all

    -- ##############   CURRENT LOAN AMOUNT ##############
    SELECT * from Vw_CurrentLoanAmount

    Union all 

    -- ##############   CURRENT PRINCIPAL BALANCE ##############
     SELECT * from Vw_CurrentPrincipalBalance

    Union all

    -- ##############   BANK OWNED BOOK BALANCE ##############
    SELECT * from Vw_BankOwnedBookBalance

    Union all

    -- ##############   BOOK BALANCE ##############
    SELECT * from Vw_BookBalance

    UNION ALL
    -- -- ##############   AVAILABLE CREDIT ##############
    SELECT * from Vw_AvailableCredit

    UNION ALL
    -- ##############   AVAILABLE CREDIT FOR PARTICIPANTS ##############
    SELECT * from vw_AvailableCreditForParticipants

    UNION ALL
    -- -- ##############   BOOK BALANCE FOR PARTICIPANTS ##############
    SELECT * from Vw_BookBalanceForParticipants

    Union all

    -- ##############  EXPOSURE ##############
    SELECT * from Vw_Exposure

    Union all

    -- ##############   LOW BALANCE YTD ##############
     SELECT * from Vw_Loan_LowBalanceYTD

    Union all

    -- ##############  COMMITMENT LIMIT ##############
    SELECT * from Vw_CommitmentLimit

    Union all
    -- ##############  HIGHT BALANCE YTD ##############

    SELECT * from Vw_Loan_HighBalanceYTD

    Union all 

    -- ##############  CURRENT APPRAISAL AMOUNT ##############
    SELECT * from Vw_CurrentAppraisalAmount

    UNION ALL 

    -- ##############   HOLD AMOUNT ##############
    SELECT * From Vw_HoldAmount

    Union all 
    -- ##############  SECOND PRINCIPAL BALANCE ##############
    SELECT * from Vw_SecondPrincipalBalance

    Union all 
    -- ##############  ASSETS UNDER MANAGEMENT ##############
    SELECT * from Vw_AssetsUnderManagement

    Union all 
    -- ##############  ASSETS UNDER CUSTODY ##############
    SELECT * from Vw_AssetsUnderCustody

    UNION ALL 
    -- ##############   MARKET VALUE ##############
    SELECT * From Vw_MarketValue

    Union all 
    -- ##############   LAST STATEMENT BALANCE ##############
    SELECT * From Vw_LastStatementBalance

    Union all 
    -- ##################  PREVIOUS MONTH AVERAGE DEPOSIT BALANCE ##################
    SELECT * From Vw_PrevMonthAvgBalance

    Union all 
    -- -- ##################  PREVIOUS DAY BALANCE ##################
    SELECT * From Vw_PreviousDayBalance

    Union all 
    -- -- ##################   ESCROW BALANCE ##################
    SELECT * From Vw_EscrowBalance

    )finance
    )tab
 """)
    df_final_FA = df_final_FA.filter("BalanceAsOfDate IS NOT NULL AND Balance IS NOT NULL")
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Count
# MAGIC %sql
# MAGIC Select count(1) from vw_final_FA

# COMMAND ----------

# DBTITLE 1,Source Dupes Check
# MAGIC %sql
# MAGIC Select financialaccountid, BalanceAsOfDate, type , count(*) 
# MAGIC from vw_final_FA
# MAGIC GROUP BY financialaccountid, BalanceAsOfDate, type  
# MAGIC HAVING count(1) > 1

# COMMAND ----------

DestinationSchema = dbutils.widgets.get('DestinationSchema')
DestinationTable = dbutils.widgets.get('DestinationTable')
AddOnType = dbutils.widgets.get('AddOnType')

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns  # get all the base columns
try:
    set_addon = df_final_FA.columns  # get only the addon columns
except Exception as e:
    set_addon = []
get_pk = spark.sql(f"""select * from config.metadata where lower(DWHTableName)='{DestinationTable.lower().strip()}' and lower(DWHSchemaName)='{DestinationSchema.lower().strip()}' """).collect()[0]['MergeKey']
get_pk_temp = get_pk.split(',')  # split the get_pk
for pk in get_pk_temp:
    set_addon.remove(pk.strip())  # remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date', 'MergeHashKey', 'CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col.lower() not in [ex_col.lower() for ex_col in excluded_columns]]

# COMMAND ----------

#get required columns from base table
df_base_required = spark.sql(f"select {','.join(filtered_basetable_columns)} from {DestinationSchema}.{DestinationTable} Where CurrentRecord='Yes'")
df_base_required.createOrReplaceTempView("vw_base")  #use this as a base table
if AddOnType == 'AddOn':
    if df_base_required.count() > 0:
        join_conditions = " and ".join([f"vw_base.{col.strip()} = vw_final_FA.{col.strip()}" for col in get_pk.split(',')])
        
        df_final_base_with_addon = spark.sql(
            f"""
            select
                vw_base.*,
                {','.join([f'vw_final_FA.{col} as {col}' for col in set_addon])}
            from 
                vw_base 
            left join 
                vw_final_FA 
            on 
                {join_conditions}
        """)
        df_final_base_with_addon.createOrReplaceTempView("vw_final_base_with_addon")
        df_final_base_with_addon.count()
    else:
        df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
        count = df_final_FA.count()
        display(count)
else:
    df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count = df_final_FA.count()
    display(count)

# COMMAND ----------

# Generate the concatenated string
base_without_pk = filtered_basetable_columns.copy()
for pk in get_pk_temp:
    base_without_pk.remove(pk.strip())
Mergehashkey_columns = list(set(set_addon + base_without_pk))
concatenated_columns = ','.join(Mergehashkey_columns)

# COMMAND ----------

  # Use the concatenated string in the SQL query
query = f"""
select
 *,
 MD5(
    CONCAT_WS(',', {concatenated_columns})
  ) AS MergeHashKey
  from
  vw_final_base_with_addon
"""
df_source = spark.sql(query)
set_addon.append('MergeHashKey')
set_addon=set(set_addon)
df_source.createOrReplaceTempView("vw_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), UID, Type from vw_source Group by UID, Type having count(1)>1

# COMMAND ----------

target_count = spark.sql(f"SELECT COUNT(*) FROM {DestinationSchema}.{DestinationTable}").collect()[0][0]

select_columns = []
for col in filtered_basetable_columns:
    if col in get_pk.split(','):
        if target_count > 0:
            select_columns.append(f"CASE WHEN {' AND '.join([f'target.{pk} IS NULL' for pk in get_pk.split(',')])} THEN source.{col} ELSE target.{col} END AS {col}")
        else:
            select_columns.append(f"source.{col}")
    else:
        select_columns.append(f"target.{col}")

query = f"""
select 
  {','.join(select_columns)},
  {','.join([f'source.{col}' for col in set_addon])},
  current_user() as DW_Created_By,
  current_timestamp() as DW_Created_Date,
  current_user() as DW_Modified_By,
  current_timestamp() as DW_Modified_Date,
  current_timestamp() as Start_Date,
  NULL as End_Date,
  'Yes' as CurrentRecord,
  CASE 
    WHEN { ' AND '.join([f'target.{col} IS NULL' for col in get_pk.split(',')]) } THEN 'Insert'
    WHEN { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) } AND source.MergeHashKey != target.MergeHashKey THEN 'Update'
    ELSE 'No Changes' 
  END As Action_Code  
from (select {','.join(set_addon)}, {','.join([f'{col}' for col in get_pk.split(',')])} from vw_source group by all) as source
left join (select * from {DestinationSchema}.{DestinationTable} where currentrecord='Yes' ) as target
on { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) }
"""

df_source = spark.sql(query)
df_source = df_source.dropDuplicates()
df_source.createOrReplaceTempView("vw_silver")
final_col = df_source.columns
final_col.remove('Action_Code')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select UID, Type, count(1) from vw_silver
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by UID,TYPE having count(1)>1

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {DestinationSchema}.{DestinationTable}({','.join(final_col)})
    SELECT {','.join(final_col)} FROM vw_silver WHERE Action_Code IN ('Insert', 'Update')
""")

spark.sql(f"""
    MERGE INTO {DestinationSchema}.{DestinationTable} AS Target
    USING (SELECT {','.join(final_col)} FROM VW_silver WHERE Action_Code='Update') AS Source
    ON { ' AND '.join([f'Target.{col} = Source.{col}' for col in get_pk.split(',')])} AND Target.MergeHashKey != Source.MergeHashKey and target.currentrecord = 'Yes'
    WHEN MATCHED THEN UPDATE SET
    Target.End_Date = CURRENT_TIMESTAMP(),
    Target.DW_Modified_Date = Source.DW_Modified_Date,
    Target.DW_Modified_By = Source.DW_Modified_By,
    Target.CurrentRecord = 'No'
""")

# COMMAND ----------

df = spark.sql(f"select * from {DestinationSchema}.{DestinationTable} where End_Date is null")

df.createOrReplaceTempView("target_view")

DFSourceNull = spark.sql(f"""
                SELECT t.*,
                    CASE WHEN s.{get_pk.split(',')[0]} IS NULL THEN 'No' ELSE 'Yes' END AS CurrentRecordTmp
                FROM target_view t
                FULL JOIN VW_silver s
                ON { ' AND '.join([f's.{col} = t.{col}' for col in get_pk.split(',')]) }
            """)
# Filter out the 'DeleteFlag' rows for next steps
DFSourceNull.createOrReplaceTempView("SourcetoInsertUpdate")

# Merge operation
MergeQuery = f"""
        MERGE INTO {DestinationSchema}.{DestinationTable} AS target
        USING SourcetoInsertUpdate AS source
        ON { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) }
        AND source.CurrentRecordTmp = 'No'
        WHEN MATCHED THEN
            UPDATE SET target.CurrentRecord = 'Deleted', target.end_date=current_timestamp(), target.DW_modified_Date=current_timestamp(),target.DW_Modified_By='Databricks'
    """

spark.sql(MergeQuery)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select UID, Type, count(1) from silver.financial_account_balance
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by UID,TYPE having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from silver.financial_account_balance
# MAGIC Where CurrentRecord='Yes'
