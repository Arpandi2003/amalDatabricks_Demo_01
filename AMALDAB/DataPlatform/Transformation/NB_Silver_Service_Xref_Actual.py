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
    (col('TableID') == 1010)
)

display(DFMetadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

TableID = 1010 
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
# MAGIC ### Create the base view

# COMMAND ----------

base_df = spark.sql("select ServiceID from silver.service_xref where currentrecord='Yes' group by 1")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# DBTITLE 1,Dupes Check
# MAGIC %sql
# MAGIC Select ServiceID, Count(1) from silver.service_xref
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by ServiceID Having count(1)>1

# COMMAND ----------

# DBTITLE 1,Data from ods_beb_services
try:
    logger.info("Joining base tabels for silver service_xref table")

    basequery = '''
            SELECT 
        CASE
            WHEN bb.profile = 'Base' THEN CAST(CONCAT('BEB-BA-', bb.Service_ID) AS STRING)
            WHEN bb.profile = 'Micro' THEN CAST(CONCAT('BEB-MI-', bb.Service_ID) AS STRING)
            WHEN bb.profile = 'Standard' THEN CAST(CONCAT('BEB-ST-', bb.Service_ID) AS STRING)
        END AS `ServiceID`,
        bb.profile AS `ApplicationDescription`,
        bb.profile AS `SubApplicationDescription`,
        bb.service AS `ServiceName`,
        CASE
            when bb.Service_ID ='362' then  'Account Recon Maint'
            when bb.Service_ID in ('87','92','94','96') then  'ACH Credit Origination'
            when bb.Service_ID in ('86','93','95','97','124') then  'ACH Debit Origination'
            when bb.Service_ID in ('69','112597') then  'ACH File Upload'
            when bb.Service_ID ='113' then  'ACH Positive Pay'
            when bb.Service_ID ='50' then  'ACH STATE TAX'
            when bb.Service_ID ='46' then  'ACH Tax Payments'
            when bb.Service_ID in ('106','112531') then  'AOTM Dashboard'
            when bb.Service_ID in ('101','108') then  'AOTM Information Reporting'
            when bb.Service_ID ='103' then  'AOTM Premium Reporting'
            when bb.Service_ID ='112501' then  'Autobooks'
            when bb.Service_ID ='141' then  'Billpay'
            when bb.Service_ID ='28' then  'BOOK TRANSFER'
            when bb.Service_ID in ('88','110','111','202','112610') then  'Check Positive Pay'
            when bb.Service_ID ='7' then  'Check Stop Payment'
            when bb.Service_ID ='80' then  'CHILD SUPPORT PAYMENT'
            when bb.Service_ID ='192' then  'COMPANY ADMIN'
            when bb.Service_ID ='58' then  'CUSTOMER MAINTAINED WIRES'
            when bb.Service_ID ='401' then  'D1UXCOMPANY'
            when bb.Service_ID ='270' then  'Decommisioned'
            when bb.Service_ID in ('112540','112573','112575') then  'Decommissioned'
            when bb.Service_ID ='68' then  'DEPOSIT IMAGING'
            when bb.Service_ID ='75' then  'DEPOSIT RECON'
            when bb.Service_ID ='172' then  'eStatements'
            when bb.Service_ID ='38' then  'EXPRESS TRANSFER'
            when bb.Service_ID ='112611' then  'EXTERNAL ACCOUNT REPORTING'
            when bb.Service_ID ='73' then  'FULL ACCOUNT RECON'
            when bb.Service_ID ='2' then  'FUNDS TRANSFER'
            when bb.Service_ID ='78' then  'Harland Clarke Checks'
            when bb.Service_ID ='90' then  'IAT FV EXCHANGE'
            when bb.Service_ID ='48' then  'IMAGING'
            when bb.Service_ID ='201' then  'IMAGING RETURNS'
            when bb.Service_ID ='55' then  'INCOMING WIRE REPORT'
            when bb.Service_ID ='24' then  'INTRADAY BALANCES'
            when bb.Service_ID ='98' then  'INVESTMENT REPORTING'
            when bb.Service_ID in ('59','60','61','62') then  'Loans'
            when bb.Service_ID in ('114','155') then  'Lockbox'
            when bb.Service_ID ='99' then  'MICRO REPORTING'
            when bb.Service_ID ='82' then  'MICROSOFT MONEY'
            when bb.Service_ID ='179' then  'MOBILE RDC'
            when bb.Service_ID ='165' then  'OTHER ACCOUNTS'
            when bb.Service_ID in ('177','178','361') then  'Out of Band Authentication'
            when bb.Service_ID ='74' then  'PARTIAL ACCOUNT RECON'
            when bb.Service_ID ='52' then  'Positive Pay'
            when bb.Service_ID in ('45','185') then  'Quickbooks'
            when bb.Service_ID ='122' then  'Remote Deposit Capture (RDC)'
            when bb.Service_ID ='127' then  'REVERSE POSITIVE PAY'
            when bb.Service_ID ='23' then  'SINGLE USER ONLY'
            when bb.Service_ID ='35' then  'SPECIAL REPORT DOWNLOAD'
            when bb.Service_ID ='102' then  'STANDARD REPORTING'
            when bb.Service_ID ='81' then  'STP 820 PAYMENT'
            when bb.Service_ID ='112506' then  'SWIFT CODE LOOKUP'
            when bb.Service_ID ='100' then  'TOKEN APPROVAL'
            when bb.Service_ID ='64' then  'TOKEN AUTHENTICATION'
            when bb.Service_ID ='120' then  'TRANSACTION MONITORING'
            when bb.Service_ID ='158' then  'Wire Domestic'
            when bb.Service_ID ='159' then  'Wire Domestic Template'
            when bb.Service_ID ='164' then  'WIRE FILE IMPORT'
            when bb.Service_ID ='149' then  'WIRE FILE UPLOAD'
            when bb.Service_ID ='160' then  'Wire FX Intl'
            when bb.Service_ID ='161' then  'Wire FX Intl Template'
            when bb.Service_ID ='162' then  'WIRE USD INTL'
            when bb.Service_ID ='163' then  'WIRE USD INTL Template'
            when bb.Service_ID ='112541' then  'ZELLE'
            when bb.Service_ID ='112601' then  'Open Connect'
            when bb.Service_ID ='112616' then  'Integrated Accounting Sync'
        end AS `ServiceType`,
        CASE
            WHEN bb.profile = 'Base' THEN 'Commercial'
            WHEN bb.profile = 'Micro' THEN 'Small Business'
            WHEN bb.profile = 'Standard' THEN 'Commercial'
        END AS `LineofBusiness`,
        bb.service_ID AS `ServiceCode`
        FROM bronze.v_ods_beb_services bb
        where bb.Profile != 'MASTER BANK SERVICES LIST'
        and bb.CurrentRecord = 'Yes' '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("ods_beb_services_VW")
except Exception as e:
    logger.error("issue while joining the base tabless")


# COMMAND ----------

# DBTITLE 1,Data from master base
try:
    logger.info("Joining base tabels for silver service table")

    basequery = """
    SELECT  --master base
  CAST(CONCAT('BEB-BA-', bb.Service_ID) AS STRING) AS `ServiceID`,
  'Base' AS `ApplicationDescription`,
  'Base' AS `SubApplicationDescription`,
  bb.service AS `ServiceName`,
  CASE
    when bb.Service_ID ='362' then  'Account Recon Maint'
    when bb.Service_ID in ('87','92','94','96') then  'ACH Credit Origination'
    when bb.Service_ID in ('86','93','95','97','124') then  'ACH Debit Origination'
    when bb.Service_ID in ('69','112597') then  'ACH File Upload'
    when bb.Service_ID ='113' then  'ACH Positive Pay'
    when bb.Service_ID ='50' then  'ACH STATE TAX'
    when bb.Service_ID ='46' then  'ACH Tax Payments'
    when bb.Service_ID in ('106','112531') then  'AOTM Dashboard'
    when bb.Service_ID in ('101','108') then  'AOTM Information Reporting'
    when bb.Service_ID ='103' then  'AOTM Premium Reporting'
    when bb.Service_ID ='112501' then  'Autobooks'
    when bb.Service_ID ='141' then  'Billpay'
    when bb.Service_ID ='28' then  'BOOK TRANSFER'
    when bb.Service_ID in ('88','110','111','202','112610') then  'Check Positive Pay'
    when bb.Service_ID ='7' then  'Check Stop Payment'
    when bb.Service_ID ='80' then  'CHILD SUPPORT PAYMENT'
    when bb.Service_ID ='192' then  'COMPANY ADMIN'
    when bb.Service_ID ='58' then  'CUSTOMER MAINTAINED WIRES'
    when bb.Service_ID ='401' then  'D1UXCOMPANY'
    when bb.Service_ID ='270' then  'Decommisioned'
    when bb.Service_ID in ('112540','112573','112575') then  'Decommissioned'
    when bb.Service_ID ='68' then  'DEPOSIT IMAGING'
    when bb.Service_ID ='75' then  'DEPOSIT RECON'
    when bb.Service_ID ='172' then  'eStatements'
    when bb.Service_ID ='38' then  'EXPRESS TRANSFER'
    when bb.Service_ID ='112611' then  'EXTERNAL ACCOUNT REPORTING'
    when bb.Service_ID ='73' then  'FULL ACCOUNT RECON'
    when bb.Service_ID ='2' then  'FUNDS TRANSFER'
    when bb.Service_ID ='78' then  'Harland Clarke Checks'
    when bb.Service_ID ='90' then  'IAT FV EXCHANGE'
    when bb.Service_ID ='48' then  'IMAGING'
    when bb.Service_ID ='201' then  'IMAGING RETURNS'
    when bb.Service_ID ='55' then  'INCOMING WIRE REPORT'
    when bb.Service_ID ='24' then  'INTRADAY BALANCES'
    when bb.Service_ID ='98' then  'INVESTMENT REPORTING'
    when bb.Service_ID in ('59','60','61','62') then  'Loans'
    when bb.Service_ID in ('114','155') then  'Lockbox'
    when bb.Service_ID ='99' then  'MICRO REPORTING'
    when bb.Service_ID ='82' then  'MICROSOFT MONEY'
    when bb.Service_ID ='179' then  'MOBILE RDC'
    when bb.Service_ID ='165' then  'OTHER ACCOUNTS'
    when bb.Service_ID in ('177','178','361') then  'Out of Band Authentication'
    when bb.Service_ID ='74' then  'PARTIAL ACCOUNT RECON'
    when bb.Service_ID ='52' then  'Positive Pay'
    when bb.Service_ID in ('45','185') then  'Quickbooks'
    when bb.Service_ID ='122' then  'Remote Deposit Capture (RDC)'
    when bb.Service_ID ='127' then  'REVERSE POSITIVE PAY'
    when bb.Service_ID ='23' then  'SINGLE USER ONLY'
    when bb.Service_ID ='35' then  'SPECIAL REPORT DOWNLOAD'
    when bb.Service_ID ='102' then  'STANDARD REPORTING'
    when bb.Service_ID ='81' then  'STP 820 PAYMENT'
    when bb.Service_ID ='112506' then  'SWIFT CODE LOOKUP'
    when bb.Service_ID ='100' then  'TOKEN APPROVAL'
    when bb.Service_ID ='64' then  'TOKEN AUTHENTICATION'
    when bb.Service_ID ='120' then  'TRANSACTION MONITORING'
    when bb.Service_ID ='158' then  'Wire Domestic'
    when bb.Service_ID ='159' then  'Wire Domestic Template'
    when bb.Service_ID ='164' then  'WIRE FILE IMPORT'
    when bb.Service_ID ='149' then  'WIRE FILE UPLOAD'
    when bb.Service_ID ='160' then  'Wire FX Intl'
    when bb.Service_ID ='161' then  'Wire FX Intl Template'
    when bb.Service_ID ='162' then  'WIRE USD INTL'
    when bb.Service_ID ='163' then  'WIRE USD INTL Template'
    when bb.Service_ID ='112541' then  'ZELLE'
    when bb.Service_ID ='112601' then  'Open Connect'
    when bb.Service_ID ='112616' then  'Integrated Accounting Sync'
  end AS `ServiceType`,
  'Commercial' AS `LineofBusiness`,
  bb.service_ID AS `ServiceCode`
FROM bronze.v_ods_beb_services bb
WHERE bb.profile = 'MASTER BANK SERVICES LIST'
and bb.CurrentRecord = 'Yes'
  AND NOT EXISTS (
    SELECT 1 
    FROM bronze.v_ods_beb_services sub_beb
    WHERE sub_beb.Service_ID = bb.Service_ID
      AND sub_beb.profile IN ('Base')
  )
    
    """
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("master_base_VW")
except Exception as e:
    logger.error("issue while joining the base tabless")

# COMMAND ----------

# DBTITLE 1,Data from master standard
try:
    logger.info("Joining base tabels for silver service table")

    basequery = '''SELECT -- master standard
  CAST(CONCAT('BEB-ST-', bb.Service_ID) AS STRING) AS `ServiceID`,
  'Standard' AS `ApplicationDescription`,
  'Standard' AS `SubApplicationDescription`,
  bb.service AS `ServiceName`,
  CASE
    when bb.Service_ID ='362' then  'Account Recon Maint'
    when bb.Service_ID in ('87','92','94','96') then  'ACH Credit Origination'
    when bb.Service_ID in ('86','93','95','97','124') then  'ACH Debit Origination'
    when bb.Service_ID in ('69','112597') then  'ACH File Upload'
    when bb.Service_ID ='113' then  'ACH Positive Pay'
    when bb.Service_ID ='50' then  'ACH STATE TAX'
    when bb.Service_ID ='46' then  'ACH Tax Payments'
    when bb.Service_ID in ('106','112531') then  'AOTM Dashboard'
    when bb.Service_ID in ('101','108') then  'AOTM Information Reporting'
    when bb.Service_ID ='103' then  'AOTM Premium Reporting'
    when bb.Service_ID ='112501' then  'Autobooks'
    when bb.Service_ID ='141' then  'Billpay'
    when bb.Service_ID ='28' then  'BOOK TRANSFER'
    when bb.Service_ID in ('88','110','111','202','112610') then  'Check Positive Pay'
    when bb.Service_ID ='7' then  'Check Stop Payment'
    when bb.Service_ID ='80' then  'CHILD SUPPORT PAYMENT'
    when bb.Service_ID ='192' then  'COMPANY ADMIN'
    when bb.Service_ID ='58' then  'CUSTOMER MAINTAINED WIRES'
    when bb.Service_ID ='401' then  'D1UXCOMPANY'
    when bb.Service_ID ='270' then  'Decommisioned'
    when bb.Service_ID in ('112540','112573','112575') then  'Decommissioned'
    when bb.Service_ID ='68' then  'DEPOSIT IMAGING'
    when bb.Service_ID ='75' then  'DEPOSIT RECON'
    when bb.Service_ID ='172' then  'eStatements'
    when bb.Service_ID ='38' then  'EXPRESS TRANSFER'
    when bb.Service_ID ='112611' then  'EXTERNAL ACCOUNT REPORTING'
    when bb.Service_ID ='73' then  'FULL ACCOUNT RECON'
    when bb.Service_ID ='2' then  'FUNDS TRANSFER'
    when bb.Service_ID ='78' then  'Harland Clarke Checks'
    when bb.Service_ID ='90' then  'IAT FV EXCHANGE'
    when bb.Service_ID ='48' then  'IMAGING'
    when bb.Service_ID ='201' then  'IMAGING RETURNS'
    when bb.Service_ID ='55' then  'INCOMING WIRE REPORT'
    when bb.Service_ID ='24' then  'INTRADAY BALANCES'
    when bb.Service_ID ='98' then  'INVESTMENT REPORTING'
    when bb.Service_ID in ('59','60','61','62') then  'Loans'
    when bb.Service_ID in ('114','155') then  'Lockbox'
    when bb.Service_ID ='99' then  'MICRO REPORTING'
    when bb.Service_ID ='82' then  'MICROSOFT MONEY'
    when bb.Service_ID ='179' then  'MOBILE RDC'
    when bb.Service_ID ='165' then  'OTHER ACCOUNTS'
    when bb.Service_ID in ('177','178','361') then  'Out of Band Authentication'
    when bb.Service_ID ='74' then  'PARTIAL ACCOUNT RECON'
    when bb.Service_ID ='52' then  'Positive Pay'
    when bb.Service_ID in ('45','185') then  'Quickbooks'
    when bb.Service_ID ='122' then  'Remote Deposit Capture (RDC)'
    when bb.Service_ID ='127' then  'REVERSE POSITIVE PAY'
    when bb.Service_ID ='23' then  'SINGLE USER ONLY'
    when bb.Service_ID ='35' then  'SPECIAL REPORT DOWNLOAD'
    when bb.Service_ID ='102' then  'STANDARD REPORTING'
    when bb.Service_ID ='81' then  'STP 820 PAYMENT'
    when bb.Service_ID ='112506' then  'SWIFT CODE LOOKUP'
    when bb.Service_ID ='100' then  'TOKEN APPROVAL'
    when bb.Service_ID ='64' then  'TOKEN AUTHENTICATION'
    when bb.Service_ID ='120' then  'TRANSACTION MONITORING'
    when bb.Service_ID ='158' then  'Wire Domestic'
    when bb.Service_ID ='159' then  'Wire Domestic Template'
    when bb.Service_ID ='164' then  'WIRE FILE IMPORT'
    when bb.Service_ID ='149' then  'WIRE FILE UPLOAD'
    when bb.Service_ID ='160' then  'Wire FX Intl'
    when bb.Service_ID ='161' then  'Wire FX Intl Template'
    when bb.Service_ID ='162' then  'WIRE USD INTL'
    when bb.Service_ID ='163' then  'WIRE USD INTL Template'
    when bb.Service_ID ='112541' then  'ZELLE'
    when bb.Service_ID ='112601' then  'Open Connect'
    when bb.Service_ID ='112616' then  'Integrated Accounting Sync'
  end AS `ServiceType`,
  'Commercial' AS `LineofBusiness`,
  bb.service_ID AS `ServiceCode`
FROM bronze.v_ods_beb_services bb
WHERE bb.profile = 'MASTER BANK SERVICES LIST'
and bb.CurrentRecord = 'Yes'
  AND NOT EXISTS (
    SELECT 1 
    FROM bronze.v_ods_beb_services sub_beb
    WHERE sub_beb.Service_ID = bb.Service_ID
      AND sub_beb.profile IN ('Standard')
  )'''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("master_standard_vw")
except Exception as e:
    logger.error("issue while joining the base tabless")


# COMMAND ----------

try:
    logger.info("Joining base tabels for silver service table")

    basequery = '''
    SELECT  -- master micro
  CAST(CONCAT('BEB-MI-', bb.Service_ID) AS STRING) AS `ServiceID`,
  'Micro' AS `ApplicationDescription`,
  'Micro' AS `SubApplicationDescription`,
  bb.service AS `ServiceName`,
  CASE
    when bb.Service_ID ='362' then  'Account Recon Maint'
    when bb.Service_ID in ('87','92','94','96') then  'ACH Credit Origination'
    when bb.Service_ID in ('86','93','95','97','124') then  'ACH Debit Origination'
    when bb.Service_ID in ('69','112597') then  'ACH File Upload'
    when bb.Service_ID ='113' then  'ACH Positive Pay'
    when bb.Service_ID ='50' then  'ACH STATE TAX'
    when bb.Service_ID ='46' then  'ACH Tax Payments'
    when bb.Service_ID in ('106','112531') then  'AOTM Dashboard'
    when bb.Service_ID in ('101','108') then  'AOTM Information Reporting'
    when bb.Service_ID ='103' then  'AOTM Premium Reporting'
    when bb.Service_ID ='112501' then  'Autobooks'
    when bb.Service_ID ='141' then  'Billpay'
    when bb.Service_ID ='28' then  'BOOK TRANSFER'
    when bb.Service_ID in ('88','110','111','202','112610') then  'Check Positive Pay'
    when bb.Service_ID ='7' then  'Check Stop Payment'
    when bb.Service_ID ='80' then  'CHILD SUPPORT PAYMENT'
    when bb.Service_ID ='192' then  'COMPANY ADMIN'
    when bb.Service_ID ='58' then  'CUSTOMER MAINTAINED WIRES'
    when bb.Service_ID ='401' then  'D1UXCOMPANY'
    when bb.Service_ID ='270' then  'Decommisioned'
    when bb.Service_ID in ('112540','112573','112575') then  'Decommissioned'
    when bb.Service_ID ='68' then  'DEPOSIT IMAGING'
    when bb.Service_ID ='75' then  'DEPOSIT RECON'
    when bb.Service_ID ='172' then  'eStatements'
    when bb.Service_ID ='38' then  'EXPRESS TRANSFER'
    when bb.Service_ID ='112611' then  'EXTERNAL ACCOUNT REPORTING'
    when bb.Service_ID ='73' then  'FULL ACCOUNT RECON'
    when bb.Service_ID ='2' then  'FUNDS TRANSFER'
    when bb.Service_ID ='78' then  'Harland Clarke Checks'
    when bb.Service_ID ='90' then  'IAT FV EXCHANGE'
    when bb.Service_ID ='48' then  'IMAGING'
    when bb.Service_ID ='201' then  'IMAGING RETURNS'
    when bb.Service_ID ='55' then  'INCOMING WIRE REPORT'
    when bb.Service_ID ='24' then  'INTRADAY BALANCES'
    when bb.Service_ID ='98' then  'INVESTMENT REPORTING'
    when bb.Service_ID in ('59','60','61','62') then  'Loans'
    when bb.Service_ID in ('114','155') then  'Lockbox'
    when bb.Service_ID ='99' then  'MICRO REPORTING'
    when bb.Service_ID ='82' then  'MICROSOFT MONEY'
    when bb.Service_ID ='179' then  'MOBILE RDC'
    when bb.Service_ID ='165' then  'OTHER ACCOUNTS'
    when bb.Service_ID in ('177','178','361') then  'Out of Band Authentication'
    when bb.Service_ID ='74' then  'PARTIAL ACCOUNT RECON'
    when bb.Service_ID ='52' then  'Positive Pay'
    when bb.Service_ID in ('45','185') then  'Quickbooks'
    when bb.Service_ID ='122' then  'Remote Deposit Capture (RDC)'
    when bb.Service_ID ='127' then  'REVERSE POSITIVE PAY'
    when bb.Service_ID ='23' then  'SINGLE USER ONLY'
    when bb.Service_ID ='35' then  'SPECIAL REPORT DOWNLOAD'
    when bb.Service_ID ='102' then  'STANDARD REPORTING'
    when bb.Service_ID ='81' then  'STP 820 PAYMENT'
    when bb.Service_ID ='112506' then  'SWIFT CODE LOOKUP'
    when bb.Service_ID ='100' then  'TOKEN APPROVAL'
    when bb.Service_ID ='64' then  'TOKEN AUTHENTICATION'
    when bb.Service_ID ='120' then  'TRANSACTION MONITORING'
    when bb.Service_ID ='158' then  'Wire Domestic'
    when bb.Service_ID ='159' then  'Wire Domestic Template'
    when bb.Service_ID ='164' then  'WIRE FILE IMPORT'
    when bb.Service_ID ='149' then  'WIRE FILE UPLOAD'
    when bb.Service_ID ='160' then  'Wire FX Intl'
    when bb.Service_ID ='161' then  'Wire FX Intl Template'
    when bb.Service_ID ='162' then  'WIRE USD INTL'
    when bb.Service_ID ='163' then  'WIRE USD INTL Template'
    when bb.Service_ID ='112541' then  'ZELLE'
    when bb.Service_ID ='112601' then  'Open Connect'
    when bb.Service_ID ='112616' then  'Integrated Accounting Sync'
  end AS `ServiceType`,
  'Small Business' AS `LineofBusiness`,
  bb.service_ID AS `ServiceCode`
FROM bronze.v_ods_beb_services bb
WHERE bb.profile = 'MASTER BANK SERVICES LIST'
and bb.CurrentRecord = 'Yes'
  AND NOT EXISTS (
    SELECT 1 
    FROM bronze.v_ods_beb_services sub_beb
    WHERE sub_beb.Service_ID = bb.Service_ID
      AND sub_beb.profile IN ('Micro')
  )
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("master_micro_vw")
except Exception as e:
    logger.error("issue while joining the base tabless")


# COMMAND ----------

# DBTITLE 1,Data from horizon
try:
    logger.info("Joining base tabels for silver service table")

    basequery = '''SELECT 
            CAST(CONCAT(prod.ProductCode) AS STRING)  `ServiceID`,
            CAST(prod.subapplicationdescription AS STRING) `ApplicationDescription`, 
            CAST(prod.subapplicationdescription AS STRING) `SubApplicationDescription`, 
            CAST(prod.ProductName AS STRING) `ServiceName`, 
            CAST(prod.subapplicationdescription AS STRING) `ServiceType`,
            CASE
                when ServiceName like '%BUS%' then 'Business'
                when ServiceName like '%DONATE%' then 'Consumer'
                when ServiceName like '%CeB%' then 'Consumer'
                else 'Commercial, Consumer, Small Business' 
            END AS `ServiceType`,
            CAST(prod.ProductNumber AS STRING) `ServiceCode`
            FROM silver.product_xref prod
            WHERE prod.ProductFamily = 'Services'
            and CurrentRecord = 'Yes'
            '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("horizon_vw")
except Exception as e:
    logger.error("issue while joining the base tabless")


# COMMAND ----------

# DBTITLE 1,Left Join
try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery="""
                SELECT 
                TAB.ServiceID,
                tab.ApplicationDescription,
                tab.SubApplicationDescription, 
                tab.ServiceName, 
                tab.ServiceType,
                tab.LineofBusiness,
                tab.ServiceCode
                FROM 
                    
                (
                    SELECT * from ods_beb_services_VW 
                UNION ALL
                    Select * from  master_base_VW 
                UNION ALL
                SELECT * FROM  master_standard_vw 
                UNION ALL
                    SELECT * FROM master_micro_vw 
                UNION ALL
                    SELECT * FROM horizon_vw
                ) tab
                        """
    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)     

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), ServiceID from vw_final_FA
# MAGIC Group by ServiceID 
# MAGIC having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dynamic Merge Logic

# COMMAND ----------

DestinationSchema = dbutils.widgets.get('DestinationSchema')
DestinationTable = dbutils.widgets.get('DestinationTable')
AddOnType = dbutils.widgets.get('AddOnType')

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

# DBTITLE 1,Extract and Filter the required columns from base table
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

# DBTITLE 1,Join with addon vw and get the required columns
#get required columns from base table
df_base_required = spark.sql(f"select {','.join(filtered_basetable_columns)} from {DestinationSchema}.{DestinationTable} where currentrecord='Yes' ")
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

# DBTITLE 1,Mergehashkey
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
# MAGIC select action_code,count(1) from vw_silver group by action_code

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
# MAGIC Select ServiceID, count(1) from silver.Service_xref
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by ServiceID having count(1)>1
