# Databricks notebook source
# MAGIC %md
# MAGIC - **Description**: This notebook gathers a detail view of loan amounts and conducts analysis to assess a zip, customer type, and climate type breakdown to our loan services as of 2024.
# MAGIC - **Created Date**: 2025-09-19
# MAGIC - **Created By**:  Sara Iaccheo
# MAGIC - **Modified Date**: NA
# MAGIC - **Modified By**: NA
# MAGIC - **Changes Made**: breaks part loan detail and agg tables

# COMMAND ----------

# DBTITLE 1,Import Packages
from pyspark.sql.functions import count, sum, col, lit, split, lpad
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DateType, TimestampType, DecimalType, FloatType
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
import sys

# COMMAND ----------

# DBTITLE 1,logger
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,utitlities
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,config
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,set catalog
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# DBTITLE 1,intialize error logs
# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.

ErrorLogger = ErrorLogs(f"loanproceeds_by_zip_overtime_detail")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for loanproceeds_by_zip_overtime_detail")

# COMMAND ----------

# DBTITLE 1,look at config.metadata
DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 1513)
)
display(DFMetadata)

# COMMAND ----------

# DBTITLE 1,get metadata
# Use metadata from config table
TableID = 1513
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

# DBTITLE 1,Distinct InstrumentIDs
try:
    df = spark.sql("""
    select distinct cast(AccountNumber as string) from
    default.climate_portfolio_project_zip """)
    df.createOrReplaceTempView("Finance")
    df.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting distinct instrumentIDs from climate_portfolio: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC C&I Finance Team View

# COMMAND ----------

# DBTITLE 1,getting loans with multiple entries
try:
    query_instruments = '''select * from default.climate_portfolio_project_zip where AccountNumber not in (select accountnumber from (select accountnumber, count(*) from default.climate_portfolio_project_zip where ZipCode != 'KY1' 
    group by 1 
    HAVING count(*) = 1 )) and ZipCode != 'KY1'  '''
    df_instruments = spark.sql(query_instruments)
    df_instruments.createOrReplaceTempView('vw_multi_inst')
    display(df_instruments)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting distinct instrumentIDs from climate_portfolio: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,create singular instrument view
try:
    query_instruments = '''select * from default.climate_portfolio_project_zip where AccountNumber in (select accountnumber from (select accountnumber, count(*) from default.climate_portfolio_project_zip where ZipCode != 'KY1' 
    group by 1 
    HAVING count(*) = 1 ))'''
    df_instruments = spark.sql(query_instruments)
    df_instruments.createOrReplaceTempView('vw_df_instruments')
    display(df_instruments)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error create singular instrument view: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,get axiom data for instruments - detail view
# TRY_CAST(AX_CurBal AS DECIMAL(18, 6))
try:
    query_ax_in = '''select distinct yrmo as DateKey,
    case when instrumentID_key is not null then instrumentID_key
    when AppCode = "ML" then '13-ML-' || lpad(cast(InstNum as string),20,'0')
    when AppCode in ('CC' , 'CI', 'CL', 'IL')  then '13-LN-' || lpad(cast(InstNum as string),20,'0')
    else 'error' end as FinancialAccountNumber,
    inst.AccountNumber as InstrumentID,
    LPAD(inst.zipcode, 5, '0') as ZipCode, 
    TRY_CAST(AX_CurBal AS DECIMAL(18, 6)) as CPBproceeds,
    TRY_CAST(AX_OrigBal AS DECIMAL(18, 6)) as OLAproceeds,
    TRY_CAST(LOCBAL AS DECIMAL(18, 6)) as CLAproceeds,
    AX_OrigDate as OpenDate,
    LNSTATUS as Status,
  case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
  when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate' 
  when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
  when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
  when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
  when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
  when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`,

  case when ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`, 
  1 as HasCollateral

  from bronze.axiom_loans loans left join bronze.axiom_acct acct on acct.ACCT= loans.ACCT left join vw_df_instruments inst on inst.AccountNumber =substring(loans.InstrumentID, 7)
  where
  substring(InstrumentID, 7) in (select accountnumber from vw_df_instruments)
  and (yrmo between '202401' and'202412') and ABReport not like '%Investment Secty - HTM%' and LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and loans.acct not in (0,126312)'''
    df_ax_in = spark.sql(query_ax_in)
    df_ax_in.createOrReplaceTempView('vw_ax_in')
    display(df_ax_in)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting axiom data for singular climate loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting the base of Axiom Loans information - excluding the C&I teams list of loans
# MAGIC
# MAGIC For the full 2024 year 

# COMMAND ----------

# DBTITLE 1,Finance_Axiom view from Matt to exclude later
# try:
#     df = spark.sql("""
#     select loans.*, 
#     case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
#     when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate' 
#     when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
#     when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
#     when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
#     when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
#     when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`,

#     case when ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`

#     from bronze.axiom_loans loans left join bronze.axiom_acct acct on acct.ACCT= loans.ACCT
#     where
#     substring(InstrumentID, 7) in (select accountnumber from Finance)
#     and (yrmo between '202401' and'202412') and ABReport not like '%Investment Secty - HTM%' and LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and loans.acct not in (0,126312)
#     """)
#     df.createOrReplaceTempView("Finance_Axiom")
#     df.display()
#  except Exception as e:
#     # Log errors as failed
#     logger.error(f"Error filtering loans to include just climate instruments: {e}")
#     UpdatePipelineStatusAndTime(TableID, "Failed")   

# COMMAND ----------

# DBTITLE 1,loan View - excluding matts list
try:
    query = '''select 
      yrmo as DateKey,
      case when instrumentID_key is not null then instrumentID_key
      when AppCode = "ML" then '13-ML-' || lpad(cast(InstNum as string),20,'0')
      when AppCode in ('CC' , 'CI', 'CL', 'IL')  then '13-LN-' || lpad(cast(InstNum as string),20,'0')
      else 'error'end as FinancialAccountNumber,
      InstNum as InstrumentID,
      TRY_CAST(AX_CurBal AS DECIMAL(18, 6)) as CurrentPrincipalBalance,
      TRY_CAST(AX_OrigBal AS DECIMAL(18, 6)) as OriginalLoanAmount,
      TRY_CAST(LOCBAL AS DECIMAL(18, 6)) as CurrentLoanAmount,
      AX_OrigDate as OpenDate,
      LNSTATUS as Status,
      case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
      when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate'
      when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
      when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
      when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
      when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
      when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`,


      case when ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`

    from bronze.axiom_loans left join bronze.axiom_acct acct on acct.ACCT= axiom_loans.ACCT
    where yrmo like '2024%'
    and InstrumentID not in (select instrumentid from 
    (select loans.*, 
    case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
    when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate' 
    when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
    when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
    when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
    when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
    when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`,

    case when ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`

    from bronze.axiom_loans loans left join bronze.axiom_acct acct on acct.ACCT= loans.ACCT
    where
    substring(InstrumentID, 7) in (select accountnumber from Finance)
    and (yrmo between '202401' and'202412') and ABReport not like '%Investment Secty - HTM%' and LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and loans.acct not in (0,126312)))
    
    and LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and ABReport not like '%Investment Secty - HTM%' and axiom_loans.acct not in (0,126312)

    '''
    df_loan = spark.sql(query)
    df_loan.createOrReplaceTempView('vw_loan')
    display(df_loan)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting axiom_loans exlcuding portfolio: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,Obtain CO_SKEY
try:
    query = '''
    select
        cta.co_skey,
        vw.datekey,
        vw.FinancialAccountNumber,
        InstrumentID,
        vw.CurrentPrincipalBalance,
        vw.OriginalLoanAmount,
        vw.CurrentLoanAmount,
        vw.OpenDate,
        vw.Status,
        vw.cust_type,
        vw.climate_type
    from vw_loan vw
    left join
    bronze.ods_ctactrel cta
    on vw.FinancialAccountNumber = cta.ACCT_SKEY
    where cta.CurrentRecord = 'Yes' and cta.crprim ='Y'
    '''
    ## may need remove primary filter
    df_coskey=spark.sql(query)
    df_coskey.createOrReplaceTempView('vw_coskey')
    display(df_coskey)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting collateral key: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,get properties
# last step of detail view for loans with collateral
try:
    prop_query = '''select
      prop.CPZIP5 as ZIP,
      co.*
    from vw_coskey co left join
    bronze.ods_ctprop prop
    on co.co_skey = prop.co_skey
    where CurrentRecord = 'Yes'
    and prop.CPZIP5 is not null 
    and prop.CPZIP5 != ''
    '''
    df_prop = spark.sql(prop_query)
    df_prop.createOrReplaceTempView('vw_prop')
    display(df_prop)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting collateral key: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Loans without Collateral

# COMMAND ----------

# DBTITLE 1,loans without collateral
# taking from view loan where is not in collateral i.e. loans with no collateral
try:
    no_collateral = '''
    select
    loan.Datekey,
    loan.FinancialAccountNumber,
    loan.instrumentID,
    loan.OpenDate,
    loan.CurrentPrincipalBalance,
    loan.OriginalLoanAmount,
    loan.CurrentLoanAmount,
    loan.cust_type,
    loan.climate_type
    from vw_loan loan
    where loan.financialaccountnumber not in (select financialaccountnumber from vw_prop)
    '''
    df_no_collateral = spark.sql(no_collateral)
    df_no_collateral.createOrReplaceTempView('vw_no_collateral')
    display(df_no_collateral)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting loan istruments with no collateral: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")   


# COMMAND ----------

# DBTITLE 1,adding primary cust_skey
# for loans with no collateral, we need to get primary customer id so we can map it to zip code
try:
    no_collateral_cust = '''select noco.*, xref.CUST_SKEY 
    from vw_no_collateral noco
    left join bronze.ods_rmxref xref on xref.ACCT_SKEY = noco.financialaccountnumber
    where xref.rxprim = 'Y' and xref.CurrentRecord = 'Yes' '''
    df_no_collateral_customer = spark.sql(no_collateral_cust)
    df_no_collateral_customer.createOrReplaceTempView('vw_no_collateral_customer')
    display(df_no_collateral_customer)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting primary custpomer id: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")   

# COMMAND ----------

# DBTITLE 1,zips from rmqinq
try:# getting all zips for all primary customers that have a loan
    df_prim_cust_zip = spark.sql('''
    select 
    RQZIP5 as Zip
    ,Cust_Skey
    ,ACCT_Skey
    from bronze.ods_rmqinq
    where CurrentRecord = 'Yes'
    and RQAPPL in ('ML','LN')
    and RQZIP5 is not null 
    and RQZIP5 != ''
    and cust_skey is not null 
    and cust_skey != ''
    ''')
    df_prim_cust_zip.createOrReplaceTempView('vw_primary_cutomer_zip')
    display(df_prim_cust_zip)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting zips from rmqinq: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")   

# COMMAND ----------

# MAGIC %md
# MAGIC ###Inner Join rationale for the below cell to create-- vw_loans_no_collateral_zip
# MAGIC
# MAGIC Logic: 
# MAGIC We only want where RQZIP5 is not null or blank (because zip code is the most important thing)
# MAGIC So when we do the join, if that primary customer has a blank, it will be a null zip code if we have a left join because the financial account number will still be in the dataset
# MAGIC
# MAGIC Looking and running multiple times, these people have addresses outside the USA and thus can be excluded from this exercise
# MAGIC
# MAGIC Results of the left join produced the folllowing to have null zip codes: 
# MAGIC
# MAGIC financialaccountnumber	cust_skey
# MAGIC
# MAGIC 13-LN-00000000000039819254	13-00000000009890 -- location is Ghana
# MAGIC
# MAGIC 13-LN-00000000000310104973	13-00000000265572 -- Loacation is France
# MAGIC
# MAGIC 13-LN-00000000002180000259	13-00000000381052 -- Location is England
# MAGIC
# MAGIC 13-LN-00000000002190000259	13-00000000381052 -- Same as above, England
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,joining for dataset of account numbers and zip codes
# joining primary customers zip codes to subset of loans with no collateral. marking 0 since no collateral 
try:
  zip_query_no_collateral = '''
  select
  ncc.DateKey,
  ncc.FinancialAccountNumber,
  ncc.instrumentID,
  ncc.OpenDate,
  ncc.CurrentPrincipalBalance as CPBproceeds,
  ncc.OriginalLoanAmount as OLAproceeds,
  ncc.CurrentLoanAmount as CLAproceeds,
  czp.Zip,
  0 as HasCollateral,
  ncc.cust_skey,
  ncc.cust_type, 
  ncc.climate_type
  from vw_no_collateral_customer ncc
  inner join vw_primary_cutomer_zip czp on czp.cust_skey = ncc.cust_skey and czp.ACCT_SKEY = ncc.FinancialAccountNumber 
    '''
  df_loans_no_collateral_zip = spark.sql(zip_query_no_collateral)
  df_loans_no_collateral_zip.createOrReplaceTempView('vw_loans_no_collateral_zip')
  display(df_loans_no_collateral_zip)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining account numbers and customer zip codes: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")   


# COMMAND ----------

# MAGIC %md
# MAGIC Finance Climate Portfolio

# COMMAND ----------

# DBTITLE 1,Clean up Climate_portofolio
# cleaning up finance climate portfolio
# adding leading zeroes for the zipcodes, took out non-numeric chrs in cla 
# to get sum of loans by zip,  group by zip & account so when we join later it will not be many to many issue
try:
    df_climate_portfolio = spark.sql('''
    select 
    CommitmentDescription, 
    AccountNumber,
    ZipCode,
    sum(currentloanamount) as CurrentLoanAmount,
    sum(col) as col

    from 
              (select 
              --'202412' as DateKey,
                CommitmentDescription, 
                AccountNumber,
                lpad(trim(ZipCode), 5, '0') as ZipCode, 
                cast(regexp_replace(` CurrentLoanAmount `, '[$,]', '') as float) as CurrentLoanAmount, 
                1 as col
              from default.climate_portfolio_project_zip
              where ZipCode != 'KY1' -- excluding cayman islands, and only looking at USA 
              )
    group by 1, 2, 3

    ''')
    df_climate_portfolio.createOrReplaceTempView('vw_climate_portfolio_cleaned')
    display(df_climate_portfolio)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error cleaing up climate_portofolio: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")   

# COMMAND ----------

# DBTITLE 1,calculating proceeds percentage
# finance team data is only one month which is why date is not a range
# scope changes from  CurrentLoanAmount to adding CurrentPrincipalBalance and OrginalLoanAmount
# we determined CurrentLoanMap maps from LOCBalance
# this is only way we can do a double check that we allocated correctly, get percent so we can get whole
try:
    df_climate_proceeds_percent = spark.sql("""
    select cp.AccountNumber
    , ax.InstrumentID
    , cp.ZipCode
    , cp.currentloanamount
    , ax.LOCBAL
    , cp.col
    , round(try_divide(ax.LOCBAL,cp.CurrentLoanAmount ),8) as proceedsLOCCLA, 

    case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
    when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate' 
    when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
    when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
    when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
    when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
    when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`, 

    case when acct.ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`



    from vw_climate_portfolio_cleaned cp
    left join bronze.axiom_loans ax on cp.accountnumber = substring(ax.InstrumentID, 7)
    left join bronze.axiom_acct acct on acct.ACCT= ax.ACCT
    where
    substring(InstrumentID, 7) in (select accountnumber from vw_multi_inst)
    and ax.yrmo = '202412' and acct.ABReport not like '%Investment Secty - HTM%'
    and ax.LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and ax.acct not in (0,126312)
    """)
    df_climate_proceeds_percent.createOrReplaceTempView("vw_climate_proceeds_percent")
    df_climate_proceeds_percent.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error calculating proceeds percentage: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,proceeds allocated
# taking balances divided by proceeds percent
# from output, we can view (ax.LOCBAL/cpp.proceedsLOCCLA) = CurrentLoanAmount
# amounts tie out to the dollar so we can see what we did for other months
try:
    df_climate_proceeds = spark.sql("""
    select 
      ax.yrmo as DateKey,
      case when ax.instrumentID_key is not null then ax.instrumentID_key 
      when ax.AppCode = "ML" then '13-ML-' || lpad(cast(ax.InstNum as string),20,'0') 
      when ax.AppCode in ('CC' , 'CI', 'CL', 'IL')  then '13-LN-' || lpad(cast(ax.InstNum as string),20,'0')
      else 'error' end as FinancialAccountNumber
    ,
    case when ax.InstrumentID ilike 'Loans_%' then substring(ax.InstrumentID, 7)
    when ax.InstrumentID ilike 'MTG_%' then substring(ax.InstrumentID, 5) else ax.InstrumentID end as InstrumentID

    ,cpp.AccountNumber
    ,AX_OrigDate as OpenDate
    ,cpp.ZipCode
    -- inspect here ---
    ,round(ax.AX_CurBal/cpp.proceedsLOCCLA, 0) as CPBproceeds
    ,round(ax.AX_OrigBal/cpp.proceedsLOCCLA, 0) as OLAproceeds
    , round(ax.LOCBAL/cpp.proceedsLOCCLA, 0) as CLAproceeds
    ,cpp.currentloanamount
    ,cpp.proceedsLOCCLA
    ,cpp.col
    ,cpp.cust_type
    ,cpp.climate_type
    from bronze.axiom_loans ax
    left join vw_climate_proceeds_percent cpp on cpp.InstrumentID = ax.InstrumentID
    where ax.InstrumentID in (select InstrumentID from vw_climate_proceeds_percent)
    and yrmo like '2024%' and ax.LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and ax.acct not in (0,126312)

    """)
    df_climate_proceeds.createOrReplaceTempView("vw_climate_allocated")
    df_climate_proceeds.display()

except Exception as e:
    # Log errors as failed
    logger.error(f"Error calculating proceeds percentage: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")  

# COMMAND ----------

# MAGIC %md
# MAGIC Axiom_PM_Consolidated

# COMMAND ----------

# DBTITLE 1,axiom pm detail
# issue here - null zips ?
# # TRY_CAST(AX_CurBal AS DECIMAL(18, 6))
try:
    nongrp_pm_query = '''select * from (
        select yrmo as Datekey,
        coalesce(zipcode,zipcode_borrower) as ZipCode,
        '13-PP-' || lpad(cast(InstrumentID as string),20,'0') as FinancialAccountNumber,
        InstrumentID,
        Origination_Date as OpenDate,
        TRY_CAST(Book_Balance AS DECIMAL(18, 6)) as Book_Bal,
        TRY_CAST(EndBookbal AS DECIMAL(18, 6)) as EndBookBal,
        TRY_CAST(original_loan_amount as DECIMAL(18, 6)) as OLAProceeds,
        TRY_CAST(Days_30 as FLOAT) as avg_30_day,
        TRY_CAST(Days_60 as FLOAT) as avg_60_day,
        TRY_CAST(Days_90 as FLOAT)  as avg_90_day,
        0 as HasCollateral, 
        case when acct.ABReport = '1-4 Family First Mortgages' then 'Residential Real Estate'
        when acct.ABReport = '1-4 Family Second Mortgages' then 'Residential Real Estate' 
        when acct.ABreport = 'C&I- Amal Cap' then 'C&I'
        when acct.ABreport = 'C&I- Commercial Banking' then 'C&I'
        when acct.ABreport = 'Multifamily Mortgages' then 'Commercial Real Estate'
        when acct.ABreport = 'Non-residential Mortgages' then 'Commercial Real Estate'
        when  acct.ABreport = 'Land & Land Development Mortgages' then 'Commercial Real Estate' else 'Other' end as `cust_type`, 
        
        case when ABreport_Det ilike '%solar%' then 'solar' else 'non-solar' end as `climate_type`

    from bronze.axiom_pm_consolidated left join bronze.axiom_acct acct on acct.ACCT=axiom_pm_consolidated.acct
    where YRMO like '2024%' and ABReport not like '%Investment Secty - HTM%' and axiom_pm_consolidated.acct not in (0,126312)
    )
    '''
    df_pm_nongrp = spark.sql(nongrp_pm_query)
    df_pm_nongrp.createOrReplaceTempView('vw_pm_nongrp')
    df_pm_nongrp.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error fetching loans from axiom_pm_consolidated: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")  

# COMMAND ----------

# MAGIC %md
# MAGIC # Union the Detail views
# MAGIC

# COMMAND ----------

# DBTITLE 1,select populated keys and generate null columns to match
# 'Book_Bal', 'EndBookBal', 'avg_30_day', 'avg_60_day', 'avg_90_day'
try:
    df1_detail = df_prop.select('ZIP','datekey','FinancialAccountNumber', 'InstrumentID', 'OpenDate', 'cust_type', 'climate_type','CurrentPrincipalBalance', 'OriginalLoanAmount', 'CurrentLoanAmount')\
    .withColumnRenamed('ZIP','ZipCode')\
    .withColumnRenamed('datekey','DateKey')\
    .withColumnRenamed('CurrentPrincipalBalance','CPBproceeds')\
    .withColumnRenamed('OriginalLoanAmount','OLAproceeds')\
    .withColumnRenamed('CurrentLoanAmount','CLAproceeds')\
    .withColumn('ZipCode',col("ZipCode").cast("string"))\
    .withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18,6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18,6)))\
        .withColumn('df_name', lit('df_prop').cast(StringType()))

    df2_detail = df_loans_no_collateral_zip.select('Zip','DateKey','FinancialAccountNumber', 'OpenDate','InstrumentID','cust_type', 'climate_type','CPBproceeds', 'OLAproceeds', 'CLAproceeds')\
    .withColumnRenamed('ZIP','ZipCode').withColumn('ZipCode',col("ZipCode").cast("string"))\
    .withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18,6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18,6)))\
        .withColumn('df_name', lit('df_loans_no_collateral_zip').cast(StringType()))

    df3_detail = df_ax_in.select('ZipCode','DateKey', 'FinancialAccountNumber', 'InstrumentID', 'OpenDate', 'cust_type', 'climate_type','CPBproceeds', 'OLAproceeds', 'CLAproceeds')\
    .withColumn('ZipCode',col("ZipCode").cast("string"))\
    .withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18,6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18,6)))\
        .withColumn('df_name', lit('df_ax_in').cast(StringType()))


    # df2 = df_loans_no_collateral_zip.select('DateKey','FinancialAccountNumber', 'CP
    df4_detail = df_pm_nongrp.select('ZipCode','DateKey',  'FinancialAccountNumber', 'InstrumentID', 'OpenDate',  'cust_type','climate_type','Book_Bal', 'EndBookBal', 'OLAProceeds', 'avg_30_day', 'avg_60_day', 'avg_90_day' ).withColumn('ZipCode',col("ZipCode").cast("string"))\
    .withColumn('CPBproceeds', lit(None).cast(DecimalType(18,6))) \
        .withColumn('CLAproceeds', lit(None).cast(DecimalType(18,6)))\
            .withColumn('df_name', lit('df_pm_nongrp').cast(StringType()))


    df5_detail = df_climate_proceeds.select('ZipCode','DateKey', 'FinancialAccountNumber', 'InstrumentID', 'OpenDate', 'cust_type', 'climate_type', 'CPBproceeds', 'OLAproceeds', 'CLAproceeds').withColumn('ZipCode',col("ZipCode").cast("string"))\
    .withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18,6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18,6)))\
        .withColumn('df_name', lit('df_climate_proceeds').cast(StringType()))

except Exception as e:
    # Log errors as failed
    logger.error(f"Error selecting keys from seperate detail views: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,normalize datatypes
try:
    # Define a common schema
    common_schema = StructType([
        StructField("DateKey", StringType(), True),
        StructField("ZipCode", StringType(), True),
        # FinancialAccountNumber
        StructField("FinancialAccountNumber", StringType(), True),
        # InstrumentID
        StructField("InstrumentID", StringType(), True),
        StructField("OpenDate", StringType(), True),
        StructField("cust_type", StringType(), True),
        StructField("climate_type", StringType(), True),
        StructField("CPBproceeds", DecimalType(18,6), True),
        StructField("OLAproceeds", DecimalType(18,6), True),
        StructField("CLAproceeds", DecimalType(18,6), True),
        StructField("avg_30_day", FloatType(), True),
        StructField("avg_60_day", FloatType(), True),
        StructField("avg_90_day", FloatType(), True),
        StructField("Book_Bal", DecimalType(18,6), True),
        StructField("EndBookBal", DecimalType(18,6), True)
    ])

    # Function to cast columns to the appropriate data types
    def apply_schema(df, schema):
        for field in schema.fields:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        return df

    # Apply the schema to each DataFrame
    df1_detail = apply_schema(df1_detail, common_schema)
    df2_detail = apply_schema(df2_detail, common_schema)
    df3_detail = apply_schema(df3_detail, common_schema)
    df4_detail = apply_schema(df4_detail,common_schema)
    df5_detail = apply_schema(df5_detail,common_schema)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error normalizing datatypes in detail views: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,union all views
try:
    detail_1= df1_detail.unionByName(df2_detail, allowMissingColumns=True)
    detail_2 = df3_detail.unionByName(df4_detail, allowMissingColumns=True)
    detail_3 = detail_1.unionByName(detail_2,  allowMissingColumns=True)
    detail_3 = detail_3.unionByName(df5_detail, allowMissingColumns=True)
    detail_3.createOrReplaceTempView('vw_detail')
    display(detail_3)

except Exception as e:
    # Log errors as failed
    logger.error(f"Error with final union: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,clean up zip

try:
    detail_3 = detail_3.withColumn("ZipCode", lpad(split(col("ZipCode"), '-')[0], 5, "0"))
    display(detail_3)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error formatting zip in detail views: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,join detail view to state/zips
try: 
    join_query = '''
    select tr.USPS_ZIP_PREF_STATE as State, vw_detail.*
    from vw_detail
    left join (
        select cast(ZIP as string) as ZIP, USPS_ZIP_PREF_STATE
        from default.zip_2010_census_tract
        group by 1,2
    ) tr
    on vw_detail.ZipCode = tr.ZIP
    '''
    detail_final = spark.sql(join_query)
    detail_final.createOrReplaceTempView('vw_detail_final')
    display(detail_final)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining census state to zips: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC Write to database

# COMMAND ----------

# DBTITLE 1,detail to database
try:
    detail_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.loanproceeds_by_zip_overtime_detail")
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
except Exception as e:
    # Log errors as failed
    logger.error(f"Error saving loanproceeds_by_zip_overtime_detail to schema: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
