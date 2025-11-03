# Databricks notebook source
# MAGIC %md
# MAGIC - **Description**: This notebook gathers an aggregate summary of loan amounts and conducts analysis to assess a zip, customer type, and climate type breakdown to our loan services as of 2024.
# MAGIC - **Created Date**: 2025-09-19
# MAGIC - **Created By**:  Sara Iaccheo
# MAGIC - **Modified Date**: NA
# MAGIC - **Modified By**: NA
# MAGIC - **Changes Made**: breaks part loan detail and agg tables

# COMMAND ----------

# DBTITLE 1,Import Packages
from pyspark.sql.functions import count, sum, col, lit, split, lpad
from pyspark.sql.types import DoubleType, StringType, IntegerType, StructField, StructType, DateType, TimestampType, DecimalType, FloatType
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

# DBTITLE 1,initialize error logs
# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.
ErrorLogger = ErrorLogs(f"loanproceeds_by_zip_overtime")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for loanproceeds_by_zip_overtime")

# COMMAND ----------

# DBTITLE 1,look at config.metadata
DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 1514)
)
display(DFMetadata)

# COMMAND ----------

# DBTITLE 1,get metadata
# Use metadata from config table
TableID = 1514
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

# DBTITLE 1,distinct instrumentIDS
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
# MAGIC C&I Finance team View
# MAGIC

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

# DBTITLE 1,create singular instrument view - C&I Finance Team View
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

# DBTITLE 1,grouping of C&I finance loans for final union
# grouping proceeds by month and zip code
# use subquery instead because we are only using it once 
try:
    grp_ax_in = '''select 
    Datekey, 
    ZipCode, 
    cust_type,
    climate_type,
    sum(CPBproceeds) as CPBproceeds,
    sum(OLAproceeds) as OLAproceeds,
    sum(CLAproceeds) as CLAproceeds,
    sum(HasCollateral) as HasCollateral
    from (select yrmo as DateKey,
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
    and (yrmo between '202401' and'202412') and ABReport not like '%Investment Secty - HTM%' and LNSTATUS in ('Active', 'Active Loan', 'Matured Loan') and loans.acct not in (0,126312))
    group by Datekey, ZipCode, cust_type, climate_type
    '''
    df_grp_ax_in = spark.sql(grp_ax_in)
    df_grp_ax_in.createOrReplaceTempView('vw_grp_ax_in')
    display(df_grp_ax_in)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error totaling axiom data for singular climate loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Geting the base of Axiom Loans information - excluding the C&I teams list of loans
# MAGIC For the full 2024 year

# COMMAND ----------

# DBTITLE 1,Loan View - excluding Matt's list
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
    logger.error(f"Error getting axiom_loans exlcuding portfolio loans: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")


# COMMAND ----------

# MAGIC %md
# MAGIC Getting collateral information

# COMMAND ----------

# DBTITLE 1,obtain co_skey
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

# DBTITLE 1,counts
# using datekey, to get proper count per month, loan
# off case they apply for credit at a difference time we will see this in data to help with trends over time 
try:
    count_query = '''select
      Datekey, FinancialAccountNumber, count(*) as count
    from vw_prop
    group by Datekey, FinancialAccountNumber
    '''
    df_count = spark.sql(count_query)
    df_count.createOrReplaceTempView('vw_count')
    df_count.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error get count per month, loan: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC FromCollateral so if decision is made to only look at loans with collateral/address then we can filter on that field 
# MAGIC
# MAGIC Proceeds is just dividing the loan amount evenly across the pieces of collateral 

# COMMAND ----------

# DBTITLE 1,loan allocation using counts
# dividing  loan amount by nunber of piece of collateral
# did this so loan is split evenly accross piece of collateral 
# cannot use appraisal amount due to missing data 
# recommended to coalese 9-17-2025 peer review: done
#  (CurrentPrincipalBalance / c.count) as CPBproceeds,
#  (OriginalLoanAmount / c.count) as OLAproceeds,
#  (CurrentLoanAmount / c.count) as CLAproceeds,
try:
    allocate_query = '''
    select p.* ,
    c.count,
    coalesce((CurrentPrincipalBalance / c.count), CurrentPrincipalBalance ) as CPBproceeds,
    coalesce((OriginalLoanAmount / c.count), OriginalLoanAmount ) as OLAproceeds,
    coalesce((CurrentLoanAmount / c.count), CurrentLoanAmount ) as CLAproceeds, 
      1 as HasCollateral
    from vw_prop p
    left join vw_count c
    on p.FinancialAccountNumber = c.FinancialAccountNumber and p.Datekey = c.Datekey
    '''
    df_allocate = spark.sql(allocate_query)
    df_allocate.createOrReplaceTempView('vw_allocate')
    display(df_allocate)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error allocation loan proceeds using counts: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_ct_zip 
# MAGIC this view is one piece of the final - this is the zip codes and proceeds form all loans that have collateral with zip codes 
# MAGIC

# COMMAND ----------

# DBTITLE 1,vw_ct_zip
# grouping proceeds by month and zip code
try:
    collateral_zip = '''select 
    Datekey, 
    ZIP as ZipCode, 
    cust_type,
    climate_type,
    sum(CPBproceeds) as CPBproceeds,
    sum(OLAproceeds) as OLAproceeds,
    sum(CLAproceeds) as CLAproceeds,
    sum(hasCollateral) as hasCollateral
    from vw_allocate
    group by Datekey, ZipCode, cust_type, climate_type
    '''
    df_ct_zp = spark.sql(collateral_zip)
    df_ct_zp.createOrReplaceTempView('vw_ct_zip')
    display(df_ct_zp)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error grouping collateral loans by date, zip, customer and climate tyoe: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Loans without Collateral

# COMMAND ----------

# DBTITLE 1,loans without collateral
# taking from view loan where is not in collateral i.e. loans with no collateral
try:
    no_collateral = '''select
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
    where loan.financialaccountnumber not in (select financialaccountnumber from vw_allocate)
    '''
    df_no_collateral = spark.sql(no_collateral)
    df_no_collateral.createOrReplaceTempView('vw_no_collateral')
    display(df_no_collateral)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting loans with no collateral: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,adding prom cust_skey
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
    logger.error(f"Error getting primary cust_skey: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,zips from rmqinq
try:
    # getting all zips for all primary customers that have a loan
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
    df_prim_cust_zip.createOrReplaceTempView('vw_prim_cust_zip')
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting zips for primary customers: {e}")
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

# COMMAND ----------

# DBTITLE 1,joining for dataset of account numbers and zip code  then group
# part of final view. this is collapsing by datekey and zip code, for sum of proceeds and collateral
try:
    query_noco ='''
    select
        Datekey,
        ZIP as ZipCode,
        cust_type,
        climate_type,
        sum(CPBproceeds) as CPBproceeds,
        sum(OLAproceeds) as OLAproceeds,
        sum(CLAproceeds) as CLAproceeds,
        sum(HasCollateral) as HasCollateral
    from 
        (select
        ncc.DateKey,
        ncc.FinancialAccountNumber,
        ncc.instrumentID,
        ncc.OpenDate,
        ncc.CurrentPrincipalBalance as CPBproceeds,
        ncc.OriginalLoanAmount as OLAproceeds,
        ncc.CurrentLoanAmount as CLAproceeds,
        pcz.Zip,
        0 as HasCollateral,
        ncc.cust_skey,
        ncc.cust_type, 
        ncc.climate_type
        from vw_no_collateral_customer ncc
        inner join vw_prim_cust_zip pcz on pcz.cust_skey = ncc.cust_skey and pcz.ACCT_SKEY = ncc.FinancialAccountNumber )
    group by Datekey, ZipCode, cust_type, climate_type'''
    df_vw_NoCO_Zips = spark.sql(query_noco)
    df_vw_NoCO_Zips.createOrReplaceTempView('vw_NoCO_Zips')
    display(df_vw_NoCO_Zips)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error summing proceeds for loans with no collateral: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Finance Climate Portfolio

# COMMAND ----------

# DBTITLE 1,clean up
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

# DBTITLE 1,calculate proceeds percentage
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

# DBTITLE 1,grouping climate proceeds
# grabbing all proceeds by Datekey and zip again
try:
    df_vw_climate = spark.sql(''' 
                            select Datekey, ZipCode, cust_type, climate_type, sum(CPBproceeds) as CPBproceeds, sum(OLAproceeds) as OLAproceeds, sum(CLAproceeds) as CLAproceeds, sum(col) as HasCollateral
                            from vw_climate_allocated
                            group by Datekey, ZipCode, cust_type, climate_type
                            ''')
    df_vw_climate.createOrReplaceTempView('vw_climate')
    df_vw_climate.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error grouping proceeds by Datekey, ZipCode, cust_type, climate_type: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Axiom_PM_Consolidated

# COMMAND ----------

# DBTITLE 1,pm grouping
try:
    pm_query = '''select * from (
        select yrmo as Datekey, 
        coalesce(zipcode,zipcode_borrower) as ZipCode,
        sum(TRY_CAST(Book_Balance AS DECIMAL(18, 6))) as Book_Bal, -- currentbalanceamount minus particpation
        sum(TRY_CAST(EndBookbal AS DECIMAL(18, 6))) as EndBookBal, -- currentbalanceamount minus particpation
        sum(TRY_CAST(original_loan_amount as DECIMAL(18, 6))) as OLAProceeds,
        AVG(TRY_CAST(Days_30 as FLOAT)) as avg_30_day,
        AVG(TRY_CAST(Days_60 as FLOAT)) as avg_60_day,
        AVG(TRY_CAST(Days_90 as FLOAT)) as avg_90_day,
        --count(*) as total_count,
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
    where YRMO like '2024%' and ABReport not like '%Investment Secty - HTM%'  and axiom_pm_consolidated.acct not in (0,126312)
    group by YRMO, coalesce(zipcode,zipcode_borrower), cust_type, climate_type
    )
    '''
    df_pm = spark.sql(pm_query)
    df_pm.createOrReplaceTempView('vw_pm')
    df_pm.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error fetching loan sums from axiom_pm_consolidated: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")  

# COMMAND ----------

# MAGIC %md
# MAGIC # Big Union all for FINAL Dataset
# MAGIC - vw_grp_ac_in - df_grp_ax_in
# MAGIC - vw_NoCO_Zips -df_vw_NoCO_Zips
# MAGIC - vw_ct_zip - df_ct_zp
# MAGIC - vw_pm - df_pm

# COMMAND ----------

# DBTITLE 1,create columns that dont exist for explicit typing
try:
    df_vw_climate = df_vw_climate.withColumn('avg_30_day', lit(None).cast(DoubleType())) \
        .withColumn('avg_60_day', lit(None).cast(DoubleType())) \
        .withColumn('avg_90_day', lit(None).cast(DoubleType())) \
        .withColumn('Book_Bal', lit(None).cast( DecimalType(18, 6))) \
        .withColumn('EndBookBal', lit(None).cast( DecimalType(18, 6)))


    df_grp_ax_in = df_grp_ax_in.withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast( DecimalType(18, 6))) \
        .withColumn('EndBookBal', lit(None).cast( DecimalType(18, 6)))


    df_vw_NoCO_Zips = df_vw_NoCO_Zips.withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18, 6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18, 6))) 

    df_ct_zp = df_ct_zp.withColumn('avg_30_day', lit(None).cast(FloatType())) \
        .withColumn('avg_60_day', lit(None).cast(FloatType())) \
        .withColumn('avg_90_day', lit(None).cast(FloatType())) \
        .withColumn('Book_Bal', lit(None).cast(DecimalType(18, 6))) \
        .withColumn('EndBookBal', lit(None).cast(DecimalType(18, 6)))

    df_pm = df_pm.withColumn('CPBproceeds', lit(None).cast(DecimalType(18, 6))) \
        .withColumn('CLAproceeds', lit(None).cast(DecimalType(18, 6)))


except Exception as e:
    # Log errors as failed
    logger.error(f"Error selecting keys from seperate loan groupings: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,normalize datatypes
try:

    # Define a common schema
    common_schema = StructType([
        StructField("DateKey", StringType(), True),
        StructField("ZipCode", StringType(), True),
        StructField("cust_type", StringType(), True),
        StructField("climate_type", StringType(), True),
        StructField("CPBproceeds", DecimalType(18, 6), True),
        StructField("OLAproceeds", DecimalType(18, 6), True),
        StructField("CLAproceeds", DecimalType(18, 6), True),
        StructField("HasCollateral", IntegerType(), True),
        StructField("avg_30_day", FloatType(), True),
        StructField("avg_60_day", FloatType(), True),
        StructField("avg_90_day", FloatType(), True),
        StructField("Book_Bal",  DecimalType(18, 6), True),
        StructField("EndBookBal",  DecimalType(18, 6), True)
    ])

    # Function to cast columns to the appropriate data types
    def apply_schema(df, schema):
        for field in schema.fields:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        return df

    # Apply the schema to each DataFrame
    df_vw_climate = apply_schema(df_vw_climate, common_schema)
    df_grp_ax_in = apply_schema(df_grp_ax_in, common_schema)
    df_vw_NoCO_Zips = apply_schema(df_vw_NoCO_Zips, common_schema)
    df_ct_zp = apply_schema(df_ct_zp, common_schema)
    df_pm = apply_schema(df_pm, common_schema)

except Exception as e:
    # Log errors as failed
    logger.error(f"Error normalizing datatypes in seperate loan groupings: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,union loan data
try:
    df1= df_grp_ax_in.unionByName(df_vw_NoCO_Zips)
    df2 = df_ct_zp.unionByName(df_pm)
    df3 = df1.unionByName(df2)
    df3 = df3.unionByName(df_vw_climate)
    df3.createOrReplaceTempView('vw_zips_merge')
    display(df3)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error with final union: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,fix zips before grouping
try:
    df3 = df3.withColumn('ZipCode',  lpad( split(df3['ZipCode'], '-')[0], 5, "0"))
except Exception as e:
    # Log errors as failed
    logger.error(f"Error formatting unioned grouping: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,last grouping and agg
try:
    df4 = df3.groupBy("Datekey", "ZipCode", "cust_type", "climate_type").agg(
        sum("CPBproceeds").alias("CPBproceeds"),
        sum("OLAproceeds").alias("OLAproceeds"),
        sum("CLAproceeds").alias("CLAproceeds"),
        sum("HasCollateral").alias("HasCollateral"),
        sum("avg_30_day").alias("avg_30_day"),
        sum("avg_60_day").alias("avg_60_day"),
        sum("avg_90_day").alias("avg_90_day"),
        sum("Book_Bal").alias("Book_Bal"),
        sum("EndBookBal").alias("EndBookBal")
    )
    df4.createOrReplaceTempView('vw_df4')
    display(df4)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error with final grouping by date, zip, customer and climate type: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,join to state
try:
    df5_query = '''
    select tr.USPS_ZIP_PREF_STATE as State, vw_df4.* from vw_df4 left join (select cast(ZIP as string) as ZIP, USPS_ZIP_PREF_STATE from default.zip_2010_census_tract group by 1,2) tr on vw_df4.ZipCode = tr.ZIP'''
    
    df5 = spark.sql(df5_query)
    df5.createOrReplaceTempView('vw_df5')
    display(df5)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining census state to zips: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,get detail view for loan  counts
try:
    detail_table = 'silver.loanproceeds_by_zip_overtime_detail'
    detail_count = spark.table(detail_table).groupBy('ZipCode', 'State','DateKey','cust_type', 'climate_type')\
    .agg(count('*').alias('loan_count'),
        sum("CPBproceeds").alias("CPBproceeds"),
        sum("OLAproceeds").alias("OLAproceeds"),
        sum("CLAproceeds").alias("CLAproceeds"),
        sum("avg_30_day").alias("avg_30_day"),
        sum("avg_60_day").alias("avg_60_day"),
        sum("avg_90_day").alias("avg_90_day"),
        sum("Book_Bal").alias("Book_Bal"),
        sum("EndBookBal").alias("EndBookBal")
    )
    display(detail_count)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting detail table for loan counts grouping: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,loan count to loan level view
try:
    joined_df = df5.join(
        detail_count,
        on=[
            #df5.State == detail_count.State, issue joining on state key
            df5.Datekey == detail_count.DateKey,
            df5.ZipCode == detail_count.ZipCode,
            df5.cust_type == detail_count.cust_type,
            df5.climate_type == detail_count.climate_type
        ],
        how='left'
    ).select(df5['*'], detail_count['loan_count'])
    joined_df.createOrReplaceTempView('vw_joined_df')
    display(joined_df)

except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining detail agg for loan counts grouping: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

try:
    query = '''select cust_type, climate_type, sum(OLAproceeds) from vw_joined_df where datekey = '202412' group by 1,2 order by 1,3 desc'''
    df = spark.sql(query)
    df.createOrReplaceTempView('vw_test')
    display(df)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error totaling OLA amounts by customer and climate type {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,write loan proceeds over time
try:
    joined_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.loanproceeds_by_zip_overtime")
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
except Exception as e:
    # Log errors as failed
    logger.error(f"Error saving loanproceeds_by_zip_overtime to schema: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
