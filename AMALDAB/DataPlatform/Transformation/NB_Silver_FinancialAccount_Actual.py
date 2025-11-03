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
    (col('TableID') == 1005)
)

display(DFMetadata)

# COMMAND ----------

TableID=1005
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

# MAGIC %sql
# MAGIC Truncate Table default.financial_account_holder

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# MAGIC %md
# MAGIC #Data from Horizon

# COMMAND ----------

# DBTITLE 1,Read data from ods_dd_master
df_ods_dd_master = spark.sql('''  
                    select cast(cast(DBACCT as bigint) as string) AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,OFF1_SKEY BankerId
                    ,null as BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when DBDTCL = 0 then null else to_timestamp(cast(DBDTCL as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,DBCIRT InterestRate
                    ,DBRTTP InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,OFF2_SKEY Officer2
                    ,case when DBDTOP = 0 then null else to_timestamp(cast(DBDTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,DBCURB PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,PROD_SKEY ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"13-" || APPL || "-STAT-" || DBSTAT Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,APPL AppType
                    ,DBGLTP GLType
                    from bronze.ods_dd_master
                    where CurrentRecord = "Yes" 
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("DD","SV")                    
                    '''
)
df_ods_dd_master.createOrReplaceTempView("vw_ods_dd_master")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  
# MAGIC from vw_ods_dd_master
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,Read data from ods_td_master
df_ods_td_master = spark.sql('''  
                    select cast(cast (TMACCT as bigint) as string) AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,OFF1_SKEY BankerId
                    ,null as BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when TMDTRD = 0 then null else to_timestamp(cast(TMDTRD as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,TMCIRT InterestRate
                    ,TMRTTP InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,case when TMMATD = 0 then null else to_timestamp(cast(TMMATD as varchar(10)),"yyyyMMdd") end MaturityDate
                    ,null NAICSCode
                    ,OFF2_SKEY Officer2
                    ,case when TMDTOP = 0 then null else to_timestamp(cast(TMDTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,TMCURB PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,PROD_SKEY ProductID
                    ,null as PurchasedCode
                    ,case when TMLREN = 0 then null else to_timestamp(cast(TMLREN as varchar(10)),"yyyyMMdd") end RenewalDate
                    ,"Horizon" SourceSystem
                    ,"13-TD-SS-" || TMSTAT Status
                    ,TMMTRM Term
                    ,0.00 TotalOutstandingAmount
                    ,APPL AppType
                    ,TMGLTP GLType
                    from bronze.ods_td_master
                    where CurrentRecord = "Yes" 
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("CD","IR")                    
                    
                    '''
)
df_ods_td_master.createOrReplaceTempView("vw_ods_td_master")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_td_master
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,ods_ln_master
df_ods_ln_master = spark.sql('''select cast(cast(ln.LMACCT as bigint) as string) AccountNumber
                    ,ln.LMREGP AmountDue
                    ,ln.LMTPDU AmountPastDue
                    ,ln.OFF1_SKEY BankerId
                    ,cast(ln.LMBOPT as varchar(1)) BillingOptionCode
                    ,ln.ORG_SKEY BranchUnitId
                    ,case when ln.LMDTCL = 0 then null else to_timestamp(cast(ln.LMDTCL as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,case when ln.LMCLED = 0 then null else to_timestamp(cast(ln.LMCLED as varchar(10)),"yyyyMMdd") end CreditLineExpirationDate
                    ,case when ln.LMLSRN = 0 then null else to_timestamp(cast(ln.LMLSRN as varchar(10)),"yyyyMMdd") end CurrentNoteDate
                    ,ln.LMDYSL DaysPastDue
                    ,ln.ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,ln.LMCYTD InterestPaidYearToDate
                    ,ln.LMERT1 InterestRate
                    ,ac.SLINTI InterestType 
                    ,case when ln.LMX1C1 = "M" then cast(ln.lmacct as string) else cast(trel.LTMACT as varchar(10)) end MasterAccountNumber
                    ,ln.LMX1C1 MasterTrancheFlag
                    ,case when ln.LMMATD = 0 then null else to_timestamp(cast(ln.LMMATD as varchar(10)),"yyyyMMdd") end MaturityDate
                    ,ln.LMNAIC NAICSCode
                    ,ln.OFF2_SKEY Officer2    
                    ,case when ln.LMDTBD = 0 then null else to_timestamp(cast(ln.LMDTBD as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,case when ln.LMDUED = 0 then null else to_timestamp(cast(ln.LMDUED as varchar(10)),"yyyyMMdd") end PaymentDueDate
                    ,ln.LMAYBL PrincipalPaidYearToDate
                    ,ln.LMTTLN PrincipalAmount
                    ,ln.PROD_SKEY ProductId
                    ,ln.LM2CH1 PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"13-LN-STAT-" || ln.LMSTAT Status
                    ,ln.LMTER1 Term
                    ,0.00 TotalOutstandingAmount
                    ,ln.LMAPPL AppType
                    ,ln.LMTYPE GLType
                    from (select * from bronze.ods_ln_master where CurrentRecord = "Yes") as ln
                    left join bronze.ods_lnm2ac ac on ln.ACCT_SKEY = ac.ACCT_SKEY and ac.CurrentRecord = "Yes"
                    left join bronze.ods_lntrel trel on ln.ACCT_SKEY = trel.ACCT2_SKEY and trel.CurrentRecord = "Yes"
                    where ln.LMPART = 0
                    and ln.CurrentRecord = "Yes" 
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("LN")
                    ''')
df_ods_ln_master.createOrReplaceTempView("vw_ods_ln_master")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_ln_master
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,read data from ods_ml_master
df_ods_ml_master=spark.sql('''select cast(cast(MBACCT as bigint) as string) AccountNumber
                    ,MBRGPY AmountDue
                    ,MBPAPD AmountPastDue
                    ,OFF_SKEY BankerId
                    ,MBBLMT BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when MBPODT = 0 then null else to_timestamp(cast(MBPODT as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,MBPDDY DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,MBYDIP InterestPaidYearToDate
                    ,MBIRAT InterestRate
                    ,case when MBINCL = 1 then "F" else "V" end InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,case when MBCMDT = 0 then null else to_timestamp(cast(MBCMDT as varchar(10)),"yyyyMMdd") end MaturityDate
                    ,MSNAIC NAICSCode
                    ,"" Officer2
                    ,case when MBORDT = 0 then null else to_timestamp(cast(MBORDT as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,case when MBDDAT = 0 then null else to_timestamp(cast(MBDDAT as varchar(10)),"yyyyMMdd") end PaymentDueDate
                    ,MBCPBL PrincipalAmount
                    ,MBPPPC PrincipalPaidYearToDate
                    ,PROD_SKEY ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"13-ML-LNST-" || cast(MBLNST as string) Status
                    --W-000905 Update the 'Term' from mbatpr to MBOTMO
                    ,MBOTMO Term
                    ,0.00 TotalOutstandingAmount
                    ,"ML" AppType
                    ,MBGLTP GLType
                    from bronze.ods_ml_master
                    where CurrentRecord = "Yes"                     
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("ML")                    
''')
df_ods_ml_master.createOrReplaceTempView("vw_ods_ml_master")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_ml_master
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,read data from ods_ag_master ->XC
df_ods_ag_master_xc = spark.sql('''select cast(cast(RSXACCT as bigint) as string)AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,OFF_SKEY BankerId
                    ,null as BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when RSXCLDT = 0 then null else to_timestamp(cast(RSXCLDT as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,RSXINTR InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,case when RSXMATD = 0 then null else to_timestamp(cast(RSXMATD as varchar(10)),"yyyyMMdd") end MaturityDate
                    ,null NAICSCode
                    ,"" Officer2
                    ,case when RSXOPDT = 0 then null else to_timestamp(cast(RSXOPDT as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,case when RSXNXDT = 0 then null else to_timestamp(cast(RSXNXDT as varchar(10)),"yyyyMMdd") end PaymentDueDate
                    ,RSXBALN PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,PROD_SKEY ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,case when RSXCLDT = 0 then "13-XR-STAT-OPN" else "13-XR-STAT-CLD" end Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,RSXAPPL AppType
                    ,null GLType
                    from bronze.ods_ag_master
                    where RSXAPPL in ("XC","XR")
                    and CurrentRecord = "Yes" 
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("XC","XR")
                     ''')
df_ods_ag_master_xc.createOrReplaceTempView("vw_ods_ag_master_xc")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_ag_master_xc
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,ods_ag_master -> Z1
df_ods_ag_master_z1=spark.sql('''select cast(cast(RSXACCT as bigint) as string)AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,OFF_SKEY BankerId
                    ,null as BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when RSXCLDT = 0 then null else to_timestamp(cast(RSXCLDT as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,RSXINTR InterestRate
                    ,"" InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,case when RSXMATD = 0 then null else to_timestamp(cast(RSXMATD as varchar(10)),"yyyyMMdd") end MaturityDate
                    ,null NAICSCode
                    ,"" Officer2
                    ,case when RSXOPDT = 0 then null else to_timestamp(cast(RSXOPDT as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,case when RSXNXDT = 0 then null else to_timestamp(cast(RSXNXDT as varchar(10)),"yyyyMMdd") end PaymentDueDate
                    ,RSXBALN PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,PROD_SKEY ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,case when RSXCLDT = 0 then "13-XR-STAT-OPN" else "13-XR-STAT-CLD" end Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,RSXAPPL AppType
                    ,null GLType
                    from bronze.ods_ag_master
                    where RSXAPPL = "Z1"  
                    and CurrentRecord = "Yes" 
                    union all
                    select prg.PAACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,ofc1.Officer_Key BankerId
                    ,null as BillingOptionCode
                    ,org.ORG_KEY BranchUnitId
                    ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,null CurrentNoteDate
                    ,0 DaysPastDue
                    ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,prg.PAIRATE InterestRate
                    ,null as InterestType
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,ofc1.Officer_Key Officer2
                    ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,0.00 PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ,prod.Product_Key ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ,"PURGED"  Status
                    ,0 Term
                    ,0.00 TotalOutstandingAmount
                    ,prg.PAAPPL AppType
                    ,0 GLType
                    from bronze.ods_rmpasr prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                    left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                    where prg.CurrentRecord = "Yes"               
                    and prg.PAAPPL in ("Z1")                    
                    ''')
df_ods_ag_master_z1.createOrReplaceTempView("vw_ods_ag_master_z1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_ag_master_z1
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,ods_rr_master -> RR
# This code identify the account number's status in the ODS tables. ("RR")
df_ods_rr_master = spark.sql('''
                select rr.RBACCT AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,rr.OFF_SKEY BankerId
                ,null as BillingOptionCode
                ,rr.ORG_SKEY BranchUnitId
                ,case when rr.RBCLDT = 0 then null else to_timestamp(cast(rr.RBCLDT as varchar(10)),"yyyyMMdd") end ClosingDate
                ,null CreditLineExpirationDate
                ,null CurrentNoteDate
                ,0 DaysPastDue
                ,rr.ACCT_SKEY FinancialAccountNumber
                ,0.00 InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,rr.RBRATE InterestRate
                ,"" InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,null MaturityDate
                ,null NAICSCode
                ,"" Officer2
                ,case when rr.RBOPDT = 0 then null else to_timestamp(cast(rr.RBOPDT as varchar(10)),"yyyyMMdd") end OpeningDate
                ,null PaymentDueDate
                ,0.00 PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,rr.PROD_SKEY ProductId
                ,null as PurchasedCode
                ,null RenewalDate
                ,"Horizon" SourceSystem
                ,"13-RR-CLSD-" || rr.RBCLSD Status
                ,0 Term
                ,0.00 TotalOutstandingAmount
                ,"RR" AppType
                ,null GLType
                from bronze.ods_rr_master rr
                where rr.CurrentRecord = "Yes"
                union all
                select prg.PAACCT AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,ofc1.Officer_Key BankerId
                ,null as BillingOptionCode
                ,org.ORG_KEY BranchUnitId
                ,case when prg.PADTPRG = 0 then null else to_timestamp(cast(prg.PADTPRG as varchar(10)),"yyyyMMdd") end ClosingDate
                ,null CreditLineExpirationDate
                ,null CurrentNoteDate
                ,0 DaysPastDue
                ,"13-" || prg.PAAPPL || "-" || lpad(cast(replace(prg.PAACCT, '.', '') as string), 20, '0') FinancialAccountNumber
                ,0.00 InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,prg.PAIRATE InterestRate
                ,null as InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,null MaturityDate
                ,null NAICSCode
                ,ofc1.Officer_Key Officer2
                ,case when prg.PADTOP = 0 then null else to_timestamp(cast(prg.PADTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                ,null PaymentDueDate
                ,0.00 PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,prod.Product_Key ProductId
                ,null as PurchasedCode
                ,null RenewalDate
                ,"Horizon" SourceSystem
                ,"PURGED"  Status
                ,0 Term
                ,0.00 TotalOutstandingAmount
                ,prg.PAAPPL AppType
                ,0 GLType
                from bronze.ods_rmpasr prg
                left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.paofcd1 and ofc1.currentrecord = "Yes"
                left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.paofcd2 and ofc2.currentrecord = "Yes"
                left join bronze.fi_core_org org on org.branch_ID = prg.PABRNC and org.currentrecord = "Yes"
                left join bronze.fi_core_product prod on prg.PAPROD = prod.Product and prg.PAAPPL = prod.Application_Code and prod.CurrentRecord = "Yes"
                where prg.CurrentRecord = "Yes"               
                and prg.PAAPPL in ("RR")                      
            ''')
df_ods_rr_master.createOrReplaceTempView("vw_ods_rr_master")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_ods_rr_master
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #Add Committment 

# COMMAND ----------

# DBTITLE 1,ods_cm_master
#W-000840 updates for including commitment products
df_ods_cm_master = spark.sql('''  
                    select prg.MC_ACCT AccountNumber
                    ,0.00 AmountDue
                    ,0.00 AmountPastDue
                    ,OFF1_SKEY BankerId
                    ,null as BillingOptionCode
                    ,ORG_SKEY BranchUnitId
                    ,case when MCDTCL = 0 then null else to_timestamp(cast(MCDTCL as varchar(10)),"yyyyMMdd") end ClosingDate
                    ,null CreditLineExpirationDate
                    ,to_timestamp(cast(MCCNOT as varchar(10)),"yyyyMMdd") as CurrentNoteDate
                    ,0 DaysPastDue
                    ,ACCT_SKEY FinancialAccountNumber
                    ,0.00 InsurancePaidYearToDate
                    ,0.00 InterestPaidYearToDate
                    ,null InterestRate
                    ,'' InterestType -- DBRTTP 
                    ,null as MasterAccountNumber 
                    ,null as MasterTrancheFlag
                    ,null MaturityDate
                    ,null NAICSCode
                    ,OFF2_SKEY Officer2
                    ,case when MCDTOP = 0 then null else to_timestamp(cast(MCDTOP as varchar(10)),"yyyyMMdd") end OpeningDate
                    ,null PaymentDueDate
                    ,MCCRPB PrincipalAmount
                    ,0.00 PrincipalPaidYearToDate
                    ---W-000840 update for including commitment products
                    ,prod.ProductCode ProductId
                    ,null as PurchasedCode
                    ,null RenewalDate
                    ,"Horizon" SourceSystem
                    ---W-000840 update for including commitment status
                    ,sf.Code_Key Status
                    ,0 Term
                    ,MCCRPB TotalOutstandingAmount
                    ,substring( ACCT_SKEY, 4,2) AppType
                    ,null as GLType
                    from bronze.ods_mc_master prg
                    left join bronze.fi_core_officer ofc1 on ofc1.Officer_code = prg.OFF1_SKEY and ofc1.currentrecord = "Yes"
                    left join bronze.fi_core_officer ofc2 on ofc2.Officer_code = prg.OFF2_SKEY and ofc2.currentrecord = "Yes"
                    left join bronze.fi_core_org org on org.Org_Key = prg.ORG_SKEY and org.currentrecord = "Yes"
                    ---W-000840 update the join key with proper join key
                    left join silver.product_xref prod on prod.ApplicationCode = prg.MCCTYP and prod.ProductNumber = prg.MCPDCD and prod.CurrentRecord = "Yes"
                    ---W-000840 added join to get the commitment status
                    left join silver.status_xref sf on sf.Status_Code = prg.MCSTAT and (sf.Application = 'MC' or sf.Application = 'IC')  and sf.CurrentRecord ='Yes'
                    where prg.CurrentRecord = "Yes"               
                  
                    '''
)
df_ods_cm_master.createOrReplaceTempView("vw_ods_cm_master")
display(df_ods_cm_master)

# COMMAND ----------

# DBTITLE 1,ods_rmxref
# df_ods_rxref=spark.sql('''select xref.ACCT_SKEY
#                 ,xref.CUST_SKEY
#                 ,xref.RXPRIM
#                 ,xref.RXLSEQ
#                 from bronze.ods_rmxref xref
#                 where xref.RXPRIM = 'Y'
#                 and xref.currentrecord = "Yes"
#                 union all
#                 select final.ACCT_SKEY
#                 ,final.CUST_SKEY
#                 ,final.RXPRIM
#                 ,final.RXLSEQ
#                 from
#                 (
#                     select xrefa.ACCT_SKEY
#                     ,xrefa.CUST_SKEY
#                     ,xrefa.RXPRIM
#                     ,xrefa.RXLSEQ
#                     ,ROW_NUMBER() OVER (PARTITION BY xrefa.acct_skey order by xrefa.rxlseq) AS rownum
#                     from bronze.ods_rmxref xrefa
#                     where not exists (select 1 from bronze.ods_rmxref xrefb where xrefb.RXPRIM = 'Y' and xrefa.ACCT_SKEY = xrefb.ACCT_SKEY and xrefb.currentrecord = "Yes")
#                     and xrefa.currentrecord = "Yes"
#                 )final
#                 where final.rownum = 1 ''')
# df_ods_rxref.createOrReplaceTempView("vw_ods_rmxref")

# COMMAND ----------

# DBTITLE 1,Horizon Transformed
df_horizon = spark.sql(""" 
select 
    siab1.`MUZXEMP#` as ABEmployeeId1,
    siab2.`MUZXEMP#` as ABEmployeeId2,
    null as ABEmployeeId3,
    cast(cast(h360.AccountNumber as bigint) as string) as AccountNumber,
    h360.AmountDue,
    h360.AmountPastDue,
    h360.BankerId,
    h360.BillingOptionCode,
    h360.BranchUnitId,
    prop.CPCNTC as CensusTrack,
    h360.ClosingDate,
    0.00 as CreditLimit,
    h360.CreditLineExpirationDate,
    sifina.SI1CH7 CreditMark,
    prop.CPCADT CurrentAppraisalDate,
    h360.CurrentNoteDate,
    case when inq.RQPNCD = "P" then "Personal Account" else "Non-Personal Account" end CustomerType,
    h360.DaysPastDue,
    sifina.SIEXCP ExceptionCRPolicy,
    case when sifina.SIDA15 = 0 then null else to_timestamp(cast(sifina.SIDA15 as varchar(10)), "yyyyMMdd") end ExtensionDate,
    h360.FinancialAccountNumber,
    glt.GTCTRNBR FinancialCostCenter,
    null as FloodCertificationNumber,
    0.00 FloodCoverageAmount,
    null as FloodZone,
    case when sifina.SIDA11 = 0 then null else to_timestamp(cast(sifina.SIDA11 as varchar(10)), "yyyyMMdd") end FyeFinancialStatementDate,
    glt.GTGLTYPDSC GLType,
    sifina.SIGUFS GuarantorFS,
    sifina.SIHCSB HMDACRASmallBusiness,
    0.00 InsurancePaidYearToDate,
    h360.InterestPaidYearToDate,
    h360.InterestRate,
    h360.InterestType,
    h360.MasterAccountNumber,
    h360.MasterTrancheFlag,
    h360.MaturityDate,
    case when sifina.SIDA17 = 0 then null else to_timestamp(cast(sifina.SIDA17 as varchar(10)), "yyyyMMdd") end ModificationDate,
    prop.CPSMSA MSACode,
    h360.NAICSCode,
    mast.RMSHRT Name,
    sifina.SI1CH5 NotforProfit,
    null as OccupancyCode,
    h360.Officer2,
    null as Officer3,
    h360.OpeningDate,
    prop.CPAPAT OriginalAppraisalAmount,
    h360.PaymentDueDate,
    h360.PrincipalAmount,
    h360.PrincipalPaidYearToDate,
    h360.ProductId,
    0.00 as PropertyTaxPaidYearToDate,
    h360.PurchasedCode,
    mast.RMREGO RegOCode,
    cod1.C1DESC RegOType,
    h360.RenewalDate,
    sifina.SIPRDF RiskRating,
    sifina.SISNCR SharedNationalCR,
    h360.SourceSystem,
    case when h360.status =  "PURGED" then "PURGED" else stat.Conformed_Status_Description end Status,
    sifina.SI2CH3 TDR,
    h360.Term,
    h360.TotalOutstandingAmount,
    sifina.SITRLG TreasuryLoanGuaranty,
    sifina.SITYCD TypeCode,
    sifina.SI1CH6 LQFlag,
    h360.AppType,
    prod.SubApplicationDescription as ApplicationDescription
from (
    select * from vw_ods_dd_master
    union all
    select * from vw_ods_td_master
    union all
    select * from vw_ods_ln_master
    union all 
    select * from vw_ods_ml_master
    union all
    select * from vw_ods_ag_master_xc
    union all 
    select * from vw_ods_ag_master_z1
    union all 
    select * from vw_ods_rr_master
    union all 
    select * from vw_ods_cm_master
) h360
left join bronze.ods_rmxref xref on h360.FinancialAccountNumber = xref.ACCT_SKEY and xref.rxprim = "Y"  and xref.CurrentRecord = "Yes" --- use flattern to get the pk, refer the customer_master
left join bronze.ods_rmqinq inq on h360.FinancialAccountNumber = inq.ACCT_SKEY and inq.CurrentRecord = "Yes"
left join bronze.ods_rmmast mast on xref.CUST_SKEY = mast.CUST_SKEY and mast.CurrentRecord = "Yes"
left join silver.product_xref prod on h360.ProductId = prod.ProductCode and prod.CurrentRecord = "Yes"
--W-000840 update the left join for status_xref 
left join silver.status_xref stat on h360.Status = stat.Code_Key and stat.CurrentRecord = "Yes"
left join bronze.ods_sifina sifina on h360.FinancialAccountNumber = sifina.ACCT_SKEY and sifina.CurrentRecord = "Yes"
left join bronze.ods_ctactrel crel on h360.FinancialAccountNumber = crel.ACCT_SKEY and crel.CRPRIM = "Y" and crel.CurrentRecord = "Yes"
left join bronze.ods_co_master cmast on crel.CO_SKEY = cmast.CO_ACCT_SKEY and cmast.CurrentRecord = "Yes"
left join bronze.ods_ctprop prop on crel.CO_SKEY = prop.CO_SKEY and prop.CurrentRecord = "Yes"
left join bronze.ods_SICOD1 cod1 on mast.RMREGO = cod1.C1USER and cod1.C1TYPE = 'REGO' and cod1.C1APPL = "RM" and cod1.CurrentRecord = "Yes"
left join bronze.ods_gltype glt on h360.AppType = glt.GTAPPLCD and h360.GLType = glt.GTGLTYPE and glt.CurrentRecord = "Yes"
left join bronze.fi_core_officer off1ID on h360.BankerId = off1ID.Officer_Key and off1ID.CurrentRecord = "Yes"
left join bronze.fi_core_officer off2ID on h360.Officer2 = off2ID.Officer_Key and off2ID.CurrentRecord = "Yes"
left join (SELECT * FROM bronze.siabusers where CurrentRecord = "Yes") siab1 on off1ID.Officer_User_ID = siab1.MUZXUID
left join (SELECT * FROM bronze.siabusers where CurrentRecord = "Yes") siab2 on off2ID.Officer_User_ID = siab2.MUZXUID
""")

df_horizon.createOrReplaceTempView("VW_Horizon")

# COMMAND ----------

df = spark.sql("""select * from VW_Horizon where financialaccountnumber not in 
(
    select financialaccountnumber from VW_Horizon
    group by 1
    having count(*)>1) """)

df.createOrReplaceTempView("VW_Horizon")

# COMMAND ----------

# DBTITLE 1,Dup Check
# MAGIC %sql
# MAGIC select financialaccountnumber from VW_Horizon
# MAGIC     group by 1
# MAGIC     having count(*)>1

# COMMAND ----------

# DBTITLE 1,committment check
# MAGIC %sql
# MAGIC select * from vw_horizon 
# MAGIC where substring(financialaccountnumber,4,2) = 'MC'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.ods_rmxref
# MAGIC where ACCT_SKEY = '13-SV-00000000001745000151'
# MAGIC and CurrentRecord = 'Yes'

# COMMAND ----------

# %sql
# select * from silver.financial_account_party
# where Financial_AccountNumber_skey in ('13-SV-00000000001745000194'
# ,'13-SV-00000000001745000182'
# ,'13-SV-00000000001745000197'
# ,'13-SV-00000000001745000152'
# ,'13-SV-00000000001745000170'
# ,'13-SV-00000000001745000165'
# ,'13-SV-00000000001745000159'
# ,'13-SV-00000000001745000167'
# ,'13-SV-00000000001745000176'
# ,'13-SV-00000000001745000157'
# ,'13-SV-00000000001745000188'
# ,'13-SV-00000000001745000180'
# ,'13-SV-00000000001745000163'
# ,'13-SV-00000000001745000185'
# ,'13-SV-00000000001745000183'
# ,'13-SV-00000000001745000193'
# ,'13-SV-00000000001745000190'
# ,'13-SV-00000000001745000184'
# ,'13-SV-00000000001745000189'
# ,'13-SV-00000000001745000173'
# ,'13-SV-00000000001745000168'
# ,'13-SV-00000000001745000196'
# ,'13-SV-00000000001745000186'
# ,'13-SV-00000000001745000178'
# ,'13-SV-00000000001745000181'
# ,'13-SV-00000000001745000175'
# ,'13-SV-00000000001745000191'
# ,'13-SV-00000000001745000195'
# ,'13-SV-00000000001745000150'
# ,'13-SV-00000000001745000174'
# ,'13-SV-00000000001745000154'
# ,'13-SV-00000000001745000158'
# ,'13-SV-00000000001745000156'
# ,'13-SV-00000000001745000171'
# ,'13-SV-00000000001745000151'
# ,'13-SV-00000000001745000179'
# ,'13-SV-00000000001745000162'
# ,'13-SV-00000000001745000172'
# ,'13-SV-00000000001745000155'
# ,'13-SV-00000000001745000161'
# --,'13-ML-00000000001121407859'
# ,'13-SV-00000000001745000166'
# ,'13-SV-00000000001745000192')
# order by Financial_AccountNumber_skey

# COMMAND ----------

# MAGIC %md
# MAGIC #Osaic

# COMMAND ----------

# DBTITLE 1,Officer per Account - Osaic
#W-000898 Added this cell with the following code to determine the officer for Osaic Account
from pyspark.sql.functions import split, col, levenshtein, lit

Acct_Officer_list = []
Acct_Broker_df = spark.table("bronze.account_osaic").select("Account_Unique_ID", "Broker_Name").distinct()

for value in Acct_Broker_df.collect():
    Officer1, Officer2, Officer3 = None, None, None
    if "/" in value.Broker_Name:
        officers = value.Broker_Name.split(' ')
        first_names, last_names = officers
        first_names_list = first_names.split('/')
        last_names_list = last_names.split('/')
        Officer1 = first_names_list[0] + ' ' + last_names_list[0]
        Officer2 = first_names_list[1] + ' ' + last_names_list[1] if len(first_names_list) > 1 else None
        Officer3 = first_names_list[2] + ' ' + last_names_list[2] if len(first_names_list) > 2 else None
    else:
        Officer1 = value.Broker_Name

    # Fuzzy match logic for Officer1
    if Officer1:
        fuzzy_match_df = spark.createDataFrame([(Officer1,)], ["Officer1"])
        fuzzy_match_df = fuzzy_match_df.withColumn("levenshtein_distance", levenshtein(col("Officer1"), lit(Officer1)))
        closest_match = fuzzy_match_df.orderBy(col("levenshtein_distance")).first()[0]
        Officer1 = closest_match

    new_row = (value.Account_Unique_ID, value.Broker_Name, Officer1, Officer2, Officer3)
    Acct_Officer_list.append(new_row)

Acct_Officer_df = spark.createDataFrame(Acct_Officer_list, ["Account_Unique_ID", "Broker_Name", "Officer_1", "Officer_2", "Officer_3"])
Acct_Officer_df.createOrReplaceTempView("vw_acct_officer_xref")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Updated Osaic FinancialAccountNumber 

# COMMAND ----------

# DBTITLE 1,Osaic-Transformed
df_osaic = spark.sql("""
                select off1.ABEmployeeNumber as ABEmployeeId1
                ,off2.ABEmployeeNumber as ABEmployeeId2
                ,off3.ABEmployeeNumber as ABEmployeeId3
                ,mapping.Account_Number  AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,off1.Officer_Key BankerId
                ,null as BillingOptionCode
                ,org.Branch_Code as BranchUnitId
                ,null as CensusTrack
                ,cast(OS.Close_Date as timestamp )ClosingDate
                ,0.00 as CreditLimit
                ,null CreditLineExpirationDate 
                ,null as CreditMark
                ,null CurrentAppraisalDate
                ,null CurrentNoteDate
                ,"Personal Account" CustomerType
                ,0 DaysPastDue
                ,null as ExceptionCRPolicy
                ,null ExtensionDate
                ,"13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0") FinancialAccountNumber
                ,null FinancialCostCenter
                ,null as FloodCertificationNumber
                ,0.00 FloodCoverageAmount
                ,null as FloodZone
                ,null FyeFinancialStatementDate
                ,null as GLType
                ,null as GuarantorFS
                ,null as HMDACRASmallBusiness
                ,0.00 InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,0.0000 InterestRate
                ,null as InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,null MaturityDate
                ,null ModificationDate
                ,null as MSACode    
                ,null NAICSCode
                ,case 
                    when OS.Middle_Name is not null and OS.Last_Name is not null then OS.First_Name || ' ' || OS.Middle_Name || ' ' || OS.Last_Name 
                    when OS.Middle_Name is not null and OS.Last_Name is null then OS.First_Name || ' ' || OS.Middle_Name 
                    when OS.Middle_Name is null and OS.Last_Name is not null then OS.First_Name || ' ' || OS.Last_Name 
                    when OS.Middle_Name is null and OS.Last_Name is null then OS.First_Name 
                end as Name 
                ,null as NotforProfit
                ,null as OccupancyCode
                ,off2.Officer_Key Officer2
                ,off3.Officer_Key Officer3
                ,cast(OS.Open_Date as timestamp )OpeningDate
                ,0.00 OriginalAppraisalAmount
                ,null as PaymentDueDate
                ,0.00 PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,prod.ProductCode ProductId
                ,0.00 as PropertyTaxPaidYearToDate
                ,null as PurchasedCode
                ,null as RegOCode
                ,null as RegOType
                ,NULL RenewalDate
                ,NULL RiskRating
                ,null as SharedNationalCR
                ,"Wealth Management" as SourceSystem
                ,case when OS.Close_Date is null then "OPEN" else "CLOSED" end Status
                ,null as TDR
                ,0 as Term
                ,0.00 TotalOutstandingAmount
                ,null as TreasuryLoanGuaranty
                ,null as TypeCode
                ,null as LQFlag 
                ,"OS" AppType
                ,prod.SubApplicationDescription ApplicationDescription
                from bronze.account_osaic OS
                --W-000898 update the to use the view vw_acct_officer_xref
                left join vw_acct_officer_xref aox on OS.Account_Unique_ID = aox.Account_Unique_ID
                left join silver.officer_xref off1 on aox.Officer_1 = off1.OS_Officer_Name and off1.CurrentRecord = "Yes"
                left join silver.officer_xref off2 on aox.Officer_2 = off2.OS_Officer_Name and off2.CurrentRecord = "Yes"
                left join silver.officer_xref off3 on aox.Officer_3 = off3.OS_Officer_Name and off3.CurrentRecord = "Yes"
                left join silver.branch_xref org on Branch_Name = org.OsaicBranchMapName and org.CurrentRecord = "Yes"
                left join silver.product_xref prod on Account_Type = prod.OsaicProductMapping and prod.CurrentRecord = "Yes"
                inner join bronze.accountmapping_osaic mapping on mapping.Unique_Id = os.Account_Unique_ID and mapping.CurrentRecord = 'Yes'
                where OS.CurrentRecord = "Yes" 
                and OS.SSN_Tax_ID is not null 
                AND OS.SSN_Tax_ID != "" 
                AND OS.SSN_Tax_ID != " "
                """)
df_osaic.createOrReplaceTempView("vw_osaic")

# COMMAND ----------

# DBTITLE 1,check if osaicmapping has everything in account_osaic
# MAGIC %sql
# MAGIC select os.Account_Unique_ID, os.Open_Date, os.Last_Name, os.First_Name, os.SSN_Tax_ID 
# MAGIC from bronze.account_osaic os
# MAGIC where os.Account_Unique_ID not in (
# MAGIC                 select Unique_Id 
# MAGIC                 from bronze.accountmapping_osaic 
# MAGIC                 where CurrentRecord = 'Yes' )
# MAGIC and os.CurrentRecord = 'Yes'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_osaic
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC # DMI
# MAGIC

# COMMAND ----------

# DBTITLE 1,DMI Heloc
df_dmi_heloc=spark.sql(""" select 
                off1ID.`MUZXEMP#` as ABEmployeeId1
                ,null as ABEmployeeId2
                ,null as ABEmployeeId3
                ,dcifLN.Loan_Number AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,offi.Officer_Key BankerId
                ,null as BillingOptionCode
                ,coalesce(org.Org_Key,"13-1") as BranchUnitId
                ,cast(dcifLN.Census_tract as string) CensusTrack
                --W-000922 - Updating the closing Date 
                ,case 
                when dcifLN.Paid_in_Full_Date is not null and dcifLN.Paid_in_Full_Stop_Code = 1  
                then cast(dcifLN.Paid_in_Full_Date as timestamp)
                when dcifLN.Service_Release_Date is not null and dcifLN.Paid_in_Full_Stop_Code = 3 
                then cast(dcifLN.Service_Release_Date as timestamp)
                else null 
                end as ClosingDate
                ,0.00 as CreditLimit
                ,null CreditLineExpirationDate 
                ,null as CreditMark
                ,cast(dcifLN.Current_Appraisal_Date as timestamp) CurrentAppraisalDate 
                ,cast(dcifLN.Loan_Note_Date as timestamp) CurrentNoteDate
                ,"Personal Account" CustomerType
                ,Days_Past_Due DaysPastDue
                ,null as ExceptionCRPolicy
                ,null ExtensionDate
                ,"13-LN-" || lpad(cast(dcifLN.Loan_Number as string),20,"0") FinancialAccountNumber
                ,null FinancialCostCenter
                ,null as FloodCertificationNumber
                ,dcifLN.Flood_Coverage_Amounts FloodCoverageAmount
                ,dcifLN.Flood_Zone FloodZone
                ,null FyeFinancialStatementDate
                ,null as GuarantorFS
                ,null as GLType
                ,null as HMDACRASmallBusiness
                ,Hazard_Premium_Monthly_Escrow_Amount InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,dcifLN.Annual_Interest_Rate InterestRate
                ,ac.SLINTI InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,cast(dcifLN.maturity_date as timestamp)MaturityDate
                ,null ModificationDate
                ,dcifLN.MSA_code MSACode                    
                ,null NAICSCode
                ,dcifln.Mortgagor_Name_Formatted_for_CBR_Reporting Name
                ,null as NotforProfit
                ,dcifLN.Occupancy_Code OccupancyCode
                ,off2.Officer_Key Officer2
                ,null as Officer3
                ,cast(dcifLN.Loan_Closing_Date as timestamp) OpeningDate
                ,0.00 OriginalAppraisalAmount
                ,cast(dcifLN.Next_Payment_Due_Date as timestamp) PaymentDueDate
                ,dcifLN.Original_Mortgage_Amount PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,coalesce(ln.PROD_SKEY,"13-LN-120") ProductId
                ,coalesce(dcifLN.City_Tax_Amount_Monthly_Escrow_Amount,0) + coalesce(dcifLN.County_Tax_Amount_Monthly_Escrow_Amount,0) as PropertyTaxPaidYearToDate
                ,null as PurchasedCode
                ,null as RegOCode
                ,null as RegOType
                ,NULL RenewalDate
                ,NULL RiskRating
                ,null as SharedNationalCR
                ,"DMI" as SourceSystem
                --W-000922 - Updating the Status
                ,case 
                when dcifLN.Paid_in_Full_Date is not null and dcifLN.Paid_in_Full_Stop_Code = 1  
                or dcifLN.Service_Release_Date is not null and dcifLN.Paid_in_Full_Stop_Code = 3 
                then "CLOSED" 
                else "OPEN" 
                end as Status
                ,null as TDR
                ,dcifLN.Loan_Term as Term
                ,dcifLN.First_Principal_Balance TotalOutstandingAmount
                ,null as TreasuryLoanGuaranty
                ,null as TypeCode
                ,null as LQFlag 
                ,prod.Sub_Application_Code AppType
                ,prod.Sub_Application_Description ApplicationDescription
                from bronze.dmi_dcif dcifLN
                left join bronze.ods_ln_master ln on dcifLN.Old_Loan_Number = ln.LMACCT and ln.LMPART = 0 and LN.CurrentRecord = "Yes"
                left join bronze.ods_lnm2ac ac on ln.ACCT_SKEY = ac.ACCT_SKEY and ac.CurrentRecord = "Yes"
                left join bronze.fi_core_org org on ln.ORG_SKEY = org.Org_Key and org.CurrentRecord = "Yes"
                left join bronze.fi_core_officer offi on ln.OFF1_SKEY = offi.Officer_Key and offi.CurrentRecord = "Yes"
                left join bronze.fi_core_officer off2 on ln.OFF2_SKEY = off2.Officer_Key and off2.CurrentRecord = "Yes"
                left join bronze.fi_core_product prod on coalesce(ln.PROD_SKEY,"13-LN-120") = prod.Product_Key and prod.CurrentRecord = "Yes"
                left join (SELECT CONCAT('13-', SPLIT(muzxuid, '213')[1]) as key, * 
                FROM bronze.siabusers) off1ID on offi.Officer_Key = off1ID.key
                where dcifLN.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcifLN.CurrentRecord = "Yes"
                and dcifLN.Investor_ID = "40H"
                and dcifLN.Category_Code = "003" 
                """)
df_dmi_heloc.createOrReplaceTempView("vw_dmi_heloc")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_dmi_heloc
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,DMI Resi
df_dmi_resi = spark.sql(""" select off1ID.`MUZXEMP#` as ABEmployeeId1
                ,null as ABEmployeeId2
                ,null as ABEmployeeId3
                ,dcifML.Loan_Number AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,offi.Officer_Key BankerId
                ,null as BillingOptionCode
                ,coalesce(org.Org_Key,"13-1") as BranchUnitId
                ,cast(dcifML.Census_tract as string) CensusTrack
                --W-000922 - Updating the ClosingDate
                ,case 
                when dcifML.Paid_in_Full_Date is not null and dcifML.Paid_in_Full_Stop_Code = 1  
                then cast(dcifML.Paid_in_Full_Date as timestamp)
                when dcifML.Service_Release_Date is not null and dcifML.Paid_in_Full_Stop_Code = 3 
                then cast(dcifML.Service_Release_Date as timestamp)
                else null 
                end as ClosingDate
                ,0.00 as CreditLimit
                ,null CreditLineExpirationDate 
                ,null as CreditMark
                ,cast(dcifML.Current_Appraisal_Date as timestamp) CurrentAppraisalDate 
                ,cast(dcifML.Loan_Note_Date as timestamp) CurrentNoteDate
                ,"Personal Account" CustomerType
                ,Days_Past_Due DaysPastDue
                ,null as ExceptionCRPolicy
                ,null ExtensionDate
                ,"13-ML-" || lpad(cast(dcifML.Loan_Number as string),20,"0") FinancialAccountNumber
                ,null FinancialCostCenter
                ,null as FloodCertificationNumber
                ,dcifML.Flood_Coverage_Amounts FloodCoverageAmount
                ,dcifML.Flood_Zone FloodZone                
                ,null FyeFinancialStatementDate
                ,null as GLType
                ,null as GuarantorFS
                ,null as HMDACRASmallBusiness
                ,Hazard_Premium_Monthly_Escrow_Amount InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,dcifML.Annual_Interest_Rate InterestRate
                ,case when ml.MBINCL = 1 then "F" else "V" end InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,cast(dcifML.maturity_date as timestamp) MaturityDate
                ,null ModificationDate
                ,dcifML.MSA_code MSACode                    
                ,null NAICSCode
                ,dcifml.Mortgagor_Name_Formatted_for_CBR_Reporting  Name
                ,null as NotforProfit
                ,dcifML.Occupancy_Code OccupancyCode
                ,null as Officer2
                ,null as Officer3
                ,cast(dcifML.Loan_Closing_Date as timestamp) OpeningDate
                ,0.00 OriginalAppraisalAmount
                ,cast(dcifML.Next_Payment_Due_Date as timestamp) PaymentDueDate
                ,dcifML.Original_Mortgage_Amount PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,coalesce(ml.PROD_SKEY,case when dcifML.arm_plan_id is not null then "13-ML-ARM" else "13-ML-600" end) ProductId
                ,coalesce(dcifML.City_Tax_Amount_Monthly_Escrow_Amount,0) + coalesce(dcifML.County_Tax_Amount_Monthly_Escrow_Amount,0) as PropertyTaxPaidYearToDate
                ,null as PurchasedCode
                ,null as RegOCode
                ,null as RegOType
                ,NULL RenewalDate
                ,NULL RiskRating
                ,null as SharedNationalCR
                ,"DMI" as SourceSystem
                --W-000922 - Updating the Status
                ,case 
                when dcifML.Paid_in_Full_Date is not null and dcifML.Paid_in_Full_Stop_Code = 1  
                or dcifML.Service_Release_Date is not null and dcifML.Paid_in_Full_Stop_Code = 3 
                then "CLOSED" 
                else "OPEN" 
                end as Status
                ,null as TDR
                ,dcifML.Loan_Term as Term
                ,dcifml.First_Principal_Balance TotalOutstandingAmount
                ,null as TreasuryLoanGuaranty
                ,null as TypeCode
                ,null as LQFlag 
                ,prod.SubApplicationCode AppType
                ,prod.ApplicationDescription ApplicationDescription
                from bronze.dmi_dcif dcifML
                left join bronze.ods_ml_master ml on dcifML.Old_Loan_Number = ml.MBACCT and ml.CurrentRecord = "Yes"
                left join bronze.fi_core_org org on ml.ORG_SKEY = org.Org_Key and org.CurrentRecord = "Yes"
                left join bronze.fi_core_officer offi on ml.OFF_SKEY = offi.Officer_Key and offi.CurrentRecord = "Yes"
                left join silver.product_xref prod on coalesce(ml.PROD_SKEY,case when dcifML.arm_plan_id is not null then "13-ML-ARM" else "13-ML-600" end) = prod.ProductCode and prod.CurrentRecord = "Yes"
                left join (SELECT CONCAT('13-', SPLIT(muzxuid, '213')[1]) as key, * 
                FROM bronze.siabusers) off1ID on offi.Officer_Key = off1ID.key
                where dcifML.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcifML.CurrentRecord = "Yes"
                and dcifML.Investor_ID = "40H"
                and dcifML.Category_Code = "002" """)
df_dmi_resi.createOrReplaceTempView("vw_dmi_resi")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_dmi_resi
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC # Global Plus
# MAGIC

# COMMAND ----------

# DBTITLE 1,Global Plus
df_global_plus = spark.sql("""select
                 null as ABEmployeeId1
                ,null as ABEmployeeId2
                ,null as ABEmployeeId3
                ,GP.portfolio AccountNumber
                ,0.00 AmountDue
                ,0.00 AmountPastDue
                ,"GP-" BankerId
                ,null as BillingOptionCode
                ,br.Branch_Code as BranchUnitId
                ,null as CensusTrack
                ,GP.date_closed ClosingDate
                ,0.00 as CreditLimit
                ,null CreditLineExpirationDate 
                ,null as CreditMark
                ,null CurrentAppraisalDate 
                ,null CurrentNoteDate
                ,"Personal Account" CustomerType
                ,0 DaysPastDue
                ,null as ExceptionCRPolicy
                ,null ExtensionDate
                ,"13-GP-" || lpad(cast(replace(GP.portfolio, '.', '') as string), 20, '0') FinancialAccountNumber
                ,null FinancialCostCenter
                ,null as FloodCertificationNumber
                ,0.00 FloodCoverageAmount
                ,null as FloodZone                      
                ,null FyeFinancialStatementDate
                ,null as GLType
                ,null as GuarantorFS
                ,null as HMDACRASmallBusiness
                ,0.00 InsurancePaidYearToDate
                ,0.00 InterestPaidYearToDate
                ,0.0000 InterestRate
                ,null as InterestType
                ,null as MasterAccountNumber 
                ,null as MasterTrancheFlag
                ,null MaturityDate
                ,null ModificationDate
                ,null as MSACode                    
                ,null NAICSCode
                ,GP.Name Name
                ,null as NotforProfit
                ,null as OccupancyCode
                ,null as Officer2
                ,null as Officer3
                ,GP.date_opened OpeningDate
                ,0.00 OriginalAppraisalAmount
                ,null as PaymentDueDate
                ,0.00 PrincipalAmount
                ,0.00 PrincipalPaidYearToDate
                ,coalesce(prod.ProductCode,"GP-OT-0") ProductId
                ,0.00 as PropertyTaxPaidYearToDate
                ,null as PurchasedCode
                ,null as RegOCode
                ,null as RegOType
                ,NULL RenewalDate
                ,NULL RiskRating
                ,null as SharedNationalCR
                ,"Global Plus" as SourceSystem
                ,case when GP.date_closed is null then "OPEN" else "CLOSED" end Status
                ,null as TDR
                ,0 as Term
                
                ,0.00 TotalOutstandingAmount
                ,null as TreasuryLoanGuaranty
                ,null as TypeCode
                ,null as LQFlag 
                ,"GP" AppType
                --W-000836 Update the ApplicationDescription to Trust
                ,"Trust" ApplicationDescription
                from bronze.portfolio GP
                left join bronze.account AC on gp.account = ac.INTERNAL and ac.CurrentRecord = "Yes"
                left join (select * FROM silver.branch_xref where currentrecord = 'Yes') br on br.GPBranchMapName = ac.BRANCH
                left join bronze.rc rc on upper(gp.responsibility_name) = upper(rc.MEANING) and rc.CurrentRecord = "Yes"
                --W-000901 Remove the filter on the ApplicationCode
                --Added filter for productcode to GP
                left join silver.product_xref prod on rc.NUMBER = prod.productnumber and prod.productcode ilike '%gp%' and prod.currentrecord = 'Yes'
                where GP.CurrentRecord = "Yes" 
                and GP.portfolio between '1000003.1' and '7000001.0'
                --W-001295 Add next filter to exclude accounts that starts with 2 or 5 series
                and not (startswith(GP.portfolio, '2') or startswith(GP.portfolio, '5'))                
                and AC.TAXID is not null 
                AND AC.TAXID != "" 
                AND AC.TAXID != " " 
                AND REGEXP_REPLACE(AC.TAXID, '[^0-9]', '') NOT IN ('999999999') 
            """)
df_global_plus.createOrReplaceTempView("vw_global_plus")

# COMMAND ----------

# DBTITLE 1,Add XAA Composite Records
# df = spark.sql('''
# select tab.ParentId
# ,tab.SF_CustKey_fKey SF_CustKey_fKey
# ,tab.SF_CustKey_fKey FAAcct_NumberSF_Custkey_Key
# ,tab.SF_CustKey_fKey Financial_AccountNumber_skey
# ,tab.AccountNumber as AccountNumber
# ,tab.src
# ,tab.RoleID
# from (
#   SELECT CASE WHEN ACCT_NBR = LEAD_ACCT_NBR AND ANLYS_APPL_CDE = "C" then ""
#   WHEN ANLYS_APPL_CDE = "D" and ACCT_NBR != LEAD_ACCT_NBR THEN Concat("C-",LEAD_ACCT_NBR) END AS ParentId
#   ,CASE WHEN ACCT_NBR = LEAD_ACCT_NBR then Concat("C-",LEAD_ACCT_NBR) END AS SF_CustKey_fKey
#   ,CASE WHEN ACCT_NBR = LEAD_ACCT_NBR then LEAD_ACCT_NBR END AS AccountNumber
#   ,ANLYS_APPL_CDE
#   ,ACCT_NBR
#   ,ACCT_NME
#   ,"Business" RoleID
#   ,"02_XAA_COMPOSITE" src
#   from bronze.xaa_ods_acct_hist
#   where ANLYS_APPL_CDE = 'C'
# ) tab
# ''').dropDuplicates()
# df.createOrReplaceTempView("vw_xaa_composite")     

 

# COMMAND ----------

# df = spark.sql("""select null as as ABEmployeeId1
#                 ,null as as ABEmployeeId2
#                 ,null as as ABEmployeeId3
#                 ,AccountNumber AccountNumber
#                 ,0.00 AmountDue
#                 ,0.00 AmountPastDue
#                 ,null as BankerId
#                 ,null as BillingOptionCode
#                 ,null as as BranchUnitId
#                 ,null as CensusTrack
#                 ,null ClosingDate
#                 ,0.00 as CreditLimit
#                 ,null CreditLineExpirationDate 
#                 ,null as CreditMark
#                 ,null CurrentAppraisalDate 
#                 ,null CurrentNoteDate
#                 ,null as CustomerType
#                 ,0 DaysPastDue
#                 ,null as ExceptionCRPolicy
#                 ,null ExtensionDate
#                 ,Financial_AccountNumber_skey FinancialAccountNumber
#                 ,null FinancialCostCenter
#                 ,null as FloodCertificationNumber
#                 ,0.00 FloodCoverageAmount
#                 ,null as FloodZone                      
#                 ,null FyeFinancialStatementDate
#                 ,null as GLType
#                 ,null as GuarantorFS
#                 ,null as HMDACRASmallBusiness
#                 ,0.00 InsurancePaidYearToDate
#                 ,0.00 InterestPaidYearToDate
#                 ,0.0000 InterestRate
#                 ,null as InterestType
#                 ,null as MasterAccountNumber 
#                 ,null as MasterTrancheFlag
#                 ,null MaturityDate
#                 ,null ModificationDate
#                 ,null as MSACode                    
#                 ,null NAICSCode
#                 ,null as Name
#                 ,null as NotforProfit
#                 ,null as OccupancyCode
#                 ,null as Officer2
#                 ,null as Officer3
#                 ,null OpeningDate
#                 ,0.00 OriginalAppraisalAmount
#                 ,null as PaymentDueDate
#                 ,0.00 PrincipalAmount
#                 ,0.00 PrincipalPaidYearToDate
#                 ,null as ProductId
#                 ,0.00 as PropertyTaxPaidYearToDate
#                 ,null as PurchasedCode
#                 ,null as RegOCode
#                 ,null as RegOType
#                 ,NULL RenewalDate
#                 ,NULL RiskRating
#                 ,null as SharedNationalCR
#                 ,null as as SourceSystem
#                 ,"OPEN" Status
#                 ,null as TDR
#                 ,0 as Term
#                 ,0.00 TotalOutstandingAmount
#                 ,null as TreasuryLoanGuaranty
#                 ,null as TypeCode
#                 ,null as LQFlag 
#                 ,"C" AppType
#                 ,"Composite" ApplicationDescription
#                 from vw_xaa_composite
#             """)
# df.createOrReplaceTempView("vw_composite")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select FinancialAccountNumber
# MAGIC ,count(*)  from vw_global_plus
# MAGIC group by FinancialAccountNumber
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge 

# COMMAND ----------

try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery = """
                select tab.FinancialAccountNumber
                ,cast(tab.AccountNumber as string)  as AccountNumber
                ,tab.FinancialCostCenter
                ,tab.OpeningDate
                ,tab.PaymentDueDate
                ,tab.BankerId
                ,tab.ABEmployeeId1
                ,tab.Officer2
                ,tab.ABEmployeeId2
                ,tab.Officer3
                ,tab.ABEmployeeId3
                ,tab.AmountDue
                ,tab.AmountPastDue
                ,0.00 AnnualInterest 
                ,0.00 AnnualPIAmount 
                ,null as BillingAccount 
                ,tab.BillingOptionCode 
                ,null as BillingOptionDescription                 
                ,tab.BranchUnitId
                ,tab.CustomerType      
                ,tab.CensusTrack
                ,null as City
                ,tab.ClosingDate
                ,NULL CompletedApplicationDate 
                ,null as CountyName 
                ,0.00 CountyTaxAmountMonthlyEscrowAmount    
                ,null as CRACompliance 
                ,NULL CreditDecisionDate 
                ,tab.CreditLineExpirationDate 
                ,tab.CreditMark   
                ,null as CRR 
                ,null as CurrentDebtService 
                ,0.00 CurrentIncome 
                ,0.00 CurrentLTV 
                ,NULL CurrentMaturityDate 
                ,tab.CurrentNoteDate 
                ,NULL CurrentResidence                 
                ,tab.DaysPastDue
                ,0 DaysPastMaturity
                ,0.0000 DebttoIncomeRatios
                ,null as EmployeeCode
                ,null as Employment
                ,null as EmploymentStatus
                ,tab.ExceptionCRPolicy
                ,null as ExistingLender
                ,tab.ExtensionDate
                ,0 as FICO
                ,tab.FloodCertificationNumber
                ,tab.FloodCoverageAmount
                ,null FloodInsuranceExpirationDate
                ,tab.FloodZone
                ,null as FloodZoneCode
                ,tab.FyeFinancialStatementDate
                ,tab.GLType
                ,tab.GuarantorFS
                ,0.00 HazardPremiumMonthlyEscrowAmount
                ,tab.HMDACRASmallBusiness
                ,null as ImpactSector
                ,0.00 InsuranceAmounts
                ,tab.InsurancePaidYearToDate 
                ,0.0000 InterestIndex                
                ,0.00 InterestPaidTo
                ,tab.InterestPaidYearToDate 
                ,tab.InterestRate
                ,tab.InterestType
                ,null LastPaymentDate 
                ,null LastReviewDate 
                ,null as LoanAmortizationTerm 
                ,null as LoanProceeds 
                ,null as LoanUtilization 
                ,0.00 LTV 
                ,0.0000 Margin 
                ,tab.MasterAccountNumber 
                ,tab.MasterTrancheFlag                   
                ,tab.MaturityDate
                ,tab.ModificationDate 
                ,0.00 MonthlyPIAmount
                ,0.00 MonthlyPayments
                ,0.00 MortgageInsurancePremiumFinancedAmount
                ,0.00 MortgageInsurancePremiumMonthlyEscrowAmount
                ,0.00 MortgageInsuranceUpFrontPremium
                ,tab.MSACode
                ,tab.NAICSCode              
                ,tab.Name
                ,null NextReviewDate
                ,tab.NotforProfit
                ,tab.OccupancyCode
                ,tab.OriginalAppraisalAmount
                ,0.00 OriginalLTV
                ,null as Ownership
                ,null as OwnershipShare
                ,null as PastDueStatus
                ,0.00 PMICoverageAmounts
                ,null as PMIErrors
                ,0.0000 PMIRate
                ,null as PoolPMIPayeeCode
                ,null as PoolPMIPolicyNumber              
                ,tab.PrincipalPaidYearToDate
                ,null as PrivateMortgageInsuranceActivitiesandStatus
                ,0.00 PrivateMortgageInsuranceCoverageAmounts
                ,NULL PrivateMortgageInsuranceExpirationDates
                ,tab.ProductId
                ,tab.PropertyTaxPaidYearToDate 
                ,null as PropertyType 
                ,tab.PurchasedCode 
                ,0.0000 RateIndex 
                ,null as RateType 
                ,tab.RegOCode 
                ,tab.RegOType 
                ,null as RelatedFinancialAccount 
                ,null ReleasedDate 
                ,null as RemainingDuration                 
                ,tab.RenewalDate
                ,0.00 ReportedAssetValue
                ,0.00 ReportedDebtAmount
                ,0.00 RevenueAmount 
                ,tab.RiskRating  
                ,null as Segment 
                ,tab.SharedNationalCR                 
                ,tab.SourceSystem
                ,tab.Status
                ,null as SyndicatedOrPurchase
                ,0.00 TaxesAmount
                ,tab.TDR
                ,tab.Term
                ,tab.TreasuryLoanGuaranty              
                ,tab.TypeCode 
                ,null as Underwriter 
                ,null as USStateCode 
                ,null as VestingInformation 
                ,0.00 MinimumRequiredBalance
                ,null as MultifamilyPropertyRentStabilizedType
                ,null as ProjectFinance
                ,null as ParticipationsSyndications
                ,null as CDFI 
                ,0.0000 ListRate 
                ,0.0000 ExceptionRate 
                ,0.00 InterestEarned  
                ,null as RepaymentType 
                ,tab.CurrentAppraisalDate 
                ,tab.LQFlag 
                ,tab.AppType
                ,tab.ApplicationDescription
                , cast(NULL as decimal(19,6)) CityTaxAmountMonthlyEscrowAmount
                , null as MultifamilyPropertyRentStabilized
                , null as CurrentYearRiskRatingTripped
                ,  cast(NULL as DATE) as GuarantorFSDate
                , null as AgentBank
                , null as CIProductCode
                , cast(NULL as decimal(25,6)) as MarginSpread
                , null TypeofParticipation
                , cast(NULL as decimal(15,2)) as AccruedInterest
                , cast(NULL as decimal(10,2)) as DebttoIncomeRatio
                , cast(NULL as DATE) as BorrowerFSDate
                , null as BorrowingBaseLoan
                , cast(NULL as DATE) as BorrowingBaseDate
                , cast(NULL as decimal(10,2)) as CurrentAppraisalAmount
                , null as GlobalIndexKey
                from 
                (
                --horizon source
               select * from vw_horizon

              union all

            select * from vw_osaic

                union all

                --dmi heloc

                select * from vw_dmi_heloc

                union all                  

                --dmi resi

                select * from vw_dmi_resi

                union all

                --global plus

                select * from vw_global_plus                  
               
                --union all
                --select * from vw_composite
                )tab
          """

    df_final_FA = spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, COunt(1) from vw_final_FA
# MAGIC Group by all having count(1)>1

# COMMAND ----------

DestinationSchema = dbutils.widgets.get('DestinationSchema')
DestinationTable = dbutils.widgets.get('DestinationTable')
AddOnType=dbutils.widgets.get("AddOnType")
print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns #get all the base columns
set_addon=df_final_FA.columns #get only the addon columns
get_pk=spark.sql(f"""select * from config.metadata where lower(DWHTableName)='financial_account' and lower(DWHSchemaName) = 'silver'""").collect()[0]['MergeKey']
set_addon.remove(get_pk) #remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date','MergeHashKey','CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col.lower() not in [ex_col.lower() for ex_col in excluded_columns]]

# COMMAND ----------

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
base_without_pk=filtered_basetable_columns.copy()
base_without_pk.remove(get_pk)
Mergehashkey_columns = list(set(set_addon + filtered_basetable_columns))
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
# MAGIC select FinancialAccountNumber, count(1) from vw_silver group by FinancialAccountNumber having count(1) > 1

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
                    CASE WHEN s.{get_pk} IS NULL THEN 'No' ELSE 'Yes' END AS CurrentRecordTmp
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
        ON target.{get_pk} = source.{get_pk} 
        AND source.CurrentRecordTmp = 'No'
        WHEN MATCHED THEN
            UPDATE SET target.CurrentRecord = 'Deleted', target.end_date=current_timestamp(), target.DW_modified_Date=current_timestamp(),target.DW_Modified_By='Databricks'
    """

spark.sql(MergeQuery)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, Count(1) from default.financial_account_holder
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by FinancialAccountNumber
# MAGIC Having Count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.financial_account_holder
