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
# MAGIC #### Calling config notebook

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Catalog

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table default.customer_master_holder

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
    (col('TableID') == 1020 )
)

display(DFMetadata)

# COMMAND ----------

TableID=1020
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
# MAGIC ### Create base view

# COMMAND ----------

# DBTITLE 1,Base view
df_base=spark.sql("select SF_Cust_Key from silver.Customer_Master group by 1")
df_base.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# DBTITLE 1,Initialize_Customer_Xref in the default catalog  - This is the working copy
# MAGIC %sql
# MAGIC --Initialize Customer XREF Default Table -- This creates a working copy of the silver table which becomes the base table for merging
# MAGIC --create or replace table default.customer_xref as select * from bronze.customer_master;
# MAGIC truncate table default.Customer_Xref;
# MAGIC --select * from default.customer_xref
# MAGIC -- select * from bronze.Customer_master;
# MAGIC -- truncate table bronze.Customer_master;

# COMMAND ----------

# DBTITLE 1,Get all AOTM Customers in Horizon
df = spark.sql('''
        select tab.Company_ID
        ,tab.cust_skey
        ,tab.tax_id
        ,tab.rownum
        from 
        (
            select beb.company_id 
            ,xref.cust_skey
            ,idm.oid Tax_ID
            ,row_number() over (partition by beb.company_id,xref.cust_skey order by xref.cust_skey asc) as rownum
            from bronze.v_ods_beb_customer beb 
            left join 
            (
                select bca.company_id
                ,CASE  when bca.Account_Type = 'Checking' then concat('13-DD-', LPAD(bca.Account_Number,20 ,'0'))
                        when bca.Account_Type = 'Savings' then concat('13-SV-', LPAD(bca.Account_Number,20 ,'0'))
                        when bca.Account_Type = 'Loan' then concat('13-LN-', LPAD(bca.Account_Number,20 ,'0'))
                        when bca.Account_Type = 'MortgageLoan' then concat('13-ML-',LPAD(bca.Account_Number,20 ,'0'))
                        when bca.Account_Type = 'CertificateOfDeposit' then concat('13-CD-',LPAD(bca.Account_Number,20 ,'0')) end AS ACCT_SKEY
                from bronze.`v_ods_beb_customer-account` bca
                where bca.CurrentRecord = 'Yes'
                and bca.account_type != "Investment" 
                union all
                select bca.company_id
                ,xref.acct_skey AS ACCT_SKEY
                from bronze.`v_ods_beb_customer-account` bca
                inner join bronze.ods_rmxref xref on LPAD(bca.Account_Number,20 ,'0') = xref.rxacct and xref.CurrentRecord = "Yes"
                where bca.CurrentRecord = 'Yes'
                and bca.account_type = "Investment" 
            ) bca on bca.company_id = beb.company_id
            left join bronze.ods_rmxref xref on bca.acct_skey = xref.ACCT_SKEY and xref.RXPRIM = 'Y'
            LEFT JOIN
            (
                select tab.OID
                ,tab.GID
                ,mast.skey
                from
                (
                    select OID
                    ,GID
                    ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
                    from silver.customer_idmap idm where CurrentRecord = "Yes"
                )tab
                left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
                where tab.rownum = 1  
            ) idm ON xref.CUST_SKEY = idm.skey
            --where beb.Company_ID = '1174919'
        )tab where tab.rownum = 1
        and tab.cust_skey is not null and tab.tax_id is not null
        union 
        select tab.Company_ID
        ,tab.cust_skey
        ,tab.Tax_ID
        ,tab.rownum
        from
        (
            select beb.Company_ID
            ,xref.cust_skey
            ,idm.oid Tax_ID
            ,concat_ws ('#',idm.GID, beb.company_ID) sf_cust_key
            ,ROW_NUMBER() OVER (PARTITION BY xref.cust_skey, idm.oid order by xref.cust_skey desc) AS rownum
            from bronze.ods_rmxref xref
            LEFT JOIN
            (
                select tab.OID
                ,tab.GID
                ,mast.skey
                from
                (
                    select OID
                    ,GID
                    ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
                    from silver.customer_idmap idm where CurrentRecord = "Yes"
                )tab
                left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
                where tab.rownum = 1  
            ) idm ON xref.CUST_SKEY = idm.skey
            inner join 
            (
                select beb.Company_ID
                ,regexp_replace(beb.Tax_ID, '[^0-9]', '') Tax_ID 
                from bronze.v_ods_beb_customer beb
                where beb.CurrentRecord = "Yes"
            ) beb on idm.oid = beb.Tax_ID
            where xref.CurrentRecord = "Yes"
        ) tab where tab.rownum = 1  
        --and tab.company_id = '1174919'      
        ''' )
df.createOrReplaceTempView("vw_cust_skey")   
df.display()


# COMMAND ----------

# DBTITLE 1,Dup TaxID per Cust_skey
df = spark.sql('''
        select tab.company_id
        ,tab.cust_skey
        ,tab.tax_id
        ,tab.rownum
        from 
        (
        select company_id
        ,cust_skey
        ,tax_id
        ,ROW_NUMBER() OVER (PARTITION BY company_id, tax_id order by cust_skey asc) AS rownum
        from vw_cust_skey
        ) tab
''' )
df.createOrReplaceTempView("vw_cust_skey_SSN")   
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_cust_skey_SSN where tax_id='133208565' and company_id='2122164'

# COMMAND ----------

# MAGIC %sql
# MAGIC select tax_id,company_id,count(1) from vw_cust_skey_SSN group by 1,2 having count(1)>1

# COMMAND ----------

# DBTITLE 1,Customer Key List by Company ID
df = spark.sql('''
select main.company_id
,cust_skey1.cust_skey
,cust_skey2.cust_skey cust_skey_2
,cust_skey3.cust_skey cust_skey_3
,cust_skey4.cust_skey cust_skey_4
,cust_skey5.cust_skey cust_skey_5
,cust_skey6.cust_skey cust_skey_6
from vw_cust_skey_SSN main
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 1 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 1
) cust_skey1 on main.company_id = cust_skey1.company_id and main.tax_id = cust_skey1.tax_id
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 2 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 2
) cust_skey2 on main.company_id = cust_skey2.company_id and main.tax_id = cust_skey2.tax_id
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 3 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 3
) cust_skey3 on main.company_id = cust_skey3.company_id and main.tax_id = cust_skey3.tax_id
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 4 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 4
) cust_skey4 on main.company_id = cust_skey4.company_id and main.tax_id = cust_skey4.tax_id
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 5 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 5
) cust_skey5 on main.company_id = cust_skey5.company_id and main.tax_id = cust_skey5.tax_id
left join (
        select company_id
        ,tax_id
        ,(case when rownum = 6 then cust_skey end) cust_skey
        from vw_cust_skey_SSN
        where rownum = 6
) cust_skey6 on main.company_id = cust_skey6.company_id and main.tax_id = cust_skey6.tax_id
group by 1,2,3,4,5,6,7
''' )
df.createOrReplaceTempView("vw_cust_skey_list")   
df.display()

# COMMAND ----------

# DBTITLE 1,Identify the company in AOTM
# A company is the focus of this code and is defined as a unique record in AOTM
# The AOTM relationship for a person will be primary 

df_src_AOTM = spark.sql('''
        Select tab.GID
        ,tab.Company_ID
        ,tab.childtaxid Tax_ID
        ,tab.cust_skey
        ,tab.cust_skey_2
        ,tab.cust_skey_3
        ,tab.cust_skey_4
        ,tab.cust_skey_5
        ,tab.cust_skey_6 
        ,tab.parentGID
        ,concat_ws('#',tab.GID,tab.Company_ID) SF_Cust_Key
        ,concat_ws('#',tab.ParentGID,tab.Company_ID) ParentID
        from (
            select beb.company_id
            ,bebidm.skey
            ,bebidm.GID ParentGID
            ,beb.Company_Name
            ,xref.cust_skey
            ,xref.cust_skey_2
            ,xref.cust_skey_3
            ,xref.cust_skey_4
            ,xref.cust_skey_5
            ,xref.cust_skey_6
            ,row_number() over (partition by beb.company_id, xref.cust_skey order by beb.company_id) as rownum
            ,idm.gid 
            ,idm.oid childtaxid
            ,rmmast.RMNPN1 AS CompanyFullName_Horizon
            ,regexp_replace(beb.Tax_ID, '[^0-9]', '') as BEB_Tax_ID
            ,concat_ws('#',idm.GID,beb.Company_ID) sf_cust_key
            from bronze.v_ods_beb_customer beb
            left join bronze.`v_ods_beb_customer-account` bca
            on beb.company_id = bca.company_id and bca.CurrentRecord = "Yes"
            left join vw_cust_skey_list xref on beb.company_id = xref.company_id 
            left join bronze.ods_rmmast rmmast
            on xref.CUST_SKEY = rmmast.CUST_SKEY
            LEFT JOIN
            (
                select tab.OID
                ,tab.GID
                ,mast.skey
                from
                (
                    select OID
                    ,GID
                    ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
                    from silver.customer_idmap idm where CurrentRecord = "Yes"
                )tab
                left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
                where tab.rownum = 1  
            ) bebidm ON regexp_replace(beb.Tax_ID, '[^0-9]', '') = bebidm.oid                   
            LEFT JOIN
            (
                select tab.OID
                ,tab.GID
                ,mast.skey
                from
                (
                    select OID
                    ,GID
                    ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
                    from silver.customer_idmap idm where CurrentRecord = "Yes"
                )tab
                left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
                where tab.rownum = 1  
            ) idm ON xref.CUST_SKEY = idm.skey
            where beb.CurrentRecord = 'Yes'
            and bca.currentrecord = 'Yes'
            and rmmast.currentrecord = 'Yes'
            --and beb.Company_id = '1174919'
         )tab where tab.rownum = 1 and tab.GID is not null
        ''' )
df_src_AOTM.createOrReplaceTempView("vw_src_AOTM")  

# COMMAND ----------

# DBTITLE 1,Insert Records to XREF from AOTM
df = spark.sql('''
              MERGE INTO  default.Customer_Xref  AS Target
              USING (SELECT * FROM vw_src_AOTM) AS Source
              ON Source.SF_Cust_key = Target.SF_Cust_Key
              --DNS Added Tax_ID
              WHEN NOT MATCHED
              THEN INSERT
              (company_id, cust_skey, cust_skey_2, cust_skey_3, cust_skey_4, cust_skey_5, cust_skey_6, gid, sf_cust_key, source, Tax_ID)
              VALUES
              (Source.company_id, Source.cust_skey, Source.cust_skey_2, Source.cust_skey_3, Source.cust_skey_4, Source.cust_skey_5, Source.cust_skey_6, Source.GID, SOURCE.sf_cust_key, '01_HZN_AOTM',source.Tax_ID) 
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - Test for Dups
# MAGIC %sql
# MAGIC --#validation test for dups
# MAGIC select SF_Cust_Key
# MAGIC ,count(1) 
# MAGIC from default.Customer_Xref
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Merge Result
# MAGIC %sql
# MAGIC select source
# MAGIC ,count(1) 
# MAGIC from default.Customer_Xref
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #Global Plus

# COMMAND ----------

# DBTITLE 1,Preparing to Merge GP Company with SF Account
# Create a lookup using the Taxid from the account table - connect to rmmast_ssn to get the cust_skey
# Use Cust key to get GID froom the bronze.aotm_hznXref table 
# if no match on company id use the acct_number from the account table for the merge step(s)

# Unique Taxid's from account table
acct = spark.sql('''SELECT * FROM (
                 SELECT TAX_ID, idm.GID ,'GP' as Source, idm.GID as SF_Cust_key
                    ,ROW_NUMBER() OVER (PARTITION BY TAXID order by TAXID desc) AS rownum  FROM 
                        (select TAXID, REGEXP_REPLACE(TAXID, '[^0-9]', '') as Tax_ID
                            from bronze.account 
                          WHERE TaxID is not null 
                            AND TAXID NOT IN ('99-9999999') 
                            AND acct_number BETWEEN '1000003' AND '7000001'
                            and CurrentRecord = "Yes"
                            group by 1
                        ) acct
                    left join silver.customer_idmap idm 
                    ON acct.Tax_ID = idm.OID
                    where CurrentRecord = "Yes")a
                    where rownum = 1
                ''');
acct.createOrReplaceTempView("vw_acct")

gp_exists_in_Hzn = spark.sql('''
                             select * from (
                             select hzn.gid, hzn.company_id, hzn.cust_skey
                             ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
                    from default.customer_xref hzn
                    inner join vw_acct acct 
                    on hzn.gid = acct.gid
                    where hzn.cust_skey is not null)
                    where rownum = 1

                     ''');

gp_exists_in_Hzn.createOrReplaceTempView("vw_gp_exists_in_Hzn")  # 155 matches on 4/23/2025                     

gp_not_exist_in_Hzn = spark.sql('''
                                SELECT * FROM (
                                SELECT TAX_ID, idm.GID, idm.GID as SF_Cust_key
                                ,ROW_NUMBER() OVER (PARTITION BY GID order by GID desc) AS rownum  FROM 
                        (select TAXID, REGEXP_REPLACE(TAXID, '[^0-9]', '') as Tax_ID
                            from bronze.account 
                          WHERE TaxID is not null 
                            AND TAXID NOT IN ('99-9999999') 
                            AND acct_number BETWEEN '1000003' AND '7000001'
                            and CurrentRecord = "Yes"
                            group by 1
                        ) acct
                    left join silver.customer_idmap idm 
                    ON acct.Tax_ID = idm.OID
                     where CurrentRecord = "Yes" and idm.GID not in (select distinct gid from vw_gp_exists_in_Hzn) )
                     where rownum = 1
                     ''')

gp_not_exist_in_Hzn.createOrReplaceTempView("vw_gp_not_exist_in_Hzn")  
###   154  matches on 4/23/2025, 1526 accounts in GP not in Horizon 



######################    Checks to ensure no duplicates ########################

distinct_gid_count = spark.sql("SELECT COUNT(DISTINCT GID) FROM vw_acct")
ct_gp_not_exist_in_Hzn = spark.sql("SELECT COUNT(DISTINCT GID) FROM vw_gp_not_exist_in_Hzn")
ct_gp_exists_in_Hzn = spark.sql("SELECT COUNT(DISTINCT GID) FROM vw_gp_exists_in_Hzn")

gid_count = spark.sql("SELECT COUNT( GID) FROM vw_acct")
tot_ct_gp_not_exist_in_Hzn = spark.sql("SELECT COUNT( GID) FROM vw_gp_not_exist_in_Hzn")
tot_ct_gp_exists_in_Hzn = spark.sql("SELECT COUNT( GID) FROM vw_gp_exists_in_Hzn")


print(f"Total distinct accounts {distinct_gid_count.collect()[0][0]}")
print(f"Total distinct accounts in GP not in Hzn {ct_gp_not_exist_in_Hzn.collect()[0][0]}")
print(f"Total distinct accounts in GP and HZN {ct_gp_exists_in_Hzn.collect()[0][0]}")

print(f"Total accounts {gid_count.collect()[0][0]}")
print(f"Total account in GP not in Hzn {tot_ct_gp_not_exist_in_Hzn.collect()[0][0]}")
print(f"Total accounts in GP and HZN {tot_ct_gp_exists_in_Hzn.collect()[0][0]}")



# COMMAND ----------

# DBTITLE 1,Insert Unmatched GP Records into default.customer_xref
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (select a.company_id, a.cust_skey, a.GID, a.SF_Cust_key, a.Source, a.Tax_ID from 
                        (SELECT '' as company_id
                            ,'' as cust_skey 
                            , GID
                            , Tax_ID
                            , SF_Cust_key
                            ,'03_Global_Plus' as Source 
                            ,ROW_NUMBER() OVER (PARTITION BY GID order by GID desc) AS rownum
                            from vw_gp_not_exist_in_Hzn
                    ) a
                    --where rownum = 1
                    ) AS Source
                ON Source.GID = Target.GID
              --DNS Added Tax_ID
              WHEN NOT MATCHED THEN INSERT
              (company_id, cust_skey, gid, sf_cust_key, Source ,Tax_ID) VALUES
              (Source.company_id, Source.cust_skey, Source.GID, Source.SF_Cust_key, Source.Source ,source.Tax_ID)
             ''')



# COMMAND ----------

# DBTITLE 1,Dup Check
# MAGIC %sql
# MAGIC select SF_Cust_Key,COUNT(*)
# MAGIC from default.customer_xref
# MAGIC GROUP BY 1
# MAGIC HAVING count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - Confirm Records Added as expected
# MAGIC %sql
# MAGIC select source, count(*) from default.customer_xref group by 1 
# MAGIC order by Source
# MAGIC --select * from default.customer_xref group by 1 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Small Business

# COMMAND ----------

# DBTITLE 1,Preparing to Merge Small Business  - No Personal accounts are added
##  Modify query so that identifies the customers who do not have aotm 
## Insert those records bronze.GP_AOTM_Bus_RMMAST using Cell 25 as an example 
## For those customers with no aotm the company id is blank - use GID as the match key, bring cust_skey just in case but no match 
## Validate that the data is correct - check for dups 
## Modify the same query to identify customers with aotm - use Gid,populate Company ID, cust_skey (Needed for more than one business sharing the same taxid)  
# One of our checks will be to compare the contents of rm mast with the final customer master table

# 4183 distinct Accounts for SB
# Unique Taxid's from account table
SB_Account = spark.sql('''
select tab.company_id
,tab.CUST_SKEY
,tab.GID
--DNS Added OID
,tab.OID as Tax_ID
,case when tab.company_id is not null then "04_SB_With_AOTM"
else "04_SB_No_AOTM" end Source
,case when tab.company_id is not null then concat_ws('#',tab.GID,tab.Company_ID)
else tab.GID end as SF_Cust_key
from
(
    Select beb.company_id
    ,xref.CUST_SKEY 
    ,idm.GID
    --DNS Added OID
    ,idm.OID
    ,row_number() OVER (PARTITION BY concat_ws('#',idm.GID) order by beb.Company_ID desc) AS rownum
    from bronze.v_trend_summary_da tr
    left join bronze.fi_Core_Product prod
    on tr.ProductLKey = prod.Product_Key AND PROD.CurrentRecord = 'Yes'
    left join bronze.fi_Core_Status stat
    on tr.StatusLKey = stat.Code_Key AND STAT.CurrentRecord = 'Yes'
    --DNS Remove the RXPRIM filter and add the RELCODE = 'A'
    inner join bronze.ods_rmxref xref on tr.AccountLKey = xref.ACCT_SKEY 
    and xref.CurrentRecord = 'Yes' and xref.REL_CODE = 'A'
    left join
    (
      select tab.OID
      ,tab.GID
      ,mast.skey
      from
      (
        select OID
        ,GID
        ,ROW_NUMBER() OVER (PARTITION BY OID order by OID desc) AS rownum
        from silver.customer_idmap idm where CurrentRecord = "Yes"
      )tab
      left join bronze.rmmast_ssn mast on tab.OID = mast.ssn
      where tab.rownum = 1  
    )idm ON idm.skey = xref.CUST_SKEY
    left join
    (
      select beb.company_id
      ,beb.Tax_ID
      ,CASE  when bca.Account_Type = 'Checking' then concat('13-DD-', LPAD(bca.Account_Number,20 ,'0'))
            when bca.Account_Type = 'Savings' then concat('13-SV-', LPAD(bca.Account_Number,20 ,'0'))
            when bca.Account_Type = 'Loan' then concat('13-LN-', LPAD(bca.Account_Number,20 ,'0'))
            when bca.Account_Type = 'MortgageLoan' then concat('13-ML-',LPAD(bca.Account_Number,20 ,'0'))
            when bca.Account_Type = 'CertificateOfDeposit' then concat('13-CD-',LPAD(bca.Account_Number,20 ,'0')) end AS `ACCT_SKEY`
      from bronze.`v_ods_beb_customer-account` bca
      inner join
      (
        select beb.company_id
        ,beb.Tax_ID
        from
        (
          select beb.company_id
          ,REGEXP_REPLACE(beb.Tax_ID, '[^0-9]', '') Tax_ID
          --DNS Fix to use the Formatted EIN on the rownum
          ,ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(beb.Tax_ID, '[^0-9]', '') order by Company_Open_Date desc) as rownum
          from bronze.v_ods_beb_customer beb
          where beb.CurrentRecord = "Yes"
          and beb.Tax_ID is not null 
          and beb.Tax_ID <> ''
          and beb.Tax_ID <> ' '
        )beb
        where beb.rownum = 1  
      )beb on beb.company_id = bca.company_id
      where bca.CurrentRecord = 'Yes'
      --DNS change the join to tax_id instead of accountkey
    )beb on idm.OID = beb.tax_id
    WHERE tr.datekey in (select max(datekey) from bronze.v_Trend_Summary_DA where CurrentRecord = "Yes")
    and tr.CurrentRecord = "Yes"
    and tr.ProductLKey IN (
        '13-DD-021',
        '13-DD-022',
        '13-DD-020',
        '13-LN-124',
        '13-LN-125',
        '13-DD-058',
        '13-SV-071',
        '13-LN-300',
        '13-LN-301',
        '13-CD-054',
        '13-CD-056',
        '13-TD-049',
        '13-TD-057',
        '13-CD-058',
        '13-CD-053',
        '13-CD-055',
        '13-DD-023',
        '13-LN-216',
        '13-LN-128'
    )
) tab
where tab.rownum = 1 and tab.gid is not null
''')

SB_Account.createOrReplaceTempView("vw_SB_Account") 

SB_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.customer_xref hzn
    inner join vw_SB_Account acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
--where rownum = 1
''');
SB_exists_in_Hzn.createOrReplaceTempView("vw_SB_exists_in_Hzn")

SB_not_exist_in_Hzn = spark.sql('''
select company_id
,CUST_SKEY
,GID
,Tax_ID
,Source
,SF_Cust_key
from vw_SB_Account
where gid not in (select distinct gid from vw_SB_exists_in_Hzn) 
''')
SB_not_exist_in_Hzn.createOrReplaceTempView("vw_SB_not_exist_in_Hzn")




# COMMAND ----------

# DBTITLE 1,Merge Small Business Accounts
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (SELECT * FROM vw_SB_not_exist_in_Hzn) AS Source
              ON Source.SF_Cust_key = Target.SF_Cust_Key
            
            --DNS Added Tax_ID
              WHEN NOT MATCHED 
              THEN INSERT
              (company_id, cust_skey, gid, sf_cust_key, source ,tax_id) VALUES
              (Source.company_id, Source.cust_skey, Source.GID, Source.sf_cust_key, Source.source, Source.tax_id)

             ''')


# COMMAND ----------

# DBTITLE 1,Debug - dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key--, source
# MAGIC ,count(1) 
# MAGIC from default.customer_xref
# MAGIC group by 1
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Confirm Records inserted as expected
# MAGIC %sql
# MAGIC select source, count(*) from default.customer_xref group by 1 order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #Personal Customers

# COMMAND ----------

# DBTITLE 1,Personal Customers
# Personal Customers who own Commercial Accounts - These are accounted for already by the business account. If they own a personal account separate from the business acocunt we will have a record # for the business and a record for the primary person on the account. 
    # and also is using aotm for both commercial and small business accounts.  
# Personal CUstomers who own small Business Accounts 
# Personal Customers in Oasaic and DMI

# So any entity on the Horizon RM_XREF Table who has RMPRIM = 'Y' will be added to the customer table when they hae not already been added by unique GID (when not matched)
# Removed rxprim = Y logic to bring in all contact types

df_src_personal = spark.sql('''
        Select tab.GID
        ,tab.Tax_ID
        ,tab.cust_skey
        ,tab.GID SF_Cust_Key
        ,"05_Horizon_Personal" Source
        from (
          select 
          idm.GID
          ,idm.oid as Tax_ID
          ,idm.skey as cust_skey
          -----DNS Added idm.cust_skey on the partition to be able to get all the related customers on the GID
          ,ROW_NUMBER() OVER (PARTITION BY idm.oid ,idm.skey order by idm.oid desc) as rownum
          ,currentRecord
          from bronze.ods_rmxref  xref
          LEFT JOIN 
          (
            select tab.OID
            ,tab.GID
            ,mast.skey
            from 
            (
              select OID
              ,GID
              ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
              from silver.customer_idmap idm where CurrentRecord = "Yes"
            )tab
            left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
            where tab.rownum = 1  
          ) idm ON xref.cust_skey = idm.skey
          where idm.oid NOT IN ('999999999','88888888888','99999999','9999999999','55555555','555555555','123456789')
          -- and xref.RXPRIM = 'Y'
          and xref.currentRecord = 'Yes'
        )tab
        where tab.rownum = 1 and tab.GID is not null 
        and tab.currentrecord = 'Yes'
        ''' )
df_src_personal.createOrReplaceTempView("vw_src_Type_P")   

Personal_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.customer_xref hzn
    inner join vw_src_Type_P acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
--where rownum = 1
''');
Personal_exists_in_Hzn.createOrReplaceTempView("vw_personal_exists_in_Hzn")

Personal_not_exist_in_Hzn = spark.sql('''
select "" Company_Id
,cust_skey
,gid
,Tax_ID
,SF_Cust_Key
from vw_src_Type_P
where gid not in (select distinct gid from vw_personal_exists_in_Hzn) 
''')
Personal_not_exist_in_Hzn.createOrReplaceTempView("vw_personal_not_exist_in_Hzn")


# COMMAND ----------

# DBTITLE 1,Identify all the customer on GID with multiple cust_skey
# Using the source "vw_src_Type_P" code snippets below identifies the duplicate GIDs with different customer key
# We will create a new SF_Cust_Key_RMCTYP using GID, CUST_SKEY and RMCTYP. 

CustomerDupByGID = spark.sql('''
select "" Company_Id
,final.cust_skey
,final.gid
,final.tax_id
,concat_ws ('#',final.gid,final.cust_skey,final.RMCTYP) sf_cust_key
,"05_Customer_Dup_By_GID" Source
from
(
    select xref.gid
    ,xref.tax_id
    ,xref.cust_skey
    ,mast.RMCTYP
    ,row_number() over(partition by xref.GID, xref.Cust_Skey order by xref.Cust_Skey desc) as rownum
    from vw_personal_not_exist_in_Hzn xref
    inner join 
    (   select tab.gid
        from
        (
            select gid
            from vw_personal_not_exist_in_Hzn 
            group by 1
            having count(*) > 1
        )tab
    )dupGID on dupGID.gid = xref.gid
    left join bronze.ods_rmmast mast on xref.Cust_Skey = mast.CUST_SKEY and mast.CurrentRecord = "Yes"
)final
where final.rownum = 1
''')
CustomerDupByGID.createOrReplaceTempView("vw_CustomerDupByGID")

# COMMAND ----------

# DBTITLE 1,Merge CustomerDupByGID
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (SELECT * FROM vw_CustomerDupByGID) AS Source
                ON Source.sf_cust_key = Target.sf_cust_key

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id, cust_skey, gid, sf_cust_key, source ,tax_id) VALUES
              (Source.company_id, Source.cust_skey, Source.GID, Source.sf_cust_key, Source.source, Source.tax_id)
             ''')

# COMMAND ----------

# DBTITLE 1,Check for dups
# MAGIC %sql
# MAGIC select SF_Cust_Key
# MAGIC ,count(1) 
# MAGIC from default.customer_xref
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Debug Merge
# MAGIC %sql 
# MAGIC SELECT source, count(*) FROM default.customer_xref
# MAGIC group by Source
# MAGIC order by Source

# COMMAND ----------

# DBTITLE 1,Insert the rest of distinct customer
# Insert the rest of the customer on the working directory

DistinctCustomers = spark.sql('''
select GID
,Tax_ID
,cust_skey
,GID SF_Cust_Key
,"06_Horizon_Personal" Source
from vw_personal_not_exist_in_Hzn
where gid not in (select distinct gid from vw_CustomerDupByGID) -- 174028 - 1640 = 172388
''')
DistinctCustomers.createOrReplaceTempView("vw_DistinctCustomers")

# COMMAND ----------

# DBTITLE 1,Merge DistinctCustomers
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (SELECT * FROM vw_DistinctCustomers) AS Source
              ON Source.SF_Cust_key = Target.SF_Cust_Key

            --DNS Added Tax_ID
              WHEN NOT MATCHED 
              THEN INSERT
              (cust_skey, gid, sf_cust_key, source ,tax_id) VALUES
              (Source.cust_skey, Source.GID, Source.sf_cust_key, Source.Source ,source.tax_id)
             ''')

# COMMAND ----------

# DBTITLE 1,check for dup
# MAGIC %sql
# MAGIC select SF_Cust_Key
# MAGIC ,count(1) 
# MAGIC from default.customer_xref
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Debug - Null check
# MAGIC %sql
# MAGIC select * from default.customer_xref
# MAGIC where sf_cust_key is null

# COMMAND ----------

# DBTITLE 1,Debug - Merge
# MAGIC %sql 
# MAGIC SELECT source, count(*) FROM default.customer_xref
# MAGIC group by Source
# MAGIC order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #Osaic

# COMMAND ----------

# DBTITLE 1,Osaic Customers
source_osaic = spark.sql('''
select a.* from (
select idm.GID, idm.GID as sf_cust_key, idm.skey as cust_skey, '07_Osaic_No_Horizon' as Source
--DNS Added OID
      ,idm.OID as Tax_ID
       ,ROW_NUMBER() OVER (PARTITION BY idm.oid order by idm.oid desc) as rownum
 from bronze.account_osaic osaic
LEFT JOIN 
(
  select tab.OID, 
  tab.GID
  ,mast.skey
  from 
  (
    select OID
    ,GID
    ,ROW_NUMBER() OVER (PARTITION BY OID order by OID desc) AS rownum
    from silver.customer_idmap idm where CurrentRecord = "Yes"
  )tab
  left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
  where tab.rownum = 1  
) idm ON REGEXP_REPLACE(SSN_Tax_ID, '[^0-9]', '') = idm.oid
where idm.oid NOT IN ('999999999','88888888888','99999999','9999999999','55555555','555555555','123456789')
and currentRecord = 'Yes'
and idm.oid is not null) a
where a.rownum = 1
''') 
source_osaic.createOrReplaceTempView("vw_src_Type_O")

osaic_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.customer_xref hzn
    inner join vw_src_Type_O acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
--where rownum = 1
''');
osaic_exists_in_Hzn.createOrReplaceTempView("vw_osaic_exists_in_Hzn")

osaic_not_exist_in_Hzn = spark.sql('''
select "" Company_Id
,cust_skey
,gid
,Tax_ID
,SF_Cust_Key
,source
from vw_src_Type_O
where gid not in (select distinct gid from vw_osaic_exists_in_Hzn) 
''')
osaic_not_exist_in_Hzn.createOrReplaceTempView("vw_osaic_not_exist_in_Hzn")

# COMMAND ----------

# DBTITLE 1,Osaic Merge
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (SELECT * FROM vw_osaic_not_exist_in_Hzn) AS Source
              ON Source.SF_Cust_key = Target.SF_Cust_Key

            --DNS Added Tax_ID
              WHEN NOT MATCHED 
              THEN INSERT
              (company_id, cust_skey, gid, sf_cust_key, source ,tax_id) VALUES
              (Source.company_id, Source.cust_skey, Source.GID, Source.sf_cust_key, Source.source, Source.tax_id)
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key--, source
# MAGIC ,count(1) 
# MAGIC from default.customer_xref
# MAGIC group by 1
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Merge results
# MAGIC %sql
# MAGIC select source, count(*) from default.customer_xref group by 1 order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #DMI

# COMMAND ----------

# DBTITLE 1,DMI
source_dmi = spark.sql('''
select a.* from (
select idm.GID, idm.GID as sf_cust_key, idm.skey as cust_skey, idm.ind,'08_DMI_No_Horizon' as Source
--DNS Added OID
       ,idm.OID as Tax_ID
       ,ROW_NUMBER() OVER (PARTITION BY idm.oid order by idm.oid desc) as rownum
 from bronze.dmi_dcif dcif
LEFT JOIN
(
  --Borrower
  select tab.OID,
  tab.GID
  ,mast.skey
  ,"Borr" ind
  from
  (
    select OID
    ,GID
    ,ROW_NUMBER() OVER (PARTITION BY OID order by OID desc) AS rownum
    from silver.customer_idmap idm where CurrentRecord = "Yes"
  )tab
  left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
  where tab.rownum = 1  
) idm ON REGEXP_REPLACE(dcif.Mortgagor_SSN, '[^0-9]', '') = idm.oid
where idm.oid NOT IN ('999999999','88888888888','99999999','9999999999','55555555','555555555','123456789')
and currentRecord = 'Yes'
and idm.oid is not null
and dcif.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcif.CurrentRecord = "Yes"
                and dcif.Investor_ID = "40H"
                and (dcif.Category_Code = "003" or dcif.Category_Code = "002")
union all
select idm.GID, idm.GID as sf_cust_key, idm.skey as cust_skey, idm.ind,'08_DMI_No_Horizon' as Source
--DNS Added OID
       ,idm.OID as Tax_ID
       ,ROW_NUMBER() OVER (PARTITION BY idm.oid order by idm.oid desc) as rownum
 from bronze.dmi_dcif dcif
LEFT JOIN
(
  --Co-Borrower
  select tab.OID,
  tab.GID
  ,mast.skey
  ,"Co-Borr" ind
  from
  (
    select OID
    ,GID
    ,ROW_NUMBER() OVER (PARTITION BY OID order by OID desc) AS rownum
    from silver.customer_idmap idm where CurrentRecord = "Yes"
  )tab
  left join bronze.rmmast_ssn mast on tab.OID = mast.SSN
  where tab.rownum = 1  
) idm ON REGEXP_REPLACE(dcif.Co_Mortgagor_SSN, '[^0-9]', '') = idm.oid
where idm.oid NOT IN ('999999999','88888888888','99999999','9999999999','55555555','555555555','123456789')
and currentRecord = 'Yes'
and idm.oid is not null
and dcif.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcif.CurrentRecord = "Yes"
                and dcif.Investor_ID = "40H"
                and (dcif.Category_Code = "003" or dcif.Category_Code = "002")
) a
where a.rownum = 1

 ''') 
source_dmi.createOrReplaceTempView("vw_src_Type_dmi")

dmi_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.customer_xref hzn
    inner join vw_src_Type_dmi acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
--where rownum = 1
''');
dmi_exists_in_Hzn.createOrReplaceTempView("vw_dmi_exists_in_Hzn")

dmi_not_exist_in_Hzn = spark.sql('''
select null Company_Id
,cust_skey
,gid
,Tax_ID
,SF_Cust_Key
,source
from vw_src_Type_dmi
where gid not in (select distinct gid from vw_dmi_exists_in_Hzn) 
''')
dmi_not_exist_in_Hzn.createOrReplaceTempView("vw_dmi_not_exist_in_Hzn")


# COMMAND ----------

# DBTITLE 1,DMI Merge
df = spark.sql('''
              MERGE INTO  default.customer_xref AS Target
              USING (SELECT * FROM vw_dmi_not_exist_in_Hzn) AS Source
              ON Source.SF_Cust_key = Target.SF_Cust_Key
              
            --DNS Added Tax_ID
              WHEN NOT MATCHED 
              THEN INSERT
              (cust_skey, gid, sf_cust_key, source ,tax_id) VALUES
              (Source.cust_skey, Source.GID, Source.sf_cust_key, Source.Source ,source.Tax_ID)
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key
# MAGIC ,count(1) 
# MAGIC from default.customer_xref
# MAGIC group by 1
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Merge
# MAGIC %sql 
# MAGIC select source, count(*) from default.customer_xref group by 1 order by 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Build Customer Master Table

# COMMAND ----------

# MAGIC %md
# MAGIC #####Created common views to make it reusable on other Sources.
# MAGIC 1. vw_rmphon
# MAGIC 2. vw_rmndem
# MAGIC 3. vw_rmnote
# MAGIC 4. vw_rminet
# MAGIC 5. vw_rmaddr
# MAGIC

# COMMAND ----------

# DBTITLE 1,ods_rmphon view
rmphon = spark.sql('''
    SELECT mast.cust_skey, HOM.HOME, CEL.CELL, BUS.BUS, FAX.FAX
    FROM (SELECT * FROM bronze.ods_rmmast WHERE currentrecord = 'Yes') mast
    LEFT OUTER JOIN (
        SELECT TAB.cust_skey, TAB.PHON HOME
        FROM (
            SELECT rhkey, cust_skey, CONCAT(rharea, rhphon) PHON, 'HOM' ind,
                   ROW_NUMBER() OVER (PARTITION BY rhkey ORDER BY RHMNDT DESC) rownum
            FROM bronze.ods_rmphon
            WHERE rhptyp = 'HOM' AND currentrecord = 'Yes'
        ) TAB
        WHERE TAB.rownum = 1
    ) HOM ON mast.cust_skey = HOM.cust_skey
    LEFT OUTER JOIN (
        SELECT TAB.rhkey, TAB.cust_skey, TAB.PHON CELL
        FROM (
            SELECT rhkey, cust_skey, CONCAT(rharea, rhphon) PHON, 'CEL' ind,
                   ROW_NUMBER() OVER (PARTITION BY rhkey ORDER BY RHMNDT DESC) rownum
            FROM bronze.ods_rmphon
            WHERE rhptyp = 'CEL' AND currentrecord = 'Yes'
        ) TAB
        WHERE TAB.rownum = 1
    ) CEL ON mast.cust_skey = CEL.cust_skey
    LEFT OUTER JOIN (
        SELECT TAB.rhkey, TAB.cust_skey, TAB.PHON BUS
        FROM (
            SELECT rhkey, cust_skey, CONCAT(rharea, rhphon) PHON, 'BUS' ind,
                   ROW_NUMBER() OVER (PARTITION BY rhkey ORDER BY RHMNDT DESC) rownum
            FROM bronze.ods_rmphon
            WHERE rhptyp = 'BUS' AND currentrecord = 'Yes'
        ) TAB
        WHERE TAB.rownum = 1
    ) BUS ON mast.cust_skey = BUS.cust_skey
    LEFT OUTER JOIN (
        SELECT TAB.rhkey, TAB.cust_skey, TAB.PHON FAX
        FROM (
            SELECT rhkey, cust_skey, CONCAT(rharea, rhphon) PHON, 'FAX' ind, RHMNDT,
                   ROW_NUMBER() OVER (PARTITION BY rhkey ORDER BY RHMNDT DESC) rownum
            FROM bronze.ods_rmphon
            WHERE rhptyp = 'FAX' AND currentrecord = 'Yes'
        ) TAB
        WHERE TAB.rownum = 1
    ) FAX ON mast.cust_skey = FAX.cust_skey
    WHERE mast.cust_skey > '' AND mast.currentrecord = 'Yes'
''')
rmphon.createOrReplaceTempView("vw_rmphon")

# COMMAND ----------

# DBTITLE 1,ods_rmndem view
rmndem = spark.sql('''select ndem.CUST_SKEY
    ,CASE 
        WHEN ndem.rnotyp = 'COR' THEN 'CORPORATION'
        WHEN ndem.rnotyp = 'DBA' THEN 'DOING BUSINESS AS'
        WHEN ndem.rnotyp = 'DFI' THEN 'Domestic financial institution'
        WHEN ndem.rnotyp = 'EST' THEN 'Estate Account'
        WHEN ndem.rnotyp = 'LP' THEN 'LIMITED PARTNERSHIP (LP)'
        WHEN ndem.rnotyp = 'ESC' THEN 'Lawyer Account (IOLA ) -Attorney Escrow'
        WHEN ndem.rnotyp = 'LLC' THEN 'Limited Liability Company (LLC)'
        WHEN ndem.rnotyp = 'LLP' THEN 'Limited Liability Partnership (LLP)'
        WHEN ndem.rnotyp = 'NP' THEN 'NON-PROFIT ORGANIZATION'
        WHEN ndem.rnotyp = 'Not Assigned' THEN 'Not Assigned'
        WHEN ndem.rnotyp = 'OTE' THEN 'Other Tax Exempt'
        WHEN ndem.rnotyp = 'PRT' THEN 'PARTNERSHIP'
        WHEN ndem.rnotyp = 'PC' THEN 'Professional Corporation (PC)'
        WHEN ndem.rnotyp = 'PUB' THEN 'Publicly traded US company'
        WHEN ndem.rnotyp = 'SP' THEN 'SOLE PROPRIETORSHIP'
        WHEN ndem.rnotyp = 'TAG' THEN 'Trust Agreements'
        WHEN ndem.rnotyp = 'TRS' THEN 'Trusts'
        WHEN ndem.rnotyp = 'ASC' THEN 'Unincorporated Assn'
        WHEN ndem.rnotyp = 'UN' THEN 'Union'   
    END AS Ownership
    ,ndem.RNSSIC
    ,ndem.`RN#EMP`
    from bronze.ods_rmndem ndem
    where ndem.currentrecord = 'Yes'
    ''') 
rmndem.createOrReplaceTempView("vw_rmndem")

# COMMAND ----------

# DBTITLE 1,ods_rmnote view
# Do not call view
rmnote = spark.sql('''select cust_skey
        ,rondsc
        ,TRUE
        ,ROMNDT 
        from bronze.ods_rmnote 
        where RONDSC like '%do not call%'
        or RONDSC like '%dont call%'
        or RONDSC like '%do not contact%'
        or RONDSC like '%dont contact%'
        and currentrecord = 'Yes'
    ''') 
rmnote.createOrReplaceTempView("vw_rmnote")

# COMMAND ----------

# DBTITLE 1,ods_rminet view
# For Customer's Website and Email
rminet = spark.sql('''SELECT TAB.riiseq
,tab.riityp
,case when tab.riityp = "WEB" then TAB.Website else null end Website
,case when tab.riityp = "EML" then TAB.Email else null end Email
,tab.rownum
,TAB.cust_skey
FROM
(
    SELECT riiseq
    ,riiadr AS Website
    ,riityp
    ,CASE 
        WHEN TRIM(riiadr) NOT REGEXP r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$"
        THEN NULL 
        ELSE TRIM(riiadr) 
        END AS Email
    ,cust_key as cust_skey
    ,rimndt
    ,ROW_NUMBER() OVER (PARTITION BY rikey,RIITYP ORDER BY rimndt DESC) AS rownum
    FROM bronze.ods_rminet
    WHERE riityp in ('WEB','EML')
    and currentrecord = 'Yes'
) TAB  where TAB.rownum = 1
    ''') 
rminet.createOrReplaceTempView("vw_rminet")

# COMMAND ----------

# DBTITLE 1,ods_rmaddr view
rmaddr = spark.sql('''
        SELECT mast.cust_skey
        ,tab.primStreet
        ,tab.primCity
        ,tab.primState
        ,tab.primZip
        ,tab.PrimCountry
        ,tab.primseq
        ,addr.secStreet
        ,addr.secCity
        ,addr.secState
        ,addr.secZip
        ,addr.secCountry
        ,addr.secseq
        FROM (select * from bronze.ods_RMMAST where currentrecord = 'Yes' )mast
        LEFT OUTER JOIN (
            SELECT cust_skey, REGEXP_REPLACE(TRIM(CONCAT(COALESCE(`RAST#1`, ' '), ' ', COALESCE(RASTN1, ''), ' ', COALESCE(`RAST#2`, ''), ' ', COALESCE(RASTN2, ''), ' ', COALESCE(`RAST#3`, ''), ' ', COALESCE(RASTN3, ''))), ' +', ' ') as primStreet
            ,RACITY as primCity
            ,RASTA as primState
            ,RAZIP AS primZip
            ,CASE When RAUSPC = 'Y' then 'USA' Else RAFRNC end PrimCountry
            ,RAASEQ primseq
            FROM bronze.ods_rmaddr
            WHERE raaseq = 0
            and currentrecord = 'Yes'
        ) tab ON tab.cust_skey = mast.cust_skey
        LEFT OUTER JOIN (
            SELECT cust_skey, REGEXP_REPLACE(TRIM(CONCAT(COALESCE(`RAST#1`, ''), ' ', COALESCE(RASTN1, ''), ' ', COALESCE(`RAST#2`, ''), ' ', COALESCE(RASTN2, ''), ' ', COALESCE(`RAST#3`, ''), ' ', COALESCE(RASTN3, ''))), ' +', ' ') as secStreet
            ,RACITY as secCity
            ,RASTA as secState
            ,RAZIP AS secZip
            ,CASE When RAUSPC = 'Y' then 'USA' Else RAFRNC end secCountry
            ,RAASEQ secseq
            FROM bronze.ods_rmaddr
            WHERE raaseq = 1
            and currentrecord = 'Yes'
        ) addr ON addr.cust_skey = mast.cust_skey
    ''') 
rmaddr.createOrReplaceTempView("vw_rmaddr")


# COMMAND ----------

src_GP = spark.sql('''
select TAB.TAXID
,TAB.Acct_number
,TAB.LONG_NAME
,TAB.DATE_OPENED
,TAB.DATE_CLOSED 
,TAB.ad_line1
,TAB.ad_line2
,TAB.ad_line3
,TAB.city
,TAB.state
,TAB.DerivedZip
,TAB.rownum
from 
(
  SELECT 
  REGEXP_REPLACE(acct.TAXID, '[^0-9]', '') AS TAXID
  ,acct.Acct_number
  ,acct.LONG_NAME
  ,acct.DATE_OPENED
  ,acct.DATE_CLOSED
  ,acct.CurrentRecord
  ,affil.ad_line1
  ,affil.ad_line2
  ,affil.ad_line3
  ,affil.city
  ,affil.state
  ,affil.DerivedZip
  ,row_number() OVER (PARTITION BY TAXID ORDER BY DATE_OPENED ASC) AS rownum
  FROM bronze.account acct
  LEFT JOIN
  (
    select split(acacct_number, '\\.')[0] as acct_number
    ,account_name
    ,afaffil_TYPE_1
    ,aftype_name
    ,name
    ,ad_line1
    ,ad_line2
    ,ad_line3
    ,city
    ,state
    ,date_closed
    ,CASE 
    WHEN ad_line3 IS NOT NULL AND ad_line3 RLIKE '.*\\s\\d+$' THEN regexp_extract(ad_line3, '\\s(\\d+)$', 1)
    WHEN ad_line2 IS NOT NULL AND ad_line2 RLIKE '.*\\s\\d+$' THEN regexp_extract(ad_line2, '\\s(\\d+)$', 1)
    ELSE NULL 
    END AS DerivedZip
    from bronze.affil 
    where ad_line1 is not null
    and date_closed is null
    and afaffil_TYPE_1 in ('117')
    and CurrentRecord = 'Yes'
    ) affil on affil.acct_number = acct.Acct_number
  WHERE acct.currentrecord = 'Yes' 
  AND acct.TAXID IS NOT NULL 
  AND acct.TAXID NOT IN ('99-9999999') 
  AND acct.acct_number BETWEEN '1000003' AND '7000001'
) TAB where TAB.rownum = 1
    ''') 
src_GP.createOrReplaceTempView("vw_src_GP")    



# COMMAND ----------

src_OS = spark.sql('''
SELECT tab.SSN_Tax_ID
,tab.IRS_Code
,tab.Date_of_Birth
,tab.First_Name
,tab.Middle_Name
,tab.Last_Name
,tab.Address_Line_1
,tab.Address_Line_2
,tab.Address_Line_3
,tab.City
,tab.State
,tab.Zip
,tab.Home_Phone
,tab.Work_Phone
,tab.Email
,tab.Broker_SSN_Tax_ID
,tab.Broker_Name
,tab.Branch_Name
,tab.Client_Name
,tab.IRS_Code 
,tab.Open_Date
,tab.ho_Contact_ID
,tab.ho_Birth_Date
,tab.ho_First_Name
,tab.ho_Last_Name
FROM
(
    SELECT REGEXP_REPLACE(os.SSN_Tax_ID, '[^0-9]', '') SSN_Tax_ID
    ,os.IRS_Code
    ,os.Date_of_Birth
    ,os.First_Name
    ,os.Middle_Name
    ,os.Last_Name
    ,os.Address_Line_1
    ,os.Address_Line_2
    ,os.Address_Line_3
    ,os.City
    ,os.State
    ,os.Zip
    ,os.Home_Phone
    ,os.Work_Phone
    ,CASE WHEN TRIM(os.Email) NOT REGEXP r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$" THEN NULL ELSE TRIM(os.Email) END AS Email
    ,os.Broker_SSN_Tax_ID
    ,os.Broker_Name
    ,os.Branch_Name
    ,os.Client_Name
    ,os.IRS_Code 
    ,os.Open_Date
    ,tab.ho_Contact_ID
    ,tab.ho_Birth_Date
    ,tab.ho_First_Name
    ,tab.ho_Last_Name
    ,ROW_NUMBER() OVER (PARTITION BY os.SSN_Tax_ID ORDER BY os.Open_Date ASC) rownum
    FROM bronze.account_osaic os 
    left join 
    (
        SELECT ho.SSN_Tax_ID
        ,ho.Contact_ID ho_Contact_ID
        ,ho.Birth_Date ho_Birth_Date
        ,ho.First_Name ho_First_Name
        ,ho.Last_Name ho_Last_Name  
        FROM
        (
            SELECT REGEXP_REPLACE(SSN_Tax_ID, '[^0-9]', '') AS SSN_Tax_ID
            ,Contact_ID
            ,Birth_Date
            ,First_Name
            ,Last_Name 
            ,ROW_NUMBER() OVER (PARTITION BY Contact_ID ORDER BY SSN_Tax_ID DESC) rownum
            FROM bronze.household_osaic where currentrecord = 'Yes'
        ) ho 
        WHERE ho.rownum = 1 
    )tab on REGEXP_REPLACE(os.SSN_Tax_ID, '[^0-9]', '') = tab.SSN_Tax_ID    
    where os.currentrecord = 'Yes' 
    and os.SSN_Tax_ID is not null
) tab WHERE rownum = 1
''') 
src_OS.createOrReplaceTempView("vw_src_OS")

# COMMAND ----------

src_DMI = spark.sql('''
select a.* from (
  select tab.Tax_ID
  ,tab.loan_number
  ,tab.Email
  ,tab.Tax_First_Name
  ,tab.Tax_Name
  ,tab.New_Billing_Name
  ,tab.Name
  ,tab.firstName
  ,tab.LastName
  ,tab.Investor_Loan_Number
  ,tab.Branch_Office_Code
  ,tab.Telephone_Number
  ,tab.Second_Telephone_Number
  ,tab.Billing_Address_Line_1
  ,tab.Billing_Address_Line_2
  ,tab.Billing_Address_Line_3
  ,tab.Billing_Address_Line_4
  ,tab.Billing_City_Name
  ,tab.Billing_State
  ,tab.Billing_Zip_Code  
  ,ROW_NUMBER() OVER (PARTITION BY tab.Tax_ID order by tab.Tax_ID desc) AS rownum
  from
  (
      select REGEXP_REPLACE(dmi.Mortgagor_SSN, '[^0-9]', '') Tax_ID
      ,dmi.loan_number
      ,CASE WHEN TRIM(dmi.Borrower_email_address) NOT REGEXP r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$" THEN NULL ELSE TRIM(dmi.Borrower_email_address) END AS Email
      ,dmi.Tax_First_Name
      ,dmi.Tax_Name
      ,dmi.New_Billing_Name
      ,dmi.Mortgagor_Name_Formatted_for_CBR_Reporting Name
      ,split(dmi.Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[1] as FirstName
      ,split(dmi.Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[0] as LastName
      ,dmi.Investor_Loan_Number
      ,dmi.Branch_Office_Code
      ,dmi.Telephone_Number
      ,dmi.Second_Telephone_Number
      ,dmi.Billing_Address_Line_1
      ,dmi.Billing_Address_Line_2
      ,dmi.Billing_Address_Line_3
      ,dmi.Billing_Address_Line_4
      ,dmi.Billing_City_Name
      ,dmi.Billing_State
      ,dmi.Billing_Zip_Code
      from bronze.dmi_dcif dmi
      where dmi.CurrentRecord = "Yes"
      and dmi.Investor_ID = "40H"
      and (dmi.Category_Code = "003" or dmi.Category_Code = "002")
      and REGEXP_REPLACE(dmi.Mortgagor_SSN, '[^0-9]', '') <> '000000000'
      union all
      select REGEXP_REPLACE(dmi.Co_Mortgagor_SSN, '[^0-9]', '') Tax_ID
      ,dmi.loan_number
      ,CASE WHEN TRIM(dmi.Co_borrower_email_address) NOT REGEXP r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$" THEN NULL ELSE TRIM(dmi.Co_borrower_email_address) END AS Email      
      ,dmi.Tax_First_Name
      ,dmi.Tax_Name
      ,dmi.New_Billing_Name
      ,dmi.Co_Mortgagor_Name_Formatted_for_CBR_Reporting Name
      ,split(dmi.Co_Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[1] as FirstName
      ,split(dmi.Co_Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[0] as LastName
      ,dmi.Investor_Loan_Number
      ,dmi.Branch_Office_Code
      ,dmi.Telephone_Number
      ,dmi.Second_Telephone_Number
      ,dmi.Billing_Address_Line_1
      ,dmi.Billing_Address_Line_2
      ,dmi.Billing_Address_Line_3
      ,dmi.Billing_Address_Line_4
      ,dmi.Billing_City_Name
      ,dmi.Billing_State
      ,dmi.Billing_Zip_Code
      from bronze.dmi_dcif dmi
      where dmi.CurrentRecord = "Yes"
      and dmi.Investor_ID = "40H"
      and (dmi.Category_Code = "003" or dmi.Category_Code = "002")
      and REGEXP_REPLACE(dmi.Co_Mortgagor_SSN, '[^0-9]', '') <> '000000000'
  ) tab
)A
where a.rownum = 1
''') 
src_DMI.createOrReplaceTempView("vw_src_DMI")

# COMMAND ----------

# DBTITLE 1,For Source 01 and 04
src_01_04 = spark.sql('''
select beb.company_name as Name
,null as FirstName
,null as LastName
,null as MiddleName
,"Business" as CustomerType
,xref.Cust_Skey as AccountNumber
,mst.OFF_SKEY Officer_Key
,usr.`MUZXEMP#` as ABEmployeeID
,beb.Company_Address_Line_1 Primary_Address_Street
,beb.Company_City Primary_Address_City
,beb.Company_State Primary_Address_State
,beb.Company_Zip Primary_Address_Zip
,beb.Company_Country Primary_Address_Country
,null as Secondary_Address_Street
,null as Secondary_Address_City
,null as Secondary_Address_State
,null as Secondary_Address_Zip
,null as Secondary_Address_Country
,null as PersonBirthdate
,null as PersonBranch
,null as PersonDoNotCall
,null as PersonEmail
,ndem.`RN#EMP` NumberofEmployees
,phn.Fax Fax
,null as PersonPrimary_Address_Street
,null as PersonPrimary_Address_City
,null as PersonPrimary_Address_State
,null as PersonPrimary_Address_Zip
,null as PersonPrimary_Address_Country
,null as PersonSecondary_Address_Street
,null as PersonSecondary_Address_City
,null as PersonSecondary_Address_State
,null as PersonSecondary_Address_Zip
,null as PersonSecondary_Address_Country
,null as PersonHomePhone
,null as PersonMobilePhone
,null as PersonBusinessPhone
,ndem.Ownership
,beb.Primary_Contact_Phone BusinessPhone
,web.Website Website
,CASE
        WHEN length(cast(mst.rmaddt as string)) < 8 THEN null
        WHEN year(cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP)) < 1900 THEN NULL
        WHEN cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP) > current_date() THEN NULL
        ELSE cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP) END as RelationshipStartDate
,CONCAT(
        FLOOR(DATEDIFF(day, relationshipstartdate, CURRENT_DATE) / 365), ' years, ',
        FLOOR((DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) / 30), ' months, ',
        (DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) % 30, ' days'
    ) AS LengthOfRelationship
,naics.NAICSDescription NAICSDescription
,ndem.RNSSIC NAICSCode
,'Customer' Type
,mst.RMTIN EncryptedTaxIDNumber
,right(xref.Tax_ID, 4) as TaxIDNum_Last4
,xref.Tax_ID DecryptedTaxIDNumber
,mst.RMTINT TinType
,null ExternalSystem
,null as Salutation
--,mst.RMSFX as Suffix
,null as Suffix
,null as AccountSite
,null as AccountSource
,null as AnnualRevenue
,null as Assistant
,null as AsstPhone
,null as EmailOptOut
,null as FaxOptOut
,null as ConsultantRating
,null as TrustTier
,null as ClientType
,null as AtRisk
,null as MarketingNeeds
--,mst.RMREGN as Region
,null as Region
,null as FICOScore
,null as FICOScoreDate
,null as Alerts
,null as RelationshipManager
,null as UnderwriterWith
,null as Comments
,null as PropertyManager
,null as CREComments
,null as CRMComments
,null as COIType
,null as CurrentIncome
,null as Employment
,null as EmploymentStatus
,null as CurrentResidenceOwnorRent
,null as DMILink
,null as FISHorizonLink
,null as PriscillaMetWithClient
,mst.RMREGO as RegO
,null as PortfolioNumber
,xref.Company_ID
,xref.Cust_Skey 
,xref.Cust_Skey_2 
,xref.Cust_Skey_3 
,xref.Cust_Skey_4 
,xref.Cust_Skey_5 
,xref.Cust_Skey_6 
,xref.GID 
,xref.SF_Cust_Key 
,null ParentID
,xref.Source
,ROW_NUMBER() OVER (PARTITION BY SF_Cust_Key ORDER BY SF_Cust_Key DESC) rownum
from default.customer_xref xref
left join bronze.v_ods_beb_customer beb on xref.Company_ID = beb.Company_ID
left join bronze.ods_rmmast mst on xref.Cust_Skey = mst.CUST_SKEY
left join (SELECT CONCAT('13-', SPLIT(muzxuid, '213')[1]) as key, * 
FROM bronze.siabusers) usr on usr.key = mst.OFF_SKEY
LEFT JOIN (select * from bronze.ods_rmpdem where currentrecord = 'Yes')pdem ON mst.CUST_SKEY = pdem.CUST_SKEY 
left join vw_rmndem ndem on ndem.cust_skey = mst.cust_skey
left join (
      select NumKey as NAICSCode, Description1 AS NAICSDescription 
      from bronze.ods_sicod_ext where codetype = 'NAICS'
      ) naics on cast(naics.NAICSCode as string) = cast(ndem.RNSSIC as string)
left join vw_rmphon phn ON phn.cust_skey = mst.CUST_SKEY
LEFT JOIN vw_rminet web ON xref.cust_skey = web.cust_skey and web.riityp = "WEB"								
where source in ('01_HZN_AOTM','04_SB_With_AOTM')
    ''') 
src_01_04.createOrReplaceTempView("vw_src_01_04")




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customer_master
# MAGIC where source = '01_HZN_AOTM'
# MAGIC and Cust_Skey is null

# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key, count(*) from vw_src_01_04
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by SF_Cust_Key

# COMMAND ----------

# DBTITLE 1,For source 02, 04, 05 and 06
src_02_04_05_06 = spark.sql('''
select case when xref.source = '02_XAA_COMPOSITE' then xref.Name
      When mst.RMCTYP = 'N' THEN mst.RMNPN1 END as Name
,CASE When mst.RMCTYP = 'P' THEN mst.RMFRST END as FirstName
,CASE When mst.RMCTYP = 'P' THEN mst.RMLAST END as LastName
,CASE When mst.RMCTYP = 'P' THEN mst.RMMIDL END  as MiddleName
,CASE When mst.RMCTYP = 'P' THEN 'Individual'
      when mst.RMCTYP = 'N' THEN 'Business' 
      else '' END as CustomerType
,xref.Cust_Skey as AccountNumber
,mst.OFF_SKEY Officer_Key
,usr.`MUZXEMP#` as ABEmployeeID
,CASE WHEN mst.rmctyp = 'N'Then addr.primStreet END AS Primary_Address_Street
,CASE WHEN mst.rmctyp = 'N'Then addr.primCity END Primary_Address_City
,CASE WHEN mst.rmctyp = 'N'Then addr.primState END Primary_Address_State
,CASE WHEN mst.rmctyp = 'N'Then addr.primZIP END Primary_Address_Zip
,CASE WHEN mst.rmctyp = 'N'Then addr.PrimCountry End Primary_Address_Country
,CASE WHEN mst.rmctyp = 'N'Then addr.secStreet END AS Secondary_Address_Street
,CASE WHEN mst.rmctyp = 'N'Then addr.secCity END Secondary_Address_City
,CASE WHEN mst.rmctyp = 'N'Then addr.secState END Secondary_Address_State
,CASE WHEN mst.rmctyp = 'N'Then addr.secZIP END Secondary_Address_Zip
,CASE WHEN mst.rmctyp = 'N'Then addr.secCountry END Secondary_Address_Country
,CASE 
    WHEN length(cast(pdem.RPDOB as string)) < 8 THEN null
    WHEN year(cast(to_date(pdem.RPDOB, 'yyyyMMdd') as TIMESTAMP)) < 1900 THEN NULL 
    WHEN cast(to_date(pdem.RPDOB, 'yyyyMMdd') as TIMESTAMP) > current_date() THEN NULL
    ELSE cast(to_date(pdem.RPDOB, 'yyyyMMdd') as TIMESTAMP) 
END AS PersonBirthdate
,CASE when mst.RMCTYP = 'P' then org.Org_Key END AS PersonBranch
,CASE when rmnote.TRUE = 'TRUE' then 'TRUE' else 'FALSE' END as PersonDoNotCall
,CASE when mst.RMCTYP = 'P' then eml.Email END as PersonEmail
,ndem.`RN#EMP` NumberofEmployees
,phn.Fax Fax
,CASE WHEN mst.rmctyp = 'P' Then addr.primStreet END AS PersonPrimary_Address_Street
,CASE WHEN mst.rmctyp = 'P' Then addr.primCity END PersonPrimary_Address_City
,CASE WHEN mst.rmctyp = 'P' Then addr.primState END PersonPrimary_Address_State
,CASE WHEN mst.rmctyp = 'P' Then addr.primZIP END PersonPrimary_Address_Zip
,CASE WHEN mst.rmctyp = 'P' Then addr.PrimCountry End PersonPrimary_Address_Country
,CASE WHEN mst.rmctyp = 'P' Then addr.secStreet END AS PersonSecondary_Address_Street
,CASE WHEN mst.rmctyp = 'P' Then addr.secCity END PersonSecondary_Address_City
,CASE WHEN mst.rmctyp = 'P' Then addr.secState END PersonSecondary_Address_State
,CASE WHEN mst.rmctyp = 'P' Then addr.secZIP END PersonSecondary_Address_Zip
,CASE WHEN mst.rmctyp = 'P' Then addr.secCountry End PersonSecondary_Address_Country
,CASE when mst.RMCTYP = 'P' then phn.HOME END as PersonHomePhone
,CASE when mst.RMCTYP = 'P' then phn.CELL END as PersonMobilePhone
,CASE when mst.RMCTYP = 'P' then phn.BUS  END as PersonBusinessPhone
,ndem.Ownership
,CASE when mst.rmctyp = 'N' then phn.BUS End as BusinessPhone
,web.Website Website
,CASE
        WHEN length(cast(mst.rmaddt as string)) < 8 THEN null
        WHEN year(cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP)) < 1900 THEN NULL
        WHEN cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP) > current_date() THEN NULL
        ELSE cast(to_date(mst.rmaddt, 'yyyyMMdd') as TIMESTAMP) END as RelationshipStartDate
,CONCAT(
        FLOOR(DATEDIFF(day, relationshipstartdate, CURRENT_DATE) / 365), ' years, ',
        FLOOR((DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) / 30), ' months, ',
        (DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) % 30, ' days'
    ) AS LengthOfRelationship
,naics.NAICSDescription NAICSDescription
,ndem.RNSSIC NAICSCode
,'Customer' Type
,mst.RMTIN EncryptedTaxIDNumber
,right(xref.Tax_ID, 4) as TaxIDNum_Last4
,xref.Tax_ID DecryptedTaxIDNumber
,mst.RMTINT TinType
,null ExternalSystem
,null as Salutation
,null as Suffix
--,mst.RMSFX as Suffix
,null as AccountSite
,null as AccountSource
,null as AnnualRevenue
,null as Assistant
,null as AsstPhone
,null as EmailOptOut
,null as FaxOptOut
,null as ConsultantRating
,null as TrustTier
,null as ClientType
,null as AtRisk
,null as MarketingNeeds
,null as Region
--,mst.RMREGN as Region
,null as FICOScore
,null as FICOScoreDate
,null as Alerts
,null as RelationshipManager
,null as UnderwriterWith
,null as Comments
,null as PropertyManager
,null as CREComments
,null as CRMComments
,null as COIType
,null as CurrentIncome
,null as Employment
,null as EmploymentStatus
,null as CurrentResidenceOwnorRent
,null as DMILink
,null as FISHorizonLink
,null as PriscillaMetWithClient
,mst.RMREGO as RegO
,null as PortfolioNumber
,xref.Company_ID
,xref.Cust_Skey 
,xref.Cust_Skey_2 
,xref.Cust_Skey_3 
,xref.Cust_Skey_4 
,xref.Cust_Skey_5 
,xref.Cust_Skey_6 
,xref.GID 
,xref.SF_Cust_Key 
,null ParentID
,xref.Source
,ROW_NUMBER() OVER (PARTITION BY SF_Cust_Key ORDER BY SF_Cust_Key DESC) rownum
from default.customer_xref xref
left join bronze.ods_rmmast mst on xref.Cust_Skey = mst.CUST_SKEY and mst.CurrentRecord = "Yes"
left join (SELECT CONCAT('13-', SPLIT(muzxuid, '213')[1]) as key, * 
FROM bronze.siabusers) usr on usr.key = mst.OFF_SKEY
LEFT JOIN (select * from bronze.ods_rmpdem where currentrecord = 'Yes')pdem ON mst.CUST_SKEY = pdem.CUST_SKEY 
left join (select * from bronze.fi_core_org where org_key != '13-' and currentrecord = 'Yes' )org on mst.RMOBRN = org.Branch_ID
left join vw_rmaddr addr on addr.cust_skey = mst.CUST_SKEY
left join vw_rmnote rmnote on rmnote.cust_skey = mst.CUST_SKEY
left join vw_rminet eml on mst.cust_skey = eml.cust_skey and eml.riityp = "EML"
left join vw_rmndem ndem on ndem.cust_skey = mst.cust_skey
left join (
      select NumKey as NAICSCode, Description1 AS NAICSDescription 
      from bronze.ods_sicod_ext where codetype = 'NAICS'
      ) naics on cast(naics.NAICSCode as string) = cast(ndem.RNSSIC as string)
left join vw_rmphon phn ON phn.cust_skey = mst.CUST_SKEY
LEFT JOIN vw_rminet web ON xref.cust_skey = web.cust_skey and web.riityp = "WEB"
where source in ('02_HZN_AOTM_XAA', '02_XAA_COMPOSITE', '04_SB_No_AOTM', '05_Customer_Dup_By_GID', '06_Horizon_Personal')

    ''') 
src_02_04_05_06.createOrReplaceTempView("vw_src_02_04_05_06")


# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql    
# MAGIC select SF_Cust_Key, count(*) from vw_src_02_04_05_06
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by SF_Cust_Key

# COMMAND ----------

# DBTITLE 1,For Source 03
src_03 = spark.sql('''
select gpa.LONG_NAME as Name
,null as FirstName
,null as LastName
,null as MiddleName
,'Business' as CustomerType
,concat('GP-',xref.tax_id) as AccountNumber
,null as Officer_Key
,null as ABEmployeeID
,concat(gpa.ad_line1,' ',gpa.ad_line2)  as Primary_Address_Street
,gpa.city as Primary_Address_City
,gpa.state as Primary_Address_State
,gpa.DerivedZip as Primary_Address_Zip
,null as Primary_Address_Country
,null as Secondary_Address_Street
,null as Secondary_Address_City
,null as Secondary_Address_State
,null as Secondary_Address_Zip
,null as Secondary_Address_Country
,null AS PersonBirthdate
,null as PersonBranch
,null as PersonDoNotCall
,null as PersonEmail
,0 as NumberofEmployees
,null as Fax
,null as PersonPrimary_Address_Street
,null as PersonPrimary_Address_City
,null as PersonPrimary_Address_State
,null as PersonPrimary_Address_Zip
,null as PersonPrimary_Address_Country
,null as PersonSecondary_Address_Street
,null as PersonSecondary_Address_City
,null as PersonSecondary_Address_State
,null as PersonSecondary_Address_Zip
,null as PersonSecondary_Address_Country
,null as PersonHomePhone
,null as PersonMobilePhone
,null as PersonBusinessPhone
,null as Ownership
,null as BusinessPhone
,null as Website
,gpa.DATE_OPENED RelationshipStartDate
,CONCAT(
        FLOOR(DATEDIFF(day, relationshipstartdate, CURRENT_DATE) / 365), ' years, ',
        FLOOR((DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) / 30), ' months, ',
        (DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) % 30, ' days'
    ) AS LengthOfRelationship
,null as NAICSDescription
,null as NAICSCode
,'Customer' Type
,concat("22222",right(xref.Tax_ID, 4)) as EncryptedTaxIDNumber
,right(xref.Tax_ID, 4) as TaxIDNum_Last4
,xref.Tax_ID DecryptedTaxIDNumber
,null as TinType
,null ExternalSystem
,null as Salutation
,null as Suffix
,null as AccountSite
,null as AccountSource
,null as AnnualRevenue
,null as Assistant
,null as AsstPhone
,null as EmailOptOut
,null as FaxOptOut
,null as ConsultantRating
,null as TrustTier
,null as ClientType
,null as AtRisk
,null as MarketingNeeds
,null as Region
,null as FICOScore
,null as FICOScoreDate
,null as Alerts
,null as RelationshipManager
,null as UnderwriterWith
,null as Comments
,null as PropertyManager
,null as CREComments
,null as CRMComments
,null as COIType
,null as CurrentIncome
,null as Employment
,null as EmploymentStatus
,null as CurrentResidenceOwnorRent
,null as DMILink
,null as FISHorizonLink
,null as PriscillaMetWithClient
,null as RegO
,null as PortfolioNumber
,xref.Company_ID
,xref.Cust_Skey 
,xref.Cust_Skey_2 
,xref.Cust_Skey_3 
,xref.Cust_Skey_4 
,xref.Cust_Skey_5 
,xref.Cust_Skey_6 
,xref.GID 
,xref.SF_Cust_Key 
,null ParentID
,xref.Source
,0 rownum
from default.customer_xref xref
left join vw_src_GP gpa on xref.Tax_ID = gpa.taxid
where source in ('03_Global_Plus')
''') 
src_03.createOrReplaceTempView("vw_src_03")


# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key, count(*) from vw_src_03
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by SF_Cust_Key

# COMMAND ----------

# DBTITLE 1,For Source 07
src_07 = spark.sql('''
select Case WHEN OS.IRS_Code = 'T' OR OS.ho_Contact_ID = '1608426' THEN OS.First_Name end as Name
,Case 
    when OS.ho_Contact_ID = '1608426' then null
    when OS.IRS_Code = 'S' and OS.First_Name is not null then OS.First_Name 
    when OS.IRS_Code = 'S' and OS.First_Name is null then OS.ho_First_Name
  end AS FirstName
,Case 
    when OS.ho_Contact_ID = '1608426' then null
    when OS.IRS_Code = 'S' and OS.Last_Name is not null then OS.Last_Name 
    when OS.IRS_Code = 'S' and OS.Last_Name is null then OS.ho_Last_Name
  end as LastName
,Case when OS.ho_Contact_ID = '1608426' then null when OS.IRS_Code = 'S' then OS.Middle_Name end as MiddleName
,Case when OS.ho_Contact_ID = '1608426' then 'Business' 
    when OS.IRS_Code = 'S' then 'Individual' 
    else 'Business' 
  end as CustomerType
,concat("13-OS-",OS.ho_Contact_ID) as AccountNumber
,null as Officer_Key
,null as ABEmployeeID
,Case when OS.IRS_Code = 'T' OR os.ho_Contact_ID = '1608426' then REGEXP_REPLACE(TRIM(CONCAT(COALESCE(OS.Address_Line_1, ''), ' ', COALESCE(OS.Address_Line_2, ''), ' ', COALESCE(OS.Address_Line_3, ''), ' ')), ' +', ' ') end as Primary_Address_Street
,case when OS.IRS_Code = 'T' OR os.ho_Contact_ID = '1608426' then OS.City end as Primary_Address_City
,Case when OS.IRS_Code = 'T' OR os.ho_Contact_ID = '1608426' then OS.State end as Primary_Address_State
,Case when OS.IRS_Code = 'T' OR os.ho_Contact_ID = '1608426' then OS.Zip end as Primary_Address_Zip
,"USA" as Primary_Address_Country
,null as Secondary_Address_Street
,null as Secondary_Address_City
,null as Secondary_Address_State
,null as Secondary_Address_Zip
,null as Secondary_Address_Country
,CASE 
    WHEN OS.ho_Birth_Date is NULL THEN NULL 
    WHEN OS.IRS_Code = 'S' and OS.ho_Contact_ID <> '1608426' AND OS.ho_Birth_Date IS NOT NULL THEN try_CAST(to_date(OS.ho_Birth_Date, 'yyyy-MM-dd') AS DATE) 
  END AS PersonBirthdate
,CASE 
   when OS.IRS_Code = 'S' and OS.ho_Contact_ID <> '1608426' then bxref.branch_code 
  END as PersonBranch
,null as PersonDoNotCall
,Case when OS.IRS_Code = 'S' then OS.Email end as PersonEmail
,0 as NumberofEmployees
,null as Fax
,null as PersonPrimary_Address_Street
,null as PersonPrimary_Address_City
,null as PersonPrimary_Address_State
,null as PersonPrimary_Address_Zip
,null as PersonPrimary_Address_Country
,null as PersonSecondary_Address_Street
,null as PersonSecondary_Address_City
,null as PersonSecondary_Address_State
,null as PersonSecondary_Address_Zip
,null as PersonSecondary_Address_Country
,Case when OS.IRS_Code = 'S' and OS.ho_Contact_ID <> '1608426' then OS.Home_Phone end as PersonHomePhone
,null as PersonMobilePhone
,Case when OS.IRS_Code = 'S' and OS.ho_Contact_ID <> '1608426' then OS.Work_Phone end as PersonBusinessPhone
,null as Ownership
,Case when OS.IRS_Code = 'T' OR OS.ho_Contact_ID <> '1608426' then OS.Work_Phone end as BusinessPhone
,null as Website
,CASE 
    WHEN OS.Open_Date is null THEN NULL 
    ELSE try_CAST(to_date(OS.Open_Date, 'yyyy-MM-dd') AS TIMESTAMP) 
  END AS RelationshipStartDate
,CONCAT(
        FLOOR(DATEDIFF(day, relationshipstartdate, CURRENT_DATE) / 365), ' years, ',
        FLOOR((DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) / 30), ' months, ',
        (DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) % 30, ' days'
    ) AS LengthOfRelationship
,null as NAICSDescription
,null as NAICSCode
,'Customer' Type
,concat("22222",right(xref.Tax_ID, 4)) as EncryptedTaxIDNumber
,right(xref.Tax_ID, 4) as TaxIDNum_Last4
,xref.Tax_ID DecryptedTaxIDNumber
,null as TinType
,null ExternalSystem
,null as Salutation
,null as Suffix
,null as AccountSite
,null as AccountSource
,null as AnnualRevenue
,null as Assistant
,null as AsstPhone
,null as EmailOptOut
,null as FaxOptOut
,null as ConsultantRating
,null as TrustTier
,null as ClientType
,null as AtRisk
,null as MarketingNeeds
,null as Region
,null as FICOScore
,null as FICOScoreDate
,null as Alerts
,null as RelationshipManager
,null as UnderwriterWith
,null as Comments
,null as PropertyManager
,null as CREComments
,null as CRMComments
,null as COIType
,null as CurrentIncome
,null as Employment
,null as EmploymentStatus
,null as CurrentResidenceOwnorRent
,null as DMILink
,null as FISHorizonLink
,null as PriscillaMetWithClient
,null as RegO
,null as PortfolioNumber
,xref.Company_ID
,xref.Cust_Skey 
,xref.Cust_Skey_2 
,xref.Cust_Skey_3 
,xref.Cust_Skey_4 
,xref.Cust_Skey_5 
,xref.Cust_Skey_6 
,xref.GID 
,xref.SF_Cust_Key
,null ParentID 
,xref.Source
,0 rownum
from default.customer_xref xref
left join vw_src_OS OS on xref.tax_id = OS.SSN_Tax_ID
left join (select * from silver.branch_xref where currentrecord = 'Yes') bxref on OS.branch_name = bxref.OsaicBranchMapName
where source in ('07_Osaic_No_Horizon')
''') 
src_07.createOrReplaceTempView("vw_src_07")

# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key, count(*) from vw_src_07
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by SF_Cust_Key

# COMMAND ----------

# DBTITLE 1,For Source 08
src_08 = spark.sql('''
select null as Name
,dmi.firstname AS FirstName
,dmi.LastName as LastName
,null as MiddleName
,'Individual'as CustomerType
,dmi.Investor_Loan_Number as AccountNumber
,null as Officer_Key
,null as ABEmployeeID
,null as Primary_Address_Street
,null as Primary_Address_City
,null as Primary_Address_State
,null as Primary_Address_Zip
,null as Primary_Address_Country
,null as Secondary_Address_Street
,null as Secondary_Address_City
,null as Secondary_Address_State
,null as Secondary_Address_Zip
,null as Secondary_Address_Country
,null PersonBirthdate
,CASE 
    WHEN dmi.Branch_Office_Code IS NOT NULL 
    THEN CAST('13-' || try_CAST(dmi.Branch_Office_Code AS STRING) AS STRING) 
    ELSE NULL 
  END PersonBranch
,null as PersonDoNotCall
,dmi.email as PersonEmail
,0 as NumberofEmployees
,null as Fax
,dmi.Billing_Address_Line_4 as PersonPrimary_Address_Street
,dmi.Billing_City_Name as PersonPrimary_Address_City
,dmi.Billing_State  as PersonPrimary_Address_State
,cast(dmi.Billing_Zip_Code as string) as PersonPrimary_Address_Zip
,"USA" as PersonPrimary_Address_Country
,null as PersonSecondary_Address_Street
,null as PersonSecondary_Address_City
,null as PersonSecondary_Address_State
,null as PersonSecondary_Address_Zip
,null as PersonSecondary_Address_Country
,dmi.Telephone_Number as PersonHomePhone
,dmi.Second_Telephone_Number as PersonMobilePhone
,null as PersonBusinessPhone
,null as Ownership
,null as BusinessPhone
,null as Website
,null AS RelationshipStartDate
,CONCAT(
        FLOOR(DATEDIFF(day, relationshipstartdate, CURRENT_DATE) / 365), ' years, ',
        FLOOR((DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) / 30), ' months, ',
        (DATEDIFF(day, relationshipstartdate, CURRENT_DATE) % 365) % 30, ' days'
    ) AS LengthOfRelationship
,null as NAICSDescription
,null as NAICSCode
,'Customer' Type
,concat("22222",right(xref.Tax_ID, 4)) as EncryptedTaxIDNumber
,right(xref.Tax_ID, 4) as TaxIDNum_Last4
,xref.Tax_ID DecryptedTaxIDNumber
,null as TinType
,null ExternalSystem
,null as Salutation
,null as Suffix
,null as AccountSite
,null as AccountSource
,null as AnnualRevenue
,null as Assistant
,null as AsstPhone
,null as EmailOptOut
,null as FaxOptOut
,null as ConsultantRating
,null as TrustTier
,null as ClientType
,null as AtRisk
,null as MarketingNeeds
,null as Region
,null as FICOScore
,null as FICOScoreDate
,null as Alerts
,null as RelationshipManager
,null as UnderwriterWith
,null as Comments
,null as PropertyManager
,null as CREComments
,null as CRMComments
,null as COIType
,null as CurrentIncome
,null as Employment
,null as EmploymentStatus
,null as CurrentResidenceOwnorRent
,null as DMILink
,null as FISHorizonLink
,null as PriscillaMetWithClient
,null as RegO
,null as PortfolioNumber
,xref.Company_ID
,xref.Cust_Skey 
,xref.Cust_Skey_2 
,xref.Cust_Skey_3 
,xref.Cust_Skey_4 
,xref.Cust_Skey_5 
,xref.Cust_Skey_6 
,xref.GID 
,xref.SF_Cust_Key 
,null ParentID
,xref.Source
,0 rownum
from default.customer_xref xref
left join vw_src_DMI dmi on xref.tax_id = DMI.Tax_ID
where source in ('08_DMI_No_Horizon')
''') 
src_08.createOrReplaceTempView("vw_src_08")

# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC select SF_Cust_Key, count(*) from vw_src_08
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by SF_Cust_Key

# COMMAND ----------

# DBTITLE 1,Regression Unit Tests
##########Testing Validation for AOTM Source
beb_company_id_with_no_taxid = spark.sql('''
select tab.TaxID
,tab.Company_ID
from
(
    select REGEXP_REPLACE(Tax_ID, '[^0-9]', '') TaxID
    ,Company_ID
    ,ROW_NUMBER() OVER (PARTITION BY Tax_ID order by Company_Open_Date desc) as rownum
    from bronze.v_ods_beb_customer
    where Tax_ID = ''
) tab
where tab.rownum = 1
''')
beb_company_id_with_no_taxid.display()

beb_company_id_no_GID = spark.sql('''
select tab.TaxID
,tab.Company_ID
from
(
    select REGEXP_REPLACE(Tax_ID, '[^0-9]', '') TaxID
    ,Company_ID
    ,ROW_NUMBER() OVER (PARTITION BY Tax_ID order by Company_Open_Date desc) as rownum
    from bronze.v_ods_beb_customer
) tab
inner join 
(
select tab.OID
,tab.GID
from 
(
    select OID
    ,GID
    ,ROW_NUMBER() OVER (PARTITION BY OID order by TID desc) AS rownum
    from silver.customer_idmap idm where CurrentRecord = "Yes"
)tab
where tab.rownum = 1  
) idm ON tab.TaxID = idm.OID
where tab.rownum = 1
and idm.GID is null
''')
beb_company_id_no_GID.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing Merge Operations into the target table

# COMMAND ----------

# MAGIC %md
# MAGIC #LEFT JOIN

# COMMAND ----------

# DBTITLE 1,Prep Data from customer_xref for SCD
try:
    logger.info("Joining base tables for silver customer_xref table")
    Transformation_sqlquery = """select tab.Name 
        ,tab.FirstName 
        ,tab.LastName 
        ,tab.MiddleName 
        ,tab.CustomerType 
        ,tab.AccountNumber 
        ,tab.Officer_Key 
        ,tab.ABEmployeeID 
        ,tab.Primary_Address_Street 
        ,tab.Primary_Address_City 
        ,tab.Primary_Address_State 
        ,tab.Primary_Address_Zip 
        ,tab.Primary_Address_Country 
        ,tab.Secondary_Address_Street 
        ,tab.Secondary_Address_City 
        ,tab.Secondary_Address_State 
        ,tab.Secondary_Address_Zip 
        ,tab.Secondary_Address_Country 
        ,tab.PersonBirthdate 
        ,tab.PersonBranch 
        ,tab.PersonDoNotCall 
        ,tab.PersonEmail 
        ,tab.NumberofEmployees 
        ,tab.Fax 
        ,tab.PersonPrimary_Address_Street 
        ,tab.PersonPrimary_Address_City 
        ,tab.PersonPrimary_Address_State 
        ,tab.PersonPrimary_Address_Zip 
        ,tab.PersonPrimary_Address_Country 
        ,tab.PersonSecondary_Address_Street 
        ,tab.PersonSecondary_Address_City 
        ,tab.PersonSecondary_Address_State 
        ,tab.PersonSecondary_Address_Zip 
        ,tab.PersonSecondary_Address_Country 
        ,tab.PersonHomePhone 
        ,tab.PersonMobilePhone 
        ,tab.PersonBusinessPhone 
        ,tab.Ownership 
        ,tab.BusinessPhone 
        ,tab.Website 
        ,tab.RelationshipStartDate 
        ,tab.LengthOfRelationship 
        ,tab.NAICSDescription 
        ,tab.NAICSCode 
        ,tab.Type 
        ,tab.EncryptedTaxIDNumber 
        ,tab.TaxIDNum_Last4 
        ,tab.DecryptedTaxIDNumber 
        ,tab.TinType 
        ,tab.ExternalSystem 
        ,tab.Salutation 
        ,tab.Suffix 
        ,tab.AccountSite 
        ,tab.AccountSource 
        ,tab.AnnualRevenue 
        ,tab.Assistant 
        ,tab.AsstPhone 
        ,tab.EmailOptOut 
        ,tab.FaxOptOut 
        ,tab.ConsultantRating 
        ,tab.TrustTier 
        ,tab.ClientType 
        ,tab.AtRisk 
        ,tab.MarketingNeeds 
        ,tab.Region 
        ,tab.FICOScore 
        ,tab.FICOScoreDate 
        ,tab.Alerts 
        ,tab.RelationshipManager 
        ,tab.UnderwriterWith 
        ,tab.Comments 
        ,tab.PropertyManager 
        ,tab.CREComments 
        ,tab.CRMComments 
        ,tab.COIType 
        ,tab.CurrentIncome 
        ,tab.Employment 
        ,tab.EmploymentStatus 

        
        ,tab.CurrentResidenceOwnorRent 
        ,tab.DMILink 
        ,tab.FISHorizonLink 
        ,tab.PriscillaMetWithClient 
        ,tab.RegO 
        ,tab.PortfolioNumber 
        ,tab.Company_ID
        ,NULL as ParentID
        ,tab.Cust_Skey 
        ,tab.Cust_Skey_2 
        ,tab.Cust_Skey_3 
        ,tab.Cust_Skey_4 
        ,tab.Cust_Skey_5 
        ,tab.Cust_Skey_6 
        ,tab.GID 
        ,tab.SF_Cust_Key 
        ,tab.Source 
        ,null as IsEmployee
        ,null as BusinessEmail
        ,null as DateofDeath
        from
        (
            select * from vw_src_01_04
            union all
            select * from vw_src_02_04_05_06
            union all
            select * from vw_src_03
            union all
            select * from vw_src_07   
            union all
            select * from vw_src_08        
        )tab
    """
    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")
except Exception as e:
    raise e
    # logger.error("Issue while joining the base tables")

# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC select sf_cust_key
# MAGIC ,count(1) from vw_final_FA
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from vw_Final_FA

# COMMAND ----------

DestinationSchema = dbutils.widgets.get("DestinationSchema")
DestinationTable = dbutils.widgets.get("DestinationTable")
AddOnType=dbutils.widgets.get("AddOnType")

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns #get all the base columns
set_addon=df_final_FA.columns #get only the addon columns
get_pk=spark.sql(f"""select * from config.metadata where lower(DWHTableName)='customer_master' and lower(DWHSchemaName) = 'silver'""").collect()[0]['MergeKey']
set_addon.remove(get_pk) #remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date','MergeHashKey','CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col.lower() not in [ex_col.lower() for ex_col in excluded_columns]]

# COMMAND ----------

#get required columns from base table
df_base_required = spark.sql(f"select {','.join(filtered_basetable_columns)} from {DestinationSchema}.{DestinationTable} where CurrentRecord = 'Yes' ")
df_base_required.createOrReplaceTempView("vw_base")  #use this as a base table
if AddOnType == 'AddOn':
    if df_base_required.count()>0:

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
                vw_base.{get_pk} = vw_final_FA.{get_pk}
        """)
        df_final_base_with_addon.createOrReplaceTempView("vw_final_base_with_addon")
        df_final_base_with_addon.count()
    else:
        print("Else")
        df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
        count=df_final_FA.count()
        print(count)
else:
    print("Else")
    df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count=df_final_FA.count()
    print(count)

# COMMAND ----------

# Generate the concatenated string
base_without_pk=filtered_basetable_columns.copy()
base_without_pk.remove(get_pk)
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
# MAGIC select currentrecord,SF_Cust_Key, * from default.customer_master_holder
# MAGIC where sf_cust_key in (
# MAGIC 'CFE375D7-2992-4339-99DD-5B6AFD3FCC7D#1919767',
# MAGIC 'BC658069-B2B2-4FC4-952D-CF6B769CB7AB#3335213',
# MAGIC '3B04912B-EDB0-4872-9D6E-DB8396DEDF17#3386863',
# MAGIC 'B5413DBC-3E9D-4608-B461-F40E33C19644#1629388',
# MAGIC '815116C9-71BC-47C1-BA97-8D3C6968999D#1189539')
