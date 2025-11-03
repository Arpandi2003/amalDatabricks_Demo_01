# Databricks notebook source
# MAGIC %md
# MAGIC #### Importing Packages

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
    (col('Zone') == 'silver') &
    (col('TableID') == 1007 )
)

display(DFMetadata)

# COMMAND ----------

TableID=1007
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

base_df = spark.sql("select FAAcct_NumberSF_Custkey_Key,RoleID from silver.financial_account_party where currentrecord='Yes' group by all")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FAAcct_NumberSF_Custkey_Key,RoleID ,Count(1) from silver.financial_account_party
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by FAAcct_NumberSF_Custkey_Key, ROleID Having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# MAGIC %md
# MAGIC #AOTM

# COMMAND ----------

# DBTITLE 1,Initialize_FA_Party_Xref in the default catalog  - This is the working copy
# MAGIC %sql
# MAGIC --Initialize Financial Account Party XREF Default Table -- This creates a working copy of the silver table which becomes the base table for merging
# MAGIC --select * from default.Financial_Account_Party
# MAGIC truncate table default.Financial_Account_Party;
# MAGIC --select * from silver.Financial_Account_Party;
# MAGIC --truncate table silver.Financial_Account_Party;

# COMMAND ----------

# DBTITLE 1,mod-Add All Customers from AOTM
df = spark.sql('''
        select tab.Company_ID
        ,tab.cust_skey
        ,tab.tax_id
        ,tab.rxprim
        ,tab.rownum
        from 
        (
            select beb.company_id 
            ,xref.cust_skey
            ,idm.oid Tax_ID
            ,xref.rxprim
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
            -- W--000878 add the current record flag
            left join bronze.ods_rmxref xref on bca.acct_skey = xref.ACCT_SKEY and xref.RXPRIM = 'Y' and xref.currentrecord='Yes'
    
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
--            where beb.Company_ID = '718773'
        )tab where tab.rownum = 1
        and tab.cust_skey is not null and tab.tax_id is not null
        --W-00878 exclude test accounts
        and tab.tax_id != '134920330'
        --and tab.Company_ID != '714380' -- exclude test accounts
        --and tab.cust_skey != '13-00000000028271'
        union 
        select tab.Company_ID
        ,tab.cust_skey
        ,tab.Tax_ID
        ,tab.rxprim
        ,tab.rownum
        from
        (
            select beb.Company_ID
            ,xref.cust_skey
            ,idm.oid Tax_ID
            ,xref.rxprim
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
            --W--000878 update the join to BEB
            left join 
            (
              select beb.company_id 
              ,bca.acct_skey
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
            )beb on xref.cust_skey = idm.skey and xref.acct_skey = beb.acct_skey              
            --W--000878 removed the old Join
            --inner join 
            --(
            --    select beb.Company_ID
            --    ,regexp_replace(beb.Tax_ID, '[^0-9]', '') Tax_ID 
            --    from bronze.v_ods_beb_customer beb
            --    where beb.CurrentRecord = "Yes"
            --) beb on idm.oid = beb.Tax_ID
            where xref.CurrentRecord = "Yes"
        ) tab where tab.rownum = 1  
        --W-00878 exclude test accounts
        and tab.tax_id != '134920330'        
        --and tab.Company_ID != '714380' -- exclude test accounts
        --and tab.cust_skey != '13-00000000028271'
--        and tab.company_id = '718773'      
        ''' )
df.createOrReplaceTempView("vw_cust_skey")   
df.display()


# COMMAND ----------

# DBTITLE 1,dns-Dup TaxID per Cust_skey
df = spark.sql('''
        select tab.company_id
        ,tab.cust_skey
        ,tab.tax_id
        ,tab.rxprim
        ,tab.rownum
        from 
        (
        select company_id
        ,cust_skey
        ,tax_id
        ,rxprim
        --W--000878 Add cust_skey on the partition
        ,ROW_NUMBER() OVER (PARTITION BY company_id, tax_id, cust_skey order by cust_skey asc) AS rownum
        from vw_cust_skey
        ) tab
''' )
df.createOrReplaceTempView("vw_cust_skey_SSN")   
df.display()

# COMMAND ----------

# DBTITLE 1,dns--Customer Key List by Company ID
df = spark.sql('''
select main.company_id
,main.rxprim
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
group by 1,2,3,4,5,6,7,8
''' )
df.createOrReplaceTempView("vw_cust_skey_list")   
df.display()

# COMMAND ----------

# DBTITLE 1,Mod - Identify the company in AOTM
# A company is the focus of this code and is defined as a unique record in AOTM
# The AOTM relationship for a person will be primary 

df_src_AOTM = spark.sql('''
        Select tab.GID
        ,tab.Company_ID
        ,tab.BEB_Tax_ID Tax_ID
        ,tab.cust_skey
        ,tab.cust_skey_2
        ,tab.cust_skey_3
        ,tab.cust_skey_4
        ,tab.cust_skey_5
        ,tab.cust_skey_6 
        ,tab.parentGID
        ,tab.rxprim
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
            ,xref.rxprim
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
         --W-00878 exclude test accounts
         and tab.childtaxid != '134920330'         
         --and tab.Company_ID != '714380' -- exclude test accounts
         --and tab.cust_skey != '13-00000000028271'
        ''' )
df_src_AOTM.createOrReplaceTempView("vw_src_AOTM")   
df_src_AOTM.display()
### Test Case select * from vw_src_AOTM where company_id = '718774'; 


# COMMAND ----------

# DBTITLE 1,W-000878 - vw_dup_by_GID
df = spark.sql('''
    select tab.gid
    ,tab.company_id
    ,tab.cust_skey
    ,tab.account_count
    from
    (
    select main.gid 
    ,main.company_id
    ,main.cust_skey
    ,beb.account_count
    ,row_number() over(partition by main.gid order by beb.account_count desc ) as rownum
    from vw_src_AOTM main
    left join 
    (
        select Company_ID
            , count(distinct Account_Number) as account_count
        from bronze.`ods_beb_customer-account`
        where CurrentRecord = "Yes"
        group by Company_ID  
    ) beb on main.company_id = beb.company_id
    where main.gid is not null
    ) tab where tab.rownum = 1
    ''' )
df.createOrReplaceTempView("vw_dup_by_GID")   
df.display()

# COMMAND ----------

# DBTITLE 1,Get Account Info
df_src_AOTM_Acct = spark.sql('''
        select tab.GID
        ,tab.Company_ID
        ,tab.Tax_ID
        ,tab.cust_skey
        ,tab.cust_skey_2
        ,tab.cust_skey_3
        ,tab.cust_skey_4
        ,tab.cust_skey_5
        ,tab.cust_skey_6        
        ,tab.SF_CustKey_fkey
        ,tab.ParentID        
        ,tab.Financial_AccountNumber_skey 
        ,tab.FAAcct_NumberSF_Custkey_Key
        ,tab.rxprim PrimaryAccountKey
        --else tab.ParentID end as PrimaryAccountKey
        --,CASE 
        --        WHEN tab.source = '01_HZN_AOTM' AND tab.SF_CustKey_fkey = tab.ParentID THEN "Y"
        --        WHEN tab.source = '01_HZN_AOTM' AND tab.SF_CustKey_fkey != tab.ParentID THEN ""
        --        WHEN tab.source = '01_HZN_AOTM' AND tab.parentID IS NULL THEN ""
        --        WHEN tab.source != '01_HZN_AOTM' THEN tab.PrimaryAccountKey 
        --ELSE 
        --        tab.PrimaryAccountKey
        --END AS PrimaryAccountKey
        ,tab.RoleID
        ,tab.Source
        from
        ( 
                select src.GID
                ,src.Company_ID
                ,src.Tax_ID
                ,src.cust_skey
                ,src.cust_skey_2
                ,src.cust_skey_3
                ,src.cust_skey_4
                ,src.cust_skey_5
                ,src.cust_skey_6    
                --W-000878 update next statement            
                ,src.SF_CustKey_fkey
                ,src.ParentID                
                ,xref.ACCT_SKEY as Financial_AccountNumber_skey 
                ,xref.RXPRIM 
                ,coalesce(rel.Relationship_Type_Description,"Business") RoleID
                --W-000878 update 3 next statements
                ,concat_ws('#',xref.ACCT_SKEY,src.SF_CustKey_fkey) FAAcct_NumberSF_Custkey_Key
                ,src.Source
                ,ROW_NUMBER() OVER (PARTITION BY xref.ACCT_SKEY,src.SF_CustKey_fkey order by src.SF_CustKey_fkey desc) as rownum  
                --W-000878 source from vw_src_AOTM changed to select-union statement
                from 
                ( 
                        select src.GID
                        ,src.Company_ID
                        ,src.Tax_ID
                        ,src.cust_skey
                        ,src.cust_skey_2
                        ,src.cust_skey_3
                        ,src.cust_skey_4
                        ,src.cust_skey_5
                        ,src.cust_skey_6                
                        ,src.SF_Cust_Key SF_CustKey_fkey
                        ,src.ParentID           
                        ,'01_HZN_AOTM' Source
                        from vw_src_AOTM src
                        inner join vw_dup_by_GID dup on src.gid = dup.gid and src.company_id = dup.company_id
                        union    
                        --
                        select src.GID
                        ,src.Company_ID
                        ,src.Tax_ID
                        ,src.cust_skey
                        ,src.cust_skey_2
                        ,src.cust_skey_3
                        ,src.cust_skey_4
                        ,src.cust_skey_5
                        ,src.cust_skey_6                
                        ,src.SF_Cust_Key SF_CustKey_fkey
                        ,src.ParentID           
                        ,'01_HZN_AOTM' Source
                        from vw_src_AOTM src
                        where (src.gid) not in (select gid from vw_dup_by_GID)    
                )src
                left join bronze.ods_rmxref xref on src.cust_skey = xref.CUST_SKEY and xref.CurrentRecord = "Yes"
                left join bronze.fi_core_relationship rel  on xref.REL_KEY = rel.Relationship_Key and rel.CurrentRecord = "Yes"
        )tab
        where tab.rownum = 1
        ''' )
df_src_AOTM_Acct.createOrReplaceTempView("vw_src_AOTM_Acct")   
df_src_AOTM_Acct.display()

# COMMAND ----------

# DBTITLE 1,Insert Records to XREF from AOTM
df = spark.sql('''
              MERGE INTO  default.financial_account_party  AS Target
              USING (SELECT * FROM vw_src_AOTM_Acct) AS Source
              
              ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key
              
              WHEN NOT MATCHED
              THEN INSERT
              (company_id ,cust_skey ,cust_skey_2, cust_skey_3, cust_skey_4, cust_skey_5, cust_skey_6 ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey,Tax_ID)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.cust_skey_2 ,Source.cust_skey_3 ,Source.cust_skey_4 ,Source.cust_skey_5 ,Source.cust_skey_6, Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey,source.Tax_ID) 
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC --#validation test for dups
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql
# MAGIC select source
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC order by Source

# COMMAND ----------

# DBTITLE 1,Horizon Commercial Accounts
# df_src_Hrz_Comm = spark.sql('''
#         select tab.GID
#         ,tab.Company_ID
#         ,tab.Tax_ID
#         ,tab.cust_skey
#         ,tab.SF_CustKey_fkey
#         ,tab.Financial_AccountNumber_skey 
#         ,tab.FAAcct_NumberSF_Custkey_Key
#         ,tab.PrimaryAccountKey
#         ,tab.RoleID
#         ,tab.Source
#         from
#         ( 
#                 select src.GID
#                 ,src.Company_ID
#                 ,src.Tax_ID
#                 ,src.cust_skey
#                 ,src.SF_Cust_Key SF_CustKey_fkey
#                 ,xref.ACCT_SKEY as Financial_AccountNumber_skey 
#                 ,xref.RXPRIM PrimaryAccountKey
#                 ,coalesce(rel.Relationship_Type_Description,"Business") RoleID
#                 ,concat_ws('#',xref.ACCT_SKEY,src.SF_Cust_Key) FAAcct_NumberSF_Custkey_Key
#                 ,'01_HZN_Commercial' Source
#                 ,ROW_NUMBER() OVER (PARTITION BY xref.ACCT_SKEY,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
#                 from vw_src_AOTM src
#                 left join bronze.ods_rmxref xref on src.cust_skey = xref.CUST_SKEY and xref.CurrentRecord = "Yes"
#                 left join bronze.fi_core_relationship rel  on xref.REL_KEY = rel.Relationship_Key and rel.CurrentRecord = "Yes"
#         )tab
#         where tab.rownum = 1
#         ''' )
# df_src_Hrz_Comm.createOrReplaceTempView("vw_src_Hrz_Comm_Acct")   


# COMMAND ----------

# DBTITLE 1,Insert Records for Horizon Commercial Accounts

# df = spark.sql('''
#               MERGE INTO  default.financial_account_party  AS Target
#               USING (SELECT * FROM vw_src_Hrz_Comm_Acct) AS Source
#               ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key
              
#               WHEN NOT MATCHED
#               THEN INSERT
#               (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey,Tax_ID)
#               VALUES
#               (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey,source.Tax_ID) 
#              ''')

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# %sql
# --#validation test for dups
# select FAAcct_NumberSF_Custkey_Key, ROleID
# ,count(1) 
# from default.financial_account_party
# group by all
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# %sql
# select source
# ,count(1) 
# from default.financial_account_party
# group by 1
# having count(*) > 1
# order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC # Global Plus

# COMMAND ----------

# DBTITLE 1,Preparing to Merge GP Company with SF Account
# Create a lookup using the Taxid from the account table - connect to rmmast_ssn to get the cust_skey
# Use Cust key to get GID froom the silver.aotm_hznXref table 
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
                    --W-00878 exclude test accounts
                    and tax_id != '134920330'                       
                ''');
acct.createOrReplaceTempView("vw_acct")

gp_exists_in_Hzn = spark.sql('''
                             select * from (
                             select hzn.gid, hzn.company_id, hzn.cust_skey, hzn.FAAcct_NumberSF_Custkey_Key, hzn.SF_CustKey_fkey
                             ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
                    from default.financial_account_party hzn
                    inner join vw_acct acct 
                    on hzn.gid = acct.gid
                    --where hzn.cust_skey is not null
                    )
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

GP_Accts = spark.sql('''
select tab.tax_id 
,tab.GID
,tab.sf_custkey_fkey
,tab.TAXID
,tab.Financial_AccountNumber_skey
,concat_ws ('#',tab.Financial_AccountNumber_skey,tab.sf_custkey_fkey) FAAcct_NumberSF_Custkey_Key
,"Y" PrimaryAccountKey
,"Business" RoleID
from
(
  select src.tax_id 
  ,src.GID
  ,case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key  end sf_custkey_fkey
  ,gpa.TAXID
  ,"13-GP-" || lpad(cast(replace(GP.portfolio, '.', '') as string), 20, '0') Financial_AccountNumber_skey
  ,ROW_NUMBER() OVER (PARTITION BY "13-GP-" || lpad(cast(replace(GP.portfolio, '.', '') as string), 20, '0'),src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
  from vw_acct src
  left join vw_gp_exists_in_Hzn vw on src.GID = vw.gid
  inner join bronze.account gpa on src.tax_id = REGEXP_REPLACE(gpa.TAXID, '[^0-9]', '') and gpa.CurrentRecord = "Yes"
  inner join bronze.portfolio gp on gp.account = gpa.INTERNAL and gp.CurrentRecord = "Yes"
) tab
where tab.rownum = 1
''')
GP_Accts.createOrReplaceTempView("vw_GP_Accts")

# COMMAND ----------

# DBTITLE 1,Insert Unmatched GP Records into default.customer_xref
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING ( SELECT'' as company_id
                      ,'' as cust_skey 
                      , GID
                      , SF_CustKey_fkey
                      , Financial_AccountNumber_skey
                      , FAAcct_NumberSF_Custkey_Key                          
                      ,'03_Global_Plus' as Source 
                      , RoleID
                      , PrimaryAccountKey
                      , taxid
                      --,ROW_NUMBER() OVER (PARTITION BY GID,Financial_AccountNumber_skey order by GID desc) AS rownum
                      from vw_GP_Accts
                    ) AS Source
                ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key

              WHEN NOT MATCHED THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey, Tax_ID)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.taxid) 
             ''')



# COMMAND ----------

# DBTITLE 1,Debug - Check for nulls in vw
# MAGIC %sql 
# MAGIC -- select * from vw_gp_not_exist_in_Hzn where sf_cust_key is null; 
# MAGIC SELECT'' as company_id
# MAGIC                             ,'' as cust_skey 
# MAGIC                             , GID
# MAGIC                             , SF_Cust_key
# MAGIC                             ,'03_Global_Plus' as Source 
# MAGIC                             ,ROW_NUMBER() OVER (PARTITION BY GID order by GID desc) AS rownum
# MAGIC                             from vw_gp_not_exist_in_Hzn
# MAGIC
# MAGIC -- delete from default.customer_xref where SF_Cust_Key is null; 

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key,RoleID,COUNT(*)
# MAGIC from default.financial_account_party
# MAGIC GROUP BY all
# MAGIC HAVING count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql
# MAGIC select source, count(*) from default.financial_account_party group by 1 
# MAGIC order by Source
# MAGIC --select * from default.customer_xref group by 1 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Small Business

# COMMAND ----------

# DBTITLE 1,Preparing to Merge Small Business  - No Personal accounts are added
##  Modify query so that identifies the customers who do not have aotm 
## Insert those records silver.GP_AOTM_Bus_RMMAST using Cell 25 as an example 
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
    left join bronze.ods_rmxref xref on tr.AccountLKey = xref.ACCT_SKEY --and xref.rxprim = "Y" 
    and xref.CurrentRecord = 'Yes' --and xref.REL_CODE = 'A'
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
--W-00878 exclude test accounts
and tab.OID != '134920330'
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
    from default.financial_account_party hzn
    inner join vw_SB_Account acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
where rownum = 1
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

SB_Acct = spark.sql('''
select tab.GID
,tab.Company_ID
,tab.Tax_ID
,tab.cust_skey
,tab.SF_CustKey_fkey
,tab.Financial_AccountNumber_skey
,tab.FAAcct_NumberSF_Custkey_Key  
,tab.Source
,case 
        when tab.rxprim = 'Y' then 'Y' 
        when tab.rxprim = 'N' then 'N' 
end PrimaryAccountKey
,tab.RoleID
from
(
  select src.company_id
  ,src.cust_skey
  ,src.GID
  ,src.Tax_ID
  ,src.Source
  ,src.SF_Cust_key SF_CustKey_fkey
  ,xref.ACCT_SKEY Financial_AccountNumber_skey
  ,xref.RXPRIM 
  ,rel.Relationship_Type_Description RoleID
  ,concat_ws('#',xref.ACCT_SKEY, src.SF_Cust_Key) FAAcct_NumberSF_Custkey_Key  
  ,ROW_NUMBER() OVER (PARTITION BY xref.ACCT_SKEY,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
  from vw_SB_not_exist_in_Hzn src
  inner join bronze.ods_rmxref xref on src.cust_skey = xref.CUST_SKEY and xref.CurrentRecord = "Yes" 
  left join bronze.fi_core_relationship rel  on xref.REL_KEY = rel.Relationship_Key and rel.CurrentRecord = "Yes"
  where src.gid is not null
)tab
where tab.rownum = 1
''')
SB_Acct.createOrReplaceTempView("vw_SB_Acct")  
SB_Acct.display()

# COMMAND ----------

# DBTITLE 1,Insert Unmatched SB Records into .default.customer_xref
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING (SELECT *  FROM vw_SB_Acct) AS Source
              ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey ,Tax_ID)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.Tax_ID) 

             ''')


# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql
# MAGIC select source, count(*) from default.financial_account_party group by 1 order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #Personal Customer

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
        --W-00878 exclude test accounts
        and tab.tax_id != '134920330'             
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
    from default.financial_account_party hzn
    inner join vw_src_Type_P acct 
    on hzn.gid = acct.gid
    where hzn.cust_skey is not null
)
where rownum = 1
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
select null as Company_Id
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
--W-00878 exclude test accounts
and final.tax_id != '134920330'
''')
CustomerDupByGID.createOrReplaceTempView("vw_CustomerDupByGID")

# COMMAND ----------

CustomerDupByGID_Acct = spark.sql('''
    select tab.GID
    ,tab.Company_ID
    ,tab.Tax_ID
    ,tab.cust_skey
    ,tab.SF_CustKey_fkey
    ,tab.Financial_AccountNumber_skey
    ,tab.FAAcct_NumberSF_Custkey_Key
    ,tab.source  
    ,case 
            when tab.rxprim = 'Y' then 'Y' 
            when tab.rxprim = 'N' then 'N' 
    end PrimaryAccountKey    
    ,tab.RoleID
    from
    (
      select src.GID
      ,null as Company_ID
      ,null as Tax_ID
      ,src.cust_skey
      ,src.SF_Cust_Key SF_CustKey_fkey
      ,xref.ACCT_SKEY Financial_AccountNumber_skey
      ,src.source
      ,concat_ws('#',xref.ACCT_SKEY, src.SF_Cust_Key) FAAcct_NumberSF_Custkey_Key  
      ,xref.RXPRIM 
      ,rel.Relationship_Type_Description RoleID
      ,ROW_NUMBER() OVER (PARTITION BY xref.ACCT_SKEY,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
      from vw_CustomerDupByGID src
      left join bronze.ods_rmxref xref on src.cust_skey = xref.CUST_SKEY and xref.CurrentRecord = "Yes"
      left join bronze.fi_core_relationship rel  on xref.REL_KEY = rel.Relationship_Key and rel.CurrentRecord = "Yes"
      where src.gid is not null
    )tab
    where tab.rownum = 1
    ''')
CustomerDupByGID_Acct.createOrReplaceTempView("vw_CustomerDupByGID_Acct")      

# COMMAND ----------

# DBTITLE 1,Merge CustomerDupByGID
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING (SELECT * FROM vw_CustomerDupByGID_Acct) AS Source
                ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey, Tax_ID)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.Tax_ID) 

             ''')

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql 
# MAGIC SELECT source, count(*) FROM default.financial_account_party
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
--W-00878 exclude test accounts
and Tax_ID != '134920330'
''')
DistinctCustomers.createOrReplaceTempView("vw_DistinctCustomers")

# COMMAND ----------

# DBTITLE 1,vw_distinct_Customers
DistinctCustomersAcct = spark.sql('''
    select tab.GID
    ,tab.Company_ID
    ,tab.Tax_ID
    ,tab.cust_skey
    ,tab.SF_CustKey_fkey
    ,tab.Financial_AccountNumber_skey
    ,case 
            when tab.rxprim = 'Y' then 'Y' 
            when tab.rxprim = 'N' then 'N' 
    end PrimaryAccountKey
    ,tab.RoleID
    ,tab.FAAcct_NumberSF_Custkey_Key  
    ,tab.source
    from
    (
      select src.GID
      ,null as Company_ID
      ,null as Tax_ID
      ,src.cust_skey
      ,src.SF_Cust_Key SF_CustKey_fkey
      ,xref.ACCT_SKEY Financial_AccountNumber_skey
      ,xref.RXPRIM 
      ,rel.Relationship_Type_Description RoleID
      ,src.source
      ,concat_ws('#',xref.ACCT_SKEY, src.SF_Cust_Key) FAAcct_NumberSF_Custkey_Key  
      ,ROW_NUMBER() OVER (PARTITION BY xref.ACCT_SKEY,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
      from vw_DistinctCustomers src
      left join bronze.ods_rmxref xref on src.cust_skey = xref.CUST_SKEY and xref.CurrentRecord = "Yes"
      left join bronze.fi_core_relationship rel  on xref.REL_KEY = rel.Relationship_Key and rel.CurrentRecord = "Yes"
      where src.gid is not null
    )tab
    where tab.rownum = 1
    ''')
DistinctCustomersAcct.createOrReplaceTempView("vw_DistinctCustomersAcct")  

# COMMAND ----------

# DBTITLE 1,Merge DistinctCustomers
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING (SELECT * FROM vw_DistinctCustomersAcct) AS Source
              ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey ,Tax_ID)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.Tax_ID) 
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql 
# MAGIC SELECT source, count(*) FROM default.financial_account_party
# MAGIC group by Source
# MAGIC order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC # Osaic

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
--W-00878 exclude test accounts
and a.tax_id != '134920330'
''') 
source_osaic.createOrReplaceTempView("vw_src_Type_O")

osaic_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,hzn.FAAcct_NumberSF_Custkey_Key
    ,hzn.SF_CustKey_fkey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.financial_account_party hzn
    inner join vw_src_Type_O acct 
    on hzn.gid = acct.gid
    --where hzn.cust_skey is not null
)
where rownum = 1
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

# DBTITLE 1,Osaic FInancial Accounts
source_osaic_acct = spark.sql('''
    select tab.GID
    ,tab.Company_ID
    ,tab.Tax_ID
    ,tab.cust_skey
    ,tab.SF_CustKey_fkey
    ,tab.Financial_AccountNumber_skey
    ,tab.FAAcct_NumberSF_Custkey_Key  
    ,tab.source
    ,"Y" PrimaryAccountKey
    ,tab.RoleID
    ,tab.rownum
    from
    (
      select src.GID
      ,null as Company_ID
      ,src.Tax_ID Tax_ID
      ,src.cust_skey
      ,case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end SF_CustKey_fkey
      ,"13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0") Financial_AccountNumber_skey
      ,src.source
      ,"Owner" as RoleID
      ,concat_ws('#',"13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0"), case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end) FAAcct_NumberSF_Custkey_Key  
      ,ROW_NUMBER() OVER (PARTITION BY "13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0"),src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
      from vw_src_Type_O src
      left join vw_osaic_exists_in_Hzn vw on src.GID = vw.gid
      inner join bronze.account_osaic os on src.tax_id = REGEXP_REPLACE(os.SSN_Tax_ID, '[^0-9]', '') and os.CurrentRecord = "Yes"
      --left join bronze.householdaccount_osaic hho on hho.Account_Unique_ID = os.Account_Unique_ID and hho.Contact_Is_Account_Owner = 'Y' and hho.CurrentRecord = "Yes"
      --left join bronze.household_osaic ho on hho.Contact_Id = ho.Contact_Id and ho.CurrentRecord = "Yes"
      left join bronze.accountmapping_osaic mapping on mapping.Unique_Id = os.Account_Unique_ID and mapping.CurrentRecord = 'Yes'
      where src.gid is not null
    ) tab
    where tab.rownum = 1
''') 
source_osaic_acct.createOrReplaceTempView("vw_src_Type_O_acct")    


# COMMAND ----------

# DBTITLE 1,Osaic Merge
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING (SELECT * FROM vw_src_Type_O_acct) AS Source
              ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey ,tax_id)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.Tax_ID) 
             ''')
             

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql
# MAGIC select source, count(*) from default.financial_account_party group by 1 order by Source

# COMMAND ----------

# MAGIC %md
# MAGIC #DMI

# COMMAND ----------

# DBTITLE 1,DMI Customers
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
--W-00878 exclude test accounts
and a.tax_id != '134920330'
 ''') 
source_dmi.createOrReplaceTempView("vw_src_Type_dmi")

dmi_exists_in_Hzn = spark.sql('''
select * 
from 
(
    select hzn.gid
    ,hzn.company_id
    ,hzn.cust_skey
    ,hzn.FAAcct_NumberSF_Custkey_Key
    ,hzn.SF_CustKey_fkey
    ,ROW_NUMBER() OVER (PARTITION BY hzn.GID order by hzn.GID desc) AS rownum
    from default.financial_account_party hzn
    inner join vw_src_Type_dmi acct 
    on hzn.gid = acct.gid 
    where hzn.cust_skey is not null
)
where rownum = 1
''');
dmi_exists_in_Hzn.createOrReplaceTempView("vw_dmi_exists_in_Hzn")

dmi_not_exist_in_Hzn = spark.sql('''
select null as Company_Id
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

# %sql
# select * from vw_dmi_exists_in_Hzn
# --where tax_id  = '068780501'
# where gid = 'C5393EF3-881B-4BEB-8C81-9631A27DC60C'

# COMMAND ----------

# DBTITLE 1,DMI Financial Accounts
source_dmi_acct = spark.sql('''
    select tab.GID
    ,tab.Company_ID
    ,tab.Tax_ID
    ,tab.cust_skey
    ,tab.SF_CustKey_fkey
    ,tab.Financial_AccountNumber_skey
    ,tab.FAAcct_NumberSF_Custkey_Key  
    ,tab.source
    ,tab.PrimaryAccountKey
    ,tab.RoleID
    ,tab.rownum
    from
    (
      select src.GID
      ,null as Company_ID
      ,src.Tax_ID Tax_ID
      ,src.cust_skey
      ,case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end SF_CustKey_fkey
      ,dmi.Financial_AccountNumber_skey
      ,src.source
      ,"Y" PrimaryAccountKey
      ,"Borrower" RoleID
      ,concat_ws('#',dmi.Financial_AccountNumber_skey, case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end) FAAcct_NumberSF_Custkey_Key  
      ,ROW_NUMBER() OVER (PARTITION BY dmi.Financial_AccountNumber_skey,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
      from vw_src_Type_dmi src
      left join vw_dmi_exists_in_Hzn vw on src.GID = vw.gid
      inner join 
      (
        select REGEXP_REPLACE(dmi.Mortgagor_SSN, '[^0-9]', '') Tax_ID
        ,case when (dmi.Investor_ID = "40H" and dmi.Category_Code = "003") then "13-LN-" || lpad(cast(dmi.Loan_Number as string),20,"0")
              when (dmi.Investor_ID = "40H" and dmi.Category_Code = "002") then "13-ML-" || lpad(cast(dmi.Loan_Number as string),20,"0")
            end Financial_AccountNumber_skey
        from bronze.dmi_dcif dmi 
        where dmi.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
        and dmi.CurrentRecord = "Yes"

      ) dmi on src.Tax_ID = dmi.Tax_ID
      where src.gid is not null
      union all
      select src.GID
      ,null as Company_ID
      ,src.Tax_ID Tax_ID
      ,src.cust_skey
      ,case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end SF_CustKey_fkey
      ,dmi.Financial_AccountNumber_skey
      ,src.source
      ,null as PrimaryAccountInd
      ,"Co-Borrower" RoleID
      ,concat_ws('#',dmi.Financial_AccountNumber_skey, case when src.gid = vw.gid then vw.SF_CustKey_fkey else src.sf_cust_key end) FAAcct_NumberSF_Custkey_Key  
      ,ROW_NUMBER() OVER (PARTITION BY dmi.Financial_AccountNumber_skey,src.SF_Cust_Key order by src.SF_Cust_Key desc) as rownum
      from vw_src_Type_dmi src
      left join vw_dmi_exists_in_Hzn vw on src.GID = vw.gid
      inner join 
      (
        select REGEXP_REPLACE(dmi.Co_Mortgagor_SSN, '[^0-9]', '') Tax_ID
        ,case when (dmi.Investor_ID = "40H" and dmi.Category_Code = "003") then "13-LN-" || lpad(cast(dmi.Loan_Number as string),20,"0")
              when (dmi.Investor_ID = "40H" and dmi.Category_Code = "002") then "13-ML-" || lpad(cast(dmi.Loan_Number as string),20,"0")
            end Financial_AccountNumber_skey
        from bronze.dmi_dcif dmi 
        where dmi.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
        and dmi.CurrentRecord = "Yes"
      ) dmi on src.Tax_ID = dmi.Tax_ID
      where src.gid is not null
    ) tab
    where tab.rownum = 1
     ''') 
source_dmi_acct.createOrReplaceTempView("vw_src_Type_dmi_acct")

# COMMAND ----------

# DBTITLE 1,DMI Merge
df = spark.sql('''
              MERGE INTO  default.financial_account_party AS Target
              USING (SELECT * FROM vw_src_Type_dmi_acct) AS Source
              ON Source.FAAcct_NumberSF_Custkey_Key = Target.FAAcct_NumberSF_Custkey_Key
              

              WHEN NOT MATCHED 
              THEN INSERT
              (company_id ,cust_skey ,gid ,sf_custkey_fkey ,source ,FAAcct_NumberSF_Custkey_Key ,Financial_AccountNumber_skey ,RoleID ,PrimaryAccountKey ,tax_id)
              VALUES
              (Source.company_id ,Source.cust_skey ,Source.GID ,SOURCE.sf_custkey_fkey ,source.Source ,Source.FAAcct_NumberSF_Custkey_Key ,Source.Financial_AccountNumber_skey ,Source.RoleID ,Source.PrimaryAccountKey ,source.tax_id)        
             ''')

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key, RoleID
# MAGIC ,count(1) 
# MAGIC from default.financial_account_party
# MAGIC group by all
# MAGIC having count(*) > 1 

# COMMAND ----------

# DBTITLE 1,Debug - Merge Result
# MAGIC %sql 
# MAGIC select source, count(*) from default.financial_account_party group by 1 order by 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - NULL Check
# MAGIC %sql
# MAGIC select * from default.financial_account_party
# MAGIC where FAAcct_NumberSF_Custkey_Key is null

# COMMAND ----------

# MAGIC %md
# MAGIC #Build Financial Account Party Table

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing Merge Operations into the target table

# COMMAND ----------

# DBTITLE 1,Prep Data from FA Party for SCD
try:
    logger.info("Joining base tables for silver financial_account_party table")
    basequery = """select tab.Financial_AccountNumber_skey
        ,tab.RoleID
        ,tab.PrimaryAccountKey
        ,tab.IsRoleActive
        ,tab.RoleStartDate
        ,tab.RoleEndDate
        ,tab.FAAcct_NumberSF_Custkey_Key
        ,tab.SF_CustKey_fkey
        ,tab.Source As SourceSystem
        from
        (
            select Financial_AccountNumber_skey
            ,RoleID
            ,PrimaryAccountKey
            ,IsRoleActive
            ,RoleStartDate
            ,RoleEndDate
            ,FAAcct_NumberSF_Custkey_Key
            ,SF_CustKey_fkey
            ,Source
            from default.financial_account_party
            where substring(FINANCIAL_ACCOUNTNUMBER_SKEY,1,5) not in ('13-02' ,'13-03','13-04','13-05','13-BX','13-Z9','13-CA','13-CT','13-CP') 
            --W-000878 remove test accounts
            and financial_accountnumber_skey not in (select account_number from default.TestAccounts)                    
        )tab
    """
    df_final_FA=spark.sql(basequery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    logger.error("Issue while joining the base tables")

# COMMAND ----------

# DBTITLE 1,dup check
# MAGIC %sql
# MAGIC Select count(1) from vw_final_FA

# COMMAND ----------

# DBTITLE 1,Dupes Check
# MAGIC %sql
# MAGIC select FAAcct_NumberSF_Custkey_Key,RoleID
# MAGIC ,count(1) from vw_final_FA
# MAGIC group by all
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dynamic Merge Logic

# COMMAND ----------

DestinationSchema = dbutils.widgets.text('DestinationSchema',' ')
DestinationTable = dbutils.widgets.text('DestinationTable',' ')
AddOnType = dbutils.widgets.text('AddOnType',' ')
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

# DBTITLE 1,dupcheck
# MAGIC %sql
# MAGIC select * from VW_silver 
# MAGIC where financial_accountnumber_skey in(select financial_accountnumber_skey from VW_silver where currentrecord='Yes' group by mergehashkey, financial_accountnumber_skey having count(1)>1)

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
# MAGIC Select FAAcct_NumberSF_Custkey_Key,RoleID, count(1) from silver.financial_account_party
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by FAAcct_NumberSF_Custkey_Key, RoleID having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from silver.financial_account_party
