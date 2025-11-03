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
# MAGIC ###configuration

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Catalog

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# This code initializes an error logger specific to the current batch process.
# It then logs an informational message indicating the start of the pipeline for the given batch.

ErrorLogger = ErrorLogs(f"NB_RawToSTage")
logger = ErrorLogger[0]
logger.info("Starting the pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the base view

# COMMAND ----------

base_df = spark.sql("select sf_cust_key from default.customer_master_holder where currentrecord='Yes' group by 1")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

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

# DBTITLE 1,Identify Parent ID
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
        ,tab.company_name
        ,tab.CompanyFullName_Horizon
        ,concat_ws('#',tab.GID,tab.Company_ID) SF_Cust_Key
--        ,Case when parentGID is null or (SF_Cust_Key = concat_ws('#',tab.ParentGID,tab.Company_ID)) then null else concat_ws('#',tab.ParentGID,tab.Company_ID) end as ParentID
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
            ,case when rmmast.RMNPN1 = ' ' then rmmast.RMSHRT
            when rmmast.RMNPN1 = '' then rmmast.RMSHRT else rmmast.RMNPN1 end AS CompanyFullName_Horizon
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
display(df_src_AOTM.filter("company_id = '1174919' AND ParentId is null"))
display(df_src_AOTM.filter("ParentID = 'FF75065F-D621-4C9D-AB24-64B5D8DFDEB1#1174919'"))
### Test Case select * from vw_src_AOTM where company_id = '718774';

# COMMAND ----------

# DBTITLE 1,Customer Status
basetable = spark.sql('''
SELECT master.SF_Cust_Key
,master.Company_ID
,b.ActiveAccountStatus FROM default.customer_master_holder master
LEFT JOIN (
    SELECT a.SF_Key,
    CASE WHEN (a.Open) + (a.Pending) + (a.Dormant) > 0 THEN 'Active Accounts' ELSE 'Inactive Accounts' END AS ActiveAccountStatus
    FROM (
        SELECT ACCT.SF_CustKey_fkey AS SF_Key,
        SUM(acct.Closed) AS Closed,
        SUM(acct.Purged) AS Purged,
        SUM(acct.Open) AS Open,
        SUM(acct.Dormant) AS Dormant,
        SUM(acct.Pending) AS Pending,
        SUM(acct.Other) AS Other,
        SUM(acct.Missing) AS Missing,
        SUM(acct.Total) AS Total,
        SUM(acct.Closed) + SUM(acct.Purged) + SUM(acct.Open) + SUM(acct.Dormant) + SUM(acct.Pending) + SUM(acct.Other) + SUM(acct.Missing) AS Check
        FROM (
            SELECT CASE WHEN a.status = 'CLOSED' and a.ClosingDate < '2023-01-01' THEN 1 ELSE 0 END AS Closed,
            CASE WHEN a.status = 'PURGED' THEN 1 ELSE 0 END AS Purged,
            CASE WHEN a.status IN('OPEN') THEN 1 
                WHEN a.status IN('CLOSED') AND a.ClosingDate >= '2023-01-01' THEN 1 ELSE 0 END AS Open,
            CASE WHEN a.status = 'DORMANT' THEN 1 ELSE 0 END AS Dormant,
            CASE WHEN a.status = 'PENDING' THEN 1 ELSE 0 END AS Pending,
            CASE WHEN a.status = ' ' OR a.status IS NULL THEN 1 ELSE 0 END AS Missing,
            CASE WHEN a.status NOT IN ('OPEN', 'CLOSED', 'PURGED', 'DORMANT', 'PENDING') THEN 1 ELSE 0 END AS Other,
            1 AS Total,
            b.SF_CustKey_fkey
            FROM silver.financial_account a 
            LEFT JOIN silver.financial_account_party b 
            ON a.financialaccountnumber = b.Financial_AccountNumber_skey AND UPPER(b.CurrentRecord) = 'YES'
            WHERE UPPER(a.CurrentRecord) = 'YES'
        ) acct
        GROUP BY acct.SF_CustKey_fkey
    )a
)b ON b.sf_key = master.SF_Cust_Key
WHERE master.CurrentRecord = 'Yes'
AND master.SF_Cust_Key IN (SELECT SF_CustKey_fkey FROM silver.financial_account_party WHERE CurrentRecord = 'Yes')
''')

basetable.createOrReplaceTempView("vw_Cust_Status")

# COMMAND ----------

# DBTITLE 1,Get Parent ID
df = spark.sql('''
select src.GID
,src.Company_ID
,src.Tax_ID
,src.cust_skey
,src.cust_skey_2
,src.cust_skey_3
,src.cust_skey_4
,src.cust_skey_5
,src.cust_skey_6
,src.parentGID
,src.company_name
,src.companyFullName_Horizon
,src.SF_Cust_Key
,case when stat.ActiveAccountStatus = 'Active Accounts' then src.Parentid else null end as Parentid
from vw_src_AOTM src
left join vw_Cust_Status stat on src.parentid = stat.SF_Cust_Key
''' )
df.createOrReplaceTempView("vw_src_ParentId")   
df.display()
    

# COMMAND ----------

# DBTITLE 1,LEFT JOIN TO CUSTOMER MASTER
df= spark.sql('''
    SELECT 
    CASE WHEN mast.source = '01_HZN_AOTM' AND mast.SF_Cust_Key = xref.ParentID THEN xref.Company_Name
        WHEN mast.source = '01_HZN_AOTM' AND mast.SF_Cust_Key != xref.ParentID THEN xref.CompanyFullName_Horizon
        WHEN mast.source = '01_HZN_AOTM' AND xref.parentID IS NULL THEN xref.CompanyFullName_Horizon
        WHEN mast.source != '01_HZN_AOTM' THEN mast.Name 
    ELSE mast.Name 
    END AS Name
    ,mast.FirstName 
    ,mast.LastName 
    ,mast.MiddleName 
    ,mast.CustomerType 
    ,mast.AccountNumber 
    ,mast.Officer_Key 
    ,mast.ABEmployeeID 
    ,mast.Primary_Address_Street 
    ,mast.Primary_Address_City 
    ,mast.Primary_Address_State 
    ,mast.Primary_Address_Zip 
    ,mast.Primary_Address_Country 
    ,mast.Secondary_Address_Street 
    ,mast.Secondary_Address_City 
    ,mast.Secondary_Address_State 
    ,mast.Secondary_Address_Zip 
    ,mast.Secondary_Address_Country 
    ,mast.PersonBirthdate 
    ,mast.PersonBranch 
    ,mast.PersonDoNotCall 
    ,mast.PersonEmail 
    ,mast.NumberofEmployees 
    ,mast.Fax 
    ,mast.PersonPrimary_Address_Street 
    ,mast.PersonPrimary_Address_City 
    ,mast.PersonPrimary_Address_State 
    ,mast.PersonPrimary_Address_Zip 
    ,mast.PersonPrimary_Address_Country 
    ,mast.PersonSecondary_Address_Street 
    ,mast.PersonSecondary_Address_City 
    ,mast.PersonSecondary_Address_State 
    ,mast.PersonSecondary_Address_Zip 
    ,mast.PersonSecondary_Address_Country 
    ,mast.PersonHomePhone 
    ,mast.PersonMobilePhone 
    ,mast.PersonBusinessPhone 
    ,mast.Ownership 
    ,mast.BusinessPhone 
    ,mast.Website 
    ,mast.RelationshipStartDate 
    ,mast.NAICSDescription 
    ,mast.NAICSCode 
    ,mast.Type 
    ,mast.EncryptedTaxIDNumber 
    ,mast.TaxIDNum_Last4 
    ,mast.DecryptedTaxIDNumber 
    ,mast.TinType 
    ,mast.ExternalSystem 
    ,mast.Salutation 
    ,mast.Suffix 
    ,mast.AccountSite 
    ,mast.AccountSource 
    ,mast.AnnualRevenue 
    ,mast.Assistant 
    ,mast.AsstPhone 
    ,mast.EmailOptOut 
    ,mast.FaxOptOut 
    ,mast.ConsultantRating 
    ,mast.TrustTier 
    ,mast.ClientType 
    ,mast.AtRisk 
    ,mast.MarketingNeeds 
    ,mast.Region 
    ,mast.FICOScore 
    ,mast.FICOScoreDate 
    ,mast.Alerts 
    ,mast.RelationshipManager 
    ,mast.UnderwriterWith 
    ,mast.Comments 
    ,mast.PropertyManager 
    ,mast.CREComments 
    ,mast.CRMComments 
    ,mast.COIType 
    ,mast.CurrentIncome 
    ,mast.Employment 
    ,mast.EmploymentStatus 
    ,mast.CurrentResidenceOwnorRent 
    ,mast.DMILink 
    ,mast.FISHorizonLink 
    ,mast.PriscillaMetWithClient 
    ,mast.RegO 
    ,mast.PortfolioNumber 
    ,mast.Company_ID 
    ,mast.Cust_Skey 
    ,mast.Cust_Skey_2
    ,mast.Cust_Skey_3 
    ,mast.Cust_Skey_4 
    ,mast.Cust_Skey_5 
    ,mast.Cust_Skey_6  
    ,mast.GID 
    ,mast.SF_Cust_Key 
    ,case when mast.SF_Cust_Key = xref.ParentID then null else xref.ParentID end as ParentID
    ,mast.Source
    FROM default.customer_master_holder mast 
    Left join vw_src_ParentId xref ON mast.SF_Cust_Key = xref.sf_cust_key
    where CurrentRecord='Yes'    
        ''')

df.createOrReplaceTempView("vw_parent_id_xref_master")
# display(df.filter("Company_ID = 1242176"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC Select  * from vw_parent_id_xref_master sf 
# MAGIC where sf.name is null
# MAGIC and (sf.name is null or sf.name = '' or sf.name = ' ') and customerType = 'Business'; 
# MAGIC
# MAGIC -- left join bronze.ods_rmmast mast
# MAGIC -- on sf.CUST_SKEY = mast.CUST_SKEY
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Debug - Dup Check
# MAGIC %sql
# MAGIC select sf_cust_key
# MAGIC ,count(*)
# MAGIC from vw_parent_id_xref_master
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #Preparing for SCD Type-2

# COMMAND ----------

try:
    logger.info("Joining base tables for silver customer_xref table")
    basequery = """select tab.Name 
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
        ,NULL as LengthOfRelationship --updated null so that it won't affect the Delta Load Process
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
        ,tab.Cust_Skey 
        ,tab.Cust_Skey_2 
        ,tab.Cust_Skey_3 
        ,tab.Cust_Skey_4 
        ,tab.Cust_Skey_5 
        ,tab.Cust_Skey_6 
        ,tab.GID 
        ,tab.SF_Cust_Key 
        ,tab.ParentID
        ,tab.Source
        ,null as IsEmployee
        ,null as DateofDeath
        ,null as BusinessEmail
        from
        (
            select * from vw_parent_id_xref_master   
        )tab
    """
    df_final_FA = spark.sql(basequery)
    df_final_FA = df_final_FA.dropDuplicates()
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    raise e
    # logger.error("Issue while joining the base tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Merge

# COMMAND ----------

DestinationSchema = dbutils.widgets.text("DestinationSchema",' ')
DestinationTable = dbutils.widgets.text("DestinationTable",' ')
AddOnType=dbutils.widgets.text("AddOnType",' ')
DestinationSchema = dbutils.widgets.get("DestinationSchema")
DestinationTable = dbutils.widgets.get("DestinationTable")
AddOnType = dbutils.widgets.get("AddOnType")


print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns #get all the base columns
set_addon = df_final_FA.columns #get only the addon columns
get_pk = spark.sql(f"select MergeKey from config.metadata where lower(DWHTableName)='customer_master'").collect()[0]['MergeKey']
set_addon = [col for col in set_addon if col != get_pk] #remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date', 'MergeHashKey', 'CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col not in excluded_columns]

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

spark.sql(f"insert into {DestinationSchema}.{DestinationTable}({','.join(final_col)}) select {','.join(final_col)} from vw_silver where Action_Code in ('Insert','Update')")

spark.sql(f"""
        MERGE INTO {DestinationSchema}.{DestinationTable} AS Target
        USING (SELECT {','.join(final_col)} FROM VW_silver WHERE Action_Code='Update') AS Source
        ON Target.{get_pk} = Source.{get_pk} AND Target.MergeHashKey != Source.MergeHashKey and Target.CurrentRecord = 'Yes'
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
