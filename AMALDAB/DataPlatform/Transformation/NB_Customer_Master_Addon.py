# Databricks notebook source
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

# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create views for Each Column

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# DBTITLE 1,Dod
df_death = spark.sql('''
                select 
                Cust_skey, 
                to_timestamp(cast(RPDOD as string), 'yyyyMMdd') as DateofDeath
                from bronze.ods_rmpdem 
                where currentrecord = 'Yes'
                and RPDOD > 0
''')
df_death.createOrReplaceTempView("vw_death")


# COMMAND ----------

# MAGIC %sql
# MAGIC select cust_skey, count(*) from vw_death
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## BusinessEmail

# COMMAND ----------

# DBTITLE 1,AOTM Email
df_aotm_email = spark.sql('''
                        select 
                        Company_ID, 
                        CASE 
                            WHEN TRIM(Primary_Contact_Email) NOT REGEXP r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' 
                            THEN NULL 
                            ELSE TRIM(Primary_Contact_Email) 
                        END as BeB_Email  
                        from bronze.v_ods_beb_customer
                        where CurrentRecord = 'Yes'
                          ''')
df_aotm_email.createOrReplaceTempView("vw_aotm_email")

# COMMAND ----------

# DBTITLE 1,Horizon Business Email
df_horizon_business_email = spark.sql('''
select cust_skey, email as Hzn_email
from 
(SELECT 
      CASE 
        WHEN TRIM(rminet.riiadr) NOT REGEXP r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' 
        THEN NULL 
        ELSE TRIM(rminet.riiadr) 
      END AS Email,
      mast.cust_skey,
      ROW_NUMBER() OVER (PARTITION BY mast.cust_skey ORDER BY rimndt DESC) AS rownum
    FROM bronze.ods_rminet rminet
    INNER JOIN (
      SELECT Cust_Skey 
      FROM bronze.ods_rmmast 
      WHERE RMCTYP = 'N' 
        AND CurrentRecord = 'Yes' 
    ) mast ON rminet.CUST_KEY = mast.Cust_Skey
    where rminet.CurrentRecord = 'Yes'
    AND rminet.riityp = 'EML'
    )tab 

    where tab.rownum = 1
''')
df_horizon_business_email.createOrReplaceTempView("vw_horizon_business_email")

# COMMAND ----------

# DBTITLE 1,BusinessEmail VIew
df_beb_hzn_business_email =spark.sql('''
WITH RankedEmails AS (
    SELECT
        a.AccountNumber,  
        a.Cust_Skey,
        a.SF_Cust_Key,
        a.Company_ID,
        b.BeB_Email,
        c.Hzn_email,
        ROW_NUMBER() OVER (PARTITION BY a.SF_Cust_Key ORDER BY CASE WHEN b.BeB_Email IS NOT NULL THEN 1 ELSE 2 END) as rn
    FROM
        (select AccountNumber,  Cust_Skey, SF_Cust_Key , Company_ID
                           from default.customer_master_holder
                            where CurrentRecord = 'Yes' )a
    LEFT JOIN
        vw_aotm_email b ON a.Company_ID = b.Company_ID
    LEFT JOIN
        vw_horizon_business_email c ON a.Cust_Skey = c.Cust_Skey
)
SELECT
    AccountNumber,
    Cust_Skey,
    SF_Cust_Key,
    Company_ID,
    CASE
        WHEN BeB_Email IS NOT NULL THEN BeB_Email
        ELSE Hzn_email
    END AS BusinessEmail
FROM
    RankedEmails
WHERE rn = 1
''')
df_beb_hzn_business_email.createOrReplaceTempView("vw_beb_hzn_business_email")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sf_cust_key, count(*)  from vw_beb_hzn_business_email
# MAGIC group by 1 
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Employee Flag 
# MAGIC This is taken from a spreadsheet provided by HR. Once we get a live connection to ADP, we can modify this to become a more automated source.

# COMMAND ----------

# DBTITLE 1,Create Employee Flag view
from pyspark.sql.functions import regexp_replace, regexp_extract

df = spark.read.csv(employee_flag_path, header=True, inferSchema=True).withColumnRenamed('TAX ID', 'TaxId')
# Refine TaxId to contain only numbers and valid SSNs
df = df.withColumn('TaxId', regexp_extract('TaxId', r'(\d{3}-\d{2}-\d{4}|\d{9})', 0))
# Remove the '-' from the TaxId
df = df.withColumn('TaxId', regexp_replace('TaxId', '-', ''))
df.display()
df.createOrReplaceTempView("vw_employee_flag")

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Left Join to CRM.Account 

# COMMAND ----------

# DBTITLE 1,Join to crm.account
df_final_FA = spark.sql(
    """ select * from (
          select a.sf_cust_key,
          b.DateofDeath
     ,c.BusinessEmail
     ,CASE WHEN empl.TaxId IS NOT NULL THEN True ELSE False END AS IsEmployee
     ,row_number() over(partition by a.sf_cust_key order by a.ParentID desc) as rownum
from 
 default.customer_master_holder a
left join vw_death b on a.cust_skey = b.cust_skey 
left join vw_beb_hzn_business_email c on a.sf_cust_key = c.SF_Cust_Key
left join vw_employee_flag empl on a.DecryptedTaxIDNumber = empl.TaxId

Where a.CurrentRecord = 'Yes' and a.Cust_Skey !='' and a.Cust_Skey !=' ' and a.Cust_Skey is not null
)
where rownum = 1
"""
).drop("a.DateofDeath","rownum")

df_final_FA.createOrReplaceTempView("vw_final_FAvw_final_FA")
# df_final_FA.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select SF_Cust_key,count(*) from vw_final_FAvw_final_FA
# MAGIC group by 1 
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Checks And Write Table

# COMMAND ----------

# MAGIC %md
# MAGIC #Dynamic Merge

# COMMAND ----------


DestinationSchema = dbutils.widgets.get('DestinationSchema')
DestinationTable = dbutils.widgets.get('DestinationTable')
AddOnType = dbutils.widgets.get('AddOnType')

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns  # get all the base columns
set_addon = df_final_FA.columns  # get only the addon columns
get_pk = spark.sql(f"""select * from config.metadata where lower(DWHTableName)='customer_master' """).collect()[0]['MergeKey']
get_pk_temp = get_pk.split(',')  # split the get_pk
for pk in get_pk_temp:
    set_addon.remove(pk.lower().strip())  # remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date', 'MergeHashKey', 'CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col.lower() not in [ex_col.lower() for ex_col in excluded_columns]]

# COMMAND ----------

#get required columns from base table
df_base_required = spark.sql(f"select {','.join(filtered_basetable_columns)} from {DestinationSchema}.{DestinationTable} where currentrecord='Yes' ")
df_base_required.createOrReplaceTempView("vw_base")  #use this as a base table
if AddOnType == 'AddOn':
  if df_base_required.count() > 0:
      join_conditions = " and ".join([f"vw_base.{col.strip()} = vw_final_FAvw_final_FA.{col.strip()}" for col in get_pk.split(',')])

      df_final_base_with_addon = spark.sql(
          f"""
          select
              vw_base.*,
              {','.join([f'vw_final_FAvw_final_FA.{col} as {col}' for col in set_addon])}
          from 
              vw_base 
          left join 
              vw_final_FAvw_final_FA 
          on 
              {join_conditions}
      """)
      df_final_base_with_addon.createOrReplaceTempView("vw_final_base_with_addon")
      print(df_final_base_with_addon.count())
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

# MAGIC %sql
# MAGIC select SF_Cust_Key, count(1) from vw_silver
# MAGIC group by all
# MAGIC having count(*) > 1

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
# MAGIC select * from default.customer_master_holder
# MAGIC where sf_cust_key in ('DBC7A785-805C-4D99-B803-D965734D6536#2045765','08F0993A-E49D-47C3-9C00-F85E16CB37F3#718840','02B72F68-EF64-44C9-AB97-7A5C1EFF87A9#718764','66323E9F-1293-4D38-98C5-92621CE981B8#718765','C87D0A03-E13A-4F85-BEBC-DEF87D7BB2E0#780491','9538603C-C3B7-4898-9375-CDEFA3F52ADF#811607','0AC1E3B4-AA2E-4F5F-8AC9-02447864294F#1106033','C87D0A03-E13A-4F85-BEBC-DEF87D7BB2E0#3559655','9822D31D-C145-4A38-868D-2F9B0F77C232#718822','C40281D4-EF8E-46C6-9B1D-C6038225F0B8#2787429','DEE1E095-3A13-4873-914D-88934CC70CDA#3622741','B1CB862F-2DB8-418C-B8A5-D51D21FA2F67#2161042','7D75EB32-CBBD-4174-8A81-3D8110326A88#2313674','822FFF5E-993E-42F4-8B9E-364372E5056C#718818','09BAD2E6-9E47-46CB-8055-BB66F41CFE70#683051','DEE1E095-3A13-4873-914D-88934CC70CDA#2025737')
# MAGIC and currentrecord = "Yes"
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from default.customer_master_holder where CurrentRecord='Yes'
