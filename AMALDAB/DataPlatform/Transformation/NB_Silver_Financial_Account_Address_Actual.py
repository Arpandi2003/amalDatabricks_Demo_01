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
# MAGIC #### Calling DataQuality Check

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
    (col('TableID') == 1013)
)

display(DFMetadata)

# COMMAND ----------

TableID=1013
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

# DBTITLE 1,Base view
base_df = spark.sql("select FinancialAccountNumber,AddressLine from silver.financial_account_address where currentrecord='Yes' group by FinancialAccountNumber,AddressLine ")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber,AddressLine, Count(1) from silver.financial_account_address
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by FinancialAccountNumber, AddressLine Having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery="""
            select tab.AccountNumber
            ,tab.FinancialAccountNumber
            ,tab.SourceSystem
            ,tab.AppType
            ,tab.AddressLine
            ,tab.City
            ,tab.State
            ,tab.ZipCode
            ,tab.Type 

            from 
            (
                select h360.AccountNumber
                ,h360.FinancialAccountNumber
                ,h360.SourceSystem
                ,h360.AppType
                ,prop.AddressLine1 AddressLine
                ,prop.City
                ,prop.State
                ,prop.ZipCode
                ,prop.Type
                from
                ( 
                    select ln.LMACCT AccountNumber
                    ,ln.ACCT_SKEY FinancialAccountNumber
                    ,"Horizon" SourceSystem
                    ,"LN" AppType
                    from bronze.ods_ln_master ln
                    left join bronze.ods_lnm2ac ac on ln.ACCT_SKEY = ac.ACCT_SKEY
                    where ln.CurrentRecord = "Yes"
                    and ln.LMPART = 0
                    union all
                    select MBACCT AccountNumber
                    ,ACCT_SKEY FinancialAccountNumber
                    ,"Horizon" SourceSystem
                    ,"ML" AppType
                    from bronze.ods_ml_master
                    where CurrentRecord = "Yes"
                ) 
                h360
                left join bronze.ods_ctactrel trel on h360.AppType = trel.CRAPPL2 
                and lpad(cast(h360.AccountNumber as string), 14, '0') = trel.CRID2 and trel.CurrentRecord = "Yes"
                left join 
                (
                    select CO_SKEY
                    ,case when prop.`CPADR#` > '' AND prop.CPADR1 > '' THEN concat(prop.`CPADR#`, ' ', prop.CPADR1)
                    when prop.`CPADR#` > '' AND prop.CPADR1 = '' THEN prop.`CPADR#`
                    when prop.`CPADR#` = '' AND prop.CPADR1 > '' THEN prop.CPADR1 END AddressLine1
                    ,prop.CPADR2 AddressLine2
                    ,prop.CPCITY City
                    ,prop.CPSTAT State
                    ,CASE WHEN prop.CPZIP4 = '' THEN prop.CPZIP5 ELSE concat(prop.CPZIP5,'-', prop.CPZIP4) END ZipCode
                    ,"Collateral Address" Type
                    from bronze.ods_ctprop prop
                    where prop.CurrentRecord = "Yes"
                ) prop on trel.CO_SKEY = prop.CO_SKEY 
                where prop.AddressLine1 is not null
                union all
                select dcifLN.Loan_Number AccountNumber
                ,"13-LN-" || lpad(cast(dcifLN.Loan_Number as string),20,"0") FinancialAccountNumber
                ,"DMI" as SourceSystem
                ,prod.Sub_Application_Code AppType
                ,dcifLN.Property_Street_Address AddressLine
                ,dcifLN.Property_City City
                ,dcifLN.Property_State_Code State
                ,dcifLN.Property_Zip_Code ZipCode   
                ,"Collateral Address" Type              
                from bronze.dmi_dcif dcifLN
                left join bronze.ods_ln_master ln on dcifLN.Old_Loan_Number = ln.LMACCT and ln.LMPART = 0 and ln.CurrentRecord = "Yes"
                left join bronze.fi_core_product prod on ln.PROD_SKEY = prod.Product_Key and prod.CurrentRecord = "Yes"
                where dcifLN.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcifLN.CurrentRecord = "Yes"
                and dcifLN.Investor_ID = "40H"
                and dcifLN.Category_Code = "003"
                union all
                select dcifML.Loan_Number AccountNumber
                ,"13-ML-" || lpad(cast(dcifML.Loan_Number as string),20,"0") FinancialAccountNumber
                ,"DMI" as SourceSystem
                ,prod.Sub_Application_Code AppType
                ,dcifML.Property_Street_Address AddressLine
                ,dcifML.Property_City City
                ,dcifML.Property_State_Code State
                ,dcifML.Property_Zip_Code ZipCode   
                ,"Collateral Address" Type              
                from bronze.dmi_dcif dcifML
                left join bronze.ods_ml_master ml on dcifML.Old_Loan_Number = ml.MBACCT and ml.CurrentRecord = "Yes"
                left join bronze.fi_core_product prod on ml.PROD_SKEY = prod.Product_Key and prod.CurrentRecord = "Yes"
                where dcifML.As_of_Date = (select max(As_of_Date) from bronze.dmi_dcif)
                and dcifML.CurrentRecord = "Yes"
                and dcifML.Investor_ID = "40H"
                and dcifML.Category_Code = "002"    
            )tab

            """

    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA = df_final_FA.dropDuplicates()
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Source Dupes Check
# MAGIC %sql
# MAGIC Select Count(1),financialaccountnumber,addressline from vw_final_FA 
# MAGIC Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,Source Count Check
# MAGIC %sql
# MAGIC Select count(1) from vw_final_FA

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
set_addon = df_final_FA.columns  # get only the addon columns
get_pk = spark.sql(f"SELECT MergeKey FROM config.metadata WHERE LOWER(TRIM(DWHTableName)) = '{DestinationTable.lower()}' and lower(TRIM(DWHSchemaName))='{DestinationSchema.lower()}'").collect()[0]['MergeKey']
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
# MAGIC Select FinancialAccountNumber, AddressLine, count(1) from silver.financial_account_address
# MAGIC where CurrentRecord='Yes'
# MAGIC group by FinancialAccountNumber, AddressLine having count(1)>1
