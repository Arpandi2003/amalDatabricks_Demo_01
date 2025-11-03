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
    (col('TableID')==1011)
)

display(DFMetadata)

# COMMAND ----------

TableID=1011
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

base_df = spark.sql("select ProductCode from silver.product_xref where currentrecord='Yes' group by 1")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select ProductCode, Count(1) from silver.product_xref
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by ProductCode Having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# DBTITLE 1,Data from fi_core_product
try:
    logger.info("Joining base tabels for silver product_xref tables")

    basequery = '''SELECT 
                    Product_Key,
                    Application_Code,
                    Application_Description,
                    Sub_Application_Code,
                    sub_Application_Description,
                    Top,
                    Product_Description,
                    product,
                    CurrentRecord,
                    ROW_NUMBER() OVER (PARTITION BY Product_Key ORDER BY Application_Code ASC) AS rownum
                FROM bronze.fi_core_product where CurrentRecord = 'Yes'
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Fi_core_product_VW")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# DBTITLE 1,Data from the fi_core_product_vw
try:
    logger.info("Joining base tabels for silver product_xref tables")

    basequery = '''
    SELECT 
    subquery.Product_Key,
    subquery.Application_Code,
    subquery.Application_Description,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Code
      when Product_Key = '13-ML-' then subquery.Application_Code
      Else subquery.Sub_Application_Code
    end as Sub_Application_Code,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Description
      when Product_Key = '13-ML-' then subquery.Application_Description
      Else subquery.Sub_Application_Description
    end as sub_Application_Description,
    Top,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Description
      when Product_Key = '13-ML-' then subquery.Application_Description
      Else subquery.Product_Description
    end as Product_Description,
    product,
    CurrentRecord
FROM (
    SELECT * from Fi_core_product_VW)subquery
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Product_Description_VW")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from Product_Description_VW

# COMMAND ----------

# DBTITLE 1,Transformation
try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery="""
select 
                    tab.ProductCode, 
                    tab.ApplicationCode, 
                    tab.ApplicationDescription, 
                    tab.SubApplicationCode, 
                    tab.SubApplicationDescription, 
                    tab.ProductFamily, 
                    tab.ProductName,
                    tab.ProductNumber,
                    tab.GPProductMapping,
                    tab.OsaicProductMapping
                    FROM 
                   (
select 
cast(fcp.Product_Key as string) as ProductCode,
cast(fcp.Application_Code as STRING) as ApplicationCode,
cast(fcp.Application_Description as string) as ApplicationDescription,
cast(fcp.Sub_Application_Code as string) as SubApplicationCode,
cast(fcp.sub_Application_Description as string) as SubApplicationDescription,
fcp.Top as ProductFamily,
fcp.Product_Description as ProductName, 
cast(fcp.product as string) as ProductNumber,
null as GPProductMapping,
null as OsaicProductMapping
from (SELECT 
    subquery.Product_Key,
    subquery.Application_Code,
    subquery.Application_Description,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Code
      when Product_Key = '13-ML-' then subquery.Application_Code
      Else subquery.Sub_Application_Code
    end as Sub_Application_Code,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Description
      when Product_Key = '13-ML-' then subquery.Application_Description
      Else subquery.Sub_Application_Description
    end as sub_Application_Description,
    Top,
    Case 
      When Product_Key = '13-LN-' then subquery.Application_Description
      when Product_Key = '13-ML-' then subquery.Application_Description
      Else subquery.Product_Description
    end as Product_Description,
    product,
    CurrentRecord
FROM (
    SELECT 
        Product_Key,
        Application_Code,
        Application_Description,
        Sub_Application_Code,
        sub_Application_Description,
        Top,
        Product_Description,
        product,
        CurrentRecord,
        ROW_NUMBER() OVER (PARTITION BY Product_Key ORDER BY Application_Code ASC) AS rownum
    FROM bronze.fi_core_product where CurrentRecord = 'Yes'
) subquery
WHERE subquery.rownum = 1 ) fcp

union all 

--tdw
select
cast(concat('GP-OT-',rc.NUMBER) as string) as ProductCode,
CAST(Case 
  when rc.number in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '17', '18', '19', '20', '21', '24', '27', '28', '30', '31', '33', '34', '35', '36', '124', '125', '126', '127', '130', '131', '132', '133', '134', '135', '138', '142', '143', '144', '145', '146', '147', '148', '149', '151', '152', '153', '154', '155', '156', '157', '158', '159', '168') then 'IM'
  when rc.number in ('25', '26', '29', '40', '42', '43', '80', '81', '82', '83', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '128', '136', '137', '139', '140', '141', '150', '160', '161') then 'CUS'
  else 'OT' 
End as STRING ) as ApplicationCode,
CAST(Case 
  when rc.number in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '17', '18', '19', '20', '21', '24', '27', '28', '30', '31', '33', '34', '35', '36', '124', '125', '126', '127', '130', '131', '132', '133', '134', '135', '138', '142', '143', '144', '145', '146', '147', '148', '149', '151', '152', '153', '154', '155', '156', '157', '158', '159', '168') then 'Investment Management'
  when rc.number in ('25', '26', '29', '40', '42', '43', '80', '81', '82', '83', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '128', '136', '137', '139', '140', '141', '150', '160', '161') then 'Custody'
  else 'Trust' 
End as STRING ) as ApplicationDescription,
ApplicationCode as SubApplicationCode,
ApplicationDescription as SubApplicationDescription,
SubApplicationDescription as ProductFamily,
rc.MEANING as ProductName, 
cast(rc.Number as string) as ProductNumber,
cast(rc.Number as string) as GPProductMapping,
null as OsaicProductMapping
from (
  SELECT r.Number, r.meaning
  FROM
  (
    SELECT Number
    ,Meaning
    ,ROW_NUMBER() OVER (PARTITION BY Number ORDER BY MEANING ASC) rownum
    FROM `bronze`.rc
    where CurrentRecord = 'Yes'
  ) r WHERE rownum = 1
)rc


union all 

select 
 
cast(concat('OS-OT-', ao.Account_Type) as string) as ProductCode,
Cast(ao.Account_Type as string) as ApplicationCode,
CAST(Case
  when ao.Account_Type = 'A' then 'Advisory'
  when ao.Account_Type = 'B' then 'Brokerage'
  when ao.Account_Type = 'D' then 'Direct'
  else 'OS-Other'
End as STRING) as ApplicationDescription,
Cast(ao.Account_Type as string) as SubApplicationCode,
CAST(Case
  when ao.Account_Type = 'A' then 'Advisory'
  when ao.Account_Type = 'B' then 'Brokerage'
  when ao.Account_Type = 'D' then 'Direct'
  else 'OS-Other'
End as STRING) as SubApplicationDescription,
Case
  when ao.Account_Type = 'A' then 'Advisory'
  when ao.Account_Type = 'B' then 'Brokerage'
  when ao.Account_Type = 'D' then 'Direct'
  else 'OS-Other'
End as ProductFamily,
Case
  when ao.Account_Type = 'A' then 'Advisory'
  when ao.Account_Type = 'B' then 'Brokerage'
  when ao.Account_Type = 'D' then 'Direct'
  else 'OS-Other'
End as ProductName,
ao.Account_Type as ProductNumber,
null as GPProductMapping,
ao.Account_Type as OsaicProductMapping
from 
( 
  SELECT tt.Account_Type
  FROM
  (
    SELECT Account_Type
    ,Branch_Name
    ,ROW_NUMBER() OVER (PARTITION BY Account_Type ORDER BY Branch_Name ASC) rownum
    FROM `bronze`.account_osaic
    where CurrentRecord = 'Yes'
  ) tt WHERE rownum = 1 ) ao


  union all 

select subquery.productcode, subquery.applicationcode, subquery.applicationdescription, subquery.subapplicationcode, subquery.subapplicationdescription, subquery.productfamily, subquery.productname, subquery.productnumber, subquery.gpproductmapping, subquery.osaicproductmapping from (
    select 
        a.ProductId ProductCode,
        app.Application_Code ApplicationCode,
        app.Application_Description ApplicationDescription,
        app.Application_Code SubApplicationCode,
        'Expired SubApplicationDesc' SubApplicationDescription,
        case 
            when app.Application_Code in ("CD","DD","IR","SV","TD","XC","XR","XS","Z1") then "Deposit"
            --W-000840 Added Commitment Application Code for MC and IC in Lending
            when app.Application_Code in ("LN","ML","RR","MC","IC") then "Lending"
            when app.Application_Code in ("02","03","04","05","BX","CA","Z9") then "Services"
        end ProductFamily,
        'Expired Product' ProductName,
        substring(a.ProductId, 7, 3) ProductNumber,
        null as GPProductMapping,
        null as OsaicProductMapping,
        ROW_NUMBER() OVER (PARTITION BY a.ProductId ORDER BY a.OpeningDate DESC) AS rownum
    from silver.financial_account a
    left outer join bronze.fi_core_application app 
        on app.Application_Code = substring(a.ProductId, 4, 2)
    -- W-00840 Adding CurrentRecord='Yes' 
    where a.ProductId not in (select Product_Key from bronze.fi_core_product Where CurrentRecord='Yes')
    and a.SourceSystem = "Horizon"
    and a.CurrentRecord='Yes'
    and app.CurrentRecord='Yes'
) subquery
where rownum = 1
union all
    select "13-ML-ARM" ProductCode
    ,"ML" ApplicationCode
    ,"Mortgage Loan" ApplicationDescription
    ,"ML" SubApplicationCode
    ,"Mortgage Loan" SubApplicationDescription
    ,"Lending" ProductFamily
    ,"ARM" ProductName
    ,null as ProductNumber
    ,null as GPProductMapping
    ,null as OsaicProductMapping

  -- Added MC and IC Products for W-000840

union all 
        Select "13-MC-" ProductCode
       ,"MC" ApplicationCode
        ,"Master Commitment" ApplicationDescription
        ,"MC" SubApplicationCode
        ,"Master Commitment" SubApplicationDescription
        ,"Lending" ProductFamily
        ,"Master Commitment" ProductName
        ," " as ProductNumber
        ,null as GPProductMapping
       ,null as OsaicProductMapping
  
union all
Select "13-IC-" ProductCode
       ,"IC" ApplicationCode
        ,"Intermediate Commitment" ApplicationDescription
        ,"IC" SubApplicationCode
        ,"Intermediate Commitment" SubApplicationDescription
        ,"Lending" ProductFamily
        ,"Intermediate Commitment" ProductName
        ," " as ProductNumber
        ,null as GPProductMapping
       ,null as OsaicProductMapping
       
union all

select concat_ws('-','13', MCCTYP,MCPRCD) ProductCode
    ,MCCTYP ApplicationCode
    ,case when MCCTYP = 'IC' then 'Intermediate Commitment'
        WHEN MCCTYP = 'MC' then 'Master Commitment'
        END ApplicationDescription
    ,MCCTYP SubApplicationCode
    ,case when MCCTYP = 'IC' then 'Intermediate Commitment'
        WHEN MCCTYP = 'MC' then 'Master Commitment'
        END SubApplicationDescription
    ,'Lending' ProductFamily
    ,MZDESC ProductName
    ,MCPRCD ProductNumber
    ,null as GPProductMapping
    ,null as OsaicProductMapping
    from bronze.ods_mcprod
    where currentrecord = "Yes"

) tab
                    """

    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")
   
except Exception as e:
    print(e)     

# COMMAND ----------

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

spark.sql(f"insert into {DestinationSchema}.{DestinationTable}({','.join(final_col)}) select {','.join(final_col)} from vw_silver where Action_Code in ('Insert','Update')")

spark.sql(f"""
        MERGE INTO {DestinationSchema}.{DestinationTable} AS Target
        USING (SELECT {','.join(final_col)} FROM VW_silver WHERE Action_Code='Update') AS Source
        ON Target.{get_pk} = Source.{get_pk} AND Target.MergeHashKey != Source.MergeHashKey
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
# MAGIC Select ProductCode, count(1) from silver.product_xref
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by ProductCode having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from silver.product_xref where currentrecord='Yes'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.product_xref order by dw_modified_date desc
