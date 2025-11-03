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
    (col('TableID') == 1021 )
)

display(DFMetadata)

# COMMAND ----------

TableID=1021
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

# DBTITLE 1,Get skinny columns
df_base=spark.sql("select code_key from silver.status_xref group by 1")
df_base.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# DBTITLE 1,Data from fi_core_stat
try:
    logger.info("Joining base tables for silver status_xref tables")

    basequery = '''
            select stat.Code_Key
            ,stat.Bankid
            ,stat.Application
            ,stat.User_Code
            ,stat.Status_Code
            ,stat.Status_Description
            ,stat.Conformed_Status_Code
            ,stat.Conformed_Status_Description
            from bronze.fi_core_status stat
            where stat.currentrecord = "Yes"
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_fi_core_stat")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Data from MC Status
try:
    logger.info("Joining base tables for silver Status_Xref tables")

    basequery = '''
          select concat_ws("-", cod1.Bankid, cod1.C1APPL, cod1.C1USER) Code_Key
          ,cod1.Bankid
          ,cod1.C1APPL Application
          ,cod1.C1TYPE User_Code
          ,cod1.C1USER Status_Code
          ,cod1.C1DESC Status_Description
          ,case when cod1.C1USER in ("A","I") then "O" 
            when cod1.C1USER = "M" then "M"
            end Conformed_Status_Code
          ,case when cod1.C1USER in ("A","I") then "OPEN" 
            when cod1.C1USER = "M" then "MATURED"
            end Conformed_Status_Description
          from bronze.ods_sicod1 cod1
          where cod1.currentrecord = "Yes"
          and cod1.C1APPL = "MC"
          and cod1.C1TYPE = "STAT"
          union all
          select "13-MC-" Code_Key
          ,"13" Bankid
          ,"MC" Application
          ,"STAT" User_Code
          ," " Status_Code
          ,"Unassigned" Status_Description
          ,"O" Conformed_Status_Code
          ,"OPEN" Conformed_Status_Description
    '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("vw_MC_Stat")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery="""SELECT tab.Code_Key	
                              ,tab.Bankid	
                              ,tab.Application	
                              ,tab.User_Code	
                              ,tab.Status_Code	
                              ,tab.Status_Description	
                              ,tab.Conformed_Status_Code	
                              ,tab.Conformed_Status_Description
                              FROM
                              (
                              Select * from vw_fi_core_stat
                              Union All
                              Select * from vw_MC_Stat
                            ) TAB
                              """

    df_final_FA=spark.sql(Transformation_sqlquery)

    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Dups Check
# MAGIC %sql
# MAGIC select Code_Key, count(1) from vw_final_FA
# MAGIC group by Code_Key
# MAGIC having count(1) > 1

# COMMAND ----------

DestinationSchema = dbutils.widgets.text("DestinationSchema",' ')
DestinationTable = dbutils.widgets.text("DestinationTable",' ')
AddOnType = dbutils.widgets.text("AddOnType",' ')

# COMMAND ----------

# DBTITLE 1,Widget
DestinationSchema = dbutils.widgets.get("DestinationSchema")
DestinationTable = dbutils.widgets.get("DestinationTable")
AddOnType = dbutils.widgets.get("AddOnType")

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

# DBTITLE 1,Filter Base columns
base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns  # get all the base columns
set_addon = df_final_FA.columns  # get only the addon columns
get_pk = spark.sql(f"SELECT MergeKey FROM config.metadata WHERE LOWER(TRIM(DWHTableName)) = '{DestinationTable.lower()}' and lower(TRIM(DWHSchemaName))='{DestinationSchema.lower()}'").collect()[0]['MergeKey']
set_addon.remove(get_pk)  # remove pk from the addon
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

# DBTITLE 1,Row Count
# MAGIC %sql
# MAGIC select count(1) from silver.status_xref where CurrentRecord='Yes'

# COMMAND ----------

# DBTITLE 1,Dups
# MAGIC %sql
# MAGIC select Code_key, count(1) from silver.status_xref where CurrentRecord='Yes'
# MAGIC group by Code_Key
# MAGIC having count(1) > 1
