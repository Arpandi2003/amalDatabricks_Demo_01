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
    (col('Zone') == 'silver') &
    (col('TableID') == 1000)
)
display(DFMetadata)

# COMMAND ----------

TableID=1000
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
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# DBTITLE 1,From Branch_xref
try:
    logger.info("Joining base tabels for silver branch tables")

    basequery = '''Select
                        bxref.Branch_Code,
                        bxref.Branch_Number,
                        oxref.Officer_Key,
                        CASE
                        when Branch_Code = 'OS-60' then '53 State Street'
                        else fco.Address_Line_1 END as BranchStreet,
                        bxref.Location as BranchCity,
                        bxref.Operational_State as BranchState,
                        CASE
                        when Branch_Code = 'OS-60' then '02109'
                        else fco.Zip_Code END as BranchZip,
                        bxref.Name
                        From silver.branch_xref bxref
                        left join bronze.fi_core_org fco on bxref.Branch_Code = fco.Org_Key
                        left join silver.officer_xref oxref on bxref.Branch_Manager = oxref.Officer_Name
                        where bxref.currentrecord = 'Yes'
                        '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Transformed_vw")
except Exception as e:
    logger.error("Issue while joining the base tables")


# COMMAND ----------

try:
    logger.info("Performing transformations for the bronze table")

    Transformation_sqlquery = """
                        SELECT
                        tab.Branch_Code,
                        tab.Branch_Number,
                        tab.Officer_Key,
                        tab.BranchStreet,
                        tab.BranchCity,
                        tab.BranchState,
                        tab.BranchZip,
                        tab.Name
                        FROM
                        (select * from Transformed_vw) Tab
                        """

    df_final_FA = spark.sql(Transformation_sqlquery)

    # Converting the dataframe into view
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Branch_Code, Count(1) from vw_final_FA
# MAGIC Group by all having count(1)>1

# COMMAND ----------

DestinationSchema = dbutils.widgets.get("DestinationSchema")
DestinationTable = dbutils.widgets.get("DestinationTable")
AddOnType = dbutils.widgets.get("AddOnType")

print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

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
# MAGIC select count(1) from vw_silver
# MAGIC where Action_Code != 'No Changes'

# COMMAND ----------

# DBTITLE 1,Row Count
# MAGIC %sql
# MAGIC select count(1) from silver.branch where CurrentRecord='Yes'

# COMMAND ----------

# DBTITLE 1,Dups
# MAGIC %sql
# MAGIC select Branch_Code,count(1) from silver.branch where CurrentRecord='Yes' group by 1 having count(1)>1
