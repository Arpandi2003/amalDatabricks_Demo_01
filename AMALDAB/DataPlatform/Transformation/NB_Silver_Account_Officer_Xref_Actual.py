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
    (col('Zone') == 'Silver') &
    (col('SourceSystem') != 'Flat_File')&
    (col('TableID') == 1017)
)

display(DFMetadata)

# COMMAND ----------

TableID=1017
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

df = spark.table("bronze.account_osaic").select("Broker_Name").distinct()

broker_list = []
for value in df.collect():
    if "/" in value.Broker_Name:
        names = value.Broker_Name.split(' ')
        if len(names) == 2:
            first_names, last_names = names
            first_names_list = first_names.split('/')
            last_names_list = last_names.split('/')
            for i in range(len(first_names_list)):
                broker_list.append(("OS-"+ first_names_list[i][0]+last_names_list[i][0], first_names_list[i][0]+last_names_list[i][0], first_names_list[i]+' '+last_names_list[i]))
    else:
        if "(" not in value.Broker_Name:
            first_name = value.Broker_Name.split(' ')[0]
            last_name = value.Broker_Name.split(' ')[1]
        else:
            first_name = value.Broker_Name.split(' ')[0] + ' ' + value.Broker_Name.split(' ')[1]
            last_name = value.Broker_Name.split(' ')[2]
        broker_list.append(("OS-"+ first_name[0]+last_name[0], first_name[0]+last_name[0], first_name+' '+last_name))
result_df = spark.createDataFrame(broker_list, ["OS_Key", "OS_Code", "Officer_Name"]).distinct().orderBy("Officer_Name")
result_df.createOrReplaceTempView('OS_Officer')

# COMMAND ----------

os_officer_df = result_df.withColumn("Base_OS_Code", F.concat(F.col("OS_Code").substr(1, 1), F.col("OS_Code").substr(2, 1)))

window_spec = Window.partitionBy("Base_OS_Code").orderBy("Officer_Name")
os_officer_df = os_officer_df.withColumn("Row_Num", F.row_number().over(window_spec))

os_officer_df = os_officer_df.withColumn(
    "Final_OS_Code",
    F.when(F.col("Row_Num") == 1, F.col("Base_OS_Code"))
     .otherwise(F.concat(F.col("Base_OS_Code"), F.col("Row_Num") - 1))
)

os_officer_df = os_officer_df.withColumn(
    "Final_OS_Key",
    F.concat(F.lit('OS-'), F.when(F.col("Row_Num") == 1, F.col("Base_OS_Code"))
     .otherwise(F.concat(F.col("Base_OS_Code"), F.col("Row_Num") - 1)))
)

#final_df = os_officer_df.select("OS_Key", "Final_OS_Code", "Officer_Name")
final_df = os_officer_df.select("Final_OS_Key", "Final_OS_Code", "Officer_Name")
final_df = final_df.withColumnRenamed("Final_OS_Key", "OS_Key")
final_df = final_df.withColumnRenamed("Final_OS_Code", "OS_Code")
final_df.createOrReplaceTempView('OS_Officer')

# COMMAND ----------

# DBTITLE 1,Data from fi_core_officer
try:
    logger.info("Joining base tabels for silver officer_xref tables")

    basequery = '''
            select  
            offi.Officer_Key
            ,offi.Officer_Code
            ,offi.Officer_Name
            ,offi.Title
            ,offi.Officer_User_ID 
            ,OS.Officer_Name as OS_Officer_Name
            ,offID.`MUZXEMP#` as ABEmployeeNumber
            from bronze.fi_core_officer offi
            left outer join (select distinct Officer_Name from OS_Officer) OS on upper(offi.Officer_Name) = upper(OS.Officer_Name)
            left outer join (SELECT * FROM bronze.siabusers where currentrecord = "Yes") offID on offi.Officer_User_ID = offID.MUZXUID
            union
            select OS_Key Officer_Key
            ,OS_Code Officer_Code
            ,Officer_Name
            ,null as Title
            ,null as  Officer_User_ID 
            ,Officer_Name as OS_Officer_Name
            ,null as ABEmployeeNumber
            from OS_Officer
            where upper(Officer_Name) not in (select distinct upper(Officer_Name) from bronze.fi_core_officer)
                '''
    base_df = spark.sql(basequery)
    base_df.createOrReplaceTempView("Fi_core_officer_VW")
except Exception as e:
    logger.error("Issue while joining the base tables")
    raise e


# COMMAND ----------

# DBTITLE 1,Final Transformation
try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery="""
        select tab.Officer_Key
        ,tab.Officer_Code
        ,tab.Officer_Name
        ,tab.Title
        ,tab.Officer_User_ID
        ,tab.OS_Officer_Name 
        ,tab.ABEmployeeNumber
        from
        (
            select  * from fi_core_officer_VW
        ) tab
        """

    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)    

# COMMAND ----------

# DBTITLE 1,Transformed Query Dups check
# MAGIC %sql
# MAGIC select Officer_Key, count(1) from vw_final_FA
# MAGIC -- where CurrentRecord = 'Yes'
# MAGIC group by Officer_Key
# MAGIC having count(1) > 1

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
      print(count)
  else:
    df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count = df_final_FA.count()
    print(count)
else:
    df_final_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count = df_final_FA.count()
    print(count)


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

# DBTITLE 1,Count Check
# MAGIC %sql
# MAGIC select count(1) from silver.officer_xref where CurrentRecord='Yes'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Officer_Key,count(1) from silver.officer_xref where CurrentRecord='Yes' group by Officer_Key having count(1)>1
