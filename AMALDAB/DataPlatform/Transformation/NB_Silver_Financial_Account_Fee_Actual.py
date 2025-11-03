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
    (col('TableID') == 1014)
)

display(DFMetadata)

# COMMAND ----------

TableID=1014
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
base_df = spark.sql("select UID from silver.financial_account_fee where currentrecord='Yes' group by 1")
base_df.createOrReplaceTempView("vw_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Performing transformations in the source table

# COMMAND ----------

# MAGIC %md
# MAGIC #Data from Horizon

# COMMAND ----------

# DBTITLE 1,Read data from depositfee_all
df_depositfee_all = spark.sql('''
            select dd.FeeAssessed AssessedAmount
            ,dd.FeeRefund RefundedAmount
            ,dd.FeeWaived WaivedAmount
            ,dd.AccountLKey FinancialAccountNumber
            ,dd.FeeCode
            ,dd.WVFEECTLD Type
            ,dd.AppCode AppType
            ,dd.DateKey
            from bronze.v_trend_depositfee_all dd
            where dd.DateKey = (select max(a.DateKey) from bronze.v_trend_depositfee_all a)
            and dd.FeeCode not in (151)
            and dd.CurrentRecord = "Yes"
            ''')
df_depositfee_all.createOrReplaceTempView("vw_depositfee_all")

# COMMAND ----------

# DBTITLE 1,Horizon Transformed
df_horizon = spark.sql(""" 
            select h360.FinancialAccountNumber
            ,h360.AssessedAmount
            ,h360.RefundedAmount
            ,h360.WaivedAmount
            ,case when (h360.AssessedAmount = h360.RefundedAmount) and (h360.AssessedAmount = h360.WaivedAmount) then 0.00
            else (h360.AssessedAmount - h360.RefundedAmount - h360.WaivedAmount) end Amount
            ,null as Frequency
            ,h360.FeeCode
            ,h360.Type
            ,"0" Rate
            ,"Horizon" SourceSystem
            ,h360.AppType
            ,h360.FinancialAccountNumber || "-" || coalesce(h360.FeeCode," ") || "-" || coalesce(h360.Type," ") as UID
            from
            ( 
                select * from vw_depositfee_all
            ) h360
""")

df_horizon.createOrReplaceTempView("VW_Horizon")

# COMMAND ----------

# MAGIC %md
# MAGIC #Osaic

# COMMAND ----------

# DBTITLE 1,Osaic-Transformed
df_osaic = spark.sql("""
            select final.FinancialAccountNumber
            ,final.AssessedAmount
            ,final.RefundedAmount
            ,final.WaivedAmount
            ,final.Amount
            ,final.Frequency
            ,final.FeeCode
            ,final.Type
            ,final.Rate
            ,final.SourceSystem
            ,final.AppType
            ,final.UID
            from
            (
            select 
            "13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0") FinancialAccountNumber
            ,sum(OS.Commission) AssessedAmount
            ,0.00 as RefundedAmount
            ,0.00 as WaivedAmount
            ,sum(OS.Commission) Amount
            ,null as Frequency
            ,OS.Commission_Type FeeCode
            ,OS.Commission_Type Type
            ,"0" Rate
            ,"Wealth Management" SourceSystem
            ,"OS" as AppType
            ,"13-OS-" || lpad(cast(mapping.Account_Number as string),20,"0") || "-" || coalesce(OS.Commission_Type," ") || "-" || coalesce(OS.Commission_Type," ") as UID
            from bronze.transactiondetail_osaic OS
            left join bronze.accountmapping_osaic mapping on mapping.Unique_Id = os.Account_Unique_ID and mapping.CurrentRecord = 'Yes'
            where OS.CurrentRecord = "Yes"
            and mapping.CurrentRecord = 'Yes'
            and OS.Commission > 0
            group by mapping.Account_Number
            ,OS.Commission_Type 
            ) final
            order by final.FinancialAccountNumber
            """)
df_osaic.createOrReplaceTempView("vw_osaic")

# COMMAND ----------

# MAGIC %md
# MAGIC #DMI

# COMMAND ----------

# df_dmi_heloc=spark.sql(""" 

#              """)
# df_dmi_heloc.createOrReplaceTempView("vw_dmi_heloc")

# COMMAND ----------

# DBTITLE 1,DMI Resi
# df_dmi_resi = spark.sql("""

#             """)
# df_dmi_resi.createOrReplaceTempView("vw_dmi_resi")

# COMMAND ----------

# MAGIC %md
# MAGIC #Global Plus

# COMMAND ----------

# DBTITLE 1,Global Plus
# df_global_plus = spark.sql("""
#              """)
# df_global_plus.createOrReplaceTempView("vw_global_plus")

# COMMAND ----------

try:
    logger.info("Performing transformations for the bronze table")
    Transformation_sqlquery = """
        select tab.FinancialAccountNumber
        ,tab.Amount 
        ,null EndDate
        ,tab.Frequency
        ,tab.Rate
        ,null StartDate
        ,tab.Type
        ,0.00 TreasuryServiceFeeIncome
        ,0.00 TreasuryServiceBillingAccount
        ,0.00 TreasuryServiceTransactionalTotal
        ,0 TreasuryServiceVolume
        ,tab.WaivedAmount
        ,tab.RefundedAmount
        ,0.00 ExceptionPricingOnFees
        ,0.0000 ExceptionRate
        ,0.0000 ListRate
        ,0.00 CurrentFees
        ,0.00 FeesPaidOvertime
        ,0.00 NetVSGrossed
        ,tab.AssessedAmount
        ,0.00 AnnualFeeIncome
        ,tab.UID 
        ,tab.SourceSystem
        ,tab.FeeCode
        ,tab.AppType
            from 
                (
                --horizon source
                select * from vw_horizon
                union all
                --osaic
                select * from vw_osaic
                --union all
                --dmi heloc
                --select * from vw_dmi_heloc
                --union all                   
                --dmi resi
                --select * from vw_dmi_resi
                --union all                   
                --global plus
                --select * from vw_global_plus
                )tab
          """
    df_final_FA=spark.sql(Transformation_sqlquery)
    df_final_FA.createOrReplaceTempView("vw_final_FA")

except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from silver.financial_account_fee
# MAGIC where CurrentRecord = 'Yes'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from vw_final_FA

# COMMAND ----------

# DBTITLE 1,Dupes Check
# MAGIC %sql
# MAGIC Select count(1),UID from vw_final_FA
# MAGIC group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dynamic Merge Logic

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

# DBTITLE 1,Target Dupe Check
# MAGIC %sql
# MAGIC Select count(1) from silver.financial_account_fee
# MAGIC Where CurrentRecord='Yes'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select UID, count(1) from silver.financial_account_fee
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by UID having count(1)>1
