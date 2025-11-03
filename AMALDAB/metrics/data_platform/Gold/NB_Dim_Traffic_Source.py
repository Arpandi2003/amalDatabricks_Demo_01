# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Dim_Traffic_Source <br>
# MAGIC **Created By:** Deeraj Rajeev <br>
# MAGIC **Created Date:** 09/01/25<br>
# MAGIC **Modified By:** Deeraj Rajeev<br>
# MAGIC **Modified Date** 09/01/24<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing necessary packages

# COMMAND ----------

import io
from datetime import datetime
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, first, when, trim, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Initialization

# COMMAND ----------

# DBTITLE 1,Configuration Notebook
# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

# DBTITLE 1,Utility Notebook
# MAGIC %run
# MAGIC ../General/NB_Utilities

# COMMAND ----------

# DBTITLE 1,LoggerNotebook
# MAGIC %run
# MAGIC ../General/NB_Logger

# COMMAND ----------

# DBTITLE 1,DQ-SourceCheck notebook
# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# DBTITLE 1,call the catalog_name from the widget
catalog_name = dbutils.widgets.get('catalog_name')
minion_catalog_name = dbutils.widgets.get('minion_catalog_name')

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

spark.sql(f"USE schema {dqmetadata_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring error logger variables

# COMMAND ----------

# Example error logger variables initialization
ErrorLogger = ErrorLogger('NB_Dim_TrafficSource')
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# DBTITLE 1,Performing the required Transformation
# Create DF_Metadata DataFrame
try:
    TableID = 'B1'
    DF_Metadata = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{TableID}'")
    MergeKey = DF_Metadata.select(col('MergeKey')).where(col('TableID') == TableID).collect()[0].MergeKey
    LoadType = DF_Metadata.select(col('LoadType')).where(col('TableID') == TableID).collect()[0].LoadType
    SourceSystem = DF_Metadata.select(col('SourceSystem')).where(col('TableID') == TableID).collect()[0].SourceSystem
    SourceTableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName
    SourceSchemaName = DF_Metadata.select(col('SourceSchema')).where(col('TableID') == TableID).collect()[0].SourceSchema
    TargetTableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == TableID).collect()[0].DWHTableName
    TargetSchemaName = DF_Metadata.select(col('DWHSchemaName')).where(col('TableID') == TableID).collect()[0].DWHSchemaName
    LastLoadDate =  DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
    SourceSelectQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == TableID).collect()[0].SourceSelectQuery
    HashKeyColumn = DF_Metadata.select(col('HashKeyColumn')).where(col('TableID') == TableID).collect()[0].HashKeyColumn

    # Print to verify DF_Metadata and TablesList
    print("successfully created a DF_Metadata")
    logger.info('Successfully Created a DF_Metadata for Dim_TrafficSource Table')

    #Updating the Pipeline StartTime
    UpdatePipelineStartTime(TableID, 'metrics')
    print("Successfully updated the Pipeline StartTime")
    logger.info('Updated the Pipeline StartTime in the Metadata')

    #Forming the Select Query with the Transformations
    DF_Dim_TrafficSource_Load = spark.sql(f'''
            WITH exploded_subverticals AS (
            SELECT
                v.Id AS vertical_id,
                posexplode(v.SubVerticals) AS (pos, subvertical_struct)
            FROM {minion_catalog_name}.pulse_configurations.vertical_silver v
            )

            SELECT
            a.Id AS traffic_source_id,
            sha2(
                coalesce(a.Name, '') ||
                coalesce(p.partner_domain_source, '') ||
                coalesce(subvertical_struct.Name, '') ||
                coalesce(subvertical_struct.Id, '') ||
                coalesce(p.Name, '') ||
                coalesce(tp.PartnerType, '') ||
                coalesce(v.Name, '') ||
                coalesce(v.Id, ''),
                256
            ) AS dwh_traffic_source_id,
            a.Name AS name,
            p.partner_domain_source AS partner_domain,
            subvertical_struct.Name AS sub_vertical,
            subvertical_struct.Id AS sub_vertical_id,
            p.Name AS traffic_partner_name,
            tp.PartnerType AS traffic_partner_type,  -- joined from traffic_partner_silver
            v.Name AS vertical,
            v.Id AS vertical_id,
            current_timestamp() AS dwh_created_at,
            current_timestamp() AS dwh_modified_at
            FROM {minion_catalog_name}.pulse_configurations.traffic_source_silver a
            LEFT JOIN {minion_catalog_name}.pulse_configurations.vertical_silver v
            ON a.VerticalId = v.Id
            LEFT JOIN LATERAL (
            SELECT subvertical_struct.*
            FROM posexplode(v.SubVerticals) AS (pos, subvertical_struct)
            WHERE subvertical_struct.Id = a.SubVerticalId
            ) subvertical_struct ON TRUE
            LEFT JOIN minion.gold_partner p
            ON a.TrafficPartnerId = p.partner_id
            LEFT JOIN {minion_catalog_name}.pulse_configurations.traffic_partner_silver tp
            ON a.TrafficPartnerId = tp.Id
            where a.DateModified > '{LastLoadDate}'
    ''')

    DF_Dim_TrafficSource_Load.createOrReplaceTempView("Vw_Dim_TrafficSourceLoad")
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Dim_TrafficSource_Load and created a view named Vw_Dim_TrafficSourceLoad')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

# DBTITLE 1,de-duplication and null handling strategy
try:
    # Exclude 'traffic_source_id' from columns to fill
    columns_to_fill = [c for c in DF_Dim_TrafficSource_Load.columns if c != 'traffic_source_id']

    # Fill null or blank values in all columns except traffic_source_id
    for column in columns_to_fill:
        fill_value = DF_Dim_TrafficSource_Load.filter(
            (col(column).isNotNull()) & (trim(col(column)) != '')
        ).select(column).first()

        if fill_value:
            value_to_fill = fill_value[column]
            DF_Dim_TrafficSource_Load = DF_Dim_TrafficSource_Load.withColumn(
                column,
                when(
                    (col(column).isNull()) | (trim(col(column)) == ''),
                    lit(value_to_fill)
                ).otherwise(col(column))
            )

    # Remove rows where traffic_source_id is null or blank
    DF_Dim_TrafficSource_Load = DF_Dim_TrafficSource_Load.filter(
        (col('traffic_source_id').isNotNull()) & (trim(col('traffic_source_id')) != '')
    )
    DF_Dim_TrafficSource_Load = DF_Dim_TrafficSource_Load.dropDuplicates()
    DF_Dim_TrafficSource_Load.createOrReplaceTempView('Vw_Dim_TrafficSourceLoad')
    
except Exception as e:
    # Log the exception
    print("Failed to de-duplicate and handle null from the dataframe")
    logger.error(f"Failed to de-duplicate and handle null from the dataframe")
    raise(e)

# COMMAND ----------

try:
    # Capture the comparison Silver target table in DF_target_code and store it in a view Vw_target_code
    schema_columns = DF_Dim_TrafficSource_Load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_traffic_source_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING Vw_Dim_TrafficSourceLoad as S
    ON T.{MergeKey} = S.{MergeKey}
    WHEN MATCHED and t.{HashKeyColumn} != s.{HashKeyColumn} THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    print(f'Loaded Table {TargetSchemaName}.{TargetTableName}')

    logger.info('Loaded the data into the metrics Zone Successfully')
    print("Loaded the data into the metrics zone successfully")

    run_hourly_dq_check_v2(
        source_df=DF_Dim_TrafficSource_Load,
        target_schema_name=TargetSchemaName,
        target_table_name=TargetTableName,
        source_system=SourceSystem,
        merge_key=MergeKey,
        last_load_date=LastLoadDate,
        table_id=TableID,
        source_schema_name=SourceSchemaName,
        source_table_name=SourceTableName
    )

    # Update the max date value in metadata
    DF_MaxDateValue = spark.sql(f"""
    SELECT MAX(DateModified) as max_date from {minion_catalog_name}.pulse_configurations.traffic_source_silver
    """)

    UpdateLastLoadDate(TableID, DF_MaxDateValue)
    print("Updated the LastLoadDate in the metadata")
    logger.info('Updated the LastLoadDate in the Metadata')

  # Update PipelineStatus and time
    UpdatePipelineStatusAndTime(TableID, 'metrics')
    print("Updated the Pipeline End Time and Status in the metadata")
    logger.info('Updated the Pipeline End Time and Status in the Metadata')

except Exception as e:
    # Log the exception
    print("Failed to Load the Data into the Table")
    logger.error(f"Failed to Load the Data into the Table")
    raise(e)
