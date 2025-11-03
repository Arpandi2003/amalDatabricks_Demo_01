# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Fact_Profile_Request_Match <br>
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

# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Utilities

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Logger

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Referential_Integrity

# COMMAND ----------

# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog Initialization

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

spark.sql(f"use schema {dqmetadata_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring error Logger variables

# COMMAND ----------

# Example error logger variables initialization
ErrorLogger = ErrorLogger('NB_Fact_Profile_Request_Match')
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# DBTITLE 1,Performing the required Transformation
# Create DF_Metadata DataFrame
try:
    TableID = 'B4'
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
    logger.info('Successfully Created a DF_Metadata for Fact_ProfileRequestMatch Table')

    #Updating the Pipeline StartTime
    UpdatePipelineStartTime(TableID, 'metrics')
    print("Successfully updated the Pipeline StartTime")
    logger.info('Updated the Pipeline StartTime in the Metadata')

    #Forming the Select Query with the Transformations
    DF_Fact_ProfileRequestMatch_Load = spark.sql(f'''
        WITH ranked_records AS (
            SELECT 
                sha2(
                concat(
                    coalesce(response.FID, ''), 
                    coalesce(response.MatchType, ''), 
                    coalesce(request.RequestTimestamp, ''), 
                    coalesce(response.ResponseTimestamp, '')
                ), 
                256
                ) AS dwh_profile_request_id,
                request.RequestTraceId AS request_trace_id, 
                response.FID AS fluent_id, 
                response.MatchType AS match_type, 
                request.RequestTimestamp AS request_timestamp, 
                response.ResponseTimestamp AS response_timestamp,
                `_etl_timestamp` AS etl_timestamp,
                current_timestamp() AS dwh_created_at,
                current_timestamp() AS dwh_modified_at,
                ROW_NUMBER() OVER (
                PARTITION BY request.RequestTraceId 
                ORDER BY response.ResponseTimestamp DESC
                ) AS rn
            FROM {SourceSchemaName}.{SourceTableName}
            where `_etl_timestamp` > '{LastLoadDate}'
            )
            SELECT 
            dwh_profile_request_id,
            request_trace_id,
            fluent_id,
            match_type,
            request_timestamp,
            response_timestamp,
            dwh_created_at,
            dwh_modified_at
            FROM ranked_records
            WHERE rn = 1

    ''') 

    DF_Fact_ProfileRequestMatch_Load = DF_Fact_ProfileRequestMatch_Load.dropDuplicates()
    DF_Fact_ProfileRequestMatch_Load.createOrReplaceTempView("Vw_Fact_ProfileRequestMatch")
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Fact_ProfileRequestMatch_Load and created a view named Vw_Fact_ProfileRequestMatch')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Referential Integrity Check
try:
    fk_mappings = {
    "metrics.fact_profile_request_match": {
        "fluent_id": ("customer.gold_customer_v1", "fluent_id")
        }
    }

    result_df = run_referential_integrity_check_on_df(  
        fact_df=DF_Fact_ProfileRequestMatch_Load,
        fact_table_name="metrics.fact_profile_request_match",  # for logging
        fk_pk_mapping=fk_mappings["metrics.fact_profile_request_match"]
    )

    if result_df.filter("message IS NOT NULL").count() > 0:
        display(result_df)
        result_df.write.mode("append").saveAsTable("metrics.referential_integrity_log")
    else:
        print("No referential integrity issues found.")

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    raise(e)


# COMMAND ----------

try:
    # Capture the comparison Silver target table in DF_target_code and store it in a view Vw_target_code
    schema_columns = DF_Fact_ProfileRequestMatch_Load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_profile_request_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING Vw_Fact_ProfileRequestMatch as S
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
        source_df=DF_Fact_ProfileRequestMatch_Load,
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
    SELECT MAX(`_etl_timestamp`) as max_date from {SourceSchemaName}.{SourceTableName}
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
