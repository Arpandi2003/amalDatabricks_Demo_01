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

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

spark.sql(f"USE schema {dqmetadata_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring error Logger variables

# COMMAND ----------

# Example error logger variables initialization
ErrorLogger = ErrorLogger('NB_Fact_sessions')
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# Create DF_Metadata DataFrame
try:
    TableID = 'B6'
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
    logger.info('Successfully Created a DF_Metadata for Fact_Sessions_Load Table')

    #Updating the Pipeline StartTime
    UpdatePipelineStartTime(TableID, 'metrics')
    print("Successfully updated the Pipeline StartTime")
    logger.info('Updated the Pipeline StartTime in the Metadata')

    #Forming the Select Query with the Transformations
    DF_Fact_Session_Load = spark.sql(f'''
               WITH cleaned AS (
        SELECT
            sessionId,
            campaignData.campaignId,
            campaignData.advertiserId,
            campaignData.creativeId,
            campaignData.conversionGoalId,
            CASE 
            WHEN campaignData.tuneOfferId IS NULL OR TRIM(campaignData.tuneOfferId) = '' 
                THEN 'Unknown'
            ELSE campaignData.tuneOfferId 
            END AS tune_offer_id,
            CASE 
            WHEN fluentid IS NULL OR TRIM(fluentid) = '' 
                THEN 'Unknown'
            ELSE fluentid 
            END AS fluentid,
            trafficSource.trafficPartnerId,
            trafficSource.sourceId,
            productScope,
            sourceReference,
            Timestamp,
            d.dwh_dim_date_key,
            t.dim_time_key
        FROM minion_event.offer_silver_clean
        left join metrics.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join metrics.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        where updateDate > '{LastLoadDate}'
        )
    SELECT
    sha2(concat(
        coalesce(campaignId, ''),
        coalesce(sessionId, ''),
        coalesce(tune_offer_id, ''),
        coalesce(fluentid, ''),
        coalesce(advertiserId, ''),
        coalesce(trafficPartnerId, ''),
        coalesce(sourceId, ''),
        coalesce(creativeId, ''),
        coalesce(productScope, '')
    ), 256) AS dwh_session_id,
    sessionId as session_id,
    campaignId as campaign_id,
    tune_offer_id,
    fluentid as fluent_id,
    advertiserId as advertiser_id,
    trafficPartnerId as partner_id,
    sourceId as traffic_source_id,
    creativeId as creative_id,
    productScope as product_scope,
    COUNT(*) AS total_actions,
    COUNT(CASE WHEN sourceReference = 'offer-click' AND LOWER(productScope) = 'adflow' THEN 1 END) AS total_clicks,
    COUNT(CASE WHEN sourceReference = 'offer-view' AND LOWER(productScope) = 'adflow' THEN 1 END) AS total_views,
    COUNT(CASE WHEN TRY_CAST(conversionGoalId AS INT) = 0 AND LOWER(productScope) = 'adflow' THEN 1 END) AS primary_conversions,
    COUNT(CASE WHEN TRY_CAST(conversionGoalId AS INT) IS NOT NULL AND LOWER(productScope) = 'adflow' THEN 1 END) AS secondary_conversions,
    CAST(
        CASE 
        WHEN MIN(CASE WHEN sourceReference = 'offer-view' THEN Timestamp END) IS NOT NULL
        AND MIN(CASE WHEN sourceReference = 'offer-convert' THEN Timestamp END) IS NOT NULL
        THEN DATEDIFF(minute,
            MIN(CASE WHEN sourceReference = 'offer-view' THEN Timestamp END),
            MIN(CASE WHEN sourceReference = 'offer-convert' THEN Timestamp END)
        )
        ELSE NULL
        END AS BIGINT
    ) AS first_conversion_window,
    MAX(CASE WHEN sourceReference = 'offer-convert' THEN 1 ELSE 0 END) AS conversion_flag,
    min(timestamp) as session_start_timestamp,
    min(dwh_dim_date_key) as session_start_date,
    min(dim_time_key) as session_start_time,
    max(timestamp) as session_end_timestamp,
    max(dwh_dim_date_key) as session_end_date,
    max(dim_time_key) as session_end_time,
    current_timestamp() as dwh_created_at,
    current_timestamp() as dwh_modified_at
    FROM cleaned
    GROUP BY
    sessionId, campaignId, tune_offer_id, fluentid, advertiserId, 
    trafficPartnerId, sourceId, creativeId, productScope''')

    DF_Fact_Session_Load.dropDuplicates()
    DF_Fact_Session_Load.createOrReplaceTempView('vw_session')
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Fact_Session_Load and created a view named Vw_Fact_Sessions_Load')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Referential_Integrity Check
try:
    fk_mappings = {
        "metrics.fact_session": {
        "fluent_id": ("customer.gold_customer_v1", "fluent_id"),
        "advertiser_id": ("minion.gold_advertiser", "advertiser_id"),
        "partner_id": ("minion.gold_partner", "partner_id"),
        "traffic_source_id": ("metrics.dim_traffic_source", "traffic_source_id"),
        "creative_id": ("minion.gold_creative", "id")
    }
    }

    result_df = run_referential_integrity_check_on_df(  
        fact_df=DF_Fact_Session_Load,
        fact_table_name="metrics.fact_session",  # for logging
        fk_pk_mapping=fk_mappings["metrics.fact_session"]
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
    schema_columns = DF_Fact_Session_Load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_session_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING vw_session as S
    ON T.{MergeKey} = S.{MergeKey}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    print(f'Loaded Table {TargetSchemaName}.{TargetTableName}')

    logger.info('Loaded the data into the metrics Zone Successfully')
    print("Loaded the data into the metrics zone successfully")

    # Update the max date value in metadata
    DF_MaxDateValue = spark.sql(f"""
    SELECT MAX(updateDate) as max_date from {SourceSchemaName}.{SourceTableName}
    """)

    run_hourly_dq_check_v2(
        source_df=DF_Fact_Session_Load,
        target_schema_name=TargetSchemaName,
        target_table_name=TargetTableName,
        source_system=SourceSystem,
        merge_key=MergeKey,
        last_load_date=LastLoadDate,
        table_id=TableID,
        source_schema_name=SourceSchemaName,
        source_table_name=SourceTableName
    )

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
