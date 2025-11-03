# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Fact_session_events <br>
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
ErrorLogger = ErrorLogger('NB_Fact_session_events')
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# Create DF_Metadata DataFrame
try:
    TableID = 'B5'
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
    logger.info('Successfully Created a DF_Metadata for Fact_SessionEvent_Load Table')

    #Updating the Pipeline StartTime
    UpdatePipelineStartTime(TableID, 'metrics')
    print("Successfully updated the Pipeline StartTime")
    logger.info('Updated the Pipeline StartTime in the Metadata')

    #Forming the Select Query with the Transformations
    DF_Fact_SessionEvents_Load = spark.sql(f'''
    SELECT 
        sha2(
            concat(
            coalesce(campaignData.campaignId, ''),
            coalesce(sessionId, ''),
            coalesce(campaignData.advertiserId, ''),
            coalesce(fluentId, ''),
            coalesce(trafficSource.trafficPartnerId, ''),
            coalesce(trafficSource.sourceId, ''),
            coalesce(campaignData.creativeId, ''),
            coalesce(campaignData.tuneOfferId, ''),
            coalesce(CAST(data.orderId AS STRING), ''),
            coalesce(productScope, ''),
            coalesce(campaignData.conversionGoalId, ''),
            coalesce(CAST(campaignData.revenue AS STRING), ''),
            coalesce(CAST(trafficSource.trafficPartnerRevenue AS STRING), ''),
            coalesce(cast(campaignData.flowImpressionPosition as STRING), ''),
            coalesce(data.email, ''),
            coalesce(data.emailmd5, ''),
            coalesce(data.emailsha256, ''),
            coalesce(data.phone, ''),
            coalesce(data.email, ''),
            coalesce(data.deviceAdvertisingId, ''),
            coalesce(data.first_name, ''),
            coalesce(data.orderId, ''),
            coalesce(data.transactionValue, ''),
            coalesce(data.ccBin, ''),
            coalesce(data.gender, ''),
            coalesce(data.zip, ''),
            coalesce(data.zippost, '')
            ), 256
        ) AS dwh_session_events_hash_id,
        sha2(concat(sourceReference, sourceReferenceId, cast(timestamp AS string)), 256) AS dwh_session_events_id,
        campaignData.campaignId AS campaign_id,
        sessionId AS session_id,
        campaignData.advertiserId AS advertiser_id,
        fluentId AS fluent_id,
        trafficSource.trafficPartnerId AS partner_id,
        trafficSource.sourceId AS traffic_source_id,
        campaignData.creativeId AS creative_id,
        campaignData.tuneOfferId AS tune_offer_id,
        data.orderId AS transaction_id,
        productScope AS product_scope,
        sourceReference AS event_type,
        campaignData.conversionGoalId AS conversion_goal_id,
        timestamp AS action_timestamp,
        campaignData.revenue AS campaign_revenue,
        CAST(trafficSource.trafficPartnerRevenue AS DECIMAL(19,4)) AS partner_revenue,
        CAST(campaignData.flowImpressionPosition as DECIMAL(19,4)) AS position,
        CAST(CASE WHEN data.email IS NOT NULL AND data.email NOT IN ('', ',') THEN 1 ELSE 0 END AS BOOLEAN) AS email_coverage_flag,
        CAST(CASE WHEN data.emailmd5 IS NOT NULL AND data.emailmd5 NOT IN ('', ',') THEN 1 ELSE 0 END AS BOOLEAN) AS emailmd5_coverage_flag,
        CAST(CASE WHEN data.emailsha256 IS NOT NULL AND data.emailsha256 NOT IN ('', ',') THEN 1 ELSE 0 END AS BOOLEAN) AS emailsha256_coverage_flag,
        CAST(CASE WHEN data.phone IS NOT NULL AND data.phone NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS telephone_coverage_flag,
        CAST(CASE WHEN data.email IS NOT NULL AND data.email NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS phone_coverage_flag,
        CAST(CASE WHEN data.deviceAdvertisingId IS NOT NULL AND data.deviceAdvertisingId NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS maid_coverage_flag,
        CAST(CASE WHEN data.first_name IS NOT NULL AND data.first_name NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS first_name_coverage_flag,
        CAST(CASE WHEN data.orderId IS NOT NULL AND data.orderId NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS order_id_coverage_flag,
        CAST(CASE WHEN data.transactionValue IS NOT NULL AND data.transactionValue NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS transaction_value_coverage_flag,
        CAST(CASE WHEN data.ccBin IS NOT NULL AND data.ccBin NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS ccbin_coverage_flag,
        CAST(CASE WHEN data.gender IS NOT NULL AND data.gender NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS gender_coverage_flag,
        CAST(CASE WHEN data.zip IS NOT NULL AND data.zip NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS zip_coverage_flag,
        CAST(CASE WHEN data.zippost IS NOT NULL AND data.zippost NOT IN ('', ',', ' ', '.') THEN 1 ELSE 0 END AS BOOLEAN) AS zippost_coverage_flag,
        sourceReferenceId AS source_reference_id,
        d.dwh_dim_date_key AS action_date_key,
        t.dim_time_key AS action_time_key,
        current_timestamp() AS dwh_created_at,
        current_timestamp() AS dwh_modified_at
        FROM minion_event.offer_silver_clean a
        left join metrics.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join metrics.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        where a.updateDate > '{LastLoadDate}'
        ''')
    
    DF_Fact_SessionEvents_Load = DF_Fact_SessionEvents_Load.filter(
        (col(MergeKey).isNotNull()) & (trim(col(MergeKey)) != '')
    )
    DF_Fact_SessionEvents_Load.dropDuplicates()
    DF_Fact_SessionEvents_Load.createOrReplaceTempView("Vw_Fact_SessionEvent_Load")
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Fact_SessionEvents_Load and created a view named Vw_Fact_SessionEvent_Load')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Referential_Integrity Check
try:
    fk_mappings = {
        "metrics.fact_session_events": {
            "campaign_id": ("minion.gold_campaign", "id"),
            "advertiser_id": ("minion.gold_advertiser", "advertiser_id"),
            "fluent_id": ("customer.gold_customer_v1", "fluent_id"),
            "partner_id": ("minion.gold_partner", "partner_id"),
            "traffic_source_id": ("metrics.dim_traffic_source", "traffic_source_id"),
            "creative_id": ("minion.gold_creative", "id")
        }
    }

    result_df = run_referential_integrity_check_on_df(  
        fact_df=DF_Fact_SessionEvents_Load,
        fact_table_name="metrics.fact_session_events",  # for logging
        fk_pk_mapping=fk_mappings["metrics.fact_session_events"]
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
    schema_columns = DF_Fact_SessionEvents_Load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_session_events_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING Vw_Fact_SessionEvent_Load as S
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
        source_df=DF_Fact_SessionEvents_Load,
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
    SELECT MAX(updateDate) as max_date from {SourceSchemaName}.{SourceTableName}
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
