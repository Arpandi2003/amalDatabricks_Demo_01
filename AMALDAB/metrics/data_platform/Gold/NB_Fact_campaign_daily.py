# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Fact_Campaign_Daily <br>
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
    TableID = 'B7'
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
    UpdatePipelineStartTime(TableID, 'Gold')
    print("Successfully updated the Pipeline StartTime")
    logger.info('Updated the Pipeline StartTime in the Metadata')

    #Forming the Select Query with the Transformations
    DF_Fact_campaign_load = spark.sql(f'''
        WITH daily_events AS (
    SELECT
        date(from_utc_timestamp(timestamp, 'America/New_York')) as event_date,
        t.campaignData.campaignId AS campaign_id,
        COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId) AS partner_id,
        t.campaignData.advertiserId AS advertiser_id,

        COUNT(CASE WHEN t.sourceReference = 'offer-view'  THEN 1 END) AS views,
        COUNT(CASE WHEN t.sourceReference = 'offer-click' THEN 1 END) AS clicks,
        COUNT(CASE WHEN t.sourceReference = 'SessionStart' THEN 1 END) AS conversions

    FROM minion_event.offer_silver_clean t
    WHERE 
        t.timestamp IS NOT NULL
        AND t.campaignData.campaignId IS NOT NULL
        AND t.campaignData.advertiserId IS NOT NULL
        and updateDate > '{LastLoadDate}'
    GROUP BY 
        date(from_utc_timestamp(timestamp, 'America/New_York')),
        t.campaignData.campaignId,
        COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId),
        t.campaignData.advertiserId
    ),

    daily_metrics AS (
    SELECT
        event_date,
        campaign_id,
        partner_id,
        advertiser_id,
        views,
        clicks,
        conversions,

        CASE WHEN views > 0 THEN CAST(clicks AS DOUBLE) / views ELSE NULL END AS current_ctr,
        CASE WHEN views > 0 THEN CAST(conversions AS DOUBLE) / views ELSE NULL END AS current_cvr

    FROM daily_events
    ),
    comparison AS (
    SELECT
        c.event_date,
        c.campaign_id,
        c.partner_id,
        c.advertiser_id,
        c.current_ctr,
        c.current_cvr,
        c.clicks AS current_clicks,
        c.conversions AS current_conversions,
        c.views AS current_views,
        p.current_ctr AS previous_year_ctr,
        p.current_cvr AS previous_year_cvr,
        p.clicks AS previous_year_clicks,
        p.conversions AS previous_year_conversions,
        p.views AS previous_year_views

    FROM daily_metrics c
    LEFT JOIN daily_metrics p 
        ON c.campaign_id = p.campaign_id
        AND c.partner_id = p.partner_id
        AND c.advertiser_id = p.advertiser_id
        AND c.event_date = add_months(p.event_date, -12) 
    ),

    -- Enrich with dim_date
    final_enriched AS (
    SELECT
        y.*,
        d.dwh_dim_date_key as date_key
    FROM comparison y
    LEFT JOIN metrics.dim_date d 
        ON DATE_FORMAT(y.event_date, 'yyyy-MM-dd') = d.full_date_text
    )

    -- Final Select
    SELECT
    sha2(concat(event_date, campaign_id, coalesce(partner_id, ''), advertiser_id), 256) AS dwh_campaign_id,
    event_date,
    date_key,
    campaign_id,
    partner_id,
    advertiser_id,
    CAST(ROUND(current_ctr * 100, 2) AS DECIMAL(19, 2)) AS current_ctr,
    CAST(
        ROUND(
        CASE 
            WHEN previous_year_ctr IS NOT NULL AND previous_year_ctr != 0
            THEN ((current_ctr - previous_year_ctr) / previous_year_ctr) * 100
            ELSE NULL 
        END, 2
        ) AS DECIMAL(19, 2)
    ) AS ctr_seasonality,
    CAST(ROUND(current_cvr * 100, 2) AS DECIMAL(19, 2)) AS current_cvr,
    CAST(
        ROUND(
        CASE 
            WHEN previous_year_cvr IS NOT NULL AND previous_year_cvr != 0
            THEN ((current_cvr - previous_year_cvr) / previous_year_cvr) * 100
            ELSE NULL 
        END, 2
        ) AS DECIMAL(19, 2)
    ) AS cvr_seasonality,
    current_timestamp() as dwh_created_at,
    current_timestamp() as dwh_modified_at
    FROM final_enriched
    ORDER BY event_date DESC, campaign_id, partner_id, advertiser_id;
                ''')

    DF_Fact_campaign_load = DF_Fact_campaign_load.dropDuplicates()
    DF_Fact_campaign_load.createOrReplaceTempView('vw_fact_campaign_load')
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Fact_campaign_load and created a view named vw_fact_campaign_load')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

try:
    # Capture the comparison Silver target table in DF_target_code and store it in a view Vw_target_code
    schema_columns = DF_Fact_campaign_load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_campaign_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING vw_fact_campaign_load as S
    ON T.{MergeKey} = S.{MergeKey}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    print(f'Loaded Table {TargetSchemaName}.{TargetTableName}')

    # logger.info('Loaded the data into the Gold Zone Successfully')
    print("Loaded the data into the gold zone successfully")

    run_hourly_dq_check_v2(
        source_df=DF_Fact_campaign_load,
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
    SELECT MAX(updateDate) as max_date from minion_event.offer_silver_clean
    """)

    UpdateLastLoadDate(TableID, DF_MaxDateValue)
    print("Updated the LastLoadDate in the metadata")
    logger.info('Updated the LastLoadDate in the Metadata')

  # Update PipelineStatus and time
    UpdatePipelineStatusAndTime(TableID, 'Gold')
    print("Updated the Pipeline End Time and Status in the metadata")
    logger.info('Updated the Pipeline End Time and Status in the Metadata')

except Exception as e:
    # Log the exception
    print("Failed to Load the Data into the Table")
    logger.error(f"Failed to Load the Data into the Table")
    raise(e)
