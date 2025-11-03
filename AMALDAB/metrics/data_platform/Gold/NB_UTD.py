# Databricks notebook source
# MAGIC %md
# MAGIC ### Unit Testing for the tables loaded
# MAGIC ####Checks included:
# MAGIC ####Null Count, row count and duplicate check

# COMMAND ----------

# DBTITLE 1,catalog initialization
# MAGIC %run
# MAGIC /Workspace/Users/drajeev@fluentco.com/data_platform/General/NB_Configuration

# COMMAND ----------

spark.sql(f"use catalog {catalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Row count Check

# COMMAND ----------

# DBTITLE 1,dim_traffic
DF_Dim_TrafficSource_Load = spark.sql(f'''
            with incremental_extract AS (
            SELECT 
                    trafficSource.sourceId,
                    trafficSource.Name,
                    trafficSource.PartnerDomain,
                    trafficSource.SubVertical,
                    trafficSource.SubVerticalId,
                    trafficSource.TrafficPartnerName,
                    trafficSource.TrafficPartnerType,
                    trafficSource.Vertical,
                    trafficSource.VerticalId,
                    timestamp,
                ROW_NUMBER() OVER (PARTITION BY trafficSource.sourceId ORDER BY updateDate DESC) AS rn
            FROM minion_event.offer_silver_clean
        )

        SELECT 
            sha2(
                concat(
                    coalesce(sourceId, ''),
                    coalesce(name, ''),
                    coalesce(PartnerDomain, ''),
                    coalesce(SubVertical, ''),
                    coalesce(SubVerticalId, ''),
                    coalesce(TrafficPartnerName, ''),
                    coalesce(TrafficPartnerType, ''),
                    coalesce(Vertical, ''),
                    coalesce(VerticalId, '')
                ), 256
            ) as dwh_traffic_source_id,
            sourceid as traffic_source_id,
            Name as name,
            PartnerDomain as partner_domain,
            SubVertical as sub_vertical,
            SubVerticalId as sub_vertical_id,
            TrafficPartnerName as traffic_partner_name,
            TrafficPartnerType as traffic_partner_type,
            Vertical,
            VerticalId as vertical_id,
            current_timestamp() as dwh_created_at,
            current_timestamp() as dwh_modified_at
        FROM incremental_extract
        WHERE rn = 1
    ''')

# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, when
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
    DF_Dim_TrafficSource_Load.dropDuplicates()
    DF_Dim_TrafficSource_Load.createOrReplaceTempView('Vw_Dim_TrafficSourceLoad')
    
except Exception as e:
    # Log the exception
    print("Failed to de-duplicate and handle null from the dataframe")
    # logger.error(f"Failed to de-duplicate and handle null from the dataframe")
    raise(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from Vw_Dim_TrafficSourceLoad
# MAGIC union all
# MAGIC select count(1) from gold.dim_traffic_source

# COMMAND ----------

# DBTITLE 1,fact_profile_request_match
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
            FROM identity_graph_api.bronze_request_logs
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

DF_Fact_ProfileRequestMatch_Load.dropDuplicates()
DF_Fact_ProfileRequestMatch_Load.createOrReplaceTempView("Vw_Fact_ProfileRequestMatch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from Vw_Fact_ProfileRequestMatch
# MAGIC union all
# MAGIC select count(1) from gold.fact_profile_request_match

# COMMAND ----------

# DBTITLE 1,fact_session_events
from pyspark.sql.functions import col, trim, lit, when

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
        left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        ''')
    
DF_Fact_SessionEvents_Load = DF_Fact_SessionEvents_Load.filter(
    (col('dwh_session_events_id').isNotNull()) & (trim(col('dwh_session_events_id')) != '')
)
DF_Fact_SessionEvents_Load.dropDuplicates()
DF_Fact_SessionEvents_Load.createOrReplaceTempView("Vw_Fact_SessionEvent_Load")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from Vw_Fact_SessionEvent_Load
# MAGIC union all
# MAGIC select count(1) from gold.fact_session_events

# COMMAND ----------

# DBTITLE 1,fact_session
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
    left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
    left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from vw_session
# MAGIC union all
# MAGIC select count(1) from gold.fact_session

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Count

# COMMAND ----------

# DBTITLE 1,dim_traffic
DF_Dim_TrafficSource_Load = spark.sql(f'''
            with incremental_extract AS (
            SELECT 
                    trafficSource.sourceId,
                    trafficSource.Name,
                    trafficSource.PartnerDomain,
                    trafficSource.SubVertical,
                    trafficSource.SubVerticalId,
                    trafficSource.TrafficPartnerName,
                    trafficSource.TrafficPartnerType,
                    trafficSource.Vertical,
                    trafficSource.VerticalId,
                    timestamp,
                ROW_NUMBER() OVER (PARTITION BY trafficSource.sourceId ORDER BY updateDate DESC) AS rn
            FROM minion_event.offer_silver_clean
        )

        SELECT 
            sha2(
                concat(
                    coalesce(sourceId, ''),
                    coalesce(name, ''),
                    coalesce(PartnerDomain, ''),
                    coalesce(SubVertical, ''),
                    coalesce(SubVerticalId, ''),
                    coalesce(TrafficPartnerName, ''),
                    coalesce(TrafficPartnerType, ''),
                    coalesce(Vertical, ''),
                    coalesce(VerticalId, '')
                ), 256
            ) as dwh_traffic_source_id,
            sourceid as traffic_source_id,
            Name as name,
            PartnerDomain as partner_domain,
            SubVertical as sub_vertical,
            SubVerticalId as sub_vertical_id,
            TrafficPartnerName as traffic_partner_name,
            TrafficPartnerType as traffic_partner_type,
            Vertical,
            VerticalId as vertical_id,
            current_timestamp() as dwh_created_at,
            current_timestamp() as dwh_modified_at
        FROM incremental_extract
        WHERE rn = 1
    ''')

# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, when
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
    DF_Dim_TrafficSource_Load.dropDuplicates()
    DF_Dim_TrafficSource_Load.createOrReplaceTempView('Vw_Dim_TrafficSourceLoad')
    
except Exception as e:
    # Log the exception
    print("Failed to de-duplicate and handle null from the dataframe")
    # logger.error(f"Failed to de-duplicate and handle null from the dataframe")
    raise(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select traffic_source_id, count(1) from Vw_Dim_TrafficSourceLoad group by traffic_source_id 
# MAGIC having count(1) > 1    

# COMMAND ----------

# MAGIC %sql
# MAGIC select traffic_source_id, count(1) from gold.dim_traffic_source group by traffic_source_id 
# MAGIC having count(1) > 1    

# COMMAND ----------

# DBTITLE 1,fact_profile_request_match
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
            FROM identity_graph_api.bronze_request_logs
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

DF_Fact_ProfileRequestMatch_Load.dropDuplicates()
DF_Fact_ProfileRequestMatch_Load.createOrReplaceTempView("Vw_Fact_ProfileRequestMatch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select request_trace_id, count(1) from Vw_Fact_ProfileRequestMatch
# MAGIC group by request_trace_id
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select request_trace_id, count(1) from gold.fact_profile_request_match
# MAGIC group by request_trace_id
# MAGIC having count(1) > 1

# COMMAND ----------

# DBTITLE 1,fact_session_events
from pyspark.sql.functions import col, trim, lit, when

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
        left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        ''')
    
DF_Fact_SessionEvents_Load = DF_Fact_SessionEvents_Load.filter(
    (col('dwh_session_events_id').isNotNull()) & (trim(col('dwh_session_events_id')) != '')
)
DF_Fact_SessionEvents_Load.dropDuplicates()
DF_Fact_SessionEvents_Load.createOrReplaceTempView("Vw_Fact_SessionEvent_Load")

# COMMAND ----------

# MAGIC %sql
# MAGIC select dwh_session_events_id, count(1) from Vw_Fact_SessionEvent_Load 
# MAGIC group by dwh_session_events_id
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select dwh_session_events_id, count(1) from gold.fact_session_events 
# MAGIC group by dwh_session_events_id
# MAGIC having count(1) > 1

# COMMAND ----------

# DBTITLE 1,fact_session
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
    left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
    left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select dwh_session_id, count(1) from vw_session
# MAGIC group by dwh_session_id
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select dwh_session_id, count(1) from gold.fact_session
# MAGIC group by dwh_session_id
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Count

# COMMAND ----------

# DBTITLE 1,dim_traffic
DF_Dim_TrafficSource_Load = spark.sql(f'''
            with incremental_extract AS (
            SELECT 
                    trafficSource.sourceId,
                    trafficSource.Name,
                    trafficSource.PartnerDomain,
                    trafficSource.SubVertical,
                    trafficSource.SubVerticalId,
                    trafficSource.TrafficPartnerName,
                    trafficSource.TrafficPartnerType,
                    trafficSource.Vertical,
                    trafficSource.VerticalId,
                    timestamp,
                ROW_NUMBER() OVER (PARTITION BY trafficSource.sourceId ORDER BY updateDate DESC) AS rn
            FROM minion_event.offer_silver_clean
        )

        SELECT 
            sha2(
                concat(
                    coalesce(sourceId, ''),
                    coalesce(name, ''),
                    coalesce(PartnerDomain, ''),
                    coalesce(SubVertical, ''),
                    coalesce(SubVerticalId, ''),
                    coalesce(TrafficPartnerName, ''),
                    coalesce(TrafficPartnerType, ''),
                    coalesce(Vertical, ''),
                    coalesce(VerticalId, '')
                ), 256
            ) as dwh_traffic_source_id,
            sourceid as traffic_source_id,
            Name as name,
            PartnerDomain as partner_domain,
            SubVertical as sub_vertical,
            SubVerticalId as sub_vertical_id,
            TrafficPartnerName as traffic_partner_name,
            TrafficPartnerType as traffic_partner_type,
            Vertical,
            VerticalId as vertical_id,
            current_timestamp() as dwh_created_at,
            current_timestamp() as dwh_modified_at
        FROM incremental_extract
        WHERE rn = 1
    ''')

# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, when
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
    DF_Dim_TrafficSource_Load.dropDuplicates()
    DF_Dim_TrafficSource_Load.createOrReplaceTempView('Vw_Dim_TrafficSourceLoad')
    
except Exception as e:
    # Log the exception
    print("Failed to de-duplicate and handle null from the dataframe")
    # logger.error(f"Failed to de-duplicate and handle null from the dataframe")
    raise(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'dwh_traffic_source_id' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE dwh_traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'name' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE name IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'partner_domain' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE partner_domain IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'sub_vertical' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE sub_vertical IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'sub_vertical_id' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE sub_vertical_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'traffic_partner_name' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE traffic_partner_name IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'traffic_partner_type' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE traffic_partner_type IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'Vertical' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE Vertical IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'vertical_id' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE vertical_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Dim_TrafficSourceLoad' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM Vw_Dim_TrafficSourceLoad WHERE dwh_modified_at IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'dwh_traffic_source_id' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE dwh_traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'name' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE name IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'partner_domain' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE partner_domain IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'sub_vertical' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE sub_vertical IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'sub_vertical_id' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE sub_vertical_id IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'traffic_partner_name' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE traffic_partner_name IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'traffic_partner_type' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE traffic_partner_type IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'Vertical' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE Vertical IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'vertical_id' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE vertical_id IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'Dim_TrafficSourceLoad' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM gold.dim_traffic_source WHERE dwh_modified_at IS NULL

# COMMAND ----------

# DBTITLE 1,fact_profile_request_match
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
            FROM identity_graph_api.bronze_request_logs
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

DF_Fact_ProfileRequestMatch_Load.dropDuplicates()
DF_Fact_ProfileRequestMatch_Load.createOrReplaceTempView("Vw_Fact_ProfileRequestMatch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'dwh_profile_request_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE dwh_profile_request_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'request_trace_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE request_trace_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'match_type' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE match_type IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'request_timestamp' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE request_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'response_timestamp' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE response_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_ProfileRequestMatch' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_ProfileRequestMatch WHERE dwh_modified_at IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'dwh_profile_request_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE dwh_profile_request_id IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'request_trace_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE request_trace_id IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'match_type' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE match_type IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'request_timestamp' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE request_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'response_timestamp' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE response_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'Fact_ProfileRequestMatch' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_profile_request_match WHERE dwh_modified_at IS NULL

# COMMAND ----------

# DBTITLE 1,fact_session_events
from pyspark.sql.functions import col, trim, lit, when

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
        left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        ''')
    
DF_Fact_SessionEvents_Load = DF_Fact_SessionEvents_Load.filter(
    (col('dwh_session_events_id').isNotNull()) & (trim(col('dwh_session_events_id')) != '')
)
DF_Fact_SessionEvents_Load.dropDuplicates()
DF_Fact_SessionEvents_Load.createOrReplaceTempView("Vw_Fact_SessionEvent_Load")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'dwh_session_events_hash_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE dwh_session_events_hash_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'dwh_session_events_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE dwh_session_events_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'campaign_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE campaign_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'session_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE session_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'advertiser_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE advertiser_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'partner_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE partner_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'creative_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE creative_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'tune_offer_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE tune_offer_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'transaction_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE transaction_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'product_scope' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE product_scope IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'event_type' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE event_type IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'conversion_goal_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE conversion_goal_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'action_timestamp' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE action_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'campaign_revenue' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE campaign_revenue IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'partner_revenue' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE partner_revenue IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'position' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE position IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'email_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE email_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'emailmd5_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE emailmd5_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'emailsha256_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE emailsha256_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'telephone_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE telephone_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'phone_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE phone_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'maid_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE maid_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'first_name_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE first_name_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'order_id_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE order_id_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'transaction_value_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE transaction_value_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'ccbin_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE ccbin_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'gender_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE gender_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'zip_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE zip_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'zippost_coverage_flag' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE zippost_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'source_reference_id' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE source_reference_id IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'action_date_key' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE action_date_key IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'action_time_key' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE action_time_key IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'Vw_Fact_SessionEvent_Load' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM Vw_Fact_SessionEvent_Load WHERE dwh_modified_at IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'dwh_session_events_hash_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE dwh_session_events_hash_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'dwh_session_events_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE dwh_session_events_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'campaign_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE campaign_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'session_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE session_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'advertiser_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE advertiser_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'partner_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE partner_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'creative_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE creative_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'tune_offer_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE tune_offer_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'transaction_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE transaction_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'product_scope' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE product_scope IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'event_type' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE event_type IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'conversion_goal_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE conversion_goal_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'action_timestamp' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE action_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'campaign_revenue' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE campaign_revenue IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'partner_revenue' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE partner_revenue IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'position' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE position IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'email_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE email_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'emailmd5_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE emailmd5_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'emailsha256_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE emailsha256_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'telephone_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE telephone_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'phone_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE phone_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'maid_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE maid_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'first_name_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE first_name_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'order_id_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE order_id_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'transaction_value_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE transaction_value_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'ccbin_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE ccbin_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'gender_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE gender_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'zip_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE zip_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'zippost_coverage_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE zippost_coverage_flag IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'source_reference_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE source_reference_id IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'action_date_key' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE action_date_key IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'action_time_key' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE action_time_key IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'gold.fact_session_events' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_session_events WHERE dwh_modified_at IS NULL

# COMMAND ----------

# DBTITLE 1,fact_session
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
    left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
    left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'vw_session' AS table_name, 'dwh_session_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE dwh_session_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'campaign_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE campaign_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'tune_offer_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE tune_offer_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'advertiser_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE advertiser_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'partner_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE partner_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'creative_id' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE creative_id IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'product_scope' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE product_scope IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'total_actions' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE total_actions IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'total_clicks' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE total_clicks IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'total_views' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE total_views IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'primary_conversions' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE primary_conversions IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'secondary_conversions' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE secondary_conversions IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'first_conversion_window' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE first_conversion_window IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'conversion_flag' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE conversion_flag IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_start_timestamp' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_start_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_start_date' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_start_date IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_start_time' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_start_time IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_end_timestamp' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_end_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_end_date' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_end_date IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'session_end_time' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE session_end_time IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'vw_session' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM vw_session WHERE dwh_modified_at IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_session' AS table_name, 'dwh_session_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE dwh_session_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'campaign_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE campaign_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'tune_offer_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE tune_offer_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'fluent_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE fluent_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'advertiser_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE advertiser_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'partner_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE partner_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'traffic_source_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE traffic_source_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'creative_id' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE creative_id IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'product_scope' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE product_scope IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'total_actions' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE total_actions IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'total_clicks' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE total_clicks IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'total_views' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE total_views IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'primary_conversions' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE primary_conversions IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'secondary_conversions' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE secondary_conversions IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'first_conversion_window' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE first_conversion_window IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'conversion_flag' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE conversion_flag IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_start_timestamp' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_start_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_start_date' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_start_date IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_start_time' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_start_time IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_end_timestamp' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_end_timestamp IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_end_date' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_end_date IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'session_end_time' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE session_end_time IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'dwh_created_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE dwh_created_at IS NULL UNION ALL 
# MAGIC SELECT 'fact_session' AS table_name, 'dwh_modified_at' AS column_name, COUNT(*) AS null_count FROM gold.fact_session WHERE dwh_modified_at IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# DBTITLE 1,dim_traffic
DF_Dim_TrafficSource_Load = spark.sql(f'''
            with incremental_extract AS (
            SELECT 
                    trafficSource.sourceId,
                    trafficSource.Name,
                    trafficSource.PartnerDomain,
                    trafficSource.SubVertical,
                    trafficSource.SubVerticalId,
                    trafficSource.TrafficPartnerName,
                    trafficSource.TrafficPartnerType,
                    trafficSource.Vertical,
                    trafficSource.VerticalId,
                    timestamp,
                ROW_NUMBER() OVER (PARTITION BY trafficSource.sourceId ORDER BY updateDate DESC) AS rn
            FROM minion_event.offer_silver_clean
        )

        SELECT 
            sha2(
                concat(
                    coalesce(sourceId, ''),
                    coalesce(name, ''),
                    coalesce(PartnerDomain, ''),
                    coalesce(SubVertical, ''),
                    coalesce(SubVerticalId, ''),
                    coalesce(TrafficPartnerName, ''),
                    coalesce(TrafficPartnerType, ''),
                    coalesce(Vertical, ''),
                    coalesce(VerticalId, '')
                ), 256
            ) as dwh_traffic_source_id,
            sourceid as traffic_source_id,
            Name as name,
            PartnerDomain as partner_domain,
            SubVertical as sub_vertical,
            SubVerticalId as sub_vertical_id,
            TrafficPartnerName as traffic_partner_name,
            TrafficPartnerType as traffic_partner_type,
            Vertical,
            VerticalId as vertical_id,
            current_timestamp() as dwh_created_at,
            current_timestamp() as dwh_modified_at
        FROM incremental_extract
        WHERE rn = 1
    ''')

# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, when
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
    DF_Dim_TrafficSource_Load.dropDuplicates()
    DF_Dim_TrafficSource_Load.createOrReplaceTempView('Vw_Dim_TrafficSourceLoad')
    
except Exception as e:
    # Log the exception
    print("Failed to de-duplicate and handle null from the dataframe")
    # logger.error(f"Failed to de-duplicate and handle null from the dataframe")
    raise(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Vw_Dim_TrafficSourceLoad
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_traffic_source
# MAGIC where dwh_traffic_source_id in ('524ea920b399255818ba3cc1f55709b2acfdd49dacc2e3ddc57bf0de2a86f716',
# MAGIC 'febef7b53bd52f087c55e9c9d9ddf8280d64955b63fc9b23c87e1b9ec91631ba',
# MAGIC '9a3195f62dc602624e512ca47fcf8d0b847a5638485ca650048c17a754b53173',
# MAGIC 'd780cbab5eade4dd555557740e998f694359dd7c64c64ea1f26bee9621a7a159',
# MAGIC '8203e1e869defbe1ecfb53afb22d756577680cc2206330a986063d198552c360') 

# COMMAND ----------

# DBTITLE 1,fact_profile_request_match
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
            FROM identity_graph_api.bronze_request_logs
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

DF_Fact_ProfileRequestMatch_Load.dropDuplicates()
DF_Fact_ProfileRequestMatch_Load.createOrReplaceTempView("Vw_Fact_ProfileRequestMatch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Vw_Fact_ProfileRequestMatch
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fact_profile_request_match
# MAGIC where dwh_profile_request_id in ('fe11123fd28a428a6f07c83c9b4c26a120edc9620847b65716b9adbff14f048a',
# MAGIC 'df53f4919f9ff7be2b7f2bfc9e5240d7112db72a2c9e1771f7c6a8a9ebe2e8b3',
# MAGIC 'ae5d1e1ded768a0d808ee0ea69202ab286ae9116b8bd30fac53f05c651036470',
# MAGIC 'a9703f1be26d1a3322aa5857026088b80f2a414258de774942a24f4fd1cd64a7',
# MAGIC '88467d8f85ec8a4142b493003d48f251d90563347684f14488edc9008c07f99b')

# COMMAND ----------

# DBTITLE 1,fact_session_event
from pyspark.sql.functions import col, trim, lit, when

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
        left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
        left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
        ''')
    
DF_Fact_SessionEvents_Load = DF_Fact_SessionEvents_Load.filter(
    (col('dwh_session_events_id').isNotNull()) & (trim(col('dwh_session_events_id')) != '')
)
DF_Fact_SessionEvents_Load.dropDuplicates()
DF_Fact_SessionEvents_Load.createOrReplaceTempView("Vw_Fact_SessionEvent_Load")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Vw_Fact_SessionEvent_Load
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fact_session_events
# MAGIC where dwh_session_events_hash_id in (
# MAGIC   'fe604d76d46d2d1af1d8faa3fc2e0affd44a6c0cbdad205f06ec42ad9f1c909a',
# MAGIC 'b96bec1c89f5c8f14c4419f2c14589b4dcf8b6aa9f5002d0c093c7529ffa340c',
# MAGIC '99ac107d42bf336b020bc7190c42fb01984b21fbe27e9dc8e12071818eed00ba',
# MAGIC '36cd33a84e749480fa859e2aee89d6a5e32b64e71f403f93ebcfe168b571d8c8',
# MAGIC '5b774a887fdeea5b24cac606bf0c9fb07c5694aa31431f11c11701cc31117259'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,fact_session
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
    left join gold.dim_date d on date_format(TIMESTAMP(timestamp), 'yyyy-MM-dd') = d.full_date_text
    left join gold.dim_time t on date_format(TIMESTAMP (timestamp), 'HH:mm:ss') = t.time
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_session
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fact_session 
# MAGIC where dwh_session_id in (
# MAGIC   '9fd9a39f2b06f5a21662df769f07dc8c460bab9b5773623297c76616cae3df9f',
# MAGIC '1e43c343a961a9d783ee0a3df48f1cf8a38da775254b3f9516bdc18a621c76eb',
# MAGIC '696f18ec3f1b586012e18110250abec81a25f91257ae050e28e056025b50982a',
# MAGIC '435c48211c9de1a9320346cc988685d7e7322ed33c271cd6a54bb06ee8220ea7',
# MAGIC 'cc37cdcbbad3252677eea0156b73e9012da3b920de6571b11bf9c9ac9ba324d8'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table minion_event.offer_silver_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily_events AS (
# MAGIC   SELECT
# MAGIC     DATE(t.timestamp) AS event_date,
# MAGIC     t.campaignData.campaignId AS campaign_id,
# MAGIC     COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId) AS partner_id,
# MAGIC     t.campaignData.advertiserId AS advertiser_id,
# MAGIC
# MAGIC     COUNT(CASE WHEN t.sourceReference = 'offer-view'  THEN 1 END) AS views,
# MAGIC     COUNT(CASE WHEN t.sourceReference = 'offer-click' THEN 1 END) AS clicks,
# MAGIC     COUNT(CASE WHEN t.sourceReference = 'SessionStart' THEN 1 END) AS conversions
# MAGIC
# MAGIC   FROM centraldata_prod.minion_event.offer_silver_clean t
# MAGIC   WHERE 
# MAGIC     t.timestamp IS NOT NULL
# MAGIC     AND t.campaignData.campaignId IS NOT NULL
# MAGIC     AND t.campaignData.advertiserId IS NOT NULL
# MAGIC   GROUP BY 
# MAGIC     DATE(t.timestamp),
# MAGIC     t.campaignData.campaignId,
# MAGIC     COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId),
# MAGIC     t.campaignData.advertiserId
# MAGIC ),
# MAGIC
# MAGIC daily_metrics AS (
# MAGIC   SELECT
# MAGIC     event_date,
# MAGIC     campaign_id,
# MAGIC     partner_id,
# MAGIC     advertiser_id,
# MAGIC     views,
# MAGIC     clicks,
# MAGIC     conversions,
# MAGIC
# MAGIC     CASE WHEN views > 0 THEN CAST(clicks AS DOUBLE) / views ELSE NULL END AS current_ctr,
# MAGIC     CASE WHEN views > 0 THEN CAST(conversions AS DOUBLE) / views ELSE NULL END AS current_cvr
# MAGIC
# MAGIC   FROM daily_events
# MAGIC ),
# MAGIC yoy_comparison AS (
# MAGIC   SELECT
# MAGIC     c.event_date,
# MAGIC     c.campaign_id,
# MAGIC     c.partner_id,
# MAGIC     c.advertiser_id,
# MAGIC     c.current_ctr,
# MAGIC     c.current_cvr,
# MAGIC     c.clicks AS current_clicks,
# MAGIC     c.conversions AS current_conversions,
# MAGIC     c.views AS current_views,
# MAGIC
# MAGIC     p.current_ctr AS previous_year_ctr,
# MAGIC     p.current_cvr AS previous_year_cvr,
# MAGIC     p.clicks AS previous_year_clicks,
# MAGIC     p.conversions AS previous_year_conversions,
# MAGIC     p.views AS previous_year_views
# MAGIC
# MAGIC   FROM daily_metrics c
# MAGIC   LEFT JOIN daily_metrics p 
# MAGIC     ON c.campaign_id = p.campaign_id
# MAGIC     AND c.partner_id = p.partner_id
# MAGIC     AND c.advertiser_id = p.advertiser_id
# MAGIC     AND c.event_date = add_months(p.event_date, -12) 
# MAGIC ),
# MAGIC
# MAGIC -- Enrich with dim_date
# MAGIC final_enriched AS (
# MAGIC   SELECT
# MAGIC     y.*,
# MAGIC     d.day_of_month,
# MAGIC     d.month,                   
# MAGIC     d.week_of_month,
# MAGIC     d.year AS calendar_year    
# MAGIC
# MAGIC   FROM yoy_comparison y
# MAGIC   LEFT JOIN metrics.dim_date d 
# MAGIC     ON DATE_FORMAT(y.event_date, 'yyyy-MM-dd') = d.full_date_text
# MAGIC )
# MAGIC
# MAGIC -- Final Select
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   day_of_month,
# MAGIC   month,      
# MAGIC   week_of_month,
# MAGIC   calendar_year AS year,  
# MAGIC   campaign_id,
# MAGIC   partner_id,
# MAGIC   advertiser_id,
# MAGIC   ROUND(current_ctr, 6) AS current_ctr,
# MAGIC   ROUND(
# MAGIC     CASE 
# MAGIC       WHEN previous_year_ctr IS NOT NULL AND previous_year_ctr != 0
# MAGIC       THEN (current_ctr - previous_year_ctr) / previous_year_ctr * 100
# MAGIC       ELSE NULL 
# MAGIC     END, 2
# MAGIC   ) AS ctr_mom_percentage_change,
# MAGIC   ROUND(current_cvr, 6) AS current_cvr,
# MAGIC   ROUND(
# MAGIC     CASE 
# MAGIC       WHEN previous_year_cvr IS NOT NULL AND previous_year_cvr != 0
# MAGIC       THEN (current_cvr - previous_year_cvr) / previous_year_cvr * 100
# MAGIC       ELSE NULL 
# MAGIC     END, 2
# MAGIC   ) AS cvr_mom_percentage_change
# MAGIC FROM final_enriched
# MAGIC ORDER BY event_date DESC, campaign_id, partner_id, advertiser_id;
