# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Fact_daily_metrics <br>
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
ErrorLogger = ErrorLogger('NB_Fact_daily_metrics')
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# Create DF_Metadata DataFrame
try:
    TableID = 'B8'
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
    DF_Fact_Daily_Load = spark.sql(f'''
        WITH daily_events AS (
            SELECT
                date(from_utc_timestamp(timestamp, 'America/New_York')) as event_date,
                t.campaignData.campaignId AS campaign_id,
                COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId) AS partner_id,
                t.campaignData.advertiserId AS advertiser_id,
                t.campaignData.tuneOfferId AS tune_offer_id,
                t.trafficSource.sourceId AS source_id,
                t.campaignData.creativeId AS creative_id,
                t.fluentId AS fluent_id,
                t.productScope AS product_scope,
                CAST(t.campaignData.flowImpressionPosition AS DECIMAL(19,4)) AS position,
                COUNT(CASE WHEN t.sourceReference = 'offer-view' THEN 1 END) AS views,
                COUNT(CASE WHEN t.sourceReference = 'offer-click' THEN 1 END) AS clicks,
                COUNT(CASE WHEN t.sourceReference = 'sessionStart' THEN 1 END) AS possible_session,
                COUNT(CASE WHEN t.sourceReference = 'offer-view' AND t.campaignData.flowImpressionPosition = 1 THEN 1 END) AS sessions,
                COUNT(DISTINCT t.sessionId) AS distinct_sessions,
                COUNT(DISTINCT t.data.orderId) AS partner_transactions,
                COUNT(CASE WHEN TRY_CAST(t.campaignData.conversionGoalId AS INT) = 0 THEN 1 END) AS primary_conversions,
                COUNT(CASE WHEN TRY_CAST(t.campaignData.conversionGoalId AS INT) IS NOT NULL THEN 1 END) AS funnel_conversions,
                SUM(t.campaignData.Revenue) AS campaign_revenue,
                SUM(t.trafficSource.trafficPartnerRevenue) AS partner_revenue,
                COUNT(CASE WHEN t.sourceReference = 'sessionStart' THEN 1 END) AS suppressions,
                COUNT(CASE WHEN t.data.email IS NOT NULL AND t.data.email NOT IN ('', ',', ' ') THEN 1 END) AS email_coverage,
                COUNT(CASE WHEN t.data.emailmd5 IS NOT NULL AND t.data.emailmd5 NOT IN ('', ',', ' ') THEN 1 END) AS emailmd5_coverage,
                COUNT(CASE WHEN t.data.emailsha256 IS NOT NULL AND t.data.emailsha256 NOT IN ('', ',', ' ') THEN 1 END) AS emailsha256_coverage,
                COUNT(CASE WHEN t.data.telephone IS NOT NULL AND t.data.telephone NOT IN ('', ',', ' ', '.') THEN 1 END) AS telephone_coverage,
                COUNT(CASE WHEN t.data.phone IS NOT NULL AND t.data.phone NOT IN ('', ',', ' ', '.') THEN 1 END) AS phone_coverage,
                COUNT(CASE WHEN t.data.deviceAdvertisingId IS NOT NULL AND t.data.deviceAdvertisingId NOT IN ('', ',', ' ', '.') THEN 1 END) AS maid_coverage,
                COUNT(CASE WHEN t.data.first_name IS NOT NULL AND t.data.first_name NOT IN ('', ',', ' ', '.') THEN 1 END) AS first_name_coverage,
                COUNT(CASE WHEN t.data.orderId IS NOT NULL AND t.data.orderId NOT IN ('', ',', ' ', '.') THEN 1 END) AS order_id_coverage,
                COUNT(CASE WHEN t.data.transactionValue IS NOT NULL AND t.data.transactionValue NOT IN ('', ',', ' ', '.') THEN 1 END) AS transaction_value_coverage,
                COUNT(CASE WHEN t.data.ccBin IS NOT NULL AND t.data.ccBin NOT IN ('', ',', ' ', '.') THEN 1 END) AS ccbin_coverage,
                COUNT(CASE WHEN t.data.gender IS NOT NULL AND t.data.gender NOT IN ('', ',', ' ', '.') THEN 1 END) AS gender_coverage,
                COUNT(CASE WHEN t.data.zip IS NOT NULL AND t.data.zip NOT IN ('', ',', ' ', '.') THEN 1 END) AS zip_coverage,
                COUNT(CASE WHEN t.data.zippost IS NOT NULL AND t.data.zippost NOT IN ('', ',', ' ', '.') THEN 1 END) AS zippost_coverage,
                CAST(
                    CASE 
                    WHEN MIN(CASE WHEN t.sourceReference = 'offer-view' THEN t.Timestamp END) IS NOT NULL
                        AND MIN(CASE WHEN t.sourceReference = 'offer-convert' THEN t.Timestamp END) IS NOT NULL
                    THEN DATEDIFF(minute,
                        MIN(CASE WHEN t.sourceReference = 'offer-view' THEN t.Timestamp END),
                        MIN(CASE WHEN t.sourceReference = 'offer-convert' THEN t.Timestamp END)
                    )
                    ELSE NULL
                    END AS BIGINT
                ) AS first_conversion_window
            FROM minion_event.offer_silver_clean t
            WHERE lower(t.productScope) = 'adflow'
            and t.updateDate > '{LastLoadDate}'
            GROUP BY 
                date(from_utc_timestamp(timestamp, 'America/New_York')),
                t.campaignData.campaignId,
                COALESCE(t.trafficSource.trafficPartnerId, t.data.PartnerId),
                t.campaignData.advertiserId,
                t.campaignData.tuneOfferId,
                t.trafficSource.sourceId,
                t.campaignData.creativeId,
                t.fluentId,
                t.productScope,
                CAST(t.campaignData.flowImpressionPosition AS DECIMAL(19,4))
        ),
        daily_metrics AS (
            SELECT
                event_date,
                campaign_id,
                partner_id,
                advertiser_id,
                tune_offer_id,
                source_id,
                creative_id,
                fluent_id,
                product_scope,
                position,
                views,
                clicks,
                primary_conversions,
                funnel_conversions,
                sessions,
                distinct_sessions,
                possible_session,
                suppressions,
                partner_transactions,
                campaign_revenue,
                partner_revenue,
                email_coverage,
                emailmd5_coverage,
                emailsha256_coverage,
                telephone_coverage,
                phone_coverage,
                maid_coverage,
                first_name_coverage,
                order_id_coverage,
                transaction_value_coverage,
                ccbin_coverage,
                gender_coverage,
                zip_coverage,
                zippost_coverage,
                first_conversion_window,
                CASE WHEN views > 0 THEN CAST(clicks AS DOUBLE) / views ELSE NULL END AS current_ctr,
                CASE WHEN sessions > 0 THEN CAST(primary_conversions AS DOUBLE) / sessions ELSE NULL END AS current_cvr
            FROM daily_events
        ),
        comparison AS (
            SELECT
                c.event_date,
                c.campaign_id,
                c.partner_id,
                c.advertiser_id,
                c.tune_offer_id,
                c.source_id,
                c.creative_id,
                c.fluent_id,
                c.product_scope,
                c.position,
                c.current_ctr,
                c.current_cvr,
                c.clicks AS current_clicks,
                c.views AS current_views,
                c.sessions,
                c.distinct_sessions,
                c.possible_session,
                c.suppressions,
                c.partner_transactions,
                c.primary_conversions,
                c.funnel_conversions,
                c.campaign_revenue,
                c.partner_revenue,
                c.email_coverage,
                c.emailmd5_coverage,
                c.emailsha256_coverage,
                c.telephone_coverage,
                c.phone_coverage,
                c.maid_coverage,
                c.first_name_coverage,
                c.order_id_coverage,
                c.transaction_value_coverage,
                c.ccbin_coverage,
                c.gender_coverage,
                c.zip_coverage,
                c.zippost_coverage,
                c.first_conversion_window,
                p.current_ctr AS previous_year_ctr,
                p.current_cvr AS previous_year_cvr,
                p.clicks AS previous_year_clicks,
                p.views AS previous_year_views
            FROM daily_metrics c
            LEFT JOIN daily_metrics p 
                ON c.campaign_id = p.campaign_id
                AND c.partner_id = p.partner_id
                AND c.advertiser_id = p.advertiser_id
                AND c.tune_offer_id = p.tune_offer_id
                AND c.source_id = p.source_id
                AND c.creative_id = p.creative_id
                AND c.fluent_id = p.fluent_id
                AND c.product_scope = p.product_scope
                AND c.position = p.position
                AND c.event_date = add_months(p.event_date, -12)
        ),
        final_enriched AS (
            SELECT y.* FROM comparison y
        )
        SELECT
            sha2(concat(
                event_date,
                coalesce(campaign_id, 'NULL'),
                coalesce(partner_id, 'NULL'),
                coalesce(advertiser_id, 'NULL'),
                coalesce(tune_offer_id, 'NULL'),
                coalesce(source_id, 'NULL'),
                coalesce(creative_id, 'NULL'),
                coalesce(fluent_id, 'NULL'),
                coalesce(product_scope, 'NULL'),
                coalesce(CAST(position AS STRING), 'NULL')
            ), 256) AS dwh_daily_id,
            event_date,
            campaign_id,
            partner_id,
            advertiser_id,
            tune_offer_id,
            source_id,
            creative_id,
            fluent_id,
            product_scope,
            position,
            current_clicks AS clicks,
            current_views AS views,
            sessions,
            distinct_sessions,
            possible_session,
            suppressions,
            partner_transactions,
            primary_conversions,
            funnel_conversions,
            CAST(campaign_revenue AS DECIMAL(19, 2)) AS campaign_revenue,
            CAST(partner_revenue AS DECIMAL(19, 2)) AS partner_revenue,
            email_coverage,
            emailmd5_coverage,
            emailsha256_coverage,
            telephone_coverage,
            phone_coverage,
            maid_coverage,
            first_name_coverage,
            order_id_coverage,
            transaction_value_coverage,
            ccbin_coverage,
            gender_coverage,
            zip_coverage,
            zippost_coverage,
            first_conversion_window,
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
            current_timestamp() AS dwh_created_at,
            current_timestamp() AS dwh_modified_at
        FROM final_enriched 
              ''')

    DF_Fact_Daily_Load = DF_Fact_Daily_Load.dropDuplicates()
    DF_Fact_Daily_Load.createOrReplaceTempView('vw_daily_load')
    print("Successfully created the dataframe")
    logger.info('Successfully Created a Dataframe named DF_Fact_Daily_Load and created a view named vw_daily_load')

except Exception as e:
    # Log the exception
    print("Failed to execute the SQL query")
    logger.error(f"Failed to get the Data from Metadata")
    raise(e)

# COMMAND ----------

# DBTITLE 1,Referential-Integrity check
try:
    fk_mappings = {
        "metrics.fact_daily_metrics": {
        "fluent_id": ("customer.gold_customer_v1", "fluent_id"),
        "advertiser_id": ("minion.gold_advertiser", "advertiser_id"),
        "partner_id": ("minion.gold_partner", "partner_id"),
        "traffic_source_id": ("metrics.dim_traffic_source", "traffic_source_id"),
        "creative_id": ("minion.gold_creative", "id")
    }
    }

    result_df = run_referential_integrity_check_on_df(  
        fact_df=DF_Fact_Daily_Load,
        fact_table_name="metrics.fact_daily_metrics",  # for logging
        fk_pk_mapping=fk_mappings["metrics.fact_daily_metrics"]
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
    schema_columns = DF_Fact_Daily_Load.columns

    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in schema_columns if col not in ['dwh_daily_id',"dwh_created_at"]])

    print(f'Loading Table {TargetSchemaName}.{TargetTableName}')

    spark.sql(f"""
    MERGE INTO {TargetSchemaName}.{TargetTableName} as T
    USING vw_daily_load as S
    ON T.{MergeKey} = S.{MergeKey}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    print(f'Loaded Table {TargetSchemaName}.{TargetTableName}')

    logger.info('Loaded the data into the metrics Zone Successfully')
    print("Loaded the data into the metrics zone successfully")

    run_hourly_dq_check_v2(
        source_df=DF_Fact_Daily_Load,
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
