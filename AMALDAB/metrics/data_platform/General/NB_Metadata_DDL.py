# Databricks notebook source
# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f'use catalog {catalog_name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metadata.Mastermetadata (
# MAGIC   SourceSystem STRING COMMENT 'Stores the name of the data source system for tracking data origin within the organization.',
# MAGIC   SourceSecretName STRING COMMENT 'Stores the name of the secret associated with the data source system for accessing secure information like credentials or keys.',
# MAGIC   SourceServerName STRING COMMENT 'Stores the server name or IP of the data source for connection and extraction',
# MAGIC   TableID STRING COMMENT 'Unique identifier for each table in the organization\'s data ecosystem.',
# MAGIC   Batch BIGINT COMMENT 'Stores information about the batch number or identifier for tracking data processing batches within the organization\'s data ecosystem.',
# MAGIC   SourceID BIGINT COMMENT 'Identifier for data source within organization for mapping to metadata.',
# MAGIC   SCDColumns STRING COMMENT 'Stores information about Slowly Changing Dimension (SCD) columns within the table, which are used to track historical changes in data over time.',
# MAGIC   SubjectArea STRING COMMENT 'Stores the designated subject area or domain to which the table belongs, providing context for the type of data it holds.',
# MAGIC   SourceDBName STRING COMMENT 'Stores the name of the data source database where the table is located.',
# MAGIC   SourceSchema STRING COMMENT 'Specifies the schema name in the data source database, revealing the structural layout of data within.',
# MAGIC   SourceTableName STRING COMMENT 'Specifies the name of the table within the data source, aiding in the identification and retrieval of specific data sets.',
# MAGIC   LoadType STRING COMMENT 'Indicates the type of data loading process used for this table, such as incremental, full, or real-time, providing insight into the data ingestion method for this particular table.',
# MAGIC   IsActive BIGINT COMMENT 'Flag showing table\'s active status in organization\'s data ecosystem for data integration.',
# MAGIC   Frequency STRING COMMENT 'Frequency of data update in table, shows data refresh rate or schedule.',
# MAGIC   BronzePath STRING COMMENT 'Stores the file path where raw data is stored before further processing.',
# MAGIC   SilverPath STRING COMMENT 'File path where refined data is stored after processing from raw data in BronzePath.',
# MAGIC   GoldPath STRING COMMENT 'Final stage data storage path post refinement in SilverPath',
# MAGIC   DWHSchemaName STRING COMMENT 'Specifies the name of the schema in the data warehouse where the table is located, representing the logical grouping of tables and objects within the data warehouse structure.',
# MAGIC   DWHTableName STRING COMMENT 'Defines the table name in the data warehouse storing processed data from the corresponding source table, advancing data integration processes.',
# MAGIC   ErrorLogPath STRING COMMENT 'Stores the path for error logs detailing data processing failures or issues encountered.',
# MAGIC   LastLoadDateColumn STRING COMMENT 'Stores the date and time of the most recent data load or processing activity for tracking the last update for the table within the organization\'s data ecosystem.',
# MAGIC   MergeKey STRING COMMENT 'Unique identifier used for merging multiple datasets or tables based on a common key, facilitating data consolidation and integration processes.',
# MAGIC   HashKeyColumn STRING COMMENT 'Column to store the hashed values generated from a specific key column for data security and integrity purposes.',
# MAGIC   DependencyTableIDs STRING COMMENT 'Stores the unique identifiers of tables that this table depends on for data processing, enabling tracking of dependencies in the organization\'s data ecosystem.',
# MAGIC   PipelineStartTime TIMESTAMP COMMENT 'Timestamp indicating the start time of a data processing pipeline for this table, capturing the initiation point of data extraction, transformation, and loading operations.',
# MAGIC   PipelineEndTime TIMESTAMP COMMENT 'Timestamp of pipeline completion for this table',
# MAGIC   PipelineRunStatus STRING COMMENT 'Shows the current status of the pipeline run for the table, indicating whether the data processing pipeline is running, completed, failed, or any other relevant state.',
# MAGIC   Zone STRING COMMENT 'Zone column specifies the data processing zone to which the table belongs, indicating the stage of data refinement and transformation within the organization\'s data ecosystem.',
# MAGIC   SourceSelectQuery STRING COMMENT 'Stores the SQL query used to extract data from the specified data source in order to populate the corresponding table within the organization\'s data ecosystem.',
# MAGIC   LastLoadDateValue TIMESTAMP COMMENT 'Stores the timestamp or date value of the most recent data loading operation for the table, aiding in tracking data freshness and determining the last update time of the table.',
# MAGIC   RowCountQuery STRING COMMENT 'Stores the SQL query used to calculate the row count of the table, providing a dynamic way to retrieve the total number of rows in the table.')
# MAGIC USING delta
# MAGIC COMMENT 'The \'mastermetadata\' table serves as a central repository for metadata related to various data sources and their corresponding tables. It contains information such as source system details, table identifiers, subject areas, load types, data warehouse paths, and error log paths. This table is crucial for tracking data lineage, managing data integration processes, and ensuring data quality within the organization\'s data ecosystem.'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.Mastermetadata (
# MAGIC   SourceSystem, SourceSecretName, TableID, Batch, SourceID, SCDColumns, SubjectArea, SourceDBName, SourceSchema,
# MAGIC   SourceTableName, LoadType, IsActive, Frequency, BronzePath, SilverPath, GoldPath, DWHSchemaName, DWHTableName,
# MAGIC   ErrorLogPath, LastLoadDateColumn, MergeKey, HashKeyColumn, DependencyTableIDs, PipelineStartTime,
# MAGIC   PipelineEndTime, PipelineRunStatus, Zone, SourceSelectQuery, LastLoadDateValue, RowCountQuery
# MAGIC ) VALUES
# MAGIC (
# MAGIC   'databricks', 'NA', 'B1', 1, NULL, NULL, 'Customer', 'pulse_configuration', 'pulse_configuration',
# MAGIC   'traffic_source_silver', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'dim_traffic_source',
# MAGIC   'NA', 'DateModified', 'traffic_source_id', 'dwh_traffic_source_id', NULL, 
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:00', NULL
# MAGIC ),
# MAGIC (
# MAGIC   'databricks', 'NA', 'B2', 1, NULL, NULL, 'Customer', 'NA', 'NA',
# MAGIC   'NA', 'One Time Load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'dim_date',
# MAGIC   'NA', 'NA', 'NA', 'NA', NULL, 
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:00', NULL
# MAGIC ),
# MAGIC (
# MAGIC   'databricks', 'NA', 'B3', 1, NULL, NULL, 'Customer', 'NA', 'NA',
# MAGIC   'NA', 'One Time Load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'dim_time',
# MAGIC   'NA', 'NA', 'NA', 'NA', NULL, 
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:00', NULL
# MAGIC ),
# MAGIC (
# MAGIC   'databricks', 'NA', 'B4', 1, NULL, NULL, 'Customer', 'NA', 'identity_graph_api',
# MAGIC   'bronze_request_logs', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'fact_profile_request_match',
# MAGIC   'NA', 'etl_timestamp', 'request_trace_id', 'dwh_profile_request_id', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL
# MAGIC ),
# MAGIC (
# MAGIC   'databricks', 'NA', 'B5', 1, NULL, NULL, 'Customer', 'NA', 'minion_event',
# MAGIC   'offer_silver_clean', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'fact_session_events',
# MAGIC   'NA', 'updateDate', 'dwh_session_events_id', 'dwh_session_events_hash_id', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL
# MAGIC ),
# MAGIC (
# MAGIC   'databricks', 'NA', 'B6', 1, NULL, NULL, 'Customer', 'NA', 'minion_event',
# MAGIC   'offer_silver_clean', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'fact_session',
# MAGIC   'NA', 'updateDate', 'dwh_session_id', 'dwh_session_hash_id', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.Mastermetadata (
# MAGIC   SourceSystem, SourceSecretName, TableID, Batch, SourceID, SCDColumns, SubjectArea, SourceDBName, SourceSchema,
# MAGIC   SourceTableName, LoadType, IsActive, Frequency, BronzePath, SilverPath, GoldPath, DWHSchemaName, DWHTableName,
# MAGIC   ErrorLogPath, LastLoadDateColumn, MergeKey, HashKeyColumn, DependencyTableIDs, PipelineStartTime,
# MAGIC   PipelineEndTime, PipelineRunStatus, Zone, SourceSelectQuery, LastLoadDateValue, RowCountQuery
# MAGIC ) VALUES
# MAGIC (
# MAGIC   'databricks', 'NA', 'B7', 1, NULL, NULL, 'Customer', 'minion_event', 'minion_event',
# MAGIC   'offer_silver_clean', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'fact_campaign_daily',
# MAGIC   'NA', 'updateDate', 'dwh_campaign_id', 'NA', NULL, 
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.Mastermetadata (
# MAGIC   SourceSystem, SourceSecretName, TableID, Batch, SourceID, SCDColumns, SubjectArea, SourceDBName, SourceSchema,
# MAGIC   SourceTableName, LoadType, IsActive, Frequency, BronzePath, SilverPath, GoldPath, DWHSchemaName, DWHTableName,
# MAGIC   ErrorLogPath, LastLoadDateColumn, MergeKey, HashKeyColumn, DependencyTableIDs, PipelineStartTime,
# MAGIC   PipelineEndTime, PipelineRunStatus, Zone, SourceSelectQuery, LastLoadDateValue, RowCountQuery
# MAGIC ) VALUES
# MAGIC (
# MAGIC   'databricks', 'NA', 'B8', 1, NULL, NULL, 'Customer', 'minion_event', 'minion_event',
# MAGIC   'offer_silver_clean', 'Incremental load', 1, '1 hour', NULL, NULL, NULL, 'metrics', 'fact_daily_metrics',
# MAGIC   'NA', 'updateDate', 'dwh_daily_id', 'NA', NULL, 
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL, 'metrics', NULL,
# MAGIC   TIMESTAMP '1900-01-01 00:00:00.000+00:06', NULL
# MAGIC )
