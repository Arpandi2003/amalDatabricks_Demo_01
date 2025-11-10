CREATE CATALOG IF NOT EXISTS `ab_dev_catalog` WITH DBPROPERTIES (owner = 'naveenap@avatest.in');

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`_data_classification`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`_data_classification`.`_errors`
USING DELTA
TBLPROPERTIES ('clusteringColumns' = '[["catalog_name"]]');

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`_data_classification`.`_result`
USING DELTA
TBLPROPERTIES ('clusteringColumns' = '[["catalog_name"],["schema_name"],["table_name"]]');

CREATE OR REPLACE VIEW `ab_dev_catalog`.`_data_classification`.`errors` AS
SELECT DISTINCT dc.*
            FROM
              `ab_dev_catalog`.`_data_classification`.`_errors` dc
              -- Join with tables to check for table owners, as ownership is not a privilege
              LEFT JOIN system.information_schema.tables t
              ON
                t.table_catalog = "ab_dev_catalog" AND
                t.table_schema = dc.schema_name AND
                t.table_name = dc.table_name
              -- Join with privileges to check for read access to the table
              LEFT JOIN system.information_schema.table_privileges tp
              ON
                tp.table_catalog = "ab_dev_catalog" AND
                tp.table_schema = dc.schema_name AND
                tp.table_name = dc.table_name
            WHERE
            -- Check if the user is an owner of this table
            (
              t.table_owner = current_user() OR
              is_account_group_member(t.table_owner)
            ) OR
            -- Check if the user had read access to this table
            (
              (
                tp.grantee = current_user() OR
                is_account_group_member(tp.grantee)
              ) AND
              tp.privilege_type IN ("SELECT", "ALL_PRIVILEGES", "MODIFY")
            );

CREATE OR REPLACE VIEW `ab_dev_catalog`.`_data_classification`.`results` AS
SELECT DISTINCT dc.*
            FROM
              `ab_dev_catalog`.`_data_classification`.`_result` dc
              -- Join with tables to check for table owners, as ownership is not a privilege
              LEFT JOIN system.information_schema.tables t
              ON
                t.table_catalog = "ab_dev_catalog" AND
                t.table_schema = dc.schema_name AND
                t.table_name = dc.table_name
              -- Join with privileges to check for read access to the table
              LEFT JOIN system.information_schema.table_privileges tp
              ON
                tp.table_catalog = "ab_dev_catalog" AND
                tp.table_schema = dc.schema_name AND
                tp.table_name = dc.table_name
            WHERE
            -- Check if the user is an owner of this table
            (
              t.table_owner = current_user() OR
              is_account_group_member(t.table_owner)
            ) OR
            -- Check if the user had read access to this table
            (
              (
                tp.grantee = current_user() OR
                is_account_group_member(tp.grantee)
              ) AND
              tp.privilege_type IN ("SELECT", "ALL_PRIVILEGES", "MODIFY")
            );

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`assetbundle`;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`bronze`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`air_quality`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`check1`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`covid`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`coviddaily`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`covidmasking1`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`customer_details`
USING DELTA
TBLPROPERTIES ('PII' = 'Name');

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`customercombined`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`customers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`data`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`datacheck_1`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`encryption_keys`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`exceldata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`fa_account`
USING DELTA;

CREATE OR REPLACE VIEW `ab_dev_catalog`.`bronze`.`get_job_details` AS
WITH JobDetails AS (
    SELECT 
        j.name AS job_name,
        jt.task_key,
        j.job_id,
        j.workspace_id,
        jr.run_id,
        jr.result_state AS status,
        jr.period_start_time AS start_date,
        jr.period_end_time AS end_date,
        (UNIX_TIMESTAMP(jr.period_end_time) - UNIX_TIMESTAMP(jr.period_start_time)) AS duration_seconds,
        jt.depends_on_keys
    FROM 
        system.lakeflow.jobs j
    JOIN 
        system.lakeflow.job_tasks jt ON j.job_id = jt.job_id
    JOIN 
        system.lakeflow.job_run_timeline jr ON j.job_id = jr.job_id
    WHERE 
        (j.name = '' OR '' IS NULL) 
        AND (DATE(jr.period_start_time) >= '' OR '' IS NULL)  
)
SELECT 
    job_name,
    task_key,
    status,
    start_date,
    end_date,
    duration_seconds,
    depends_on_keys
FROM 
    JobDetails
ORDER BY 
    start_date DESC;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`lineage_metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`loan`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`new`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`new_table`
USING DELTA;

CREATE OR REPLACE VIEW `ab_dev_catalog`.`bronze`.`pii_tags` AS
(
  SELECT
    concat(catalog_name, '.', schema_name, '.', table_name) AS securable,
    'table' AS securable_type,
    sort_array(collect_set(tag_name)) AS tags
  FROM
    system.information_schema.table_tags
  GROUP BY
    1,
    2
  UNION ALL
  SELECT
    concat(catalog_name, '.', schema_name) AS securable,
    'schema' AS securable_type,
    sort_array(collect_set(tag_name)) AS tags
  FROM
    system.information_schema.schema_tags
  GROUP BY
    1,
    2
  UNION ALL
  SELECT
    concat(catalog_name) AS securable,
    'catalog' AS securable_type,
    sort_array(collect_set(tag_name)) AS tags
  FROM
    system.information_schema.catalog_tags
  GROUP BY
    1,
    2
  UNION ALL
  SELECT
    concat(catalog_name, '.', schema_name, '.', table_name) AS securable,
    'table' AS securable_type,
    sort_array(collect_set(tag_name)) AS tags
  FROM
    system.information_schema.column_tags
  GROUP BY
    1,
    2
);

CREATE OR REPLACE VIEW `ab_dev_catalog`.`bronze`.`privileges` AS
(
  SELECT
    concat(
      table_catalog,
      '.',
      table_schema,
      '.',
      table_name
    ) AS securable,
    'table' AS securable_type,
    grantee,
    privilege_type
  FROM
    system.information_schema.table_privileges
  UNION ALL
  SELECT
    concat(catalog_name, '.', schema_name) AS securable,
    'schema' AS securable_type,
    grantee,
    privilege_type
  FROM
    system.information_schema.schema_privileges
  UNION ALL
  SELECT
    catalog_name AS securable,
    'catalog' AS securable_type,
    grantee,
    privilege_type
  FROM
    system.information_schema.catalog_privileges
);

CREATE OR REPLACE VIEW `ab_dev_catalog`.`bronze`.`privileges_and_tags` AS
(
  SELECT
    t.securable,
    t.securable_type,
    p.grantee,
    p.privilege_type,
    t.tags
  FROM
    pii_tags t
    JOIN privileges p ON t.securable = p.securable
    AND t.securable_type = p.securable_type
  WHERE
    NOT (
      startswith(p.securable, '__databricks_internal.')
      OR contains(p.securable, 'information_schema')
    )
    AND array_contains(t.tags, 'pii')
);

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`samplecode`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`sampletest`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`test`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_advanced`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_advanced_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_raw`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_validation_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`transaction`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`bronze`.`users`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `ab_dev_catalog`.`bronze`.`titanic_raw_data`;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`config`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`dqcomparison`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`dqlogs`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`dqmetadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`last_run_tracker`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`config`.`metadata_demo`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `ab_dev_catalog`.`config`.`test_abank`;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`crypto`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`crypto`.`employee_data`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`crypto`.`key_vault`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`account`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`externaltrigger`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`lineage_metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`loan`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`titanic_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`transaction`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`default`.`users_encrypted`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `ab_dev_catalog`.`default`.`holder`;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`gold`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`gold`.`dimcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`gold`.`dimorder`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'ab_dev_catalog' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'ab_dev_catalog';

CREATE OR REPLACE VIEW `ab_dev_catalog`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'ab_dev_catalog';

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`masking`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`account`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`customer_data_masked`
USING DELTA;

CREATE OR REPLACE VIEW `ab_dev_catalog`.`masking`.`customer_data_masked_view` AS
SELECT
  customer_id,
  name,
  CASE 
    WHEN is_member('keyvault_user') THEN ssn 
    ELSE double_hash_complex(ssn, 'secure_salt_123') 
  END AS ssn,
  CASE 
    WHEN is_member('keyvault_user') THEN other_sensitive_data 
    ELSE '*****' 
  END AS other_sensitive_data
FROM customer_data_masked;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`customers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`loan`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`order_items`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`product_categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`products`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`masking`.`transaction`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`silver`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`silver`.`covid`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`silver`.`covid_daily`
USING DELTA;

CREATE OR REPLACE VIEW `ab_dev_catalog`.`silver`.`covid_masked` AS
SELECT
    CASE
        WHEN is_account_group_member('admin') THEN PrimaryAddress1
        -- WHEN is_account_group_member(CONCAT('ABankCheck_', PrimaryAddress1)) THEN PrimaryAddress1
        ELSE '****'
    END AS PrimaryAddress1_Masked,
    *
FROM silver.covid;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`silver`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`silver`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`silver`.`shipping`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`source`;

CREATE SCHEMA IF NOT EXISTS `ab_dev_catalog`.`validation_test`;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`validation_test`.`fa_account`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`validation_test`.`fa_household_osaic`
USING DELTA;

CREATE TABLE IF NOT EXISTS `ab_dev_catalog`.`validation_test`.`fa_portfolio`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `ab_dev_catalog`.`validation_test`.`test`;

CREATE CATALOG IF NOT EXISTS `bronze` WITH DBPROPERTIES (owner = 'dharun@avatest.in');

CREATE SCHEMA IF NOT EXISTS `bronze`.`abank`;

CREATE OR REPLACE VIEW `bronze`.`abank`.`abank_check` AS
SELECT 
  DataMasking_new(Country) AS masked_name,
  product,
  DataMasking(country) AS masked_salary
FROM 
  abank.coviddaily;

CREATE TABLE IF NOT EXISTS `bronze`.`abank`.`covid_daily`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`abank`.`coviddaily`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`abank`.`metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`abank`.`sample_flatcheck`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`abank`.`sample_test`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `bronze`.`buckeye_poc`;

CREATE VOLUME IF NOT EXISTS `bronze`.`buckeye_poc`.`test_volume`;

CREATE SCHEMA IF NOT EXISTS `bronze`.`config`;

CREATE TABLE IF NOT EXISTS `bronze`.`config`.`customer`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `bronze`.`config`.`default` COMMENT 'storage for the logger data';

CREATE VOLUME IF NOT EXISTS `bronze`.`config`.`flatfile`;

CREATE VOLUME IF NOT EXISTS `bronze`.`config`.`logger`;

CREATE SCHEMA IF NOT EXISTS `bronze`.`dbo`;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`agent_documents`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`agents`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`agents_test`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`carr_equip`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`carrier_reason_code`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`carriers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`celebree_calendar`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`center`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`children`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`classroom`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`disenrollments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`employee`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`families`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`funnel_goals`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`organization`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`status_tracking`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`dbo`.`test`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `bronze`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `bronze`.`default`.`agents`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `bronze`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'bronze' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'bronze';

CREATE OR REPLACE VIEW `bronze`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'bronze';

CREATE SCHEMA IF NOT EXISTS `bronze`.`silver`;

CREATE TABLE IF NOT EXISTS `bronze`.`silver`.`agent_and_document`
USING DELTA;

CREATE TABLE IF NOT EXISTS `bronze`.`silver`.`carrier_and_carrier_equip`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `dw_nfi_catalog` WITH DBPROPERTIES (owner = 'kinlush@avatest.in');

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`bronze`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`customer_details`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`employee`
USING DELTA
LOCATION 'abfss://nfi-poc@zebdsgenaipocstorageacc.dfs.core.windows.net/bronze/employee';

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`employee_details`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`employee_info`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`global_superstore`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nascent_2071_missionstatus`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nascent_2071_missiontype`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nascent_2071_visitstatistics`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nfi_2137_silver_customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nfi_2148_silver_customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`nfi_2289_silver_carrier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`silver_customer`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`service_account_nm` LOCATION 'abfss://nfi-poc@zebdsgenaipocstorageacc.dfs.core.windows.net/ServiceACC';

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`bennet_usecase_order`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`destination_table`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`mastermetadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`source_table`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`default`.`volume`;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`fabric_onelake`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`fabric_onelake`.`center`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`genetltesting`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`genetltesting`.`adhocmaintenancerepairinfo`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`genetltesting`.`genetl_testing` COMMENT 'Silver & Gold Transformation for Complex tables';

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'dw_nfi_catalog' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'dw_nfi_catalog';

CREATE OR REPLACE VIEW `dw_nfi_catalog`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'dw_nfi_catalog';

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`customer_details`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`employee`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`employee_details`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`employee_info`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`master_metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`metadatatable`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`metadatatest`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`mastermetadata`.`sand-box` LOCATION 'abfss://demo@zebdsgenaipocstorageacc.dfs.core.windows.net/sand-box';

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`client`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`dimcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`dimmachine`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`dimproduct`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`factinventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`factmaintenancenew`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`factpurchaseorder`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`factsalesorder`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`taxpayer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`mccaindemo`.`userengagement`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`pbix`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`rls_ibo`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`rls_salesperson`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`rls_salesperson_alignment_all`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`rls_salesperson_updated`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vw_salesperson`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vwloadsisense_prod`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vwmove_bkp_prod`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vwshipmentsisense_prod`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vwuszipcodes_prod`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`pbix`.`vwweeklyclaims`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`rombronze`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`rombronze`.`customer`
USING DELTA
LOCATION 'abfss://nfi-poc@zebdsgenaipocstorageacc.dfs.core.windows.net/customer';

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`rombronze`.`customer_details`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`test`;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`test`.`poc`;

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`test_internal`;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`test_internal`.`bronze_dimcarrier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`test_internal`.`bronze_dimcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`test_internal`.`bronze_factloadsh`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`test_internal`.`mode_testdata`;

CREATE CATALOG IF NOT EXISTS `genetl` WITH DBPROPERTIES (owner = 'balakumaran.r@avatest.in');

CREATE SCHEMA IF NOT EXISTS `genetl`.`abank`;

CREATE SCHEMA IF NOT EXISTS `genetl`.`bronze`;

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`agreement`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Bronze/dbo/Agreement/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`agreementfee`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Bronze/dbo/AgreementFee/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`agreementpromotion`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Bronze/dbo/AgreementPromotion/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`equipmentusage`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/EquipmentUsage/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`materialusage`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/MaterialUsage/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`product`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/Product/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`productionperformance`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/ProductionPerformance/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`promotion`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Bronze/dbo/Promotion/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`qualitycontrol`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/QualityControl/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`bronze`.`rawmaterial`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Bronze/dbo/RawMaterial/delta';

CREATE SCHEMA IF NOT EXISTS `genetl`.`default` COMMENT 'Default schema (auto-created)';

CREATE SCHEMA IF NOT EXISTS `genetl`.`general`;

CREATE TABLE IF NOT EXISTS `genetl`.`general`.`logtable`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/LogTable/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`general`.`metadata`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Metadata/delta';

CREATE SCHEMA IF NOT EXISTS `genetl`.`gold`;

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimagreementfee`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Gold/dw/AgreementFee/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimagreementpromotion`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Gold/dw/AgreementPromotion/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimagrepromotionreference`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Gold/dw/AgrePromotionReference/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimproductmaster`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/DimProductMaster/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimrawmaterial`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/DimRawMaterial/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`dimrentalagreement`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Gold/dw/DimRentalAgreement/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`factagreement`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Gold/dw/FactAgreement/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`factequipmentusage`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/FactEquipmentUsage/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`factmaterialusage`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/FactMaterialUsage/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`factproduction`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/FactProduction/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`gold`.`factqualitycontrol`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Gold/dw/FactQualityControl/delta';

CREATE SCHEMA IF NOT EXISTS `genetl`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'genetl' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'genetl';

CREATE OR REPLACE VIEW `genetl`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'genetl';

CREATE SCHEMA IF NOT EXISTS `genetl`.`silver`;

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`agreement`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Silver/dw/Agreement/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`agreementfee`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Silver/dw/AgreementFee/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`agreementpromotion`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Silver/dw/AgreementPromotion/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`dimproductmaster`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Silver/dw/DimProductMaster/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`dimrawmaterial`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Manufacturing/Silver/dw/DimRawMaterial/delta';

CREATE TABLE IF NOT EXISTS `genetl`.`silver`.`promotion`
USING DELTA
LOCATION 'abfss://ava-eus-synapse-ds-poc@avaeussynapsepoc.dfs.core.windows.net/Retail/Silver/dw/AgrePromotionReference/delta';

CREATE SCHEMA IF NOT EXISTS `genetl`.`test` COMMENT 'For testing purposes';

CREATE TABLE IF NOT EXISTS `genetl`.`test`.`bronze_customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `genetl`.`test`.`insurance_policies`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `main` COMMENT 'Main catalog (auto-created)' WITH DBPROPERTIES (owner = 'janani@avatest.in');

CREATE SCHEMA IF NOT EXISTS `main`.`default` COMMENT 'Default schema (auto-created)';

CREATE SCHEMA IF NOT EXISTS `main`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `main`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'main' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `main`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'main';

CREATE OR REPLACE VIEW `main`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'main';

CREATE CATALOG IF NOT EXISTS `samples` COMMENT 'These sample datasets are made available by third party data providers as well as open data sources. You can learn more about each data set by clicking on each one.

To discover more instantly available, free data sets across a wide range of industry use cases, visit [Databricks Marketplace](/marketplace).

Please note that the third party data sets represent a reduced portion of the available data attributes, volume, and data types available from providers, and are intended for educational rather than production purposes.' WITH DBPROPERTIES (owner = 'System user');

CREATE SCHEMA IF NOT EXISTS `samples`.`accuweather`;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_daily_calendar_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_daily_calendar_metric`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_daynight_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_daynight_metric`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_hourly_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`forecast_hourly_metric`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_daily_calendar_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_daily_calendar_metric`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_daynight_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_daynight_metric`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_hourly_imperial`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`accuweather`.`historical_hourly_metric`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`bakehouse`;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`media_customer_reviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`media_gold_reviews_chunked`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`sales_customers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`sales_franchises`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`sales_suppliers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`bakehouse`.`sales_transactions`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`healthverity`;

CREATE TABLE IF NOT EXISTS `samples`.`healthverity`.`claims_sample_synthetic`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'samples' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `samples`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'samples';

CREATE OR REPLACE VIEW `samples`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'samples';

CREATE SCHEMA IF NOT EXISTS `samples`.`nyctaxi`;

CREATE TABLE IF NOT EXISTS `samples`.`nyctaxi`.`trips`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`tpcds_sf1`;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`call_center`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`catalog_page`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`catalog_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`catalog_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`customer_address`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`customer_demographics`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`date_dim`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`household_demographics`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`income_band`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`item`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`promotion`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`reason`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`ship_mode`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`store_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`store_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`time_dim`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`warehouse`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`web_page`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`web_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`web_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1`.`web_site`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`tpcds_sf1000`;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`call_center`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`catalog_page`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`catalog_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`catalog_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`customer_address`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`customer_demographics`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`date_dim`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`household_demographics`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`income_band`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`item`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`promotion`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`reason`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`ship_mode`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`store_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`store_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`time_dim`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`warehouse`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`web_page`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`web_returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`web_sales`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpcds_sf1000`.`web_site`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`tpch`;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`lineitem`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`nation`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`part`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`partsupp`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`region`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`tpch`.`supplier`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `samples`.`wanderbricks`;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`amenities`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`booking_updates`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`bookings`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`clickstream`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`countries`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`customer_support_logs`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`destinations`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`hosts`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`page_views`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`properties`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`property_amenities`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`property_images`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`reviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `samples`.`wanderbricks`.`users`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `sap_dev_demo` WITH DBPROPERTIES (owner = 'naveenap@avatest.in');

CREATE SCHEMA IF NOT EXISTS `sap_dev_demo`.`bronze`;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`bronze`.`2lis_11_vahdr`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`bronze`.`2lis_12_vchdr`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`bronze`.`mara`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `sap_dev_demo`.`config`;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`config`.`metadata`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `sap_dev_demo`.`config`.`logger`;

CREATE SCHEMA IF NOT EXISTS `sap_dev_demo`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`default`.`2lis_11_vahdr`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`default`.`2lis_12_vchdr`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`default`.`metadata`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `sap_dev_demo`.`default`.`log_details`;

CREATE SCHEMA IF NOT EXISTS `sap_dev_demo`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'sap_dev_demo' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'sap_dev_demo';

CREATE OR REPLACE VIEW `sap_dev_demo`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'sap_dev_demo';

CREATE SCHEMA IF NOT EXISTS `sap_dev_demo`.`source`;

CREATE TABLE IF NOT EXISTS `sap_dev_demo`.`source`.`mara`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `sys` WITH DBPROPERTIES (owner = 'naveenap@avatest.in');

CREATE SCHEMA IF NOT EXISTS `sys`.`bronze`;

CREATE TABLE IF NOT EXISTS `sys`.`bronze`.`customer`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `sys`.`crypto`;

CREATE TABLE IF NOT EXISTS `sys`.`crypto`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sys`.`crypto`.`key_vault`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sys`.`crypto`.`users`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `sys`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `sys`.`default`.`titanic_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sys`.`default`.`titanic_raw`
USING DELTA;

CREATE TABLE IF NOT EXISTS `sys`.`default`.`users_encrypted`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `sys`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'sys' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `sys`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'sys';

CREATE OR REPLACE VIEW `sys`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'sys';

CREATE CATALOG IF NOT EXISTS `test_catalog` WITH DBPROPERTIES (owner = 'dharun@avatest.in');

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`bronze`;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`address`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`agreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`customerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`orderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`products`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`return`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`shipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`supplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`bronze`.`vendors`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`config`;

CREATE TABLE IF NOT EXISTS `test_catalog`.`config`.`dqcomparison`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`config`.`dqlogs`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`config`.`dqlogss`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`config`.`dqmetadata`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`default` COMMENT 'Default schema (auto-created)';

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`gold`;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimagreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimcategories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimcustomerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimemployees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`diminventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimorder`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimorderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimorders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimpayments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimproducts`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimreturns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimshipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimstore`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimsupplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`dimvendors`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`factcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`gold`.`factstore`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'test_catalog' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'test_catalog';

CREATE OR REPLACE VIEW `test_catalog`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'test_catalog';

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`metadata`;

CREATE TABLE IF NOT EXISTS `test_catalog`.`metadata`.`mastermetadata`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `test_catalog`.`silver`;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`agreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`customerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`orderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`products`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`shipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`supplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `test_catalog`.`silver`.`vendors`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `uc_test_catalog` WITH DBPROPERTIES (owner = 'dharun@avatest.in');

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`bronze`;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`agreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`customerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`encrypted_data`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`orderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`orders_encrypted`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`return`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`shipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`supplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`vendors`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`bronze`.`vendors_encrypted`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `uc_test_catalog`.`bronze`.`qmcpoc`;

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`default`.`account`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`default`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`default`.`loan`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`default`.`product`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`default`.`transaction`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`gold`;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimagreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimcategories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimcustomerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimemployees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`diminventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimorder`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimorderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimorders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimpayments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimproducts`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimreturns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimshipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimstore`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimsupplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`dimvendors`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`factcustomer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`gold`.`factstore`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'uc_test_catalog' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'uc_test_catalog';

CREATE OR REPLACE VIEW `uc_test_catalog`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'uc_test_catalog';

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`masking`;

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`metadata`;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`metadata`.`mastermetadata`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `uc_test_catalog`.`silver`;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`agreement`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`customer`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`customerreviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`employees`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`orderdetails`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`products`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`returns`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`shipping`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`store`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`supplier`
USING DELTA;

CREATE TABLE IF NOT EXISTS `uc_test_catalog`.`silver`.`vendors`
USING DELTA;

CREATE CATALOG IF NOT EXISTS `xanterra_poc` WITH DBPROPERTIES (owner = 'dharun@avatest.in');

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`bronze` COMMENT 'Creating this schema to perform data load using Pyspark code.';

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`addresses`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`cartitems`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`customers`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`logfile_lookup`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`orderitems`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`orders`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`payments`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`products`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`reviews`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`shoppingcart`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`bronze`.`student_scores`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `xanterra_poc`.`bronze`.`pdf`;

CREATE VOLUME IF NOT EXISTS `xanterra_poc`.`bronze`.`spark`;

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`bronze_c__fivetran_user`;

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`default`.`avatest_categories`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`default`.`products`
USING DELTA;

CREATE VOLUME IF NOT EXISTS `xanterra_poc`.`default`.`drivers`;

CREATE VOLUME IF NOT EXISTS `xanterra_poc`.`default`.`logger` COMMENT 'Creating this volume for to store all the Logger files';

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`information_schema` COMMENT 'Information schema (auto-created)';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`catalog_privileges` AS
SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`catalog_tags` AS
SELECT * FROM system.information_schema.catalog_tags WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`catalogs` AS
SELECT * FROM system.information_schema.catalogs WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`check_constraints` AS
SELECT * FROM system.information_schema.check_constraints WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`column_masks` AS
SELECT * FROM system.information_schema.column_masks WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`column_tags` AS
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`columns` AS
SELECT * FROM system.information_schema.columns WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`constraint_column_usage` AS
SELECT * FROM system.information_schema.constraint_column_usage WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`constraint_table_usage` AS
SELECT * FROM system.information_schema.constraint_table_usage WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`information_schema_catalog_name` AS
SELECT 'xanterra_poc' AS CATALOG_NAME;

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`key_column_usage` AS
SELECT * FROM system.information_schema.key_column_usage WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`parameters` AS
SELECT * FROM system.information_schema.parameters WHERE specific_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`referential_constraints` AS
SELECT * FROM system.information_schema.referential_constraints WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`routine_columns` AS
SELECT * FROM system.information_schema.routine_columns WHERE specific_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`routine_privileges` AS
SELECT * FROM system.information_schema.routine_privileges WHERE specific_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`routines` AS
SELECT * FROM system.information_schema.routines WHERE specific_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`row_filters` AS
SELECT * FROM system.information_schema.row_filters WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`schema_privileges` AS
SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`schema_tags` AS
SELECT * FROM system.information_schema.schema_tags WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`schemata` AS
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`table_constraints` AS
SELECT * FROM system.information_schema.table_constraints WHERE constraint_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`table_privileges` AS
SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`table_tags` AS
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`tables` AS
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`views` AS
SELECT * FROM system.information_schema.views WHERE table_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`volume_privileges` AS
SELECT * FROM system.information_schema.volume_privileges WHERE volume_catalog = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`volume_tags` AS
SELECT * FROM system.information_schema.volume_tags WHERE catalog_name = 'xanterra_poc';

CREATE OR REPLACE VIEW `xanterra_poc`.`information_schema`.`volumes` AS
SELECT * FROM system.information_schema.volumes WHERE volume_catalog = 'xanterra_poc';

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`mastermetadata`;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`mastermetadata`.`metadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`mastermetadata`.`metadata1`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`postgres_public`;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_public`.`calendar`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`postgres_src`;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`calendar`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`cusportinv`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`cutrkcntpf`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`oetrlgrp`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`ordr`
USING DELTA;

CREATE TABLE IF NOT EXISTS `xanterra_poc`.`postgres_src`.`unbldshs`
USING DELTA;

CREATE SCHEMA IF NOT EXISTS `xanterra_poc`.`silver`;
