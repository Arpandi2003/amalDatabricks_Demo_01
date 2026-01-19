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

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`orders`;

CREATE VOLUME IF NOT EXISTS `dw_nfi_catalog`.`bronze`.`service_account_nm` LOCATION 'abfss://nfi-poc@zebdsgenaipocstorageacc.dfs.core.windows.net/ServiceACC';

CREATE SCHEMA IF NOT EXISTS `dw_nfi_catalog`.`default` COMMENT 'Default schema (auto-created)';

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`bennet_usecase_order`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`configuration_inventory`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`destination_table`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`factshipmentitems`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`mastermetadata`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`source_table`
USING DELTA;

CREATE TABLE IF NOT EXISTS `dw_nfi_catalog`.`default`.`test_artifact_inventory`
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
