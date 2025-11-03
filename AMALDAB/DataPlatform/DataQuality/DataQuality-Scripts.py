# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Quality Scripts
# MAGIC

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use schema config;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table config.DQMetadata
# MAGIC (
# MAGIC   Table_ID Int,
# MAGIC   Source_System String,
# MAGIC   Schema_Name string,
# MAGIC   Table_Name string,
# MAGIC   Renamed_Table_Name string,
# MAGIC   Primary_Key string,
# MAGIC   Load_Type string,
# MAGIC   IsActive boolean
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table config.DQlogs
# MAGIC (
# MAGIC     Source_System string,
# MAGIC     Schema_Name string,
# MAGIC     Table_Name string,
# MAGIC     Column_Name string,
# MAGIC     Validation_Date timestamp,
# MAGIC     Check_Name string,
# MAGIC     Count_Value bigint,
# MAGIC     `Status` string,
# MAGIC     Value_Difference bigint,
# MAGIC     Threshold_Difference double,
# MAGIC     Comments string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table config.DQComparison
# MAGIC (
# MAGIC   Validation_Date timestamp,
# MAGIC   Source_System string,
# MAGIC   Check_Name string,
# MAGIC   Source_Schema string,
# MAGIC   Source_Table string,
# MAGIC   Source_Column string,
# MAGIC   Source_Count string,
# MAGIC   Target_Schema string,
# MAGIC   Target_Table string,
# MAGIC   Target_Column string,
# MAGIC   Target_Count string,
# MAGIC   Is_Match string,
# MAGIC   Comments string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (1, 'H360', 'bronze', 'ods_rmxref', 'ods_rmxref', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (2, 'H360', 'bronze', 'fi_core_application', 'fi_core_application', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (3, 'H360', 'bronze', 'ods_ddahis', 'ods_ddahis', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (4, 'H360', 'bronze', 'ods_ddatrc', 'ods_ddatrc', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (5, 'H360', 'bronze', 'ods_LN_Master', 'ods_LN_Master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (6, 'H360', 'bronze', 'ods_ML_Master', 'ods_ML_Master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (7, 'H360', 'bronze', 'ods_RMADDR', 'ods_RMADDR', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (8, 'H360', 'bronze', 'ods_RMCUSR', 'ods_RMCUSR', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (9, 'H360', 'bronze', 'ods_RMINET', 'ods_RMINET', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (10, 'H360', 'bronze', 'ods_RMMAST', 'ods_RMMAST', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (11, 'H360', 'bronze', 'ods_RMNDEM', 'ods_RMNDEM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (12, 'H360', 'bronze', 'ods_RMPDEM', 'ods_RMPDEM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (13, 'H360', 'bronze', 'ods_RMPHON', 'ods_RMPHON', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (14, 'H360', 'bronze', 'ods_RMNOTE', 'ods_RMNOTE', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (15, 'H360', 'bronze', 'XAA_ODS_SERV_FEE', 'XAA_ODS_SERV_FEE', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (16, 'H360', 'bronze', 'fi_Core_Org', 'fi_Core_Org', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (17, 'H360', 'bronze', 'fi_core_product', 'fi_core_product', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (18, 'H360', 'bronze', 'fi_core_relationship', 'fi_core_relationship', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (19, 'H360', 'bronze', 'fi_Core_Application_Sub', 'fi_Core_Application_Sub', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (20, 'H360', 'bronze', 'ods_DD_Master', 'ods_DD_Master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (21, 'H360', 'bronze', 'fi_core_ratetype', 'fi_core_ratetype', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (22, 'H360', 'bronze', 'ods_glxref', 'ods_glxref', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (23, 'H360', 'bronze', 'fi_core_call', 'fi_core_call', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (24, 'H360', 'bronze', 'ods_sicod_ext', 'ods_sicod_ext', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (25, 'H360', 'bronze', 'fi_core_officer', 'fi_core_officer', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (26, 'H360', 'bronze', 'fi_core_nonaccrual', 'fi_core_nonaccrual', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (27, 'H360', 'bronze', 'fi_core_purpose', 'fi_core_purpose', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (28, 'H360', 'bronze', 'fi_core_snapshotdate', 'fi_core_snapshotdate', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (29, 'H360', 'bronze', 'fi_core_status', 'fi_core_status', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (30, 'TDW', 'bronze', 'Portfolio', 'Portfolio', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (31, 'H360', 'bronze', 'ods_siudf', 'ods_siudf', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (33, 'H360', 'bronze', 'Ceb_ods_authenticationevents', 'Ceb_ods_authenticationevents', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (34, 'H360', 'bronze', 'ceb_ods_cebtransactions', 'ceb_ods_cebtransactions', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (35, 'H360', 'bronze', 'ceb_ods_customer', 'ceb_ods_customer', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (36, 'H360', 'bronze', 'ceb_ods_enrollmentevents', 'ceb_ods_enrollmentevents', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (37, 'H360', 'bronze', 'ceb_ods_loginevents', 'ceb_ods_loginevents', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (38, 'H360', 'bronze', 'ceb_ods_accounts', 'ceb_ods_accounts', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (39, 'H360', 'bronze', 'ods_co_master', 'ods_co_master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (41, 'H360', 'bronze', 'ods_rmbori', 'ods_rmbori', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (42, 'H360', 'bronze', 'ods_rmpasr', 'ods_rmpasr', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (43, 'H360', 'bronze', 'ods_rmqinq', 'ods_rmqinq', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (44, 'H360', 'bronze', 'ods_sifina', 'ods_sifina', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (45, 'H360', 'bronze', 'ods_fact_summary_ME_Archive', 'ods_fact_summary_ME_Archive', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (46, 'TDW', 'bronze', 'Account', 'Account', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (47, 'TDW', 'bronze', 'mia', 'mia', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (48, 'TDW', 'bronze', 'affil', 'affil', 'afinternal', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (49, 'TDW', 'bronze', 'fee_review', 'fee_review', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (50, 'Flat_File', 'NA', 'Account', 'Account_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (51, 'Flat_File', 'NA', 'Security', 'Security_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (52, 'Flat_File', 'NA', 'HouseholdAccount', 'HouseholdAccount_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (53, 'Flat_File', 'NA', 'Position', 'Position_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (54, 'Flat_File', 'NA', 'Household', 'Household_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (55, 'Flat_File', 'NA', 'Suitability', 'Suitability_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (56, 'Flat_File', 'NA', 'TransactionDetail', 'TransactionDetail_Osaic', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (75, 'H360', 'bronze', 'ods_DDAACTT', 'ods_DDAACTT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (76, 'H360', 'bronze', 'ods_DDABALME', 'ods_DDABALME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (77, 'H360', 'bronze', 'ods_DDAITY', 'ods_DDAITY', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (78, 'H360', 'bronze', 'ods_DDANSF', 'ods_DDANSF', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (79, 'H360', 'bronze', 'ods_DDRTPACT', 'ods_DDRTPACT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (80, 'H360', 'bronze', 'ods_DMLBALME', 'ods_DMLBALME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (81, 'H360', 'bronze', 'ods_DMLEVLME', 'ods_DMLEVLME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (82, 'H360', 'bronze', 'ods_DMMLNEVH', 'ods_DMMLNEVH', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (83, 'H360', 'bronze', 'ods_DMMLNEVL', 'ods_DMMLNEVL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (84, 'H360', 'bronze', 'ods_DMMLNPMT', 'ods_DMMLNPMT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (85, 'H360', 'bronze', 'ods_Fact_Summary', 'ods_Fact_Summary', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (86, 'H360', 'bronze', 'ods_Fact_Summary_ME', 'ods_Fact_Summary_ME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (87, 'H360', 'bronze', 'ods_GLCENTER', 'ods_GLCENTER', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (88, 'H360', 'bronze', 'ods_GLTYPE', 'ods_GLTYPE', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (89, 'H360', 'bronze', 'ods_LNBILL', 'ods_LNBILL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (90, 'H360', 'bronze', 'ods_LNFERN', 'ods_LNFERN', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (91, 'H360', 'bronze', 'ods_LNFERNME', 'ods_LNFERNME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (92, 'H360', 'bronze', 'ods_LNHIST', 'ods_LNHIST', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (93, 'H360', 'bronze', 'ods_LNM2AC', 'ods_LNM2AC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (94, 'H360', 'bronze', 'ods_LNM2ACME', 'ods_LNM2ACME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (95, 'H360', 'bronze', 'ods_LNMASTME', 'ods_LNMASTME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (96, 'H360', 'bronze', 'ods_LNMLSF', 'ods_LNMLSF', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (97, 'H360', 'bronze', 'ods_LNMLSFME', 'ods_LNMLSFME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (98, 'H360', 'bronze', 'ods_LNPRIM', 'ods_LNPRIM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (99, 'H360', 'bronze', 'ods_LNPRIMME', 'ods_LNPRIMME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (100, 'H360', 'bronze', 'ods_LNPSCH', 'ods_LNPSCH', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (101, 'H360', 'bronze', 'ods_LNPSCHME', 'ods_LNPSCHME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (102, 'H360', 'bronze', 'ods_LNPSSM', 'ods_LNPSSM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (103, 'H360', 'bronze', 'ods_LNTREL', 'ods_LNTREL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (104, 'H360', 'bronze', 'ods_LNTRNC', 'ods_LNTRNC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (105, 'H360', 'bronze', 'ods_MC_MAster', 'ods_MC_MAster', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (106, 'H360', 'bronze', 'ods_mcxref', 'ods_mcxref', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (107, 'H360', 'bronze', 'ods_MLFEEBAL', 'ods_MLFEEBAL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (108, 'H360', 'bronze', 'ods_MLFEEBALME', 'ods_MLFEEBALME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (109, 'H360', 'bronze', 'ods_MLMERINF', 'ods_MLMERINF', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (110, 'H360', 'bronze', 'ods_MLMSTMAC', 'ods_MLMSTMAC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (111, 'H360', 'bronze', 'ods_MLMSTMPD', 'ods_MLMSTMPD', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (112, 'H360', 'bronze', 'ods_MLMSTMTN', 'ods_MLMSTMTN', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (113, 'H360', 'bronze', 'ods_MLTRNCOD', 'ods_MLTRNCOD', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (114, 'H360', 'bronze', 'ods_RGCLSCOD', 'ods_RGCLSCOD', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (115, 'H360', 'bronze', 'ods_RGCLSTYP', 'ods_RGCLSTYP', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (116, 'H360', 'bronze', 'ods_RGCREXDP', 'ods_RGCREXDP', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (117, 'H360', 'bronze', 'ods_RGCREXLN', 'ods_RGCREXLN', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (118, 'H360', 'bronze', 'ods_RMAPOP', 'ods_RMAPOP', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (119, 'H360', 'bronze', 'ods_RMIDT', 'ods_RMIDT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (120, 'H360', 'bronze', 'ods_RMIDTYPE', 'ods_RMIDTYPE', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (121, 'H360', 'bronze', 'ods_RMOFAC', 'ods_RMOFAC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (122, 'H360', 'bronze', 'ods_RR_Master', 'ods_RR_Master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (123, 'H360', 'bronze', 'ods_RRSBALME', 'ods_RRSBALME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (124, 'H360', 'bronze', 'ods_RRSHIS', 'ods_RRSHIS', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (125, 'H360', 'bronze', 'ods_RWMAST', 'ods_RWMAST', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (126, 'H360', 'bronze', 'ods_SIAGGBAL', 'ods_SIAGGBAL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (127, 'H360', 'bronze', 'ods_SICOD1', 'ods_SICOD1', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (128, 'H360', 'bronze', 'ods_SIRISK', 'ods_SIRISK', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (129, 'H360', 'bronze', 'ods_SIRTINDX', 'ods_SIRTINDX', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (130, 'H360', 'bronze', 'ods_SIRTTIER', 'ods_SIRTTIER', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (131, 'H360', 'bronze', 'ods_TDACTV', 'ods_TDACTV', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (132, 'H360', 'bronze', 'ods_TDASUM', 'ods_TDASUM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (133, 'H360', 'bronze', 'ods_TDMASTME', 'ods_TDMASTME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (134, 'H360', 'bronze', 'ods_TDPENT', 'ods_TDPENT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (135, 'H360', 'bronze', 'ods_TDPLAN', 'ods_TDPLAN', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (136, 'H360', 'bronze', 'ods_TDRATE', 'ods_TDRATE', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (137, 'H360', 'bronze', 'ods_TDTRCTL', 'ods_TDTRCTL', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (138, 'H360', 'bronze', 'XAA_ODS_ACCT', 'XAA_ODS_ACCT', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (139, 'H360', 'bronze', 'XAA_ODS_ACCT_ADDR', 'XAA_ODS_ACCT_ADDR', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (140, 'H360', 'bronze', 'XAA_ODS_ACCT_HIST', 'XAA_ODS_ACCT_HIST', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (141, 'H360', 'bronze', 'XAA_ODS_OVR_PRC', 'XAA_ODS_OVR_PRC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (142, 'H360', 'bronze', 'XAA_ODS_R12M_RES_SUM', 'XAA_ODS_R12M_RES_SUM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (143, 'H360', 'bronze', 'XAA_ODS_R12M_SERV_FEE_SUM', 'XAA_ODS_R12M_SERV_FEE_SUM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (144, 'H360', 'bronze', 'XAA_ODS_RES_CYC', 'XAA_ODS_RES_CYC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (145, 'H360', 'bronze', 'XAA_ODS_STD_PRC', 'XAA_ODS_STD_PRC', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (146, 'H360', 'bronze', 'XAA_ODS_YTD_RES_SUM', 'XAA_ODS_YTD_RES_SUM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (147, 'H360', 'bronze', 'XAA_ODS_YTD_SERV_FEE_SUM', 'XAA_ODS_YTD_SERV_FEE_SUM', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (148, 'H360', 'bronze', 'ods_TD_Master', 'ods_TD_Master', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (149, 'H360', 'bronze', 'fi_Core_GLType', 'fi_Core_GLType', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (150, 'H360', 'bronze', 'fi_Core_GLOrg', 'fi_Core_GLOrg', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (152, 'H360', 'bronze', 'ods_SINPIDTA_UII_XREF', 'ods_SINPIDTA_UII_XREF', 'GID', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (153, 'H360', 'bronze', 'v_Trend_Summary_DA', 'v_Trend_Summary_DA', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (154, 'H360', 'bronze', 'ods_LNIMPR', 'ods_LNIMPR', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (155, 'H360', 'bronze', 'ods_LNIMPRME', 'ods_LNIMPRME', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (157, 'H360', 'bronze', 'v_Trend_DepositFee_All', 'v_Trend_DepositFee_All', 'NA', 'Full Load', 'TRUE');
# MAGIC INSERT INTO dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES (500, 'Flat_File', 'NA', 'DCIF', 'DMI_DCIF', 'NA', 'Full Load', 'TRUE');
# MAGIC

# COMMAND ----------

# DBTITLE 1,DQ Metadata Final Scripts
# MAGIC %sql
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (38,'H360','Bronze','ceb_ods_accounts','ceb_ods_accounts','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (7,'H360','Bronze','ods_RMADDR','ods_RMADDR','CUST_SKEY,RAASEQ','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (119,'H360','Bronze','ods_RMIDT','ods_RMIDT','CUST_SKEY,RDSEQ,RDIDTP','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (147,'H360','Bronze','XAA_ODS_YTD_SERV_FEE_SUM','XAA_ODS_YTD_SERV_FEE_SUM','ACCT_NBR, ANLYS_APPL_CDE, SERV_YTD_DTE, serv_nbr,SERV_GRP_NBR,SERV_FEE_DPSTN_CDE, SERV_TOT_FEE_AMT,Process_Date,BAL_FEE_TYP_CDE','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (143,'H360','Bronze','XAA_ODS_R12M_SERV_FEE_SUM','XAA_ODS_R12M_SERV_FEE_SUM','ACCT_NBR,ANLYS_APPL_CDE,SERV_GRP_NBR,SERV_TYP_CDE,SERV_NBR,YTD_LST_UPDT_DTE, SERV_FEE_DPSTN_CDE,SERV_UNIT_CNT, SERV_TOT_FEE_AMT, SERV_RB_AMT, BAL_FEE_TYP_CDE','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (14,'H360','Bronze','ods_RMNOTE','ods_RMNOTE','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (10,'H360','Bronze','ods_RMMAST','ods_RMMAST','CUST_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (42,'H360','Bronze','ods_rmpasr','ods_rmpasr','PAAPPL,PAACCT,PADTPRG','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (1,'H360','Bronze','ods_rmxref','ods_rmxref','rxkey,rxacct, RecID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (86,'H360','Bronze','ods_Fact_Summary_ME','ods_Fact_Summary_ME','AccountLKey,DateKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (78,'H360','Bronze','ods_DDANSF','ods_DDANSF','ACCT_SKEY,`DNDATE`,`DNCNTR`,`DNSEQ#`','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (131,'H360','Bronze','ods_TDACTV','ods_TDACTV','ACCT_SKEY,TATXCD,TASORT,TAEFDT,TAPRDT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (3,'H360','Bronze','ods_ddahis','ods_ddahis','DDACCT,DDTXDT,DDTRAN,DDTRAC,DDCHNG','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (13,'H360','Bronze','ods_RMPHON','ods_RMPHON','CUST_SKEY,RHPSEQ,RHPTYP,RHPHONF,RHEXTN','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (409,'NYHQ_DEV','Bronze','LOAD_EVENT_SENT','LOAD_EVENT_SENT','SentSID, SubscriberID, EmailAddress','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (406,'NYHQ_DEV','Bronze','LOAD_EVENT_OPEN','LOAD_EVENT_OPEN','OpenSID, SendID, SubscriberID, EmailAddress','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (400,'NYHQ_DEV','Bronze','LOAD_EVENT_ATTRIBUTES','LOAD_EVENT_ATTRIBUTES','AttributeSID,ClientID,SubscriberID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (401,'NYHQ_DEV','Bronze','LOAD_EVENT_BOUNCES','LOAD_EVENT_BOUNCES','BouncesSID, SendID, SubscriberID, EmailAddress','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (117,'H360','Bronze','ods_RGCREXLN','ods_RGCREXLN','ACCT_SKEY,CLDATE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (404,'NYHQ_DEV','Bronze','LOAD_EVENT_CLICKS','LOAD_EVENT_CLICKS','ClickSID, SendID, SubscriberID, EmailAddress','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (45,'H360','Bronze','ods_fact_summary_ME_Archive','ods_fact_summary_ME_Archive','accountLKey,Frequency,DateKey','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (403,'NYHQ_DEV','Bronze','LOAD_EVENT_COMPLAINTS','LOAD_EVENT_COMPLAINTS','ComplaintsSID, SendID, SubscriberID, EmailAddress,sUser','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (31,'H360','Bronze','ods_siudf','ods_siudf','Account_Key,Field_Sequence,Field_Name,Field_Value,Numeric_Value','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (402,'NYHQ_DEV','Bronze','LOAD_EVENT_CLICKIMPRESSION','LOAD_EVENT_CLICKIMPRESSION','sendurlid, SendID, SubscriberID, EmailAddress, ClickImpSID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (140,'H360','Bronze','XAA_ODS_ACCT_HIST','XAA_ODS_ACCT_HIST','ACCT_NBR, ANLYS_APPL_CDE, LST_CHRG_CYC_DTE, DEMO_EFF_DTE,Process_Date','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (33,'H360','Bronze','Ceb_ods_authenticationevents','Ceb_ods_authenticationevents','EventDateTime,SessionID,AuthenticationActivityID','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (37,'H360','Bronze','ceb_ods_loginevents','ceb_ods_loginevents','DigitalUserID, IPAddress,LoginSource, StatusCode, LoginDateAndTime,Last_Modified_Date','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (34,'H360','Bronze','ceb_ods_cebtransactions','ceb_ods_cebtransactions','CoreCustomerID, DigitalUserID,FromAccountNumber,ProcessDate, ToAccountNumber,TransferAmount,TransferProcessedEventDate,Last_Modified_Date,StatusOfTransferProcessing,TransferType,FromApplicationCode','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (43,'H360','Bronze','ods_rmqinq','ods_rmqinq','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (139,'H360','Bronze','XAA_ODS_ACCT_ADDR','XAA_ODS_ACCT_ADDR','NA','Truncate and Load',0);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (153,'H360','Bronze','v_Trend_Summary_DA','v_Trend_Summary_DA','AccountLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (410,'NYHQ_DEV','Bronze','LOAD_EVENT_UNSUBS','LOAD_EVENT_UNSUBS','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (405,'NYHQ_DEV','Bronze','LOAD_EVENT_CONVERSIONS','LOAD_EVENT_CONVERSIONS','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (53,'Flat_File','Bronze','Position','Position_Osaic','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (408,'NYHQ_DEV','Bronze','LOAD_EVENT_SENDJOBS','LOAD_EVENT_SENDJOBS','SendJobSID,SendID','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (157,'H360','Bronze','v_Trend_DepositFee_All','v_Trend_DepositFee_All','accountLKey, FeeCode','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (55,'Flat_File','Bronze','Suitability','Suitability_Osaic','Contact_Id','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (50,'Flat_File','Bronze','Account','Account_Osaic','Account_Unique_ID','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (56,'Flat_File','Bronze','TransactionDetail','TransactionDetail_Osaic','TXN_Number','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (51,'Flat_File','Bronze','Security','Security_Osaic','CUSIP,Product_Name,Vendor_Name','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (54,'Flat_File','Bronze','Household','Household_Osaic','Contact_Group_ID,Contact_Group_Name,Contact_ID','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (15,'H360','Bronze','XAA_ODS_SERV_FEE','XAA_ODS_SERV_FEE','ACCT_NBR, CYC_END_DTE, serv_nbr,SERV_TYP_CDE, SERV_SEQ_NBR,SERV_RB_AMT, PREPRC_CREATE_TS,Process_Date','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (52,'Flat_File','Bronze','HouseholdAccount','HouseholdAccount_Osaic','Contact_Group_ID,Contact_Group_Name,Contact_Id,Account_Unique_ID','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (141,'H360','Bronze','XAA_ODS_OVR_PRC','XAA_ODS_OVR_PRC','ACCT_NBR,ANLYS_APPL_CDE,OP_SERV_NBR, SERV_TYP_CDE,OP_SERV_SUB_TYP_CDE,OP_CYC_END_DTE, OP_ANLYS_EXCP_PRC_SRC_CDE,Process_Date','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (500,'Flat_File','Bronze','DCIF','DMI_DCIF','Loan_Number','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (411,'NYHQ_DEV','Bronze','v_ods_Active_Customer_Dim','v_ods_Active_Customer_Dim','Customer_Key,Account_Key,REL_KEY,Br_13_Previous_Branch,RXPRIM,AsofDate','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (44,'H360','Bronze','ods_sifina','ods_sifina','ACCT_SKEY','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (26,'H360','Bronze','fi_core_nonaccrual','fi_core_nonaccrual','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (25,'H360','Bronze','fi_core_officer','fi_core_officer','Officer_Key','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (27,'H360','Bronze','fi_core_purpose','fi_core_purpose','PurposeLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (84,'H360','Bronze','ods_DMMLNPMT','ods_DMMLNPMT','ACCT_SKEY,MYDUED','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (23,'H360','Bronze','fi_core_call','fi_core_call','CallLKey,CallCode','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (5,'H360','Bronze','ods_LN_Master','ods_LN_Master','ACCT_SKEY,LMPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (88,'H360','Bronze','ods_GLTYPE','ods_GLTYPE','GTAPPLCD,GTGLTYPE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (22,'H360','Bronze','ods_glxref','ods_glxref','GXAPPLCD, GXGLTYPE, GXINDXPRFX, GXINDXSUFX','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (2,'H360','Bronze','fi_core_application','fi_core_application','Application_Code,Application_Description','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (18,'H360','Bronze','fi_core_relationship','fi_core_relationship','Relationship_Type_Code,Relationship_Type_Description','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (19,'H360','Bronze','fi_Core_Application_Sub','fi_Core_Application_Sub','Sub_Application_Code,Sub_Application_Description','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (17,'H360','Bronze','fi_core_product','fi_core_product','Product_Key,Sub_Application_Code,Sub_Application_Description','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (47,'TDW','Bronze','mia','mia','Number','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (46,'TDW','Bronze','Account','Account','ACCT_Number','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (49,'TDW','Bronze','fee_review','fee_review','Account','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (4,'H360','Bronze','ods_ddatrc','ods_ddatrc','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (93,'H360','Bronze','ods_LNM2AC','ods_LNM2AC','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (81,'H360','Bronze','ods_DMLEVLME','ods_DMLEVLME','MUACCT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (83,'H360','Bronze','ods_DMMLNEVL','ods_DMMLNEVL','MUACCT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (80,'H360','Bronze','ods_DMLBALME','ods_DMLBALME','MBACCT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (75,'H360','Bronze','ods_DDAACTT','ods_DDAACTT','PROD_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (6,'H360','Bronze','ods_ML_Master','ods_ML_Master','MBACCT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (30,'TDW','Bronze','Portfolio','Portfolio','Account,internal','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (29,'H360','Bronze','fi_core_status','fi_core_status','Code_Key','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (87,'H360','Bronze','ods_GLCENTER','ods_GLCENTER','GNCTRNBR','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (21,'H360','Bronze','fi_core_ratetype','fi_core_ratetype','RateTypeLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (39,'H360','Bronze','ods_co_master','ods_co_master','Acct_Skey,CO_ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (90,'H360','Bronze','ods_LNFERN','ods_LNFERN','ACCT_SKEY,LFPINT,LFSINT,LFFPTR','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (79,'H360','Bronze','ods_DDRTPACT','ods_DDRTPACT','ACCT_SKEY,PRTCODE,PRTSTDT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (28,'H360','Bronze','fi_core_snapshotdate','fi_core_snapshotdate','DateKey,FrequencyCode','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (91,'H360','Bronze','ods_LNFERNME','ods_LNFERNME','ACCT_SKEY,LFPINT,LFSINT,LFFPTR','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (41,'H360','Bronze','ods_rmbori','ods_rmbori','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (16,'H360','Bronze','fi_Core_Org','fi_Core_Org','Org_Key','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (77,'H360','Bronze','ods_DDAITY','ods_DDAITY','INTT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (94,'H360','Bronze','ods_LNM2ACME','ods_LNM2ACME','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (82,'H360','Bronze','ods_DMMLNEVH','ods_DMMLNEVH','MCACCT,MCSEQN','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (106,'H360','Bronze','ods_mcxref','ods_mcxref','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (20,'H360','Bronze','ods_DD_Master','ods_DD_Master','Acct_Skey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (76,'H360','Bronze','ods_DDABALME','ods_DDABALME','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (11,'H360','Bronze','ods_RMNDEM','ods_RMNDEM','RNKEY,CUST_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (89,'H360','Bronze','ods_LNBILL','ods_LNBILL','ACCT_SKEY,LBDATE,LBPDEM','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (36,'H360','Bronze','ceb_ods_enrollmentevents','ceb_ods_enrollmentevents','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (9,'H360','Bronze','ods_RMINET','ods_RMINET','CUST_KEY,RIITYP,RIISEQ,RIMNDT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (35,'H360','Bronze','ceb_ods_customer','ceb_ods_customer','DigitalUserID, taxID','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (8,'H360','Bronze','ods_RMCUSR','ods_RMCUSR','CUST_SKEY,REL_CUST_SKEY,CUST_REL_SKEY,RCBOPCT','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (101,'H360','Bronze','ods_LNPSCHME','ods_LNPSCHME','acct_skey,LPPART,LPTYPE,LPNXDT,LPFREQ,`LP#REM`,LPAMT','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (159,'TDW','Bronze','rc','rc','NUMBER','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (12,'H360','Bronze','ods_RMPDEM','ods_RMPDEM','CUST_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (102,'H360','Bronze','ods_LNPSSM','ods_LNPSSM','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (132,'H360','Bronze','ods_TDASUM','ods_TDASUM','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (113,'H360','Bronze','ods_MLTRNCOD','ods_MLTRNCOD','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (109,'H360','Bronze','ods_MLMERINF','ods_MLMERINF','MVACCT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (130,'H360','Bronze','ods_SIRTTIER','ods_SIRTTIER','TITIER','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (149,'H360','Bronze','fi_Core_GLType','fi_Core_GLType','GLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (123,'H360','Bronze','ods_RRSBALME','ods_RRSBALME','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (133,'H360','Bronze','ods_TDMASTME','ods_TDMASTME','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (108,'H360','Bronze','ods_MLFEEBALME','ods_MLFEEBALME','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (120,'H360','Bronze','ods_RMIDTYPE','ods_RMIDTYPE','RTIDTP','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (150,'H360','Bronze','fi_Core_GLOrg','fi_Core_GLOrg','GLOrgLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (148,'H360','Bronze','ods_TD_Master','ods_TD_Master','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (250,'Horizon DB2','Bronze','RMZCDTBAL','RMZCDTBAL','RZACCTID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (251,'Horizon DB2','Bronze','RMZICTBAL','RMZICTBAL','RZICACCTID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (134,'H360','Bronze','ods_TDPENT','ods_TDPENT','TFTYPE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (125,'H360','Bronze','ods_RWMAST','ods_RWMAST','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (122,'H360','Bronze','ods_RR_Master','ods_RR_Master','ACCT_SKEy','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (105,'H360','Bronze','ods_MC_MAster','ods_MC_MAster','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (118,'H360','Bronze','ods_RMAPOP','ods_RMAPOP','RQAPPL,RQGRP','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (127,'H360','Bronze','ods_SICOD1','ods_SICOD1','CODE_SKEY,C1OWN','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (137,'H360','Bronze','ods_TDTRCTL','ods_TDTRCTL','TIAPPL,TICODE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (136,'H360','Bronze','ods_TDRATE','ods_TDRATE','TRNUMB,TREFFD','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (154,'H360','Bronze','ods_LNIMPR','ods_LNIMPR','ACCT_SKEY,LSPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (103,'H360','Bronze','ods_LNTREL','ods_LNTREL','ACCT_SKEY,LTSACT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (96,'H360','Bronze','ods_LNMLSF','ods_LNMLSF','ACCT_SKEY,LRPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (111,'H360','Bronze','ods_MLMSTMPD','ods_MLMSTMPD','ACCT_SKEY','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (114,'H360','Bronze','ods_RGCLSCOD','ods_RGCLSCOD','RCCLSC,RCCDSC','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (115,'H360','Bronze','ods_RGCLSTYP','ods_RGCLSTYP','RTCLST,RTCDSC','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (95,'H360','Bronze','ods_LNMASTME','ods_LNMASTME','ACCT_SKEY,LMPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (155,'H360','Bronze','ods_LNIMPRME','ods_LNIMPRME','ACCT_SKEY,LSPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (107,'H360','Bronze','ods_MLFEEBAL','ods_MLFEEBAL','ACCT_SKEY,MMFTYP','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (97,'H360','Bronze','ods_LNMLSFME','ods_LNMLSFME','ACCT_SKEY,LRPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (135,'H360','Bronze','ods_TDPLAN','ods_TDPLAN','TPPLAN,TPCIFK,TPSDAT,TPEFFD,TPBDAT,TPSTIM','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (48,'TDW','Bronze','affil','affil','afinternal','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (104,'H360','Bronze','ods_LNTRNC','ods_LNTRNC','TCCode','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (128,'H360','Bronze','ods_SIRISK','ods_SIRISK','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (112,'H360','Bronze','ods_MLMSTMTN','ods_MLMSTMTN','NA','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (24,'H360','Bronze','ods_sicod_ext','ods_sicod_ext','CodeID','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (116,'H360','Bronze','ods_RGCREXDP','ods_RGCREXDP','ACCT_SKEY','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (98,'H360','Bronze','ods_LNPRIM','ods_LNPRIM','ACCT_SKEY,lipart,LISEQN','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (129,'H360','Bronze','ods_SIRTINDX','ods_SIRTINDX','GlobalIndexKey,RTEFDT','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (99,'H360','Bronze','ods_LNPRIMME','ods_LNPRIMME','ACCT_SKEY,LIPART,LISEQN','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (110,'H360','Bronze','ods_MLMSTMAC','ods_MLMSTMAC','ACCT_SKEY, MLSTMDATE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (124,'H360','Bronze','ods_RRSHIS','ods_RRSHIS','ACCT_SKEY,RHPSDT,RHEFDT,RHSEQ,RHPSTS','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (92,'H360','Bronze','ods_LNHIST','ods_LNHIST','ACCT_SKEY,LHRPDT,LHTRCN,LHOSEQ,LHTRDT,LHCCRL','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (146,'H360','Bronze','XAA_ODS_YTD_RES_SUM','XAA_ODS_YTD_RES_SUM','ACCT_NBR,ANLYS_APPL_CDE,YTD_DTE','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (100,'H360','Bronze','ods_LNPSCH','ods_LNPSCH','acct_skey,LPPART,LPTYPE,LPNXDT,LPFREQ,`LP#REM`,LPAMT','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (126,'H360','Bronze','ods_SIAGGBAL','ods_SIAGGBAL','ACCT_SKEY,ABPART','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (138,'H360','Bronze','XAA_ODS_ACCT','XAA_ODS_ACCT','ACCT_NBR,ACCT_NME','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (142,'H360','Bronze','XAA_ODS_R12M_RES_SUM','XAA_ODS_R12M_RES_SUM','ACCT_NBR','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (152,'H360','Bronze','ods_SINPIDTA_UII_XREF','ods_SINPIDTA_UII_XREF','NA','Truncate and Load',0);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (85,'H360','Bronze','ods_Fact_Summary','ods_Fact_Summary','AccountLKey','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (407,'NYHQ_DEV','Bronze','LOAD_EVENT_SENDIMPRESSION','LOAD_EVENT_SENDIMPRESSION','SendImpSID, SendID, SubscriberID, EmailAddress','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (144,'H360','Bronze','XAA_ODS_RES_CYC','XAA_ODS_RES_CYC','ACCT_NBR,CYC_END_DTE,Process_Date','Incremental Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (121,'H360','Bronze','ods_RMOFAC','ods_RMOFAC','CUST_SKEY,AccountKey','Truncate and Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (158,'H360','Bronze','ods_AG_Master','ods_AG_Master','RSXACCT,RSXAPPL','Full Load',1);
# MAGIC Insert INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type,IsActive)  VALUES (145,'H360','Bronze','XAA_ODS_STD_PRC','XAA_ODS_STD_PRC','NA','Truncate and Load',1);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (552,'Flat_File','bronze','YRMO','YRMO','YRMO','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (530,'Flat_File','bronze','axiom_acct','acct','ACCT','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (538,'Flat_File','bronze','axiom_dept','dept','DEPT','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (540,'Flat_File','bronze','axiom_gl2024','gl2024','NA','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (532,'Flat_File','bronze','axiom_CDs','CDs','InstrumentID,YRMO','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (541,'Flat_File','bronze','axiom_gl2025','gl2025','NA','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (553,'Flat_File','bronze','YRMODAY','YRMODAY','YRMODAY','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (539,'Flat_File','bronze','axiom_dtype','dtype','dtype','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (533,'Flat_File','bronze','axiom_concur','concur','IDNum','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (548,'Flat_File','bronze','axiom_ratio','ratio','RatioID','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (545,'Flat_File','bronze','axiom_loans','loans','YRMO,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (534,'Flat_File','bronze','axiom_ConcurAP','ConcurAP','IDNum','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (537,'Flat_File','bronze','axiom_deposits','deposits','YRMO,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (549,'Flat_File','bronze','axiom_ratio2025','ratio2025','RatioID','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (531,'Flat_File','bronze','axiom_BGT2025','BGT2025','ACCT,DEPT,DTYPE','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (520,'Flat_File','Bronze','axiom_daily_CDs','daily_cds','YRMODAY,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (550,'Flat_File','bronze','axiom_RatioBud2025','RatioBud2025','RatioID','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (536,'Flat_File','bronze','axiom_daily_loans','daily_loans','YRMODAY,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (543,'Flat_File','bronze','axiom_glpriorday','glpriorday','ACCT,DEPT,DTYPE','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (542,'Flat_File','bronze','axiom_gldaily2025','gldaily2025','ACCT,DEPT,DTYPE','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (544,'Flat_File','bronze','axiom_gltransaction','gltransaction','AXTranCd,AXSrcID,ACCT,DEPT,DTYPE','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (551,'Flat_File','bronze','axiom_Securities_Borrowings','Securities_Borrowings','IDNum,YRMO,Security_ID_Number','Truncate and Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (546,'Flat_File','bronze','axiom_Pace_consolidated','Pace_consolidated','YRMO,PORTFOLIO,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (535,'Flat_File','bronze','axiom_daily_Deposits','daily_Deposits','YRMODAY,InstrumentID','Incremental Load');
# MAGIC INSERT INTO config.dqmetadata (Table_ID,Source_System,Schema_Name,Table_Name,Renamed_Table_Name,Primary_Key,Load_Type) VALUES (547,'Flat_File','bronze','axiom_PM_consolidated','PM_consolidated','YRMO,InstrumentID','Incremental Load');
