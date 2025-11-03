# Databricks notebook source
scope='kv-az-dbricks-uat-001'
catalog='ab_uat_catalog'
logger_directory='/Volumes/ab_uat_catalog/config/logger'
axiom_archive = 'abfss://uat-external-location@stgazdbricksdev001.dfs.core.windows.net/archive/axiom/'
axiom_input = 'abfss://uat-external-location@stgazdbricksdev001.dfs.core.windows.net/input/axiom/'
axiom_volume="/Volumes/ab_uat_catalog/config/axiom/"
employee_flag_path='/Volumes/ab_uat_catalog/bronze/unstructured/Employee_Flag/employee_flag.csv'
marketing_source_system = 'NYHQ_PROD'

