# Databricks notebook source
# NOTE: This file is a template. Values are automatically replaced during deployment
# based on the target environment (DEV/UAT/PROD) defined in databricks.yml
scope='kv-az-dbricks-dev-001'
catalog='ab_dev_catalog'
logger_directory='/Volumes/ab_dev_catalog/config/logger'
axiom_archive = 'abfss://dev-external-location@stgazdbricksdev001.dfs.core.windows.net/archive/axiom/'
axiom_input = 'abfss://dev-external-location@stgazdbricksdev001.dfs.core.windows.net/input/axiom/'
axiom_volume="/Volumes/ab_dev_catalog/config/axiom/"
employee_flag_path='/Volumes/ab_dev_catalog/bronze/unstructured/Employee_Flag/employee_flag.csv'
marketing_source_system = 'NYHQ_PROD'

