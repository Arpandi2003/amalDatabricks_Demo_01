# Databricks notebook source
# MAGIC %sql
# MAGIC -- Allow the service principal to see all schemas in the catalog
# MAGIC GRANT USAGE ON SCHEMA centraldata_prod.metrics TO `198707ba-e9d3-4ed9-95f7-be0bd7b6647e`;
# MAGIC -- Allow the service principal to read all tables/views in a schema
# MAGIC GRANT SELECT ON SCHEMA centraldata_prod.metrics TO `198707ba-e9d3-4ed9-95f7-be0bd7b6647e`;
