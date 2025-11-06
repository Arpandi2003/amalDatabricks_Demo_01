# Databricks notebook source
import os

instance = os.environ.get('DATABRICKS_HOST')
token = os.environ.get("DATABRICKS_TOKEN")


print(instance)
print(token)
