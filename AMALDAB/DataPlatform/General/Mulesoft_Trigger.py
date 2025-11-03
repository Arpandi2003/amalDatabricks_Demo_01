# Databricks notebook source
# MAGIC %run "./NB_Configuration"

# COMMAND ----------

client_id = dbutils.secrets.get(scope, key='mulesoft-client-id')
client_secret = dbutils.secrets.get(scope, key='mulesoft-client-secret-id')
mulesoft_endpoint = dbutils.secrets.get(scope, key='mulesoft-endpoint')

# COMMAND ----------

import requests

url = mulesoft_endpoint
headers = {
    "Content-Length": "0",
    "client_id": client_id,
    "client_secret": client_secret
}

response = requests.post(url, headers=headers)

if response:
    print(response.json())
else:
    raise 'error'
