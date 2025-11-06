# Databricks notebook source
import os

instance = os.environ.get('DATABRICKS_HOST')
token = os.environ.get("DATABRICKS_TOKEN")


print(instance)
print(token)

# COMMAND ----------

import requests
import json

# Replace these with your Databricks workspace URL and token
DATABRICKS_INSTANCE = instance  # e.g., "https://dbc-12345678-9101.cloud.databricks.com"
DATABRICKS_TOKEN = token

# Job configuration
job_config = {
    "name": "Check the Vibe",
    "email_notifications": {
        "on_failure": [
            "pandi.anbu@avatest.in"
        ],
        "no_alert_for_skipped_runs": True
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "WidgetPara",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/pandi.anbu@avatest.in/RBAC-ABAC/Data",
                "base_parameters": {
                    "batch": "30"
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": "0130-103345-kb0sn6su",
            "timeout_seconds": 0,
            "email_notifications": {},
            "webhook_notifications": {}
        }
    ],
    "run_as": {
        "user_name": "dharun@avatest.in"
    }
}

# Create the job
response = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.1/jobs/create",
    headers={
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    },
    json=job_config
)

job_id = response.json()["job_id"]

# Check the response
if response.status_code == 200:
    print("Job created successfully!")
    print("Job ID:", response.json()["job_id"])
else:
    print("Failed to create job. Status code:", response.status_code)
    print("Response:", response.json())

# Create the job
response = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now",
    headers={
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    },
    json={
        "job-id": job_id
    }
)

# Check if the job was triggered successfully
if response.status_code == 200:
    run_id = response.json()["run_id"]
    print("Job triggered successfully!")
    print("Run ID:", run_id)
else:
    print("Failed to trigger job. Status code:", response.status_code)
    print("Response:", response.json())
