# Databricks notebook source
import os
token = os.environ.get('DATABRICKS_TOKEN')

# COMMAND ----------

# DBTITLE 1,Policies
# import requests
# import json
# import yaml
# import os

# # Databricks configuration
# DATABRICKS_INSTANCE = "https://adb-201068313543333.13.azuredatabricks.net"
# DATABRICKS_TOKEN = {token}

# def get_all_policies():
#     """Extract all cluster policies from Databricks workspace"""
#     headers = {
#         "Authorization": f"Bearer {DATABRICKS_TOKEN}",
#         "Content-Type": "application/json"
#     }
    
#     # Get all policies
#     response = requests.get(
#         f"{DATABRICKS_INSTANCE}/api/2.0/policies/clusters/list",
#         headers=headers
#     )
    
#     if response.status_code != 200:
#         raise Exception(f"Failed to fetch policies: {response.text}")
    
#     policies_data = response.json()
    
#     # Get detailed policy definitions
#     policies_with_definitions = []
#     for policy in policies_data.get('policies', []):
#         policy_id = policy['policy_id']
        
#         # Get policy definition
#         def_response = requests.get(
#             f"{DATABRICKS_INSTANCE}/api/2.0/policies/clusters/get",
#             headers=headers,
#             params={"policy_id": policy_id}
#         )
        
#         if def_response.status_code == 200:
#             policy_detail = def_response.json()
#             policies_with_definitions.append(policy_detail)
#         else:
#             print(f"Warning: Could not fetch details for policy {policy_id}")
    
#     return policies_with_definitions

# def convert_policy_to_bundle_format(policy):
#     """Convert Databricks policy to Asset Bundle format"""
#     policy_name = policy['name'].lower().replace(' ', '-').replace('_', '-')
    
#     bundle_policy = {
#         'name': policy['name'],
#         'definition': policy.get('definition', {})
#     }
    
#     # Add description if exists
#     if policy.get('description'):
#         bundle_policy['description'] = policy['description']
    
#     return policy_name, bundle_policy

# def generate_bundle_yaml(policies):
#     """Generate complete Databricks Asset Bundle YAML"""
    
#     # Convert policies to bundle format
#     bundle_policies = {}
#     for policy in policies:
#         policy_name, bundle_policy = convert_policy_to_bundle_format(policy)
#         bundle_policies[policy_name] = bundle_policy
    
#     # Policies resources
#     policies_config = {
#         'resources': {
#             'policies': bundle_policies
#         }
#     }
    
#     return  policies_config

# def main():
#     print("Extracting policies from Databricks workspace...")
    
#     # Get all policies
#     policies = get_all_policies()
#     print(f"Found {len(policies)} policies")
    
#     # Generate YAML configurations
#     policies_config = generate_bundle_yaml(policies)
    
#     # Create directory structur
    
#     # Write policies file
#     with open('../resources/SharedObjects/policies.yml', 'w') as f:
#         yaml.dump(policies_config, f, default_flow_style=False, indent=2)
    
#     print("✅ Policy extraction completed!")
#     print("📁 Files created:")
#     print("   - resources/policies.yml")

# if __name__ == "__main__":
#     main()

# COMMAND ----------

import requests
import json
import yaml
import os

def get_databricks_instance():
    return "https://adb-201068313543333.13.azuredatabricks.net"

def get_databricks_token():
    return os.environ.get('DATABRICKS_TOKEN')

def get_all_clusters():
    """Extract all active clusters from Databricks workspace"""
    instance = get_databricks_instance()
    token = get_databricks_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    if len(token)>0:
        print(token)
    else:
        raise "issue.................." 
    response = requests.get(
        f"{instance}/api/2.0/clusters/list",
        headers=headers
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch clusters: {response.text}")
    clusters_data = response.json()
    # Only return clusters that are not job clusters
    return [c for c in clusters_data.get('clusters', []) if c.get('cluster_source') != 'JOB']

def convert_cluster_to_bundle_format(cluster):
    """Convert Databricks cluster to Asset Bundle format"""
    cluster_name = cluster['cluster_name'].lower().replace(' ', '-').replace('_', '-')
    bundle_cluster = {
        'cluster_name': cluster['cluster_name'],
        'spark_version': cluster.get('spark_version'),
        'node_type_id': cluster.get('node_type_id'),
        'num_workers': cluster.get('num_workers'),
        'autotermination_minutes': cluster.get('autotermination_minutes'),
        'custom_tags': cluster.get('custom_tags', {}),
        'cluster_log_conf': cluster.get('cluster_log_conf', {}),
        'init_scripts': cluster.get('init_scripts', []),
        'aws_attributes': cluster.get('aws_attributes', {}),
        'azure_attributes': cluster.get('azure_attributes', {}),
        'gcp_attributes': cluster.get('gcp_attributes', {}),
        'policy_id': cluster.get('policy_id')
    }
    # Remove empty fields
    bundle_cluster = {k: v for k, v in bundle_cluster.items() if v}
    return cluster_name, bundle_cluster

def get_all_sql_warehouses():
    """Extract all SQL warehouses from Databricks workspace"""
    instance = get_databricks_instance()
    token = get_databricks_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.get(
        f"{instance}/api/2.0/sql/warehouses",
        headers=headers
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch SQL warehouses: {response.text}")
    warehouses_data = response.json()
    return warehouses_data.get('warehouses', [])

def convert_sql_warehouse_to_bundle_format(warehouse):
    """Convert Databricks SQL warehouse to Asset Bundle format"""
    warehouse_name = warehouse['name'].lower().replace(' ', '-').replace('_', '-')
    bundle_warehouse = {
        'name': warehouse['name'],
        'cluster_size': warehouse.get('cluster_size'),
        'min_num_clusters': warehouse.get('min_num_clusters'),
        'max_num_clusters': warehouse.get('max_num_clusters'),
        'auto_stop_mins': warehouse.get('auto_stop_mins'),
        'enable_serverless_compute': warehouse.get('enable_serverless_compute'),
        'spot_instance_policy': warehouse.get('spot_instance_policy'),
        'tags': warehouse.get('tags', {}),
        'channel': warehouse.get('channel'),
        'warehouse_type': warehouse.get('warehouse_type')
    }
    # Remove empty fields
    bundle_warehouse = {k: v for k, v in bundle_warehouse.items() if v is not None}
    return warehouse_name, bundle_warehouse

def generate_clusters_bundle_yaml(clusters):
    """Generate Databricks Asset Bundle YAML for clusters"""
    bundle_clusters = {}
    for cluster in clusters:
        # Exclude job clusters
        if cluster.get('cluster_source') == 'JOB':
            continue
        cluster_name, bundle_cluster = convert_cluster_to_bundle_format(cluster)
        # Separate by cluster type
        cluster_type = cluster.get('cluster_source', 'ALL_PURPOSE')
        if cluster_type != 'JOB':
            bundle_clusters[cluster_name] = bundle_cluster
    if bundle_clusters:
        clusters_config = {
            'resources': {
                'clusters': bundle_clusters
            }
        }
        return clusters_config
    return None

def generate_job_clusters_bundle_yaml(clusters):
    """Generate Databricks Asset Bundle YAML for job clusters (not needed)"""
    return None  # Not needed as per user request

def generate_sql_warehouses_bundle_yaml(warehouses):
    """Generate Databricks Asset Bundle YAML for SQL warehouses"""
    bundle_warehouses = {}
    for warehouse in warehouses:
        warehouse_name, bundle_warehouse = convert_sql_warehouse_to_bundle_format(warehouse)
        bundle_warehouses[warehouse_name] = bundle_warehouse
    if bundle_warehouses:
        warehouses_config = {
            'resources': {
                'sql_warehouses': bundle_warehouses
            }
        }
        return warehouses_config
    return None

print("Extracting clusters and SQL warehouses from Databricks workspace...")
clusters = get_all_clusters()
for row in clusters:
    if row.get('cluster_source') != 'JOB':
        print(row['cluster_source'])
warehouses = get_all_sql_warehouses()
print(f"Found {len(clusters)} clusters")
print(f"Found {len(warehouses)} SQL warehouses")
os.makedirs('../resources/SharedObjects', exist_ok=True)
# All-purpose clusters
clusters_config = generate_clusters_bundle_yaml(clusters)
if clusters_config:
    with open('../resources/SharedObjects/all_purpose_clusters.yml', 'w') as f:
        yaml.dump(clusters_config, f, default_flow_style=False, indent=2)
    print("   - resources/all_purpose_clusters.yml")
# SQL warehouses
warehouses_config = generate_sql_warehouses_bundle_yaml(warehouses)
if warehouses_config:
    with open('../resources/SharedObjects/sql_warehouses.yml', 'w') as f:
        yaml.dump(warehouses_config, f, default_flow_style=False, indent=2)
    print("   - resources/sql_warehouses.yml")
print("✅ Extraction completed!")
