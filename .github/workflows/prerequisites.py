# Databricks notebook source
import os
import requests
import json
import yaml

def get_databricks_instance():
    """Get Databricks instance from environment variable"""
    instance = os.environ.get('DATABRICKS_HOST')
    if not instance:
        raise ValueError("DATABRICKS_HOST environment variable is not set")
    
    # Ensure it has https:// prefix
    if not instance.startswith('https://'):
        instance = f"https://{instance}"
    
    return instance

def get_databricks_token():
    """Get Databricks token from environment variable"""
    token = os.environ.get('DATABRICKS_TOKEN')
    if not token:
        raise ValueError("DATABRICKS_TOKEN environment variable is not set")
    return token

def get_all_clusters():
    """Extract all active clusters from Databricks workspace"""
    instance = get_databricks_instance()
    token = get_databricks_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print(f"Making request to: {instance}/api/2.0/clusters/list")
    
    response = requests.get(
        f"{instance}/api/2.0/clusters/list",
        headers=headers,
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch clusters: {response.status_code} - {response.text}")
    
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
        headers=headers,
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch SQL warehouses: {response.status_code} - {response.text}")
    
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
        cluster_name, bundle_cluster = convert_cluster_to_bundle_format(cluster)
        bundle_clusters[cluster_name] = bundle_cluster
    
    if bundle_clusters:
        clusters_config = {
            'resources': {
                'clusters': bundle_clusters
            }
        }
        return clusters_config
    return None

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

def main():
    """Main function to extract clusters and SQL warehouses"""
    print("Extracting clusters and SQL warehouses from Databricks workspace...")
    
    # Debug environment variables
    print(f"DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST')}")
    print(f"DATABRICKS_TOKEN length: {len(os.environ.get('DATABRICKS_TOKEN', ''))}")
    
    clusters = get_all_clusters()
    warehouses = get_all_sql_warehouses()
    
    print(f"Found {len(clusters)} clusters")
    print(f"Found {len(warehouses)} SQL warehouses")
    
    # Create directory structure
    os.makedirs('../resources/SharedObjects', exist_ok=True)
    
    # All-purpose clusters
    clusters_config = generate_clusters_bundle_yaml(clusters)
    if clusters_config:
        with open('../resources/SharedObjects/all_purpose_clusters.yml', 'w') as f:
            yaml.dump(clusters_config, f, default_flow_style=False, indent=2)
        print("   - resources/SharedObjects/all_purpose_clusters.yml")
    
    # SQL warehouses
    warehouses_config = generate_sql_warehouses_bundle_yaml(warehouses)
    if warehouses_config:
        with open('../resources/SharedObjects/sql_warehouses.yml', 'w') as f:
            yaml.dump(warehouses_config, f, default_flow_style=False, indent=2)
        print("   - resources/SharedObjects/sql_warehouses.yml")
    
    print("SUCCESS: Extraction completed!")

if __name__ == "__main__":
    main()
