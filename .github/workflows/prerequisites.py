# Databricks notebook source
#!/usr/bin/env python3
# prerequisites.py
# Databricks Disaster Recovery Backup Script
# Extracts configuration-as-code for compute, UC metadata, and permissions.

#known issue 
    #Update get_databricks_token function to generate the access token for SP

import os
import requests
import json
import yaml
import argparse
import urllib.parse
from typing import Any, Dict, List, Optional

# ===================================================================
# 🛠️ CONFIGURATION & UTILITIES
# ===================================================================

import os
import argparse
import sys

def get_databricks_instance() -> str:
    """Get Databricks workspace URL from CLI args or env vars (CLI args take precedence)."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--DATABRICKS_HOST",
        default=os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_WORKSPACE_URL") or os.getenv("DATABRICKS_URL"),
        help="Databricks workspace URL (e.g., https://adb-123.azuredatabricks.net)"
    )
    # Parse only known args (in case other args passed)
    args, _ = parser.parse_known_args()
    
    instance = args.DATABRICKS_HOST
    if not instance:
        raise ValueError(
            "Databricks URL not set. Provide --DATABRICKS_HOST or set DATABRICKS_HOST / DATABRICKS_WORKSPACE_URL."
        )
    
    instance = instance.strip()
    if not instance.startswith(("https://", "http://")):
        instance = f"https://{instance}"
    return instance.rstrip("/")


def get_databricks_token() -> str:
    """Get Databricks token from CLI args or env vars (CLI args take precedence)."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--DATABRICKS_TOKEN",
        default=os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_ACCESS_TOKEN"),
        help="Databricks personal access token (PAT) or OAuth token"
    )
    args, _ = parser.parse_known_args()
    
    token = args.DATABRICKS_TOKEN
    if not token:
        raise ValueError(
            "Databricks token missing. Provide --DATABRICKS_TOKEN or set DATABRICKS_TOKEN / DATABRICKS_ACCESS_TOKEN."
        )
    return token.strip()

def get_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_databricks_token()}",
        "Content-Type": "application/json"
    }

def api_get(url: str, timeout: int = 60) -> Any:
    resp = requests.get(url, headers=get_headers(), timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def safe_write_yml(filepath: str, data: Dict) -> None:
    if not data:
        print(f"Skipping empty YAML: {filepath}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, indent=2, sort_keys=False)
    print(f" Wrote: {filepath}")

def safe_write_sql(filepath: str, content: str) -> None:
    if not content.strip():
        print(f" Skipping empty SQL: {filepath}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content.strip() + "\n")
    print(f" Wrote: {filepath}")

def to_bundle_name(name: str) -> str:
    return (
        name.lower()
        .replace(" ", "-")
        .replace("_", "-")
        .replace(".", "-")
        .replace("@", "-at-")
    )

def sanitize_dict(d: Dict[str, Any], exclude_keys: List[str]) -> Dict[str, Any]:
    """Remove sensitive keys (case-insensitive match on suffix)."""
    return {
        k: v for k, v in d.items()
        if not any(k.lower().endswith(ex_key.lower()) for ex_key in exclude_keys)
    }

# ===================================================================
# 📥 FETCHERS — INFRASTRUCTURE
# ===================================================================

def get_all_clusters() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.0/clusters/list"
    data = api_get(url)
    return [c for c in data.get("clusters", []) if c.get("cluster_source") != "JOB"]

def get_cluster_policies() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.0/policies/clusters/list"
    data = api_get(url)
    return data.get("policies", [])

def get_sql_warehouses() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.0/sql/warehouses"
    data = api_get(url)
    return data.get("warehouses", [])

def get_storage_credentials() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/storage-credentials"
    data = api_get(url)
    return data.get("storage_credentials", [])

def get_external_locations() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/external-locations"
    data = api_get(url)
    return data.get("external_locations", [])

# def get_jobs() -> List[Dict]:
#     url = f"{get_databricks_instance()}/api/2.1/jobs/list?expand_tasks=true&limit=100"
#     data = api_get(url)
#     jobs = data.get("jobs", [])
#     # Paginate if needed
#     while data.get("has_more"):
#         offset = data["limit"] + data.get("offset", 0)
#         data = api_get(f"{url}&offset={offset}")
#         jobs.extend(data.get("jobs", []))
#     return jobs

def get_connections() -> List[Dict]:
    # Note: Connections API is /2.0/, NOT /2.1/unity-catalog/
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/connections"
    try:
        data = api_get(url)
        return data.get("connections", [])
    except Exception as e:
        print(f" Skipping connections (API may be disabled): {e}")
        return []

# ===================================================================
# 📥 FETCHERS — UNITY CATALOG METADATA
# ===================================================================

def get_catalogs() -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/catalogs"
    data = api_get(url)
    return [c for c in data.get("catalogs", []) if c["name"] not in {"system", "information_schema"}]

def get_schemas(catalog_name: str) -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/schemas?catalog_name={urllib.parse.quote(catalog_name)}"
    data = api_get(url)
    return data.get("schemas", [])

def get_tables(catalog_name: str, schema_name: str) -> List[Dict]:
    url = (
        f"{get_databricks_instance()}/api/2.1/unity-catalog/tables"
        f"?catalog_name={urllib.parse.quote(catalog_name)}"
        f"&schema_name={urllib.parse.quote(schema_name)}"
    )
    data = api_get(url)
    return data.get("tables", [])

def get_volumes(catalog_name: str, schema_name: str) -> List[Dict]:
    url = (
        f"{get_databricks_instance()}/api/2.1/unity-catalog/volumes"
        f"?catalog_name={urllib.parse.quote(catalog_name)}"
        f"&schema_name={urllib.parse.quote(schema_name)}"
    )
    data = api_get(url)
    return data.get("volumes", [])

def get_functions(catalog_name: str, schema_name: str) -> List[Dict]:
    url = (
        f"{get_databricks_instance()}/api/2.1/unity-catalog/functions"
        f"?catalog_name={urllib.parse.quote(catalog_name)}"
        f"&schema_name={urllib.parse.quote(schema_name)}"
    )
    data = api_get(url)
    return data.get("functions", [])

def get_grants_on(securable_type: str, full_name: str) -> List[Dict]:
    url = f"{get_databricks_instance()}/api/2.1/unity-catalog/permissions/{securable_type}/{urllib.parse.quote(full_name)}"
    try:
        data = api_get(url)
        return data.get("privilege_assignments", [])
    except Exception as e:
        print(f"  Grants on {securable_type} `{full_name}`: {e}")
        return []

# ===================================================================
# 🔁 CONVERTERS — TO ASSET BUNDLE FORMAT
# ===================================================================

def convert_cluster(cluster: Dict) -> (str, Dict):
    out = {
        "cluster_name": cluster["cluster_name"],
        "spark_version": cluster.get("spark_version"),
        "node_type_id": cluster.get("node_type_id"),
        "driver_node_type_id": cluster.get("driver_node_type_id"),
        "num_workers": cluster.get("num_workers", 0),
        "autoscale": cluster.get("autoscale"),
        "autotermination_minutes": cluster.get("autotermination_minutes"),
        "custom_tags": cluster.get("custom_tags") or {},
        "cluster_log_conf": cluster.get("cluster_log_conf") or {},
        "init_scripts": cluster.get("init_scripts") or [],
        "aws_attributes": cluster.get("aws_attributes") or {},
        "azure_attributes": cluster.get("azure_attributes") or {},
        "gcp_attributes": cluster.get("gcp_attributes") or {},
        "policy_id": cluster.get("policy_id"),
        "single_user_name": cluster.get("single_user_name"),
        "data_security_mode": cluster.get("data_security_mode"),
        "runtime_engine": cluster.get("runtime_engine"),
    }
    out = {k: v for k, v in out.items() if v not in (None, {}, [])}
    return to_bundle_name(cluster["cluster_name"]), out

def convert_policy(policy: Dict) -> (str, Dict):
    definition = json.loads(policy["definition"]) if isinstance(policy["definition"], str) else policy["definition"]
    out = {
        "name": policy["name"],
        "definition": definition,
        "description": policy.get("description") or "",
        "max_clusters_per_user": policy.get("max_clusters_per_user"),
    }
    out = {k: v for k, v in out.items() if v not in (None, "")}
    return to_bundle_name(policy["name"]), out

def convert_sql_warehouse(wh: Dict) -> (str, Dict):
    out = {
        "name": wh["name"],
        "cluster_size": wh.get("cluster_size"),
        "min_num_clusters": wh.get("min_num_clusters"),
        "max_num_clusters": wh.get("max_num_clusters"),
        "auto_stop_mins": wh.get("auto_stop_mins"),
        "enable_photon": wh.get("enable_photon"),
        "enable_serverless_compute": wh.get("enable_serverless_compute"),
        "spot_instance_policy": wh.get("spot_instance_policy"),
        "channel": wh.get("channel"),
        "tags": wh.get("tags") or {},
        "warehouse_type": wh.get("warehouse_type"),
    }
    out = {k: v for k, v in out.items() if v not in (None, {})}
    return to_bundle_name(wh["name"]), out

def convert_sc(sc: Dict) -> (str, Dict):
    out = {
        "name": sc["name"],
        "owner": sc.get("owner"),
        "read_only": sc.get("read_only", False),
        "comment": sc.get("comment"),
        "aws_iam_role": sc.get("aws_iam_role") or {},
        "azure_managed_identity": sc.get("azure_managed_identity") or {},
    }
    out = {k: v for k, v in out.items() if v not in (None, {})}
    return to_bundle_name(sc["name"]), out

def convert_el(el: Dict) -> (str, Dict):
    out = {
        "name": el["name"],
        "url": el["url"],
        "credential_name": el["credential_name"],
        "owner": el.get("owner"),
        "comment": el.get("comment"),
        "skip_validation": el.get("skip_validation", False),
    }
    out = {k: v for k, v in out.items() if v is not None}
    return to_bundle_name(el["name"]), out

def convert_job(job: Dict) -> (str, Dict):
    settings = job.get("settings", {})
    out = {
        "name": settings.get("name", f"job-{job['job_id']}"),
        "tasks": settings.get("tasks", []),
        "schedule": settings.get("schedule"),
        "email_notifications": settings.get("email_notifications"),
        "timeout_seconds": settings.get("timeout_seconds"),
        "git_source": settings.get("git_source"),
        "max_concurrent_runs": settings.get("max_concurrent_runs", 1),
    }
    out = {k: v for k, v in out.items() if v not in (None, {})}
    return to_bundle_name(out["name"]), out

def convert_connection(conn: Dict) -> (str, Dict):
    clean_conn = {
        "name": conn["name"],
        "connection_type": conn.get("connection_type"),
        "options": sanitize_dict(conn.get("options", {}), ["secret", "password", "token", "key", "client_secret"]),
    }
    return to_bundle_name(conn["name"]), clean_conn

# ===================================================================
# 📜 DDL & GRANT GENERATORS — UNITY CATALOG
# ===================================================================

def uc_catalog_ddl(cat: Dict) -> str:
    name = cat["name"]
    comment = cat.get("comment", "")
    owner = cat.get("owner", "")
    ddl = f"CREATE CATALOG IF NOT EXISTS `{name}`"
    if comment:
        ddl += f" COMMENT '{comment}'"
    if owner:
        ddl += f" WITH DBPROPERTIES (owner = '{owner}')"
    return ddl + ";"

def uc_schema_ddl(cat_name: str, sch: Dict) -> str:
    name = sch["name"]
    comment = sch.get("comment", "")
    ddl = f"CREATE SCHEMA IF NOT EXISTS `{cat_name}`.`{name}`"
    if comment:
        ddl += f" COMMENT '{comment}'"
    return ddl + ";"

def uc_table_ddl(cat_name: str, sch_name: str, tbl: Dict) -> Optional[str]:
    name = tbl["name"]
    full_name = f"`{cat_name}`.`{sch_name}`.`{name}`"
    tbl_type = tbl.get("table_type", "MANAGED")
    data_source = tbl.get("data_source_format", "DELTA")

    if tbl_type == "VIEW":
        view_def = tbl.get("view_definition")
        return f"CREATE OR REPLACE VIEW {full_name} AS\n{view_def};" if view_def else None

    # Regular (managed/external) table
    ddl = f"CREATE TABLE IF NOT EXISTS {full_name}\nUSING {data_source}"

    # Only include LOCATION for EXTERNAL tables — NOT for managed!
    storage_location = tbl.get("storage_location", "")
    if tbl_type == "EXTERNAL" and storage_location:
        ddl += f"\nLOCATION '{storage_location}'"

    # Add TBLPROPERTIES (but filter read-only Delta props)
    props = tbl.get("properties", {})
    filtered_props = {
        k: v for k, v in props.items()
        if not k.startswith(("delta.", "spark.sql.statistics"))
    }
    if filtered_props:
        prop_list = ", ".join([f"'{k}' = '{v}'" for k, v in filtered_props.items()])
        ddl += f"\nTBLPROPERTIES ({prop_list})"

    return ddl + ";"

def uc_volume_ddl(cat_name: str, sch_name: str, vol: Dict) -> str:
    name = vol["name"]
    full_name = f"`{cat_name}`.`{sch_name}`.`{name}`"
    vol_type = vol.get("volume_type", "MANAGED")
    comment = vol.get("comment", "")
    location = vol.get("storage_location", "")

    ddl = f"CREATE VOLUME IF NOT EXISTS {full_name}"
    if comment:
        ddl += f" COMMENT '{comment}'"
    if vol_type == "EXTERNAL" and location:
        ddl += f" LOCATION '{location}'"
    return ddl + ";"

def grant_to_sql(grant: Dict, securable_type: str, full_name: str) -> str:
    principal = grant["principal"].replace("'", "''")
    full_name_esc = full_name.replace("'", "''")
    privs = ", ".join(grant["privileges"])
    return f"GRANT {privs} ON {securable_type} `{full_name_esc}` TO `{principal}`;"

# ===================================================================
# 🚀 MAIN EXECUTION
# ===================================================================

def main():
    print(" Starting Databricks DR Configuration Backup")
    base_dir = "./AMALDAB/resources/SharedObjects"
    ddl_dir = "./AMALDAB/resources/uc_ddl"

    # ===== 1. INFRASTRUCTURE =====
    print("\n Fetching infrastructure...")
    clusters = get_all_clusters()
    policies = get_cluster_policies()
    warehouses = get_sql_warehouses()
    scs = get_storage_credentials()
    els = get_external_locations()
    # jobs = get_jobs()
    connections = get_connections()

    print(f"   • Clusters: {len(clusters)}")
    print(f"   • Policies: {len(policies)}")
    print(f"   • SQL Warehouses: {len(warehouses)}")
    print(f"   • Storage Credentials: {len(scs)}")
    print(f"   • External Locations: {len(els)}")
    # print(f"   • Jobs: {len(jobs)}")
    print(f"   • Connections (sanitized): {len(connections)}")

    resources = {}

    if clusters:
        for cluster in clusters:
            cluster['cluster_name'] = cluster['cluster_name'].replace(" ", "-").replace("_", "-").replace(".", "-").replace("@", "-at-").replace("'",'')
        resources["clusters"] = {convert_cluster(c)[0]: convert_cluster(c)[1] for c in clusters}
        safe_write_yml(f"{base_dir}/all_purpose_clusters.yml", {"resources": {"clusters": resources["clusters"]}})

    if policies:
        resources["cluster_policies"] = {convert_policy(p)[0]: convert_policy(p)[1] for p in policies}
        safe_write_yml(f"{base_dir}/cluster_policies.yml", {"resources": {"cluster_policies": resources["cluster_policies"]}})

    if warehouses:
        resources["sql_warehouses"] = {convert_sql_warehouse(w)[0]: convert_sql_warehouse(w)[1] for w in warehouses}
        safe_write_yml(f"{base_dir}/sql_warehouses.yml", {"resources": {"sql_warehouses": resources["sql_warehouses"]}})

    if scs:
        resources["storage_credentials"] = {convert_sc(sc)[0]: convert_sc(sc)[1] for sc in scs}
        safe_write_yml(f"{base_dir}/storage_credentials.yml", {"resources": {"storage_credentials": resources["storage_credentials"]}})

    if els:
        resources["external_locations"] = {convert_el(el)[0]: convert_el(el)[1] for el in els}
        safe_write_yml(f"{base_dir}/external_locations.yml", {"resources": {"external_locations": resources["external_locations"]}})

    # if jobs:
    #     resources["jobs"] = {convert_job(j)[0]: convert_job(j)[1] for j in jobs}
    #     safe_write_yml(f"{base_dir}/jobs.yml", {"resources": {"jobs": resources["jobs"]}})

    if connections:
        resources["connections"] = {convert_connection(c)[0]: convert_connection(c)[1] for c in connections}
        safe_write_yml(f"{base_dir}/connections.yml", {"resources": {"connections": resources["connections"]}})

    # ===== 2. UNITY CATALOG METADATA =====
    print("\n Fetching Unity Catalog metadata...")
    catalogs = get_catalogs()
    ddl_statements = []
    grant_statements = []

    for cat in catalogs:
        cat_name = cat["name"]
        print(f"Catalog: {cat_name}")

        # Catalog DDL
        ddl_statements.append(uc_catalog_ddl(cat))

        # Grants on catalog
        grants = get_grants_on("CATALOG", cat_name)
        grant_statements.extend(grant_to_sql(g, "CATALOG", cat_name) for g in grants)

        # Schemas + Tables + Volumes
        for sch in get_schemas(cat_name):
            sch_name = sch["name"]
            full_sch = f"{cat_name}.{sch_name}"
            print(f" Schema: {full_sch}")

            ddl_statements.append(uc_schema_ddl(cat_name, sch))

            grants = get_grants_on("SCHEMA", full_sch)
            grant_statements.extend(grant_to_sql(g, "SCHEMA", full_sch) for g in grants)

            for tbl in get_tables(cat_name, sch_name):
                ddl = uc_table_ddl(cat_name, sch_name, tbl)
                if ddl:
                    ddl_statements.append(ddl)

                full_tbl = f"{cat_name}.{sch_name}.{tbl['name']}"
                grants = get_grants_on("TABLE", full_tbl)
                grant_statements.extend(grant_to_sql(g, "TABLE", full_tbl) for g in grants)

            for vol in get_volumes(cat_name, sch_name):
                ddl_statements.append(uc_volume_ddl(cat_name, sch_name, vol))

                full_vol = f"{cat_name}.{sch_name}.{vol['name']}"
                grants = get_grants_on("VOLUME", full_vol)
                grant_statements.extend(grant_to_sql(g, "VOLUME", full_vol) for g in grants)

            # Functions: API doesn’t return DDL — skip or add stub
            funcs = get_functions(cat_name, sch_name)
            if funcs:
                print(f" {len(funcs)} functions found (DDL not available via REST API)")

    # ===== 3. PERSIST OUTPUTS =====
    if ddl_statements:
        safe_write_sql(f"{ddl_dir}/00_catalogs_schemas_tables_volumes.ddl.sql", "\n\n".join(ddl_statements))
    if grant_statements:
        safe_write_sql(f"{ddl_dir}/99_grants.ddl.sql", "\n".join(grant_statements))

    print("\n DR backup completed!")
    print(f" Bundle configs: {base_dir}")
    print(f" UC DDL & grants: {ddl_dir}")

if __name__ == "__main__":
    main()
