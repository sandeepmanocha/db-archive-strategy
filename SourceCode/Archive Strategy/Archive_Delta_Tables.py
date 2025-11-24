# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Archive Process (Parameter-Driven)
# MAGIC
# MAGIC This notebook archives Delta tables by year using input parameters.
# MAGIC
# MAGIC ## Features:
# MAGIC - Fully parameter-driven (no config files needed)
# MAGIC - Archives from source catalog/schema/table to direct storage locations (S3, ADLS, GCS, etc.)
# MAGIC - **Self-contained year folders**: Each year is an independent Delta table with its own _delta_log
# MAGIC - **Easy restoration**: Copy individual year folders without dependencies
# MAGIC - Optional deletion from source table after archiving
# MAGIC - Supports date filtering columns
# MAGIC - Writes to direct cloud storage paths (S3, ADLS, GCS) or Unity Catalog Volumes
# MAGIC - Can be scheduled in Databricks Workflows
# MAGIC - Comprehensive audit logging
# MAGIC
# MAGIC ## Required Parameters:
# MAGIC - **Source**: catalog, schema, table, date_column
# MAGIC - **Target**: base_path (direct storage location, e.g., s3://bucket/path or abfss://container@account.dfs.core.windows.net/path)
# MAGIC - **Filter**: year (specific year to archive)
# MAGIC - **Options**: delete_after_archive (true/false) - whether to delete from source after archiving
# MAGIC
# MAGIC ## Archive Structure:
# MAGIC ```
# MAGIC s3://one-env-uc-external-location/sm-field-demo/wiki/
# MAGIC ├── year_2013/              ← Self-contained Delta table
# MAGIC │   ├── _delta_log/         ← Independent transaction log
# MAGIC │   └── *.parquet           ← Data files
# MAGIC └── year_2014/              ← Self-contained Delta table
# MAGIC     ├── _delta_log/
# MAGIC     └── *.parquet
# MAGIC ```
# MAGIC
# MAGIC ## Use Cases:
# MAGIC - **Archive & Delete** (delete_after_archive=true): Free up space in production tables
# MAGIC - **Archive Only** (delete_after_archive=false): Create backups while keeping source data intact
# MAGIC - **Point-in-time Restore**: Copy specific year folders for independent restoration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Parameters

# COMMAND ----------

# Create input widgets for parameterization
dbutils.widgets.text("source_catalog", "source_catalog", "01. Source Catalog")
dbutils.widgets.text("source_schema", "on_boarding", "02. Source Schema")
dbutils.widgets.text("source_table", "wikipedia_articles2", "03. Source Table")
dbutils.widgets.text("date_column", "revisionTimestamp", "04. Date Column")
dbutils.widgets.text("target_base_path", "s3://one-env-uc-external-location/sm-field-demo/wiki", "05. Target Base Path")
dbutils.widgets.text("year", "2013", "06. Year")
dbutils.widgets.dropdown("delete_after_archive", "false", ["true", "false"], "07. Delete After Archive")

# Get widget values
source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
date_column = dbutils.widgets.get("date_column").strip()
target_base_path = dbutils.widgets.get("target_base_path").strip()
year = dbutils.widgets.get("year").strip()
delete_after_archive = dbutils.widgets.get("delete_after_archive").strip().lower() == "true"

# Validate required parameters
print("="*80)
print("PARAMETER VALIDATION")
print("="*80)

required_params = {
    "Source Catalog": source_catalog,
    "Source Schema": source_schema,
    "Source Table": source_table,
    "Date Column": date_column,
    "Target Base Path": target_base_path,
    "Year": year
}

missing_params = [name for name, value in required_params.items() if not value]

if missing_params:
    error_msg = f"ERROR: Missing required parameters: {', '.join(missing_params)}"
    print(error_msg)
    print("="*80)
    raise ValueError(error_msg)

# Validate year is numeric
try:
    year_int = int(year)
except ValueError:
    error_msg = f"ERROR: Year must be a valid integer. Got: '{year}'"
    print(error_msg)
    print("="*80)
    raise ValueError(error_msg)

print("✓ All required parameters provided")
print()
print("SOURCE CONFIGURATION")
print("-"*80)
print(f"Catalog: {source_catalog}")
print(f"Schema: {source_schema}")
print(f"Table: {source_table}")
print(f"Date Column: {date_column}")
print()
print("TARGET CONFIGURATION")
print("-"*80)
print(f"Base Path: {target_base_path}")
print()
print("ARCHIVE OPTIONS")
print("-"*80)
print(f"Archive Year: {year_int}")
print(f"Delete After Archive: {'Yes' if delete_after_archive else 'No (Archive Only)'}")
print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def archive_table_by_year(source_catalog, source_schema, source_table, date_column, 
                          target_base_path, year):
    """
    Archive a specific year of data from source table to target storage location.
    Each year is stored as a self-contained, independent Delta table.
    This allows easy copying/restoration of individual years without dependencies.
    
    Args:
        source_catalog: Source catalog name
        source_schema: Source schema name
        source_table: Source table name
        date_column: Date column for filtering
        target_base_path: Direct storage path (e.g., s3://bucket/path, abfss://container@account.dfs.core.windows.net/path)
        year: Year to archive
    """
    # Construct archive path - each year is a separate, independent Delta table
    # Remove trailing slash if present to ensure consistent path formatting
    base_path = target_base_path.rstrip('/')
    archive_path = f"{base_path}/year_{year}"
    
    print(f"Archiving {source_catalog}.{source_schema}.{source_table} for year {year}...")
    print(f"Target path: {archive_path}/")
    
    # Read data for the specific year
    df = spark.sql(f"""
        SELECT *
        FROM {source_catalog}.{source_schema}.{source_table}
        WHERE YEAR({date_column}) = {year}
    """)
    
    record_count = df.count()
    print(f"Records to archive: {record_count}")
    
    if record_count > 0:
        # Write to archive location as independent Delta table
        # Each year folder contains its own _delta_log for complete independence
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(archive_path)
        
        print(f"✓ Successfully archived {record_count} records for year {year}")
        print(f"  Self-contained Delta table at: {archive_path}/")
        print(f"  Can be copied/restored independently")
        return record_count
    else:
        print(f"No records found for year {year}")
        return 0

# COMMAND ----------

def delete_archived_data(source_catalog, source_schema, source_table, date_column, year):
    """
    Delete archived data from the source table after successful archive.
    """
    print(f"Deleting archived data for year {year} from {source_catalog}.{source_schema}.{source_table}...")
    
    delete_query = f"""
    DELETE FROM {source_catalog}.{source_schema}.{source_table}
    WHERE YEAR({date_column}) = {year}
    """
    
    spark.sql(delete_query)
    print(f"✓ Deleted year {year} data from source table")

# COMMAND ----------

def log_archive_activity(source_catalog, source_schema, source_table, 
                         target_base_path, year, record_count, status, 
                         audit_catalog=None, audit_schema=None):
    """
    Log archive activity to an audit table.
    Create the audit table if it doesn't exist.
    
    Args:
        source_catalog: Source catalog name
        source_schema: Source schema name
        source_table: Source table name
        target_base_path: Direct storage path where archive is stored
        year: Year archived
        record_count: Number of records archived
        status: Archive status
        audit_catalog: Optional catalog for audit log (defaults to source_catalog)
        audit_schema: Optional schema for audit log (defaults to source_schema)
    """
    # Default audit location to source catalog/schema if not specified
    audit_catalog = audit_catalog or source_catalog
    audit_schema = audit_schema or source_schema
    audit_table = f"{audit_catalog}.{audit_schema}.archive_audit_log"
    
    # Create audit table if it doesn't exist
    create_audit_table = f"""
    CREATE TABLE IF NOT EXISTS {audit_table} (
        archive_timestamp TIMESTAMP,
        source_catalog STRING,
        source_schema STRING,
        source_table STRING,
        target_base_path STRING,
        archive_year INT,
        record_count BIGINT,
        status STRING,
        archived_by STRING
    )
    USING DELTA
    """
    
    spark.sql(create_audit_table)
    
    # Insert audit record using SQL to avoid schema conflicts
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Escape single quotes in fields to prevent SQL injection
    safe_status = status.replace("'", "''")
    safe_base_path = target_base_path.replace("'", "''")
    
    insert_query = f"""
    INSERT INTO {audit_table}
    VALUES (
        CAST('{timestamp_str}' AS TIMESTAMP),
        '{source_catalog}',
        '{source_schema}',
        '{source_table}',
        '{safe_base_path}',
        {year},
        {record_count},
        '{safe_status}',
        '{current_user}'
    )
    """
    
    spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Archive Process

# COMMAND ----------

print("="*80)
print("STARTING ARCHIVE PROCESS")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()

# Archive status tracking
archived_count = 0
archive_status = "SUCCESS"
error_message = None
deleted_from_source = False

# COMMAND ----------

print("-"*80)
print(f"Processing: {source_catalog}.{source_schema}.{source_table}")
print(f"Year: {year_int}")
print(f"Delete After Archive: {'Yes' if delete_after_archive else 'No'}")
print("-"*80)

try:
    # Archive the data
    archived_count = archive_table_by_year(
        source_catalog, source_schema, source_table, date_column,
        target_base_path, year_int
    )
    
    if archived_count > 0:
        # Conditionally delete from source table
        if delete_after_archive:
            delete_archived_data(source_catalog, source_schema, source_table, date_column, year_int)
            deleted_from_source = True
            status_message = "SUCCESS - Archived and Deleted"
        else:
            print(f"ℹ Skipping deletion (delete_after_archive=false)")
            status_message = "SUCCESS - Archived Only (Not Deleted)"
        
        # Log the activity
        log_archive_activity(
            source_catalog, source_schema, source_table,
            target_base_path, year_int, archived_count, status_message
        )
        
        print(f"✓ Archive completed successfully!")
    else:
        print(f"⚠ No records found for year {year_int}")
        log_archive_activity(
            source_catalog, source_schema, source_table,
            target_base_path, year_int, 0, "NO_DATA"
        )
    
    print()
    
except Exception as e:
    error_message = str(e)
    archive_status = "FAILED"
    print(f"✗ Error archiving data: {error_message}")
    log_archive_activity(
        source_catalog, source_schema, source_table,
        target_base_path, year_int, 0, f"FAILED: {error_message}"
    )
    print()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archive Summary

# COMMAND ----------

print("="*80)
print("ARCHIVE PROCESS COMPLETED")
print("="*80)
print(f"End Time: {datetime.now()}")
print()
print(f"Source: {source_catalog}.{source_schema}.{source_table}")
print(f"Target: {target_base_path}")
print(f"Year: {year_int}")
print(f"Records Archived: {archived_count:,}")
print(f"Deleted from Source: {'Yes' if deleted_from_source else 'No'}")
print(f"Status: {archive_status}")
print()

if error_message:
    print("Error Details:")
    print(f"  {error_message}")
else:
    if deleted_from_source:
        print("✓ Archive completed successfully! Data archived and deleted from source.")
    else:
        print("✓ Archive completed successfully! Data archived (source table unchanged).")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Log

# COMMAND ----------

# View recent archive activity
audit_table = f"{source_catalog}.{source_schema}.archive_audit_log"
audit_query = f"""
SELECT 
    archive_timestamp,
    source_catalog,
    source_schema,
    source_table,
    target_base_path,
    archive_year,
    record_count,
    status,
    archived_by
FROM {audit_table}
WHERE archive_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY archive_timestamp DESC
"""

display(spark.sql(audit_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Archive Storage

# COMMAND ----------

# List archived files in storage location
print(f"Verifying archive at: {target_base_path}")
print()

try:
    files = dbutils.fs.ls(target_base_path)
    print(f"Archive contents:")
    for file in files:
        print(f"  - {file.name}")
        if file.isDir():
            try:
                sub_files = dbutils.fs.ls(file.path)
                print(f"    Files: {len(sub_files)}")
            except:
                pass
except Exception as e:
    print(f"No archives found or error: {str(e)}")
