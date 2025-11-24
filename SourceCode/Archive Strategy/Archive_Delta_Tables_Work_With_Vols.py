# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Archive Process (Parameter-Driven)
# MAGIC
# MAGIC <div style="
# MAGIC     background-color: #ffcccc; /* Lighter red background */
# MAGIC     border: 3px solid #e60000; /* Stronger red border */
# MAGIC     padding: 15px; 
# MAGIC     margin: 20px 0; 
# MAGIC     border-radius: 5px; 
# MAGIC     color: #333333; /* Keep text dark for readability */
# MAGIC     font-family: Arial, sans-serif;
# MAGIC ">
# MAGIC     <h2 style="color: #e60000; margin-top: 0; font-size: 1.5em;">&#9888; IMPORTANT LIMITATION: DO NOT USE THIS NOTEBOOK &#9888;</h2>
# MAGIC     <p style="font-size: 1.1em; line-height: 1.4;">
# MAGIC         <strong style="color: #b30000;">WE CANNOT USE VOLUMES FOR EXPORT</strong>, because we cannot create External Tables pointing to Volumes.
# MAGIC     </p>
# MAGIC     <p style="font-size: 1.1em; line-height: 1.4;">
# MAGIC         This notebook is left as a reminder of this limitation. 
# MAGIC         <strong style="color: #e60000;">PLEASE USE ANOTHER NOTEBOOK </strong>
# MAGIC     </p>
# MAGIC </div>
# MAGIC
# MAGIC This notebook archives Delta tables by year using input parameters.
# MAGIC
# MAGIC ## Features:
# MAGIC - Fully parameter-driven (no config files needed)
# MAGIC - Archives from source catalog/schema/table to target catalog/schema/volume/folder
# MAGIC - **Self-contained year folders**: Each year is an independent Delta table with its own _delta_log
# MAGIC - **Easy restoration**: Copy individual year folders without dependencies
# MAGIC - Optional deletion from source table after archiving
# MAGIC - Supports date filtering columns
# MAGIC - Writes to Unity Catalog Volumes
# MAGIC - Can be scheduled in Databricks Workflows
# MAGIC - Comprehensive audit logging
# MAGIC
# MAGIC ## Required Parameters:
# MAGIC - **Source**: catalog, schema, table, date_column
# MAGIC - **Target**: catalog, schema, volume, folder
# MAGIC - **Filter**: year (specific year to archive)
# MAGIC - **Options**: delete_after_archive (true/false) - whether to delete from source after archiving
# MAGIC
# MAGIC ## Archive Structure:
# MAGIC ```
# MAGIC /Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_folder}/
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
dbutils.widgets.text("target_catalog", "target_catalog", "05. Target Catalog")
dbutils.widgets.text("target_schema", "archive", "06. Target Schema")
dbutils.widgets.text("target_volume", "archive_vol", "07. Target Volume")
dbutils.widgets.text("target_folder", "wiki", "08. Target Folder")
dbutils.widgets.text("year", "2013", "09. Year")
dbutils.widgets.dropdown("delete_after_archive", "false", ["true", "false"], "10. Delete After Archive")

# Get widget values
source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
date_column = dbutils.widgets.get("date_column").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_volume = dbutils.widgets.get("target_volume").strip()
target_folder = dbutils.widgets.get("target_folder").strip()
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
    "Target Catalog": target_catalog,
    "Target Schema": target_schema,
    "Target Volume": target_volume,
    "Target Folder": target_folder,
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
print(f"Catalog: {target_catalog}")
print(f"Schema: {target_schema}")
print(f"Volume: {target_volume}")
print(f"Folder: {target_folder}")
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
                          target_catalog, target_schema, target_volume, target_folder, year):
    """
    Archive a specific year of data from source table to target volume.
    Each year is stored as a self-contained, independent Delta table.
    This allows easy copying/restoration of individual years without dependencies.
    """
    # Construct archive path - each year is a separate, independent Delta table
    archive_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_folder}/year_{year}"
    
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
                         target_catalog, target_schema, target_volume, target_folder,
                         year, record_count, status):
    """
    Log archive activity to an audit table in the target catalog/schema.
    Create the audit table if it doesn't exist.
    """
    audit_table = f"{target_catalog}.{target_schema}.archive_audit_log"
    
    # Create audit table if it doesn't exist
    create_audit_table = f"""
    CREATE TABLE IF NOT EXISTS {audit_table} (
        archive_timestamp TIMESTAMP,
        source_catalog STRING,
        source_schema STRING,
        source_table STRING,
        target_catalog STRING,
        target_schema STRING,
        target_volume STRING,
        target_folder STRING,
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
    
    # Escape single quotes in status field to prevent SQL injection
    safe_status = status.replace("'", "''")
    
    insert_query = f"""
    INSERT INTO {audit_table}
    VALUES (
        CAST('{timestamp_str}' AS TIMESTAMP),
        '{source_catalog}',
        '{source_schema}',
        '{source_table}',
        '{target_catalog}',
        '{target_schema}',
        '{target_volume}',
        '{target_folder}',
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

print("-"*80)
print(f"Processing: {source_catalog}.{source_schema}.{source_table}")
print(f"Year: {year_int}")
print(f"Delete After Archive: {'Yes' if delete_after_archive else 'No'}")
print("-"*80)

try:
    # Archive the data
    archived_count = archive_table_by_year(
        source_catalog, source_schema, source_table, date_column,
        target_catalog, target_schema, target_volume, target_folder,
        year_int
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
            target_catalog, target_schema, target_volume, target_folder,
            year_int, archived_count, status_message
        )
        
        print(f"✓ Archive completed successfully!")
    else:
        print(f"⚠ No records found for year {year_int}")
        log_archive_activity(
            source_catalog, source_schema, source_table,
            target_catalog, target_schema, target_volume, target_folder,
            year_int, 0, "NO_DATA"
        )
    
    print(archive_status)
    
except Exception as e:
    error_message = str(e)
    archive_status = "FAILED"
    print(f"✗ Error archiving data: {error_message}")
    log_archive_activity(
        source_catalog, source_schema, source_table,
        target_catalog, target_schema, target_volume, target_folder,
        year_int, 0, f"FAILED: {error_message}"
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
print(f"Target: {target_catalog}.{target_schema}.{target_volume}/{target_folder}")
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
# MAGIC ## Verify Archive Volumes

# COMMAND ----------

# List archived files in volume
archive_base_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_folder}"

print(f"Verifying archive at: {archive_base_path}")
print()

try:
    files = dbutils.fs.ls(archive_base_path)
    print(f"Archive contents for {target_folder}:")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore/Rehydrate Archived Data
# MAGIC
# MAGIC ### Example: Restore Specific Year
# MAGIC Because each year is a self-contained Delta table, restoration is straightforward.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Copy Entire Year Folder
# MAGIC ```python
# MAGIC # Copy entire self-contained year folder to another location
# MAGIC source_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_folder}/year_{year_int}"
# MAGIC dest_path = f"/Volumes/restore_catalog/restore_schema/restore_volume/year_{year_int}"
# MAGIC
# MAGIC dbutils.fs.cp(source_path, dest_path, recurse=True)
# MAGIC print(f"✓ Copied year {year_int} archive to {dest_path}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Query Archived Data Directly
# MAGIC ```python
# MAGIC # Query archived data without restoring
# MAGIC archive_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_folder}/year_{year_int}"
# MAGIC
# MAGIC df = spark.read.format("delta").load(archive_path)
# MAGIC df.createOrReplaceTempView("archived_data")
# MAGIC
# MAGIC # Run queries on archived data
# MAGIC spark.sql("SELECT * FROM archived_data WHERE condition = 'value' LIMIT 10").display()
# MAGIC ```
