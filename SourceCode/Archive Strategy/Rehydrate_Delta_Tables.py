# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Rehydration Process
# MAGIC
# MAGIC This notebook rehydrates archived Delta tables by creating external tables for specific years
# MAGIC and a unified view combining the main table with archived data.
# MAGIC
# MAGIC ## Features:
# MAGIC - Parameterized for ad-hoc execution
# MAGIC - Creates tables for each archived year using no-copy methods
# MAGIC - Creates unified views combining main + archived data
# MAGIC - Rehydrates to separate schema
# MAGIC - Supports multiple year selection
# MAGIC - Supports direct storage paths (S3, ADLS, GCS) and Unity Catalog Volumes
# MAGIC - Archive path format: `{base_path}/year_{year}/` (e.g., s3://bucket/path/year_2020/)
# MAGIC
# MAGIC ## Zero-Copy Table Creation:
# MAGIC The notebook uses **no-copy methods only** to ensure no data duplication:
# MAGIC 1. **External Table** (preferred) - Points to archive location without copying
# MAGIC 2. **SHALLOW CLONE** (fallback) - Shares storage with archive, zero-copy operation
# MAGIC
# MAGIC If both methods fail, the process will error out rather than copy data.
# MAGIC
# MAGIC ## Parameters:
# MAGIC
# MAGIC ### Archive Source Parameters:
# MAGIC - `archive_base_path`: Direct storage path where archives are stored (e.g., s3://bucket/path, abfss://container@account.dfs.core.windows.net/path)
# MAGIC - `source_catalog`: Source catalog name (for main table reference)
# MAGIC - `source_schema`: Source schema name (for main table reference)
# MAGIC - `source_table`: Source table name (for main table reference)
# MAGIC - `years`: Comma-separated list of years (e.g., "2020,2021,2022")
# MAGIC
# MAGIC ### Rehydrated Destination Parameters:
# MAGIC - `rehydrated_catalog`: Catalog where external tables will be created
# MAGIC - `rehydrated_schema`: Schema where external tables will be created
# MAGIC - `rehydrated_table_name`: Target table name for rehydrated data
# MAGIC - `rehydrated_table_prefix`: Prefix for external table names (default: "ext")
# MAGIC - `create_unified_view`: Whether to create a unified view (true/false)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Parameters (Widgets)

# COMMAND ----------

# Create widgets for parameters
# Archive Source Parameters
dbutils.widgets.text("archive_base_path", "s3://one-env-uc-external-location/sm-field-demo/wiki", "01.Archive Base Path")
dbutils.widgets.text("source_catalog", "source_catalog", "02.Source Catalog")
dbutils.widgets.text("source_schema", "on_boarding", "03.Source Schema")
dbutils.widgets.text("source_table", "wikipedia_articles2", "04.Source Table")
dbutils.widgets.text("years", "2013,2014", "05.Years (comma-separated)")

# Rehydrated Destination Parameters
dbutils.widgets.text("rehydrated_catalog", "rehydrated_catalog", "06.Rehydrated Catalog")
dbutils.widgets.text("rehydrated_schema", "hyderated", "07.Rehydrated Schema")
dbutils.widgets.text("rehydrated_table_name", "wiki", "08.Rehydrated Table Name")
dbutils.widgets.text("rehydrated_table_prefix", "ext", "09.Rehydrated Table Prefix")
dbutils.widgets.dropdown("create_unified_view", "true", ["true", "false"], "10.Create Unified View")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

# Retrieve parameter values - Archive Source
archive_base_path = dbutils.widgets.get("archive_base_path").strip()
source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
years_param = dbutils.widgets.get("years")

# Retrieve parameter values - Rehydrated Destination
rehydrated_catalog = dbutils.widgets.get("rehydrated_catalog")
rehydrated_schema = dbutils.widgets.get("rehydrated_schema")
rehydrated_table_name = dbutils.widgets.get("rehydrated_table_name")
rehydrated_table_prefix = dbutils.widgets.get("rehydrated_table_prefix")
create_unified_view_param = dbutils.widgets.get("create_unified_view")

# Parse parameters
years_to_rehydrate = [int(year.strip()) for year in years_param.split(",")]
create_unified_view = create_unified_view_param.lower() == "true"

# Derived parameters
# Remove trailing slash if present to ensure consistent path formatting
archive_base_path = archive_base_path.rstrip('/')
full_rehydrated_schema = f"{rehydrated_catalog}.{rehydrated_schema}"
full_main_table = f"{source_catalog}.{source_schema}.{source_table}"

print("="*80)
print("REHYDRATION PARAMETERS")
print("="*80)
print("\nARCHIVE SOURCE:")
print(f"  Base Path: {archive_base_path}")
print(f"  Archive Format: year_{{year}} (e.g., year_2020, year_2021)")
print(f"  Years: {years_to_rehydrate}")
print("\nSOURCE TABLE (for main table reference):")
print(f"  Catalog: {source_catalog}")
print(f"  Schema: {source_schema}")
print(f"  Table: {source_table}")
print(f"  Full Table Name: {full_main_table}")
print("\nREHYDRATED DESTINATION:")
print(f"  Catalog: {rehydrated_catalog}")
print(f"  Schema: {rehydrated_schema}")
print(f"  Table Name: {rehydrated_table_name}")
print(f"  Table Prefix: {rehydrated_table_prefix}")
print(f"  Full Schema: {full_rehydrated_schema}")
print(f"  Create Unified View: {create_unified_view}")
print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_rehydrated_schema_if_not_exists(rehydrated_catalog, rehydrated_schema):
    """
    Create the rehydrated schema if it doesn't exist.
    """
    full_schema = f"{rehydrated_catalog}.{rehydrated_schema}"
    
    create_schema_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {full_schema}
    COMMENT 'Schema for rehydrated archived tables'
    """
    
    spark.sql(create_schema_sql)
    print(f"✓ Rehydrated schema ready: {full_schema}")

# COMMAND ----------

def verify_archive_exists(archive_base_path, year):
    """
    Verify that the archive exists for the given year.
    Archive path format: {base_path}/year_{year}/
    
    Args:
        archive_base_path: Direct storage path (e.g., s3://bucket/path)
        year: Year to verify
    """
    archive_path = f"{archive_base_path}/year_{year}"
    
    try:
        files = dbutils.fs.ls(archive_path)
        if len(files) > 0:
            print(f"✓ Archive verified for year {year}: {archive_path}")
            return archive_path
        else:
            print(f"✗ No files found in archive for year {year}: {archive_path}")
            return None
    except Exception as e:
        print(f"✗ Archive not found for year {year}: {str(e)}")
        return None

# COMMAND ----------

def create_external_table_for_year(rehydrated_catalog, rehydrated_schema, rehydrated_table_name, 
                                    rehydrated_table_prefix, year, archive_path):
    """
    Create a table pointing to the archived data using zero-copy methods only.
    
    Attempts two no-copy strategies:
    1. External table with LOCATION (preferred) - Points to existing data
    2. SHALLOW CLONE (fallback) - Shares storage with source, zero-copy
    
    Both methods ensure no data duplication.
    """
    external_table_name = f"{rehydrated_table_prefix}_{rehydrated_table_name}_{year}"
    full_external_table = f"{rehydrated_catalog}.{rehydrated_schema}.{external_table_name}"
    
    print(f"Creating table: {full_external_table}")
    print(f"  Archive path: {archive_path}")
    
    # Drop if exists
    spark.sql(f"DROP TABLE IF EXISTS {full_external_table}")
    
    # Strategy 1: Try creating external table with LOCATION (preferred)
    try:
        print(f"  → Attempting external table with LOCATION...")
        create_table_sql = f"""
        CREATE TABLE {full_external_table}
        USING DELTA
        LOCATION '{archive_path}'
        """
        spark.sql(create_table_sql)
        
        # Verify and get count
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_external_table}").collect()[0].cnt
        print(f"  ✓ External table created successfully (zero-copy)")
        print(f"  ✓ Records: {count:,}")
        
        return full_external_table, count
        
    except Exception as e1:
        error_msg = str(e1)
        print(f"  ✗ External table failed: {error_msg[:150]}")
        
        # Strategy 2: Try SHALLOW CLONE (fallback, still zero-copy)
        try:
            print(f"  → Attempting SHALLOW CLONE (zero-copy fallback)...")
            clone_sql = f"""
            CREATE TABLE {full_external_table}
            SHALLOW CLONE delta.`{archive_path}`
            """
            spark.sql(clone_sql)
            
            # Verify and get count
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_external_table}").collect()[0].cnt
            print(f"  ✓ SHALLOW CLONE created successfully (zero-copy)")
            print(f"  ✓ Records: {count:,}")
            
            return full_external_table, count
            
        except Exception as e2:
            error_msg2 = str(e2)
            print(f"  ✗ SHALLOW CLONE failed: {error_msg2[:150]}")
            print(f"\n✗ ERROR: Both zero-copy methods failed for year {year}")
            print(f"  - External table error: {error_msg[:200]}")
            print(f"  - SHALLOW CLONE error: {error_msg2[:200]}")
            raise Exception(f"Failed to create zero-copy table for year {year}. Both external table and SHALLOW CLONE failed.")

# COMMAND ----------

def create_unified_view(source_catalog, source_schema, source_table, rehydrated_catalog, 
                        rehydrated_schema, rehydrated_table_name, external_tables):
    """
    Create a unified view that combines the main table with all archived external tables.
    
    Args:
        source_catalog: Source catalog for main table
        source_schema: Source schema for main table
        source_table: Source table name
        rehydrated_catalog: Catalog for rehydrated tables
        rehydrated_schema: Schema for rehydrated tables
        rehydrated_table_name: Table name for rehydrated data
        external_tables: List of (table_name, year) tuples
    """
    view_name = f"{rehydrated_table_name}_unified"
    full_view_name = f"{rehydrated_catalog}.{rehydrated_schema}.{view_name}"
    
    print(f"\nCreating unified view: {full_view_name}")
    
    # Build UNION ALL query
    main_table = f"{source_catalog}.{source_schema}.{source_table}"
    
    # Start with main table (check if it exists)
    union_parts = []
    try:
        spark.sql(f"DESCRIBE TABLE {main_table}")
        union_parts.append(f"SELECT *, 'main' as data_source FROM {main_table}")
        print(f"  Including main table: {main_table}")
    except Exception as e:
        print(f"  Main table not found, creating view with external tables only: {main_table}")
    
    # Add each external table
    for ext_table, year in external_tables:
        union_parts.append(f"SELECT *, '{year}' as data_source FROM {ext_table}")
    
    if not union_parts:
        print(f"✗ No tables available to create unified view")
        return None
    
    union_query = " UNION ALL ".join(union_parts)
    
    # Create view
    create_view_sql = f"""
    CREATE OR REPLACE VIEW {full_view_name} AS
    {union_query}
    """
    
    spark.sql(create_view_sql)
    
    # Get total count
    total_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_view_name}").collect()[0].cnt
    
    print(f"✓ Unified view created with {total_count:,} total records")
    
    return full_view_name

# COMMAND ----------

def log_rehydration_activity(archive_base_path, source_catalog, source_schema, source_table,
                             rehydrated_catalog, rehydrated_schema, rehydrated_table_name, 
                             years, status, external_tables_created):
    """
    Log rehydration activity to an audit table.
    
    Args:
        archive_base_path: Direct storage path where archives are stored
        source_catalog: Source catalog for main table
        source_schema: Source schema for main table
        source_table: Source table name
        rehydrated_catalog: Catalog where external tables were created
        rehydrated_schema: Schema where external tables were created
        rehydrated_table_name: Table name for rehydrated data
        years: List of years rehydrated
        status: Rehydration status
        external_tables_created: Number of external tables created
    """
    audit_table = f"{rehydrated_catalog}.{rehydrated_schema}.rehydration_audit_log"
    
    # Create audit table if it doesn't exist
    create_audit_table = f"""
    CREATE TABLE IF NOT EXISTS {audit_table} (
        rehydration_timestamp TIMESTAMP,
        archive_base_path STRING,
        source_catalog STRING,
        source_schema STRING,
        source_table STRING,
        rehydrated_catalog STRING,
        rehydrated_schema STRING,
        rehydrated_table_name STRING,
        years_rehydrated STRING,
        external_tables_created INT,
        status STRING,
        rehydrated_by STRING
    )
    USING DELTA
    """
    
    spark.sql(create_audit_table)
    
    # Insert audit record using SQL to avoid schema conflicts
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Convert years list to comma-separated string for easier storage and querying
    years_str = ','.join(str(y) for y in years)
    
    # Escape single quotes in fields to prevent SQL injection
    safe_status = status.replace("'", "''")
    safe_base_path = archive_base_path.replace("'", "''")
    
    insert_query = f"""
    INSERT INTO {audit_table}
    VALUES (
        CAST('{timestamp_str}' AS TIMESTAMP),
        '{safe_base_path}',
        '{source_catalog}',
        '{source_schema}',
        '{source_table}',
        '{rehydrated_catalog}',
        '{rehydrated_schema}',
        '{rehydrated_table_name}',
        '{years_str}',
        {external_tables_created},
        '{safe_status}',
        '{current_user}'
    )
    """
    
    spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cloud Storage Tier Copy Process Placeholder

# COMMAND ----------

def trigger_azure_copy_process(archive_base_path, years):
    """
    Placeholder for cloud storage tier copy process.
    Moves data from cool/archive storage tier to hot storage tier.
    
    This function would:
    1. Trigger cloud provider pipeline (Azure Data Factory, AWS Lambda, etc.)
    2. Copy specific year folders from cool/archive tier to hot tier
    3. Wait for copy completion
    4. Verify data integrity
    
    For now, this is a placeholder that logs the intended action.
    
    Args:
        archive_base_path: Direct storage path (e.g., s3://bucket/path)
        years: List of years to copy
    """
    print("\n" + "="*80)
    print("CLOUD STORAGE TIER COPY PROCESS (PLACEHOLDER)")
    print("="*80)
    print("This process would copy archived data from cool/archive tier to hot tier.")
    print()
    print("Configuration needed:")
    print(f"  - Storage Path: {archive_base_path}")
    print(f"  - Archive Format: year_{{year}}/")
    print(f"  - Years to Copy: {years}")
    print()
    print("Folders to copy:")
    for year in years:
        year_path = f"  - {archive_base_path}/year_{year}"
        print(year_path)
    print()
    print("Implementation options:")
    print("  AWS:")
    print("    - AWS Lambda with S3 APIs")
    print("    - AWS Step Functions")
    print("    - S3 Storage Class transition")
    print("  Azure:")
    print("    - Azure Data Factory with Copy Activity")
    print("    - Azure Functions with Blob Storage APIs")
    print("    - Azure Storage Account lifecycle management")
    print("  GCP:")
    print("    - Cloud Functions with GCS APIs")
    print("    - Cloud Storage transfer service")
    print()
    print("Status: NOT IMPLEMENTED - Placeholder only")
    print("="*80)
    print()
    
    # In production, this would:
    # - Call cloud provider API to trigger copy
    # - Monitor copy progress
    # - Return success/failure status
    
    return "PLACEHOLDER_SUCCESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Rehydration Process

# COMMAND ----------

print("="*80)
print("STARTING REHYDRATION PROCESS")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()

# Track statistics
external_tables_created = []
failed_years = []

try:
    # Step 1: Create rehydrated schema if it doesn't exist
    create_rehydrated_schema_if_not_exists(rehydrated_catalog, rehydrated_schema)
    print()
    
    # Step 2: Placeholder - Trigger cloud storage tier copy process (if needed)
    copy_status = trigger_azure_copy_process(archive_base_path, years_to_rehydrate)
    
    # Step 3: Process each year
    for year in years_to_rehydrate:
        print("-"*80)
        print(f"Processing Year: {year}")
        print("-"*80)
        
        try:
            # Verify archive exists and get path
            archive_path = verify_archive_exists(archive_base_path, year)
            
            if not archive_path:
                failed_years.append(year)
                print(f"⊘ Skipping year {year} - archive not found")
                print()
                continue
            
            # Create external table
            ext_table, record_count = create_external_table_for_year(
                rehydrated_catalog, rehydrated_schema, rehydrated_table_name, 
                rehydrated_table_prefix, year, archive_path
            )
            
            external_tables_created.append((ext_table, year))
            print()
            
        except Exception as e:
            error_msg = f"Error processing year {year}: {str(e)}"
            print(f"✗ {error_msg}")
            failed_years.append(year)
            print()
    
    # Step 4: Create unified view if requested and we have any external tables
    if create_unified_view and external_tables_created:
        unified_view = create_unified_view(
            source_catalog, source_schema, source_table, rehydrated_catalog, 
            rehydrated_schema, rehydrated_table_name, external_tables_created
        )
    elif not create_unified_view:
        print("\n⊘ Unified view creation skipped (create_unified_view=false)")
    
    # Step 5: Log activity
    status = "SUCCESS" if not failed_years else f"PARTIAL_SUCCESS (failed years: {failed_years})"
    log_rehydration_activity(
        archive_base_path, source_catalog, source_schema, source_table,
        rehydrated_catalog, rehydrated_schema, rehydrated_table_name, 
        years_to_rehydrate, status, len(external_tables_created)
    )
    
except Exception as e:
    error_msg = f"Critical error during rehydration: {str(e)}"
    print(f"\n✗ {error_msg}")
    log_rehydration_activity(
        archive_base_path, source_catalog, source_schema, source_table,
        rehydrated_catalog, rehydrated_schema, rehydrated_table_name, 
        years_to_rehydrate, f"FAILED: {str(e)}", len(external_tables_created)
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rehydration Summary

# COMMAND ----------

print("="*80)
print("REHYDRATION PROCESS COMPLETED")
print("="*80)
print(f"End Time: {datetime.now()}")
print()
print(f"Years Requested: {len(years_to_rehydrate)}")
print(f"External Tables Created: {len(external_tables_created)}")
print()

if external_tables_created:
    print("Created Tables:")
    for ext_table, year in external_tables_created:
        print(f"  - {ext_table} (Year: {year})")
    print()
    if create_unified_view:
        print(f"Unified View: {rehydrated_catalog}.{rehydrated_schema}.{rehydrated_table_name}_unified")

if failed_years:
    print()
    print(f"Failed Years: {failed_years}")
else:
    print()
    print("✓ All years rehydrated successfully!")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Rehydrated Data

# COMMAND ----------

# Sample query from unified view
if external_tables_created and create_unified_view:
    sample_query = f"""
    SELECT 
        data_source,
        COUNT(*) as record_count
    FROM {rehydrated_catalog}.{rehydrated_schema}.{rehydrated_table_name}_unified
    GROUP BY data_source
    ORDER BY data_source
    """
    
    print("Record Distribution by Source:")
    display(spark.sql(sample_query))
elif external_tables_created and not create_unified_view:
    print("Unified view was not created. Query individual external tables directly.")
else:
    print("No external tables were created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List All External Tables

# COMMAND ----------

# List all external tables in the rehydrated schema
list_tables_query = f"""
SHOW TABLES IN {rehydrated_catalog}.{rehydrated_schema}
"""

print(f"Tables in {rehydrated_catalog}.{rehydrated_schema}:")
display(spark.sql(list_tables_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Commands (Optional)
# MAGIC
# MAGIC Run these commands if you need to remove rehydrated tables/views.
# MAGIC
# MAGIC **Note:** All tables are created using zero-copy methods (External Table or SHALLOW CLONE).
# MAGIC Dropping these tables only removes metadata - your archive data remains completely intact.

# COMMAND ----------

# Uncomment to drop external tables and views
# for ext_table, year in external_tables_created:
#     spark.sql(f"DROP TABLE IF EXISTS {ext_table}")
#     print(f"Dropped: {ext_table}")

# if create_unified_view:
#     unified_view = f"{rehydrated_catalog}.{rehydrated_schema}.{rehydrated_table_name}_unified"
#     spark.sql(f"DROP VIEW IF EXISTS {unified_view}")
#     print(f"Dropped: {unified_view}")
