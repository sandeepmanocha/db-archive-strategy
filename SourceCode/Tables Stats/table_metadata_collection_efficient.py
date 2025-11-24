# Databricks notebook source
# MAGIC %md
# MAGIC # Table Metadata Collection for Storage Optimization 
# MAGIC
# MAGIC ## Why ThreadPoolExecutor?
# MAGIC **Reality check**: For operations like `DESCRIBE DETAIL` and `DESCRIBE HISTORY`:
# MAGIC - ❌ **UDFs don't work** - Can't serialize SparkSession to executors
# MAGIC - ❌ **mapInPandas doesn't work** - Same serialization issue
# MAGIC - ✅ **ThreadPoolExecutor works** - Runs on driver with shared SparkSession
# MAGIC
# MAGIC ## Parameters:
# MAGIC - **source_catalog**: Catalog to scan for tables
# MAGIC - **source_schema**: Schema to scan (use 'ALL' to scan all schemas)
# MAGIC - **governance_catalog**: Catalog where metadata will be stored
# MAGIC - **governance_schema**: Schema where metadata will be stored
# MAGIC - **target_table**: Table name for storing metadata

# COMMAND ----------

# DBTITLE 1,Setup Parameters
# Create Widget Parameters
dbutils.widgets.text("source_catalog", "", "01. Source Catalog")
dbutils.widgets.text("source_schema", "ALL", "02. Source Schema (ALL for all schemas)")
dbutils.widgets.text("governance_catalog", "governance", "03. Governance Catalog")
dbutils.widgets.text("governance_schema", "metadata", "04. Governance Schema")
dbutils.widgets.text("target_table", "table_inventory", "05. Target Table Name")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
# Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, datediff, current_date,
    count, sum as spark_sum, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    IntegerType, TimestampType
)

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import traceback

# COMMAND ----------

# DBTITLE 1,Read Parameters
# Read Parameters and Display
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
governance_catalog = dbutils.widgets.get("governance_catalog")
governance_schema = dbutils.widgets.get("governance_schema")
target_table = dbutils.widgets.get("target_table")

# Validate required parameters
if not source_catalog:
    raise ValueError("source_catalog parameter is required")

# Display parameters
print("=" * 80)
print("Table Metadata Collection - Efficient Approach")
print("=" * 80)
print("Parameters:")
print(f"  Source Catalog: {source_catalog}")
print(f"  Source Schema: {source_schema}")
print(f"  Governance Catalog: {governance_catalog}")
print(f"  Governance Schema: {governance_schema}")
print(f"  Target Table: {target_table}")
print(f"  Target Full Path: {governance_catalog}.{governance_schema}.{target_table}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Bulk Discovery Using Information Schema
# Bulk Discovery Using Information Schema
print("\n" + "=" * 80)
print("BULK DISCOVERY")
print("=" * 80)
print("\n📊 Querying information_schema for table list...")

# Build the schema filter
if source_schema.upper() == "ALL":
    schema_filter = f"table_catalog = '{source_catalog}'"
    print(f"Scanning ALL schemas in catalog '{source_catalog}'")
else:
    schema_filter = f"table_catalog = '{source_catalog}' AND table_schema = '{source_schema}'"
    print(f"Scanning schema '{source_catalog}.{source_schema}'")

# Query information_schema
tables_query = f"""
    SELECT 
        table_catalog as catalog,
        table_schema as schema,
        table_name,
        CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
        table_type
    FROM {source_catalog}.information_schema.tables
    WHERE {schema_filter}
        AND table_type IN ('MANAGED', 'EXTERNAL')
    ORDER BY table_schema, table_name
"""

tables_df = spark.sql(tables_query)
tables_list = tables_df.collect()
table_count = len(tables_list)

print(f"\n✓ Discovered {table_count} table(s) in {len(set([t.schema for t in tables_list]))} schema(s)")

# Show schema breakdown
schema_counts = {}
for t in tables_list:
    schema_counts[t.schema] = schema_counts.get(t.schema, 0) + 1

print("\nTables by schema:")
for schema_name, cnt in sorted(schema_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"  - {schema_name}: {cnt} table(s)")

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Detailed Collection
# Detailed Collection
print("\n" + "=" * 80)
print("DETAILED COLLECTION")
print("=" * 80)

import multiprocessing

# Determine optimal number of workers
cpu_count = multiprocessing.cpu_count()
max_workers = min(cpu_count * 4, 64)  # 4x CPU cores for I/O-bound operations

print(f"\n🚀 Using ThreadPoolExecutor with {max_workers} workers")
print(f"   CPU cores: {cpu_count}")
print(f"   Parallelism: {max_workers} concurrent operations")
print(f"   Execution: All on driver with shared SparkSession")
print()

def collect_table_metrics(table_row):
    """
    Collect detailed metrics for a single table.
    Runs in parallel threads on the driver node.
    """
    catalog = table_row.catalog
    schema_name = table_row.schema
    table_name = table_row.table_name
    full_table_name = table_row.full_table_name
    
    metrics = {
        'catalog': catalog,
        'schema': schema_name,
        'table_name': table_name,
        'size_bytes': None,
        'row_count': None,
        'table_format': None,
        'num_files': None,
        'avg_file_size_bytes': None,
        'created_at': None,
        'partition_columns': None,
        'clustering_columns': None,
        'zorder_columns': None,
        'last_optimize_ts': None,
        'last_vacuum_ts': None
    }
    
    try:
        # Get DESCRIBE DETAIL
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
        detail_row = detail_df.first()
        
        if detail_row:
            metrics['size_bytes'] = detail_row.sizeInBytes if hasattr(detail_row, 'sizeInBytes') else None
            metrics['table_format'] = detail_row.format if hasattr(detail_row, 'format') else None
            metrics['num_files'] = detail_row.numFiles if hasattr(detail_row, 'numFiles') else None
            metrics['created_at'] = detail_row.createdAt if hasattr(detail_row, 'createdAt') else None
            
            # Calculate avg file size
            if metrics['num_files'] and metrics['num_files'] > 0 and metrics['size_bytes']:
                metrics['avg_file_size_bytes'] = metrics['size_bytes'] // metrics['num_files']
            
            # Get partition columns
            if hasattr(detail_row, 'partitionColumns') and detail_row.partitionColumns:
                metrics['partition_columns'] = ','.join(detail_row.partitionColumns)
            
            # Get clustering columns
            if hasattr(detail_row, 'clusteringColumns') and detail_row.clusteringColumns:
                metrics['clustering_columns'] = ','.join(detail_row.clusteringColumns)
        
        # Get row count (only for Delta tables to avoid long scans)
        if metrics['table_format'] and metrics['table_format'].upper() == 'DELTA':
            try:
                count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
                metrics['row_count'] = count_df.first().cnt
            except:
                pass
        
        # Get history for Delta tables
        if metrics['table_format'] and metrics['table_format'].upper() == 'DELTA':
            try:
                history_df = spark.sql(f"DESCRIBE HISTORY {full_table_name}")
                
                # Get last OPTIMIZE
                optimize_df = history_df.filter(col("operation") == "OPTIMIZE")
                if optimize_df.count() > 0:
                    optimize_row = optimize_df.orderBy(col("timestamp").desc()).first()
                    metrics['last_optimize_ts'] = optimize_row.timestamp
                    
                    # Check for Z-ORDER
                    if hasattr(optimize_row, 'operationParameters') and optimize_row.operationParameters:
                        params = optimize_row.operationParameters
                        if 'zOrderBy' in params and params['zOrderBy']:
                            metrics['zorder_columns'] = params['zOrderBy'].strip('[]').replace('"', '').replace("'", "")
                
                # Get last VACUUM
                vacuum_df = history_df.filter(col("operation") == "VACUUM END")
                if vacuum_df.count() > 0:
                    vacuum_row = vacuum_df.orderBy(col("timestamp").desc()).first()
                    metrics['last_vacuum_ts'] = vacuum_row.timestamp
            except:
                pass
                
    except Exception as e:
        # Silently handle errors - metrics remain None
        pass
    
    return metrics

# Parallel collection with progress tracking
all_metrics = []
completed_count = 0
error_count = 0
start_time = time.time()

print(f"Collecting detailed metrics for {table_count} tables...\n")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit all tasks
    future_to_table = {
        executor.submit(collect_table_metrics, table_row): table_row 
        for table_row in tables_list
    }
    
    # Process completed tasks
    for future in as_completed(future_to_table):
        table_row = future_to_table[future]
        completed_count += 1
        
        try:
            metrics = future.result()
            all_metrics.append(metrics)
            
            # Progress updates
            if completed_count % 10 == 0 or completed_count in [1, 5, table_count]:
                elapsed = time.time() - start_time
                rate = completed_count / elapsed if elapsed > 0 else 0
                remaining = (table_count - completed_count) / rate if rate > 0 else 0
                
                print(f"Progress: {completed_count}/{table_count} " +
                      f"({completed_count/table_count*100:.1f}%) | " +
                      f"Rate: {rate:.1f} tables/sec | " +
                      f"ETA: {int(remaining)}s")
                
        except Exception as e:
            error_count += 1

elapsed_time = time.time() - start_time

print(f"\n✅ Collection complete!")
print(f"   Time: {elapsed_time:.1f} seconds")
print(f"   Successfully processed: {len(all_metrics)} tables")
print(f"   Average rate: {len(all_metrics)/elapsed_time:.1f} tables/second")
if error_count > 0:
    print(f"   Errors: {error_count}")

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Calculate Derived Metrics
# Calculate Derived Metrics
print("\n" + "=" * 80)
print("DERIVED METRICS")
print("=" * 80)
print("\n📈 Converting to Spark DataFrame and calculating derived metrics...")

# Define schema
metrics_schema = StructType([
    StructField("catalog", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("size_bytes", LongType(), True),
    StructField("row_count", LongType(), True),
    StructField("table_format", StringType(), True),
    StructField("num_files", LongType(), True),
    StructField("avg_file_size_bytes", LongType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("partition_columns", StringType(), True),
    StructField("clustering_columns", StringType(), True),
    StructField("zorder_columns", StringType(), True),
    StructField("last_optimize_ts", TimestampType(), True),
    StructField("last_vacuum_ts", TimestampType(), True)
])

# Create DataFrame
enriched_df = spark.createDataFrame(all_metrics, schema=metrics_schema)

# Calculate bytes per row
metrics_df = enriched_df.withColumn(
    "bytes_per_row",
    when(
        (col("row_count").isNotNull()) & (col("row_count") > 0) & (col("size_bytes").isNotNull()),
        (col("size_bytes") / col("row_count")).cast(LongType())
    ).otherwise(None)
)

# Calculate days since optimize
metrics_df = metrics_df.withColumn(
    "days_since_optimize",
    when(
        col("last_optimize_ts").isNotNull(),
        datediff(current_date(), col("last_optimize_ts"))
    ).otherwise(None).cast(IntegerType())
)

# Calculate days since vacuum
metrics_df = metrics_df.withColumn(
    "days_since_vacuum",
    when(
        col("last_vacuum_ts").isNotNull(),
        datediff(current_date(), col("last_vacuum_ts"))
    ).otherwise(None).cast(IntegerType())
)

# Calculate optimization priority score
metrics_df = metrics_df.withColumn(
    "priority_score",
    when(lower(col("table_format")) == "delta", lit(0)).otherwise(None)
)

# Add points for file issues
metrics_df = metrics_df.withColumn(
    "priority_score",
    when(
        col("priority_score").isNotNull(),
        col("priority_score") + 
        when(col("num_files") > 1000, 100)
        .when(col("num_files") > 500, 50)
        .otherwise(0) +
        when(col("avg_file_size_bytes") < 128 * 1024 * 1024, 80).otherwise(0)
    ).otherwise(col("priority_score"))
)

# Add points for optimization staleness
metrics_df = metrics_df.withColumn(
    "priority_score",
    when(
        col("priority_score").isNotNull(),
        col("priority_score") + 
        when(col("last_optimize_ts").isNull(), 60)
        .when(col("days_since_optimize") > 60, 40)
        .when(col("days_since_optimize") > 30, 20)
        .otherwise(0)
    ).otherwise(col("priority_score"))
)

# Add points for large unorganized tables
metrics_df = metrics_df.withColumn(
    "priority_score",
    when(
        col("priority_score").isNotNull(),
        col("priority_score") + 
        when(
            (col("partition_columns").isNull()) & 
            (col("clustering_columns").isNull()) & 
            (col("zorder_columns").isNull()) & 
            (col("size_bytes") > 10 * 1024 * 1024 * 1024),
            30
        ).otherwise(0)
    ).otherwise(col("priority_score"))
)

# Assign priority labels
metrics_df = metrics_df.withColumn(
    "optimization_priority",
    when(col("priority_score").isNull(), "N/A")
    .when(col("priority_score") >= 100, "CRITICAL")
    .when(col("priority_score") >= 50, "HIGH")
    .when(col("priority_score") >= 20, "MEDIUM")
    .otherwise("LOW")
)

# Drop temporary priority_score
metrics_df = metrics_df.drop("priority_score")

# Add collection timestamp
metrics_df = metrics_df.withColumn("collection_timestamp", current_timestamp())

# Reorder columns
metrics_df = metrics_df.select(
    "catalog", "schema", "table_name",
    "size_bytes", "row_count", "table_format",
    "num_files", "avg_file_size_bytes", "created_at",
    "partition_columns", "clustering_columns", "zorder_columns",
    "last_optimize_ts", "last_vacuum_ts",
    "bytes_per_row", "days_since_optimize", "days_since_vacuum",
    "optimization_priority", "collection_timestamp"
)

print(f"✓ DataFrame created with {metrics_df.count()} records")
print(f"✓ Derived metrics calculated")

# Display sample
print("\nSample data:")
display(metrics_df.limit(10))

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Write to Governance Table
# Write to Governance Table
print("\n" + "=" * 80)
print("WRITE TO GOVERNANCE TABLE")
print("=" * 80)

target_full_path = f"{governance_catalog}.{governance_schema}.{target_table}"

print(f"\nWriting data to {target_full_path}...")

try:
    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {governance_catalog}.{governance_schema}")
    print(f"✓ Schema '{governance_catalog}.{governance_schema}' ready")
    
    # Write data (append mode for historical tracking)
    metrics_df.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable(target_full_path)
    
    record_count = metrics_df.count()
    print(f"✓ Data written successfully")
    print(f"  Records written: {record_count}")
    
except Exception as e:
    print(f"✗ Error writing data: {str(e)}")
    traceback.print_exc()
    raise

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Display Summary Statistics
# Display Summary Statistics
print("\n" + "=" * 80)
print("COLLECTION SUMMARY")
print("=" * 80)

# Calculate statistics
summary_stats = metrics_df.agg(
    count("table_name").alias("total_tables"),
    spark_sum("size_bytes").alias("total_size"),
    spark_sum("row_count").alias("total_rows")
).first()

total_tables = summary_stats.total_tables
total_size = summary_stats.total_size or 0
total_rows = summary_stats.total_rows or 0

# Convert size to human-readable format
def format_bytes(bytes_val):
    if bytes_val is None or bytes_val == 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while bytes_val >= 1024 and i < len(units) - 1:
        bytes_val /= 1024.0
        i += 1
    return f"{bytes_val:.2f} {units[i]}"

print(f"\nSource Catalog: {source_catalog}")
print(f"Source Schema: {source_schema}")
print(f"")
print(f"Total Tables Scanned: {total_tables:,}")
print(f"Total Size: {format_bytes(total_size)} ({total_size:,} bytes)")
print(f"Total Rows: {total_rows:,}")
print(f"")
print(f"Results stored in: {target_full_path}")
print(f"Collection timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# Show breakdown by schema
print("\nBreakdown by Schema:")
schema_summary = metrics_df.groupBy("schema") \
    .agg(
        count("table_name").alias("table_count"),
        spark_sum("size_bytes").alias("total_size_bytes"),
        spark_sum("row_count").alias("total_rows")
    ) \
    .orderBy(col("total_size_bytes").desc())

display(schema_summary)

# Show optimization priorities
print("\n" + "=" * 80)
print("STORAGE OPTIMIZATION PRIORITIES")
print("=" * 80)

priority_summary = metrics_df.groupBy("optimization_priority") \
    .agg(
        count("table_name").alias("table_count"),
        spark_sum("size_bytes").alias("total_size_bytes")
    ) \
    .orderBy(
        col("optimization_priority").isin(["CRITICAL", "HIGH", "MEDIUM", "LOW"]).desc(),
        col("total_size_bytes").desc()
    )

print("\nOptimization Priority Distribution:")
display(priority_summary)

# Show tables needing immediate attention
critical_and_high = metrics_df.filter(col("optimization_priority").isin(["CRITICAL", "HIGH"])) \
    .orderBy(col("optimization_priority"), col("size_bytes").desc()) \
    .select(
        "schema", "table_name", "optimization_priority", "num_files",
        (col("avg_file_size_bytes") / (1024*1024)).alias("avg_file_size_mb"),
        (col("size_bytes") / (1024**3)).alias("size_gb"),
        "days_since_optimize"
    ) \
    .limit(20)

if critical_and_high.count() > 0:
    print("\n⚠️  CRITICAL & HIGH PRIORITY TABLES (Top 20):")
    display(critical_and_high)
else:
    print("\n✅ No critical or high priority tables found!")

print("\n" + "=" * 80)
print("✅ NOTEBOOK EXECUTION COMPLETE")
print("=" * 80)

