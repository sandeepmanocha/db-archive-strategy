# Databricks notebook source
# MAGIC %md
# MAGIC # Table Metadata Analysis
# MAGIC
# MAGIC This notebook provides **comprehensive SQL-based analysis** of table metadata collected by the metadata collection notebook.
# MAGIC
# MAGIC ## 📊 Analysis Categories:
# MAGIC
# MAGIC 1. **Executive Summary** - High-level metrics and KPIs
# MAGIC 2. **Storage Overview** - Size distribution and growth trends
# MAGIC 3. **Optimization Priorities** - Tables requiring immediate attention
# MAGIC 4. **Small Files Problem** - File count and size analysis
# MAGIC 5. **Never Optimized Tables** - Tables missing OPTIMIZE operations
# MAGIC 6. **Stale Optimizations** - Tables with old optimization timestamps
# MAGIC 7. **Data Organization** - Partitioning and clustering analysis
# MAGIC 8. **Cost Savings Opportunities** - Potential storage reduction
# MAGIC 9. **Growth Tracking** - Historical trends (if multiple collections)
# MAGIC
# MAGIC ## Parameters:
# MAGIC - **governance_catalog**: Catalog where metadata is stored
# MAGIC - **governance_schema**: Schema where metadata is stored
# MAGIC - **metadata_table**: Table containing the collected metadata

# COMMAND ----------

# DBTITLE 1,Setup Parameters
# Setup Parameters
dbutils.widgets.text("governance_catalog", "governance", "01. Governance Catalog")
dbutils.widgets.text("governance_schema", "metadata", "02. Governance Schema")
dbutils.widgets.text("metadata_table", "table_inventory", "03. Metadata Table Name")

# Get parameters
governance_catalog = dbutils.widgets.get("governance_catalog")
governance_schema = dbutils.widgets.get("governance_schema")
metadata_table = dbutils.widgets.get("metadata_table")

# Construct full path
metadata_full_path = f"{governance_catalog}.{governance_schema}.{metadata_table}"

print("=" * 80)
print("Table Metadata Analysis Dashboard")
print("=" * 80)
print(f"Analyzing metadata from: {metadata_full_path}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Executive Summary
# MAGIC
# MAGIC High-level overview of your data estate

# COMMAND ----------

# DBTITLE 1,Switch Catalog & Schema to Governance
# MAGIC %sql
# MAGIC use catalog identifier(:governance_catalog);
# MAGIC use schema identifier(:governance_schema);

# COMMAND ----------

# DBTITLE 1,Overall Statistics
# MAGIC %sql
# MAGIC -- 1.1 Overall Statistics
# MAGIC SELECT 
# MAGIC   '📊 Overall Statistics' as section,
# MAGIC   COUNT(DISTINCT catalog) as total_catalogs,
# MAGIC   COUNT(DISTINCT schema) as total_schemas,
# MAGIC   COUNT(*) as total_tables,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 4), 2) as total_size_tb,
# MAGIC   SUM(row_count) as total_rows,
# MAGIC   COUNT(DISTINCT table_format) as table_formats,
# MAGIC   MAX(collection_timestamp) as last_collection
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC AND table_name not like '__materialization_mat_%'

# COMMAND ----------

# DBTITLE 1,Priority Distribution
# MAGIC %sql
# MAGIC -- 1.2 Priority Distribution
# MAGIC SELECT 
# MAGIC   '⚠️ Optimization Priorities' as section,
# MAGIC   optimization_priority,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(size_bytes) / POWER(1024, 3), 2) as avg_size_gb,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct_of_tables
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY optimization_priority
# MAGIC ORDER BY 
# MAGIC   CASE optimization_priority 
# MAGIC     WHEN 'CRITICAL' THEN 1 
# MAGIC     WHEN 'HIGH' THEN 2 
# MAGIC     WHEN 'MEDIUM' THEN 3 
# MAGIC     WHEN 'LOW' THEN 4 
# MAGIC     ELSE 5 
# MAGIC   END

# COMMAND ----------

# DBTITLE 1,Delta Lake Adoption
# MAGIC %sql
# MAGIC -- 1.3 Delta Lake Adoption
# MAGIC SELECT 
# MAGIC   '📦 Table Formats' as section,
# MAGIC   table_format,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct_of_tables,
# MAGIC   ROUND(100.0 * SUM(size_bytes) / SUM(SUM(size_bytes)) OVER (), 1) as pct_of_storage
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY table_format
# MAGIC ORDER BY total_size_gb DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Storage Overview
# MAGIC
# MAGIC Detailed breakdown of storage usage

# COMMAND ----------

# DBTITLE 1,Top 20 Largest Tables
# MAGIC %sql
# MAGIC -- 2.1 Top 20 Largest Tables
# MAGIC SELECT 
# MAGIC   '🔝 Largest Tables' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   table_format,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   row_count,
# MAGIC   num_files,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   optimization_priority,
# MAGIC   CONCAT(catalog, '.', schema, '.', table_name) as full_table_name
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND size_bytes IS NOT NULL
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC ORDER BY size_bytes DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Storage by Schema
# MAGIC %sql
# MAGIC -- 2.2 Storage by Schema
# MAGIC SELECT 
# MAGIC   '📁 Storage by Schema' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(size_bytes) / POWER(1024, 2), 2) as avg_size_mb,
# MAGIC   SUM(row_count) as total_rows,
# MAGIC   SUM(num_files) as total_files
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC GROUP BY catalog, schema
# MAGIC ORDER BY total_size_gb DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Size Distribution
# MAGIC %sql
# MAGIC -- 2.3 Size Distribution
# MAGIC SELECT 
# MAGIC   '📊 Size Distribution' as section,
# MAGIC   CASE 
# MAGIC     WHEN size_bytes < 1024 * 1024 THEN '< 1 MB'
# MAGIC     WHEN size_bytes < 10 * 1024 * 1024 THEN '1-10 MB'
# MAGIC     WHEN size_bytes < 100 * 1024 * 1024 THEN '10-100 MB'
# MAGIC     WHEN size_bytes < POWER(1024, 3) THEN '100 MB - 1 GB'
# MAGIC     WHEN size_bytes < 10 * POWER(1024, 3) THEN '1-10 GB'
# MAGIC     WHEN size_bytes < 100 * POWER(1024, 3) THEN '10-100 GB'
# MAGIC     WHEN size_bytes < POWER(1024, 4) THEN '100 GB - 1 TB'
# MAGIC     ELSE '> 1 TB'
# MAGIC   END as size_range,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct_of_tables
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND size_bytes IS NOT NULL
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN size_bytes < 1024 * 1024 THEN '< 1 MB'
# MAGIC     WHEN size_bytes < 10 * 1024 * 1024 THEN '1-10 MB'
# MAGIC     WHEN size_bytes < 100 * 1024 * 1024 THEN '10-100 MB'
# MAGIC     WHEN size_bytes < POWER(1024, 3) THEN '100 MB - 1 GB'
# MAGIC     WHEN size_bytes < 10 * POWER(1024, 3) THEN '1-10 GB'
# MAGIC     WHEN size_bytes < 100 * POWER(1024, 3) THEN '10-100 GB'
# MAGIC     WHEN size_bytes < POWER(1024, 4) THEN '100 GB - 1 TB'
# MAGIC     ELSE '> 1 TB'
# MAGIC   END
# MAGIC ORDER BY 
# MAGIC   CASE size_range
# MAGIC     WHEN '< 1 MB' THEN 1
# MAGIC     WHEN '1-10 MB' THEN 2
# MAGIC     WHEN '10-100 MB' THEN 3
# MAGIC     WHEN '100 MB - 1 GB' THEN 4
# MAGIC     WHEN '1-10 GB' THEN 5
# MAGIC     WHEN '10-100 GB' THEN 6
# MAGIC     WHEN '100 GB - 1 TB' THEN 7
# MAGIC     ELSE 8
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Optimization Priorities
# MAGIC
# MAGIC Tables requiring immediate attention

# COMMAND ----------

# DBTITLE 1,CRITICAL Priority Tables
# MAGIC %sql
# MAGIC -- 3.1 CRITICAL Priority Tables
# MAGIC SELECT 
# MAGIC   '🚨 CRITICAL Priority' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   num_files,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   days_since_optimize,
# MAGIC   CASE 
# MAGIC     WHEN num_files > 1000 THEN '⚠️ Too many files'
# MAGIC     WHEN num_files > 10 and avg_file_size_bytes < 128 * 1024 * 1024 THEN '⚠️ Small files'
# MAGIC     WHEN last_optimize_ts IS NULL THEN '⚠️ Never optimized'
# MAGIC     ELSE '⚠️ Multiple issues'
# MAGIC   END as primary_issue,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as suggested_action
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND optimization_priority = 'CRITICAL'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC ORDER BY size_bytes DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,HIGH Priority Tables
# MAGIC %sql
# MAGIC -- 3.2 HIGH Priority Tables
# MAGIC SELECT 
# MAGIC   '⚠️ HIGH Priority' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   num_files,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   days_since_optimize,
# MAGIC   CASE 
# MAGIC     WHEN num_files > 500 THEN 'Too many files'
# MAGIC     WHEN num_files > 10 and avg_file_size_bytes < 128 * 1024 * 1024 THEN 'Small files'
# MAGIC     WHEN days_since_optimize > 60 THEN 'Stale optimization'
# MAGIC     ELSE 'Multiple issues'
# MAGIC   END as primary_issue,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as suggested_action
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND optimization_priority = 'HIGH'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC ORDER BY size_bytes DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Small Files Problem
# MAGIC
# MAGIC Identifying and quantifying the small files issue

# COMMAND ----------

# DBTITLE 1,File Count Distribution
# MAGIC %sql
# MAGIC -- 4.1 File Count Distribution
# MAGIC SELECT 
# MAGIC   '📂 File Count Distribution' as section,
# MAGIC   CASE 
# MAGIC     WHEN num_files < 10 THEN '1-9 files'
# MAGIC     WHEN num_files < 50 THEN '10-49 files'
# MAGIC     WHEN num_files < 100 THEN '50-99 files'
# MAGIC     WHEN num_files < 500 THEN '100-499 files'
# MAGIC     WHEN num_files < 1000 THEN '500-999 files'
# MAGIC     ELSE '1000+ files (CRITICAL)'
# MAGIC   END as file_range,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(num_files), 0) as avg_files,
# MAGIC   ROUND(AVG(avg_file_size_bytes) / POWER(1024, 2), 1) as avg_file_size_mb
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND num_files IS NOT NULL
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN num_files < 10 THEN '1-9 files'
# MAGIC     WHEN num_files < 50 THEN '10-49 files'
# MAGIC     WHEN num_files < 100 THEN '50-99 files'
# MAGIC     WHEN num_files < 500 THEN '100-499 files'
# MAGIC     WHEN num_files < 1000 THEN '500-999 files'
# MAGIC     ELSE '1000+ files (CRITICAL)'
# MAGIC   END
# MAGIC ORDER BY 
# MAGIC   CASE file_range
# MAGIC     WHEN '1-9 files' THEN 1
# MAGIC     WHEN '10-49 files' THEN 2
# MAGIC     WHEN '50-99 files' THEN 3
# MAGIC     WHEN '100-499 files' THEN 4
# MAGIC     WHEN '500-999 files' THEN 5
# MAGIC     ELSE 6
# MAGIC   END

# COMMAND ----------

# DBTITLE 1,Tables with Most Files
# MAGIC %sql
# MAGIC -- 4.2 Tables with Most Files
# MAGIC SELECT 
# MAGIC   '📁 Tables with Most Files' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   num_files,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   days_since_optimize,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as optimize_command
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND num_files IS NOT NULL
# MAGIC ORDER BY num_files DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Tables with Smallest Average File Size
# MAGIC %sql
# MAGIC -- 4.3 Tables with Smallest Average File Size
# MAGIC SELECT 
# MAGIC   '🔻 Smallest Average File Size' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   num_files,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   days_since_optimize,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as optimize_command
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND avg_file_size_bytes IS NOT NULL
# MAGIC   AND size_bytes > 100 * 1024 * 1024  -- Only tables > 100 MB
# MAGIC   AND num_files >= 5
# MAGIC ORDER BY avg_file_size_bytes ASC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Never Optimized Tables
# MAGIC
# MAGIC Delta tables that have never been optimized

# COMMAND ----------

# DBTITLE 1,Never Optimized - Large Tables First
# MAGIC %sql
# MAGIC -- 5.1 Never Optimized - Large Tables First
# MAGIC SELECT 
# MAGIC   '🔧 Never Optimized Tables' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   num_files,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   DATEDIFF(CURRENT_DATE(), created_at) as table_age_days,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as optimize_command
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND last_optimize_ts IS NULL
# MAGIC   AND size_bytes > 100 * 1024 * 1024  -- Only tables > 100 MB
# MAGIC ORDER BY size_bytes DESC
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,Never Optimized Summary
# MAGIC %sql
# MAGIC -- 5.2 Never Optimized Summary
# MAGIC SELECT 
# MAGIC   '📊 Never Optimized Summary' as section,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(size_bytes) / POWER(1024, 2), 2) as avg_size_mb,
# MAGIC   SUM(num_files) as total_files,
# MAGIC   ROUND(AVG(num_files), 0) as avg_files_per_table,
# MAGIC   ROUND(try_divide(100.0 * COUNT(*), (SELECT COUNT(*) FROM IDENTIFIER(:metadata_table) WHERE table_format = 'DELTA' AND collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table)))), 1) as pct_of_delta_tables
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND last_optimize_ts IS NULL
# MAGIC   AND size_bytes > 100 * 1024 * 1024

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Stale Optimizations
# MAGIC
# MAGIC Tables with old OPTIMIZE operations

# COMMAND ----------

# DBTITLE 1,Most Stale Optimizations
# MAGIC %sql
# MAGIC -- 6.1 Most Stale Optimizations (>60 days)
# MAGIC SELECT 
# MAGIC   '⏰ Stale Optimizations' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   days_since_optimize,
# MAGIC   last_optimize_ts,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   num_files,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name) as optimize_command
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND days_since_optimize > 7
# MAGIC ORDER BY days_since_optimize DESC, size_bytes DESC
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,Optimization Age Distribution
# MAGIC %sql
# MAGIC -- 6.2 Optimization Age Distribution
# MAGIC SELECT 
# MAGIC   '📅 Optimization Age Distribution' as section,
# MAGIC   CASE 
# MAGIC     WHEN last_optimize_ts IS NULL THEN 'Never optimized'
# MAGIC     WHEN days_since_optimize <= 7 THEN 'Last 7 days'
# MAGIC     WHEN days_since_optimize <= 30 THEN '8-30 days'
# MAGIC     WHEN days_since_optimize <= 60 THEN '31-60 days'
# MAGIC     WHEN days_since_optimize <= 90 THEN '61-90 days'
# MAGIC     ELSE '> 90 days'
# MAGIC   END as optimization_age,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(num_files), 0) as avg_files
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN last_optimize_ts IS NULL THEN 'Never optimized'
# MAGIC     WHEN days_since_optimize <= 7 THEN 'Last 7 days'
# MAGIC     WHEN days_since_optimize <= 30 THEN '8-30 days'
# MAGIC     WHEN days_since_optimize <= 60 THEN '31-60 days'
# MAGIC     WHEN days_since_optimize <= 90 THEN '61-90 days'
# MAGIC     ELSE '> 90 days'
# MAGIC   END
# MAGIC ORDER BY 
# MAGIC   CASE optimization_age
# MAGIC     WHEN 'Last 7 days' THEN 1
# MAGIC     WHEN '8-30 days' THEN 2
# MAGIC     WHEN '31-60 days' THEN 3
# MAGIC     WHEN '61-90 days' THEN 4
# MAGIC     WHEN '> 90 days' THEN 5
# MAGIC     ELSE 6
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Data Organization
# MAGIC
# MAGIC Analysis of partitioning, clustering, and Z-ordering

# COMMAND ----------

# DBTITLE 1,Data Organization Summary
# MAGIC %sql
# MAGIC -- 7.1 Data Organization Summary
# MAGIC SELECT 
# MAGIC   '📊 Data Organization Status' as section,
# MAGIC   CASE 
# MAGIC     WHEN partition_columns IS NOT NULL THEN 'Partitioned'
# MAGIC     WHEN clustering_columns IS NOT NULL THEN 'Clustered'
# MAGIC     WHEN zorder_columns IS NOT NULL THEN 'Z-Ordered'
# MAGIC     ELSE 'No Organization'
# MAGIC   END as organization_type,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(size_bytes) / POWER(1024, 3), 2) as avg_size_gb
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN partition_columns IS NOT NULL THEN 'Partitioned'
# MAGIC     WHEN clustering_columns IS NOT NULL THEN 'Clustered'
# MAGIC     WHEN zorder_columns IS NOT NULL THEN 'Z-Ordered'
# MAGIC     ELSE 'No Organization'
# MAGIC   END
# MAGIC ORDER BY total_size_gb DESC

# COMMAND ----------

# DBTITLE 1,Large Unorganized Tables
# MAGIC %sql
# MAGIC -- 7.2 Large Unorganized Tables (>10 GB)
# MAGIC SELECT 
# MAGIC   '🔍 Large Unorganized Tables' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   row_count,
# MAGIC   num_files,
# MAGIC   partition_columns,
# MAGIC   clustering_columns,
# MAGIC   zorder_columns,
# MAGIC   '💡 Consider adding partitioning or liquid clustering' as recommendation
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND size_bytes > 1 * POWER(1024, 3)  -- > 1 GB
# MAGIC   AND partition_columns IS NULL
# MAGIC   AND clustering_columns IS NULL
# MAGIC   AND zorder_columns IS NULL
# MAGIC ORDER BY size_bytes DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Partitioning Analysis
# MAGIC %sql
# MAGIC -- 7.3 Partitioning Analysis
# MAGIC SELECT 
# MAGIC   '📁 Partitioning Patterns' as section,
# MAGIC   partition_columns,
# MAGIC   COUNT(*) as table_count,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   ROUND(AVG(num_files), 0) as avg_files
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC   AND partition_columns IS NOT NULL
# MAGIC GROUP BY partition_columns
# MAGIC ORDER BY table_count DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Top Savings Opportunities
# MAGIC %sql
# MAGIC -- 8.3 Top Savings Opportunities
# MAGIC SELECT 
# MAGIC   '💎 Top Savings Opportunities' as section,
# MAGIC   catalog,
# MAGIC   schema,
# MAGIC   table_name,
# MAGIC   ROUND(size_bytes / POWER(1024, 3), 2) as size_gb,
# MAGIC   num_files,
# MAGIC   ROUND(avg_file_size_bytes / POWER(1024, 2), 1) as avg_file_size_mb,
# MAGIC   days_since_optimize,
# MAGIC   days_since_vacuum,
# MAGIC   -- Estimated savings
# MAGIC   ROUND(
# MAGIC     CASE 
# MAGIC       WHEN num_files > 1000 THEN size_bytes * 0.25
# MAGIC       WHEN num_files > 500 OR avg_file_size_bytes < 128 * 1024 * 1024 THEN size_bytes * 0.20
# MAGIC       WHEN last_optimize_ts IS NULL THEN size_bytes * 0.15
# MAGIC       ELSE size_bytes * 0.10
# MAGIC     END / POWER(1024, 3), 2
# MAGIC   ) as estimated_savings_gb,
# MAGIC   CONCAT('OPTIMIZE ', catalog, '.', schema, '.', table_name, '; VACUUM ', catalog, '.', schema, '.', table_name, ' RETAIN 168 HOURS;') as suggested_commands
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC WHERE collection_timestamp = (SELECT MAX(collection_timestamp) FROM IDENTIFIER(:metadata_table))
# MAGIC   AND lower(table_format) = 'delta'
# MAGIC   AND table_name not like '__materialization_mat_%'
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN num_files > 1000 THEN size_bytes * 0.25
# MAGIC     WHEN num_files > 500 OR avg_file_size_bytes < 128 * 1024 * 1024 THEN size_bytes * 0.20
# MAGIC     WHEN last_optimize_ts IS NULL THEN size_bytes * 0.15
# MAGIC     ELSE size_bytes * 0.10
# MAGIC   END DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Growth Tracking
# MAGIC
# MAGIC Historical trends (requires multiple collection runs)

# COMMAND ----------

# DBTITLE 1,Collection History
# MAGIC %sql
# MAGIC -- 9.1 Collection History
# MAGIC SELECT 
# MAGIC   '📅 Collection History' as section,
# MAGIC   DATE(collection_timestamp) as collection_date,
# MAGIC   COUNT(*) as tables_scanned,
# MAGIC   ROUND(SUM(size_bytes) / POWER(1024, 3), 2) as total_size_gb,
# MAGIC   SUM(row_count) as total_rows
# MAGIC FROM IDENTIFIER(:metadata_table)
# MAGIC GROUP BY DATE(collection_timestamp)
# MAGIC ORDER BY collection_date DESC

# COMMAND ----------

# DBTITLE 1,Growth Trends (Comparing Last Two Collections)
# MAGIC %sql
# MAGIC -- 9.2 Growth Trends (Comparing Last Two Collections)
# MAGIC WITH latest_two AS (
# MAGIC   SELECT 
# MAGIC     catalog, schema, table_name, size_bytes, collection_timestamp,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY catalog, schema, table_name ORDER BY collection_timestamp DESC) as rn
# MAGIC   FROM IDENTIFIER(:metadata_table)
# MAGIC )
# MAGIC SELECT 
# MAGIC   '📈 Growth Analysis' as section,
# MAGIC   l1.catalog,
# MAGIC   l1.schema,
# MAGIC   l1.table_name,
# MAGIC   ROUND(l1.size_bytes / POWER(1024, 3), 2) as current_size_gb,
# MAGIC   ROUND(l2.size_bytes / POWER(1024, 3), 2) as previous_size_gb,
# MAGIC   ROUND((l1.size_bytes - l2.size_bytes) / POWER(1024, 3), 2) as growth_gb,
# MAGIC   ROUND(100.0 * (l1.size_bytes - l2.size_bytes) / NULLIF(l2.size_bytes, 0), 1) as growth_pct,
# MAGIC   DATEDIFF(l1.collection_timestamp, l2.collection_timestamp) as days_between
# MAGIC FROM latest_two l1
# MAGIC LEFT JOIN latest_two l2 
# MAGIC   ON l1.catalog = l2.catalog 
# MAGIC   AND l1.schema = l2.schema 
# MAGIC   AND l1.table_name = l2.table_name 
# MAGIC   AND l2.rn = 2
# MAGIC WHERE l1.rn = 1
# MAGIC   AND l2.size_bytes IS NOT NULL
# MAGIC   AND l1.size_bytes > l2.size_bytes  -- Only growing tables
# MAGIC ORDER BY (l1.size_bytes - l2.size_bytes) DESC
# MAGIC LIMIT 20
