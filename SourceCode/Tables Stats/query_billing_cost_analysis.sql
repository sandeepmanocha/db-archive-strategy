-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query Execution & Billing Cost Analysis
-- MAGIC
-- MAGIC This notebook joins **system.query.history**, **system.billing.usage**, and **system.billing.list_prices** to provide comprehensive cost attribution and analysis.
-- MAGIC
-- MAGIC **Data Sources:**
-- MAGIC - `system.query.history` - Individual query execution details
-- MAGIC - `system.billing.usage` - Daily aggregated billing/usage data (DBUs consumed)
-- MAGIC - `system.billing.list_prices` - Pricing information for SKUs
-- MAGIC
-- MAGIC **Key Analyses:**
-- MAGIC - Cost per query and per user
-- MAGIC - Warehouse-level cost attribution
-- MAGIC - Query source cost breakdown (Jobs, Dashboards, Notebooks)
-- MAGIC - Top cost-driving queries
-- MAGIC - Time series cost trends

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration

-- COMMAND ----------

-- Configuration Variables
DECLARE OR REPLACE VARIABLE analysis_days INT DEFAULT 30;
DECLARE OR REPLACE VARIABLE top_n_results INT DEFAULT 20;

-- Display configuration
SELECT 
  'Analysis Configuration' AS section,
  analysis_days AS analysis_period_days,
  top_n_results AS top_results_to_show,
  CURRENT_TIMESTAMP() AS analysis_run_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Temporary Tables
-- MAGIC
-- MAGIC Materialize filtered temporary tables to avoid repeatedly querying base system tables.
-- MAGIC These are **temporary tables** (not views), so data is materialized once for better performance.
-- MAGIC
-- MAGIC **Performance Benefits:**
-- MAGIC - Base system tables queried only once
-- MAGIC - Data is cached for fast subsequent queries
-- MAGIC - Pricing join computed once upfront
-- MAGIC
-- MAGIC **Billing Filters:**
-- MAGIC - `usage_type = 'COMPUTE_TIME'` → All compute costs (SQL Warehouses, Serverless, Jobs, All-Purpose)
-- MAGIC - `record_type = 'ORIGINAL'` → Original billing records only (excludes corrections for simplicity)
-- MAGIC - Excludes: Storage, Networking, API operations, RETRACTION/RESTATEMENT records
-- MAGIC
-- MAGIC **User Attribution Strategy:**
-- MAGIC - Uses `COALESCE(run_as, owned_by, created_by)` for user attribution
-- MAGIC - `run_as`: The user who actually ran the workload (jobs, notebooks)
-- MAGIC - `owned_by`: The warehouse owner (typically populated for SQL warehouses when run_as is NULL)
-- MAGIC - `created_by`: The user who created the resource (apps, agents)
-- MAGIC
-- MAGIC This ensures cost attribution even when `run_as` is NULL.

-- COMMAND ----------

-- Create temporary table for filtered query history
-- Drop first to ensure clean recreation on re-runs
DROP TABLE IF EXISTS temp_query_history;

CREATE TEMPORARY TABLE temp_query_history AS
SELECT 
  account_id,
  workspace_id,
  statement_id,
  session_id,
  execution_status,
  compute,
  executed_by_user_id,
  executed_by,
  statement_text,
  statement_type,
  error_message,
  client_application,
  client_driver,
  cache_origin_statement_id,
  total_duration_ms,
  waiting_for_compute_duration_ms,
  waiting_at_capacity_duration_ms,
  execution_duration_ms,
  compilation_duration_ms,
  total_task_duration_ms,
  result_fetch_duration_ms,
  start_time,
  end_time,
  update_time,
  read_partitions,
  pruned_files,
  read_files,
  read_rows,
  produced_rows,
  read_bytes,
  read_io_cache_percent,
  from_result_cache,
  spilled_local_bytes,
  written_bytes,
  written_rows,
  written_files,
  shuffle_read_bytes,
  query_source,
  query_parameters,
  executed_as,
  executed_as_user_id
FROM system.query.history
WHERE start_time >= CURRENT_DATE - INTERVAL '30' DAYS;

-- COMMAND ----------

-- Create temporary table for billing usage with pricing
-- Drop first to ensure clean recreation on re-runs
DROP TABLE IF EXISTS temp_billing_usage_with_pricing;

CREATE TEMPORARY TABLE temp_billing_usage_with_pricing AS
SELECT 
  bu.record_id,
  bu.account_id,
  bu.workspace_id,
  bu.sku_name,
  bu.cloud,
  bu.usage_start_time,
  bu.usage_end_time,
  bu.usage_date,
  bu.custom_tags,
  bu.usage_unit,
  bu.usage_quantity,
  bu.usage_metadata,
  bu.identity_metadata,
  -- Create a unified user field that tries run_as first, then owned_by, then created_by
  COALESCE(
    bu.identity_metadata.run_as,
    bu.identity_metadata.owned_by,
    bu.identity_metadata.created_by
  ) AS attributed_user,
  bu.record_type,
  bu.ingestion_date,
  bu.billing_origin_product,
  bu.product_features,
  bu.usage_type,
  lp.pricing.default AS unit_price,
  bu.usage_quantity * lp.pricing.default AS total_cost
FROM system.billing.usage bu
INNER JOIN system.billing.list_prices lp
  ON bu.sku_name = lp.sku_name
  AND bu.usage_date >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR bu.usage_date < lp.price_end_time)
WHERE bu.usage_date >= CURRENT_DATE - INTERVAL '30' DAYS
  -- Filter for compute-related usage only (exclude storage, networking, etc.)
  AND bu.usage_type = 'COMPUTE_TIME'
  -- Use ORIGINAL records only for simpler cost estimates (excludes corrections)
  AND bu.record_type = 'ORIGINAL';

-- COMMAND ----------

-- Verify temp tables created successfully
SELECT 
  'temp_query_history' AS table_name,
  COUNT(*) AS record_count,
  MIN(start_time) AS earliest_query,
  MAX(start_time) AS latest_query
FROM temp_query_history

UNION ALL

SELECT 
  'temp_billing_usage_with_pricing' AS table_name,
  COUNT(*) AS record_count,
  MIN(usage_date) AS earliest_date,
  MAX(usage_date) AS latest_date
FROM temp_billing_usage_with_pricing;

-- COMMAND ----------

-- Show user attribution breakdown
SELECT
  CASE 
    WHEN identity_metadata.run_as IS NOT NULL THEN 'run_as'
    WHEN identity_metadata.owned_by IS NOT NULL THEN 'owned_by'
    WHEN identity_metadata.created_by IS NOT NULL THEN 'created_by'
    ELSE 'none'
  END AS attribution_source,
  COUNT(*) AS record_count,
  ROUND(SUM(usage_quantity), 2) AS total_dbus,
  ROUND(SUM(total_cost), 2) AS total_cost_usd,
  COUNT(DISTINCT attributed_user) AS unique_users
FROM temp_billing_usage_with_pricing
GROUP BY attribution_source
ORDER BY record_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality Check: Billing Records
-- MAGIC
-- MAGIC Check for potential duplicate records in the billing temp table

-- COMMAND ----------

-- Check if we have duplicate record_ids (should be unique)
SELECT
  'Billing Record Uniqueness Check' AS check_type,
  COUNT(*) AS total_records,
  COUNT(DISTINCT record_id) AS unique_record_ids,
  COUNT(*) - COUNT(DISTINCT record_id) AS potential_duplicates
FROM temp_billing_usage_with_pricing;

-- COMMAND ----------

-- Show billing granularity: records per warehouse per day
SELECT
  usage_metadata.warehouse_id,
  usage_date,
  COUNT(*) AS records_per_day,
  COUNT(DISTINCT sku_name) AS distinct_skus,
  COUNT(DISTINCT attributed_user) AS distinct_users,
  ROUND(SUM(usage_quantity), 2) AS daily_dbus,
  ROUND(SUM(total_cost), 2) AS daily_cost
FROM temp_billing_usage_with_pricing
WHERE usage_metadata.warehouse_id IS NOT NULL
GROUP BY usage_metadata.warehouse_id, usage_date
ORDER BY daily_cost DESC
LIMIT 10;

-- COMMAND ----------

-- Check if multiple price records match one usage record (join issue)
WITH raw_billing AS (
  SELECT 
    bu.record_id,
    bu.usage_date,
    bu.sku_name,
    COUNT(*) AS price_matches
  FROM system.billing.usage bu
  INNER JOIN system.billing.list_prices lp
    ON bu.sku_name = lp.sku_name
    AND bu.usage_date >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR bu.usage_date < lp.price_end_time)
  WHERE bu.usage_date >= CURRENT_DATE - INTERVAL '30' DAYS
    AND bu.usage_type = 'COMPUTE_TIME'
    AND bu.record_type = 'ORIGINAL'
  GROUP BY bu.record_id, bu.usage_date, bu.sku_name
  HAVING COUNT(*) > 1
)
SELECT 
  'Price Join Duplicates Check' AS check_type,
  COUNT(*) AS records_with_multiple_price_matches
FROM raw_billing;


-- COMMAND ----------

-- Sanity check: Compare raw billing vs temp table
WITH raw_total AS (
  SELECT
    COUNT(*) AS raw_records,
    ROUND(SUM(bu.usage_quantity), 2) AS raw_dbus,
    COUNT(DISTINCT bu.usage_metadata.warehouse_id) AS raw_warehouses
  FROM system.billing.usage bu
  WHERE bu.usage_date >= CURRENT_DATE - INTERVAL '30' DAYS
    AND bu.usage_type = 'COMPUTE_TIME'
    AND bu.record_type = 'ORIGINAL'
),
temp_total AS (
  SELECT
    COUNT(*) AS temp_records,
    ROUND(SUM(usage_quantity), 2) AS temp_dbus,
    COUNT(DISTINCT usage_metadata.warehouse_id) AS temp_warehouses
  FROM temp_billing_usage_with_pricing
)
SELECT
  'Raw Billing Table' AS source,
  rt.raw_records AS record_count,
  rt.raw_dbus AS total_dbus,
  rt.raw_warehouses AS warehouses
FROM raw_total rt
UNION ALL
SELECT
  'Temp Table (with pricing)',
  tt.temp_records,
  tt.temp_dbus,
  tt.temp_warehouses
FROM temp_total tt;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cost Totals Verification
-- MAGIC
-- MAGIC Verify that aggregations are correct by comparing different groupings

-- COMMAND ----------

WITH total_billing AS (
  SELECT
    'Total Billing (Source)' AS aggregation_level,
    ROUND(SUM(usage_quantity), 2) AS total_dbus,
    ROUND(SUM(total_cost), 2) AS total_cost_usd
  FROM temp_billing_usage_with_pricing
),
by_user AS (
  SELECT
    'Sum by User' AS aggregation_level,
    ROUND(SUM(usage_quantity), 2) AS total_dbus,
    ROUND(SUM(total_cost), 2) AS total_cost_usd
  FROM temp_billing_usage_with_pricing
  WHERE attributed_user IS NOT NULL
  GROUP BY attributed_user
),
by_warehouse AS (
  SELECT
    'Sum by Warehouse' AS aggregation_level,
    ROUND(SUM(usage_quantity), 2) AS total_dbus,
    ROUND(SUM(total_cost), 2) AS total_cost_usd
  FROM temp_billing_usage_with_pricing
  WHERE usage_metadata.warehouse_id IS NOT NULL
  GROUP BY usage_metadata.warehouse_id
)
SELECT * FROM total_billing
UNION ALL
SELECT 
  'Sum by User (Total)',
  SUM(total_dbus),
  SUM(total_cost_usd)
FROM by_user
UNION ALL
SELECT 
  'Sum by Warehouse (Total)',
  SUM(total_dbus),
  SUM(total_cost_usd)
FROM by_warehouse;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Overall Cost & Query Summary
-- MAGIC
-- MAGIC Get a high-level view of total queries, costs, and resource consumption.
-- MAGIC
-- MAGIC **Note:** Uses ORIGINAL billing records only for straightforward cost estimates.

-- COMMAND ----------

WITH query_summary AS (
  SELECT 
    COUNT(DISTINCT statement_id) AS total_queries,
    COUNT(DISTINCT executed_by) AS unique_users,
    COUNT(DISTINCT workspace_id) AS workspaces,
    COUNT(DISTINCT compute.warehouse_id) AS warehouses_used,
    SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS successful_queries,
    SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
    ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_compute_hours,
    ROUND(SUM(read_bytes) / 1024 / 1024 / 1024 / 1024, 2) AS total_tb_scanned
  FROM temp_query_history
),
billing_summary AS (
  SELECT
    COUNT(*) AS billing_records,
    ROUND(SUM(usage_quantity), 2) AS total_dbus,
    ROUND(SUM(total_cost), 2) AS total_cost_usd,
    COUNT(DISTINCT sku_name) AS distinct_skus,
    COUNT(DISTINCT usage_metadata.warehouse_id) AS warehouses_with_usage
  FROM temp_billing_usage_with_pricing
)
SELECT
  'SUMMARY (Last 30 Days)' AS analysis,
  qs.total_queries,
  qs.successful_queries,
  ROUND(qs.successful_queries * 100.0 / NULLIF(qs.total_queries, 0), 1) AS success_rate_pct,
  qs.failed_queries,
  qs.unique_users,
  qs.warehouses_used,
  qs.total_compute_hours,
  qs.total_tb_scanned,
  bs.billing_records,
  bs.warehouses_with_usage,
  bs.distinct_skus,
  bs.total_dbus,
  bs.total_cost_usd,
  ROUND(bs.total_cost_usd / NULLIF(qs.total_queries, 0), 4) AS avg_cost_per_query
FROM query_summary qs
CROSS JOIN billing_summary bs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Daily Cost & Query Trends
-- MAGIC
-- MAGIC Analyze trends over time to identify patterns and anomalies

-- COMMAND ----------

WITH daily_queries AS (
  SELECT
    DATE(start_time) AS query_date,
    COUNT(DISTINCT statement_id) AS query_count,
    COUNT(DISTINCT executed_by) AS active_users,
    SUM(total_duration_ms) / 1000 / 3600 AS compute_hours,
    SUM(read_bytes) / 1024 / 1024 / 1024 AS gb_scanned,
    SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS successful_queries,
    SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries
  FROM temp_query_history
  GROUP BY DATE(start_time)
),
daily_costs AS (
  SELECT
    usage_date,
    SUM(usage_quantity) AS total_dbus,
    SUM(total_cost) AS total_cost
  FROM temp_billing_usage_with_pricing
  GROUP BY usage_date
)
SELECT
  dq.query_date,
  dq.query_count,
  dq.active_users,
  ROUND(dq.compute_hours, 2) AS compute_hours,
  ROUND(dq.gb_scanned, 2) AS gb_scanned,
  dq.successful_queries,
  dq.failed_queries,
  ROUND(dc.total_dbus, 2) AS total_dbus,
  ROUND(dc.total_cost, 2) AS total_cost_usd,
  ROUND(dc.total_cost / NULLIF(dq.query_count, 0), 4) AS cost_per_query
FROM daily_queries dq
LEFT JOIN daily_costs dc ON dq.query_date = dc.usage_date
ORDER BY dq.query_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Cost by User
-- MAGIC
-- MAGIC Identify which users are consuming the most resources and incurring the highest costs.
-- MAGIC
-- MAGIC **Note:** User attribution uses `COALESCE(run_as, owned_by, created_by)`:
-- MAGIC - For jobs/notebooks: uses the user who ran the workload
-- MAGIC - For SQL warehouses: uses the warehouse owner when run_as is NULL
-- MAGIC - For apps/agents: uses the creator

-- COMMAND ----------

WITH user_queries AS (
  SELECT
    executed_by,
    COUNT(DISTINCT statement_id) AS query_count,
    SUM(total_duration_ms) / 1000 / 3600 AS compute_hours,
    SUM(read_bytes) / 1024 / 1024 / 1024 / 1024 AS tb_scanned,
    SUM(read_rows) AS total_rows_read,
    AVG(total_duration_ms) / 1000 AS avg_query_seconds
  FROM temp_query_history
  WHERE execution_status = 'FINISHED'
  GROUP BY executed_by
),
user_costs AS (
  SELECT
    attributed_user AS user,
    SUM(usage_quantity) AS total_dbus,
    SUM(total_cost) AS total_cost
  FROM temp_billing_usage_with_pricing
  WHERE attributed_user IS NOT NULL
  GROUP BY attributed_user
)
SELECT
  uq.executed_by AS user,
  uq.query_count,
  ROUND(uq.compute_hours, 2) AS compute_hours,
  ROUND(uq.tb_scanned, 3) AS tb_scanned,
  ROUND(uq.avg_query_seconds, 2) AS avg_query_seconds,
  ROUND(uc.total_dbus, 2) AS total_dbus,
  ROUND(uc.total_cost, 2) AS total_cost_usd,
  ROUND(uc.total_cost / NULLIF(uq.query_count, 0), 4) AS cost_per_query
FROM user_queries uq
LEFT JOIN user_costs uc ON uq.executed_by = uc.user
WHERE uc.total_cost IS NOT NULL
ORDER BY uc.total_cost DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Cost by Warehouse
-- MAGIC
-- MAGIC Analyze costs at the warehouse level for resource optimization

-- COMMAND ----------

WITH warehouse_queries AS (
  SELECT
    compute.warehouse_id AS warehouse_id,
    COUNT(DISTINCT statement_id) AS query_count,
    COUNT(DISTINCT executed_by) AS unique_users,
    SUM(total_duration_ms) / 1000 / 3600 AS compute_hours,
    SUM(read_bytes) / 1024 / 1024 / 1024 / 1024 AS tb_scanned,
    AVG(total_duration_ms) / 1000 AS avg_query_seconds,
    MAX(total_duration_ms) / 1000 AS max_query_seconds
  FROM temp_query_history
  WHERE execution_status = 'FINISHED'
    AND compute.warehouse_id IS NOT NULL
  GROUP BY compute.warehouse_id
),
warehouse_costs AS (
  SELECT
    usage_metadata.warehouse_id AS warehouse_id,
    SUM(usage_quantity) AS total_dbus,
    SUM(total_cost) AS total_cost,
    MAX(sku_name) AS sku_name
  FROM temp_billing_usage_with_pricing
  WHERE usage_metadata.warehouse_id IS NOT NULL
  GROUP BY usage_metadata.warehouse_id
)
SELECT
  wq.warehouse_id,
  wq.query_count,
  wq.unique_users,
  ROUND(wq.compute_hours, 2) AS compute_hours,
  ROUND(wq.tb_scanned, 3) AS tb_scanned,
  ROUND(wq.avg_query_seconds, 2) AS avg_query_seconds,
  ROUND(wq.max_query_seconds, 2) AS max_query_seconds,
  ROUND(wc.total_dbus, 2) AS total_dbus,
  ROUND(wc.total_cost, 2) AS total_cost_usd,
  ROUND(wc.total_cost / NULLIF(wq.query_count, 0), 4) AS cost_per_query,
  wc.sku_name
FROM warehouse_queries wq
LEFT JOIN warehouse_costs wc ON wq.warehouse_id = wc.warehouse_id
WHERE wc.total_cost IS NOT NULL
ORDER BY wc.total_cost DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Query Activity by Source
-- MAGIC
-- MAGIC Understand which systems (Jobs, Dashboards, Notebooks) are running queries.
-- MAGIC
-- MAGIC **Note:** Cost attribution by query source is not shown here because billing records
-- MAGIC don't contain query source metadata, making accurate attribution impossible without
-- MAGIC complex proportional allocation that would still be estimates.

-- COMMAND ----------

SELECT
  CASE 
    WHEN query_source.job_info.job_id IS NOT NULL THEN 'Job'
    WHEN query_source.dashboard_id IS NOT NULL THEN 'Dashboard'
    WHEN query_source.legacy_dashboard_id IS NOT NULL THEN 'Legacy Dashboard'
    WHEN query_source.notebook_id IS NOT NULL THEN 'Notebook'
    WHEN query_source.alert_id IS NOT NULL THEN 'Alert'
    WHEN query_source.genie_space_id IS NOT NULL THEN 'Genie Space'
    ELSE 'Direct Query'
  END AS query_source_type,
  COUNT(DISTINCT statement_id) AS total_queries,
  COUNT(DISTINCT executed_by) AS unique_users,
  COUNT(DISTINCT compute.warehouse_id) AS warehouses_used,
  ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_compute_hours,
  ROUND(AVG(total_duration_ms) / 1000, 2) AS avg_duration_seconds,
  ROUND(SUM(read_bytes) / 1024 / 1024 / 1024 / 1024, 3) AS total_tb_scanned,
  ROUND(AVG(read_bytes) / 1024 / 1024 / 1024, 2) AS avg_gb_per_query,
  SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) AS cached_queries,
  ROUND(SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS cache_hit_rate_pct
FROM temp_query_history
WHERE execution_status = 'FINISHED'
GROUP BY query_source_type
ORDER BY total_queries DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Top 20 Most Expensive Queries
-- MAGIC
-- MAGIC Identify specific queries that consume the most resources for optimization.
-- MAGIC
-- MAGIC **Cost Attribution Method:**
-- MAGIC - Billing data is aggregated daily per warehouse/user
-- MAGIC - Each query gets a **proportional share** of the daily cost based on its compute time
-- MAGIC - Formula: `(query_duration / total_daily_duration) × daily_cost`
-- MAGIC - This provides estimated per-query costs for optimization prioritization

-- COMMAND ----------

WITH query_details AS (
  SELECT
    statement_id,
    workspace_id,
    executed_by,
    compute.warehouse_id AS warehouse_id,
    DATE(start_time) AS query_date,
    start_time,
    statement_type,
    SUBSTRING(statement_text, 1, 200) AS query_preview,
    total_duration_ms / 1000 AS duration_seconds,
    total_duration_ms AS duration_ms,
    read_bytes / 1024 / 1024 / 1024 AS gb_scanned,
    read_rows,
    produced_rows,
    from_result_cache,
    CASE 
      WHEN query_source.job_info.job_id IS NOT NULL THEN CONCAT('Job: ', query_source.job_info.job_id)
      WHEN query_source.dashboard_id IS NOT NULL THEN CONCAT('Dashboard: ', query_source.dashboard_id)
      WHEN query_source.notebook_id IS NOT NULL THEN CONCAT('Notebook: ', query_source.notebook_id)
      ELSE 'Direct Query'
    END AS source_info
  FROM temp_query_history
  WHERE execution_status = 'FINISHED'
    AND compute.warehouse_id IS NOT NULL
),
daily_costs_aggregated AS (
  -- Aggregate billing data by date/warehouse/user to avoid duplication
  SELECT
    usage_date,
    usage_metadata.warehouse_id AS warehouse_id,
    attributed_user,
    SUM(usage_quantity) AS daily_dbus,
    SUM(total_cost) AS daily_cost
  FROM temp_billing_usage_with_pricing
  WHERE attributed_user IS NOT NULL
  GROUP BY usage_date, usage_metadata.warehouse_id, attributed_user
),
daily_query_totals AS (
  -- Calculate total compute time per day/warehouse/user for proportional allocation
  SELECT
    query_date,
    warehouse_id,
    executed_by,
    SUM(duration_ms) AS total_daily_duration_ms
  FROM query_details
  GROUP BY query_date, warehouse_id, executed_by
),
query_costs AS (
  SELECT
    qd.statement_id,
    qd.executed_by,
    qd.warehouse_id,
    qd.query_date,
    qd.start_time,
    qd.statement_type,
    qd.query_preview,
    qd.duration_seconds,
    qd.gb_scanned,
    qd.read_rows,
    qd.produced_rows,
    qd.from_result_cache,
    qd.source_info,
    -- Proportionally allocate daily cost based on query duration
    CASE 
      WHEN dqt.total_daily_duration_ms > 0 THEN
        (qd.duration_ms / dqt.total_daily_duration_ms) * dc.daily_dbus
      ELSE 0
    END AS estimated_dbus,
    CASE 
      WHEN dqt.total_daily_duration_ms > 0 THEN
        (qd.duration_ms / dqt.total_daily_duration_ms) * dc.daily_cost
      ELSE 0
    END AS estimated_cost
  FROM query_details qd
  LEFT JOIN daily_costs_aggregated dc
    ON qd.query_date = dc.usage_date
    AND qd.warehouse_id = dc.warehouse_id
    AND qd.executed_by = dc.attributed_user
  LEFT JOIN daily_query_totals dqt
    ON qd.query_date = dqt.query_date
    AND qd.warehouse_id = dqt.warehouse_id
    AND qd.executed_by = dqt.executed_by
)
SELECT
  statement_id,
  executed_by,
  warehouse_id,
  start_time,
  statement_type,
  query_preview,
  ROUND(duration_seconds, 2) AS duration_seconds,
  ROUND(gb_scanned, 2) AS gb_scanned,
  read_rows,
  produced_rows,
  from_result_cache,
  source_info,
  ROUND(estimated_dbus, 2) AS estimated_dbus,
  ROUND(estimated_cost, 4) AS estimated_cost_usd
FROM query_costs
ORDER BY estimated_cost DESC NULLS LAST
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Cached Query Analysis
-- MAGIC
-- MAGIC Identify cost savings from query result caching

-- COMMAND ----------

WITH cache_analysis AS (
  SELECT
    DATE(start_time) AS query_date,
    COUNT(DISTINCT statement_id) AS total_queries,
    SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) AS cached_queries,
    SUM(CASE WHEN from_result_cache = FALSE THEN 1 ELSE 0 END) AS uncached_queries,
    -- Metrics for cached queries
    AVG(CASE WHEN from_result_cache = TRUE THEN total_duration_ms END) / 1000 AS avg_cached_duration_sec,
    -- Metrics for uncached queries
    AVG(CASE WHEN from_result_cache = FALSE THEN total_duration_ms END) / 1000 AS avg_uncached_duration_sec,
    SUM(CASE WHEN from_result_cache = FALSE THEN read_bytes END) / 1024 / 1024 / 1024 AS gb_scanned_uncached
  FROM temp_query_history
  WHERE execution_status = 'FINISHED'
  GROUP BY DATE(start_time)
)
SELECT
  query_date,
  total_queries,
  cached_queries,
  uncached_queries,
  ROUND(cached_queries * 100.0 / NULLIF(total_queries, 0), 1) AS cache_hit_rate_pct,
  ROUND(avg_cached_duration_sec, 2) AS avg_cached_duration_sec,
  ROUND(avg_uncached_duration_sec, 2) AS avg_uncached_duration_sec,
  ROUND(gb_scanned_uncached, 2) AS gb_scanned_uncached,
  ROUND((avg_uncached_duration_sec - avg_cached_duration_sec) * cached_queries / 3600, 2) AS saved_compute_hours
FROM cache_analysis
ORDER BY query_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Failed Queries Analysis
-- MAGIC
-- MAGIC Identify patterns in failed queries and their impact

-- COMMAND ----------

SELECT
  DATE(start_time) AS query_date,
  executed_by,
  statement_type,
  COUNT(*) AS failed_count,
  SUBSTRING(MAX(error_message), 1, 150) AS sample_error_message,
  SUBSTRING(MAX(statement_text), 1, 150) AS sample_query
FROM temp_query_history
WHERE execution_status = 'FAILED'
GROUP BY DATE(start_time), executed_by, statement_type
ORDER BY failed_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Query Performance Insights
-- MAGIC
-- MAGIC Identify queries with potential optimization opportunities

-- COMMAND ----------

SELECT
  statement_id,
  executed_by,
  start_time,
  statement_type,
  SUBSTRING(statement_text, 1, 200) AS query_preview,
  -- Performance metrics
  ROUND(total_duration_ms / 1000, 2) AS total_duration_sec,
  ROUND(execution_duration_ms / 1000, 2) AS execution_duration_sec,
  ROUND(compilation_duration_ms / 1000, 2) AS compilation_duration_sec,
  ROUND(waiting_for_compute_duration_ms / 1000, 2) AS wait_compute_sec,
  -- Data metrics
  ROUND(read_bytes / 1024 / 1024 / 1024, 2) AS gb_scanned,
  read_rows,
  produced_rows,
  read_files,
  pruned_files,
  -- I/O efficiency
  read_io_cache_percent,
  ROUND(spilled_local_bytes / 1024 / 1024 / 1024, 2) AS gb_spilled,
  -- Optimization flags
  CASE 
    WHEN waiting_for_compute_duration_ms > 10000 THEN 'High wait time'
    WHEN spilled_local_bytes > 1073741824 THEN 'Disk spill detected'
    WHEN read_bytes > 107374182400 AND read_io_cache_percent < 50 THEN 'Low cache utilization'
    WHEN compilation_duration_ms > execution_duration_ms THEN 'High compilation time'
    ELSE 'Normal'
  END AS optimization_flag
FROM temp_query_history
WHERE start_time >= CURRENT_DATE - INTERVAL '7' DAYS
  AND execution_status = 'FINISHED'
  AND total_duration_ms > 60000  -- Queries taking more than 1 minute
ORDER BY total_duration_ms DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Weekly Cost Comparison
-- MAGIC
-- MAGIC Compare costs week-over-week to identify trends
-- MAGIC
-- MAGIC **Note:** This analysis looks at 90 days of data. If your temp table only contains 30 days, 
-- MAGIC you may need to adjust the configuration variable at the top or extend the temp table date range.

-- COMMAND ----------

WITH extended_billing AS (
  -- Extend to 90 days for weekly trend analysis
  SELECT 
    bu.usage_date,
    bu.usage_quantity,
    bu.usage_quantity * lp.pricing.default AS total_cost
  FROM system.billing.usage bu
  INNER JOIN system.billing.list_prices lp
    ON bu.sku_name = lp.sku_name
    AND bu.usage_date >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR bu.usage_date < lp.price_end_time)
  WHERE bu.usage_date >= CURRENT_DATE - INTERVAL '90' DAYS
    AND bu.usage_type = 'COMPUTE_TIME'
    AND bu.record_type = 'ORIGINAL'
),
weekly_metrics AS (
  SELECT
    DATE_TRUNC('WEEK', usage_date) AS week_start,
    SUM(usage_quantity) AS total_dbus,
    SUM(total_cost) AS total_cost,
    COUNT(DISTINCT usage_date) AS days_in_week
  FROM extended_billing
  GROUP BY DATE_TRUNC('WEEK', usage_date)
)
SELECT
  week_start,
  days_in_week,
  ROUND(total_dbus, 2) AS total_dbus,
  ROUND(total_cost, 2) AS total_cost_usd,
  ROUND(total_cost / NULLIF(days_in_week, 0), 2) AS avg_daily_cost,
  ROUND(
    (total_cost - LAG(total_cost) OVER (ORDER BY week_start)) * 100.0 / 
    NULLIF(LAG(total_cost) OVER (ORDER BY week_start), 0), 
    1
  ) AS cost_change_pct
FROM weekly_metrics
ORDER BY week_start DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary & Next Steps
-- MAGIC
-- MAGIC **Key Takeaways:**
-- MAGIC - Review top cost-driving queries for optimization opportunities
-- MAGIC - Monitor users and warehouses with highest resource consumption
-- MAGIC - Leverage query result caching to reduce costs
-- MAGIC - Address failed queries to avoid wasted resources
-- MAGIC - Consider right-sizing warehouses based on usage patterns
-- MAGIC
-- MAGIC **Optimization Actions:**
-- MAGIC 1. **High-cost queries**: Apply partitioning, Z-ordering, or liquid clustering
-- MAGIC 2. **Low cache hit rates**: Enable query result caching and configure TTLs
-- MAGIC 3. **High wait times**: Consider auto-scaling or larger warehouse sizes
-- MAGIC 4. **Disk spilling**: Increase warehouse size or optimize query logic
-- MAGIC 5. **Failed queries**: Fix permission issues and query syntax errors
-- MAGIC