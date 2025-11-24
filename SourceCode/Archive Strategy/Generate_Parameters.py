# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Parameters for Archive/Rehydrate Jobs
# MAGIC
# MAGIC Reads archive_config.json and sets task values for downstream processing.

# COMMAND ----------

import json

# COMMAND ----------

# Read config file
with open('./config/archive_config.json', 'r') as f:
    config = json.load(f)

# COMMAND ----------

# Get active tables only
active_tables = [table for table in config["tables"] if table.get("is_active", False)]

print(f"Found {len(active_tables)} active tables:")
for table in active_tables:
    print(f"  - {table['source_catalog']}.{table['source_schema']}.{table['source_table']}")

# COMMAND ----------

# Set task values for downstream ForEach
dbutils.jobs.taskValues.set(key="tables", value=active_tables)

print(f"✅ Set {len(active_tables)} table configs as task values")
