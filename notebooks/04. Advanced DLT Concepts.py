# Databricks notebook source
# MAGIC %md
# MAGIC When creating the pipeline:
# MAGIC * add the following configuration to point to the data source `data_source_path` : `/databricks-datasets/online_retail/data-001/`
# MAGIC * add the following configuration to read the country list from the correct schema/db : `dbName`: `username_workshop_db`
# MAGIC * add notebook number 2 (`02. Building Delta Live Tables Pipelines-SQL`) to the notebook Paths

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Making pipelines Configure-less-code with DLT

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Load Configs From Table or Files
## Loading Distinct Countries from DLT Table that is automatically managed and kept up to date

## Configs can be anything
# 1. Mappings
# 2. Optimizations
# 3. Table/Env configs
# 4. Table definitions
# 5. SQL Expressions/Full logic

dbName = spark.conf.get("dbName")

## Get all countries you want to make a separate table for -- then you can create DBSQL Dashboards, and share these tables
countries_list = [i[0] for i in spark.table(f"{dbName}.distinct_countries_retail").select("Country").coalesce(1).collect()]
print(countries_list)

all_tbl_properties = {
  "quality":"silver",
  "delta.tuneFileSizesForRewrites":"true",
  "pipelines.autoOptimize.zOrderCols":"CustomerID, InvoiceNo"
}

expectations_configs = {"has_invoice_number":"CAST(InvoiceNo AS INTEGER) IS NOT NULL","has_customer_number":"CAST(CustomerID AS INTEGER) IS NOT NULL"}

access_configs = {"accessible_via_pii_group": ['United Kingdom']}

# COMMAND ----------

# DBTITLE 1,Define Wrapper Function for Reproducible Operation(s)
### Use meta-programming model

def generate_bronze_tables(call_table, filter):
  #@dlt.expect_all_or_drop(expectations_configs)
  @dlt.table(
    name=call_table,
    comment=f"Bronze Table {call_table} By for Country: {filter}",
    table_properties=all_tbl_properties,
    spark_conf={"pipelines.trigger.interval" : "1 day"}
  )
  def create_call_table():
    
    df = (dlt.read_stream("quality_retail_split_by_country")
          .filter(col("Country") == lit(filter))
         )
    
    return df

# COMMAND ----------

# DBTITLE 1,Declaratively Implement Function
for country in countries_list:
  clean_name = country.replace(" ","_")
  table_name = "sales_for_" + clean_name + "_bronze"
  generate_bronze_tables(table_name, country)

# COMMAND ----------

# DBTITLE 1,Dynamic APPLY CHANGES function
def generate_silver_tables(target_table, source_table, merge_keys, sequence_key):
  
  ### Auto-zorder by merge keys, and others by a config
  zorder_str = ",".join(merge_keys)
  
  ### Dynamically config target tables as little or as much as you want
  dlt.create_streaming_live_table(
  name = target_table,
  comment = "Silver Table",
  spark_conf={"pipelines.trigger.interval" : "1 hour"},
  table_properties= {"quality":"silver",
                     "delta.autoOptimize.optimizeWrite":"true",
                     "delta.tuneFileSizesForRewrites":"true",
                     "pipelines.autoOptimize.zOrderCols":zorder_str}
  #partition_cols=["<partition-column>", "<partition-column>"],
  #path="<storage-location-path>",
  #schema="schema-definition"
  )
    
  ### Run Merge -- This includes CDC Data 
  dlt.apply_changes(
    target = target_table,
    source = source_table,
    keys = merge_keys,
    sequence_by = sequence_key,
    ignore_null_updates = False,
    apply_as_deletes = None,
    column_list = None,
    except_column_list = None
    )
    
  return

# COMMAND ----------

# DBTITLE 1,This many tables to manage updates for is too much, lets do it all at once...
for country in countries_list:
  clean_name = country.replace(" ","_")
  source_table_name = "sales_for_" + clean_name + "_bronze"
  target_table_name = "sales_for_" + clean_name + "_silver"
  merge_keys = ["InvoiceNo", "CustomerID"]
  sequence_key = "InvoiceDatetime"
  generate_silver_tables(target_table = target_table_name, 
                         source_table = source_table_name, 
                         merge_keys = merge_keys, 
                         sequence_key = sequence_key)

