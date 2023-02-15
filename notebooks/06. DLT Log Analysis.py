# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline log analysis
# MAGIC 
# MAGIC Please make sure you specify your own Database and Storage location. You'll find this information in the configuration menu of your [Delta Live Table Pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/5b2ef462-558e-4f8d-8ad8-c80bce9da954).
# MAGIC 
# MAGIC **NOTE:** Please use Databricks Runtime 9.1 or above when running this notebook

# COMMAND ----------

# Full username, e.g. "<first>.<last>@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# Database name
dbName = user+"_workshop_db"

storage_location = f"/tmp/delta-stream-dltworkshop/{user}/dlt_storage"

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('storage_location', storage_location)
dbutils.widgets.text('latest_update_id', 'Update_ID_fromUpdateDetails')

# COMMAND ----------

# MAGIC %md
# MAGIC Set correct `Update ID` and storage location

# COMMAND ----------

events_table_location = f"{dbutils.widgets.get('storage_location')}/system/events" #dbfs:/storage_location/system/events/
latest_updateID = dbutils.widgets.get('latest_update_id')

# COMMAND ----------

# DBTITLE 1,Find the Metrics Table in DBFS
display(dbutils.fs.ls(events_table_location))

# COMMAND ----------

# DBTITLE 1,Write the Metrics Table to our DLT Database
df = spark.read.load(events_table_location)
df.write.mode("overwrite").saveAsTable(f"{dbName}.metrics_table")
display(df)

# COMMAND ----------

spark.sql(f"USE {dbName}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Event Logs Analysis
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC 
# MAGIC | Type of event | behavior |
# MAGIC | --- | --- |
# MAGIC | `user_action` | Events occur when taking actions like creating the pipeline |
# MAGIC | `flow_definition`| Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information |
# MAGIC | `output_dataset` and `input_datasets` | output table/view and its upstream table(s)/view(s) |
# MAGIC | `flow_type` | whether this is a complete or append flow |
# MAGIC | `explain_text` | the Spark explain plan |
# MAGIC | `flow_progress`| Events occur when a data flow starts running or finishes processing a batch of data |
# MAGIC | `metrics` | currently contains `num_output_rows` |
# MAGIC | `data_quality` (`dropped_records`), (`expectations`: `name`, `dataset`, `passed_records`, `failed_records`)| contains an array of the results of the data quality rules for this particular dataset   * `expectations`|

# COMMAND ----------

# DBTITLE 1,Monitor Data Quality Over All Runs
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dlt_dataquality
# MAGIC SELECT
# MAGIC   id,
# MAGIC   timestamp,
# MAGIC   status_update,
# MAGIC   expectations.dataset,
# MAGIC   expectations.name,
# MAGIC   expectations.failed_records,
# MAGIC   expectations.passed_records
# MAGIC FROM(
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status as status_update,
# MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
# MAGIC              , schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records': 42, 'failed_records': 42}]"))) expectations
# MAGIC   FROM metrics_table
# MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality;
# MAGIC   
# MAGIC SELECT * FROM dlt_dataquality

# COMMAND ----------

# DBTITLE 1,Create Data Lineage Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dlt_lineage
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:flow_definition.output_dataset,
# MAGIC   details:flow_definition.input_datasets,
# MAGIC   details:flow_definition.flow_type,
# MAGIC   details:flow_definition.schema,
# MAGIC   details:flow_definition
# MAGIC FROM metrics_table
# MAGIC WHERE details:flow_definition IS NOT NULL
# MAGIC ORDER BY timestamp;
# MAGIC 
# MAGIC SELECT * FROM dlt_lineage

# COMMAND ----------

# MAGIC %md Let's head to our [Databricks SQL Dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/88b89069-58f8-471f-9d60-a7943b93fa23-dlt-workshop--dlt-metrics?o=1444828305810485) where we can visualize our new tables.

# COMMAND ----------

# MAGIC %md These are the SQL Queries used to generate the visualizations on our dashboard:

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Failed Record Rate */
# MAGIC SELECT sum(failed_records) / sum(failed_records + passed_records) * 100
# MAGIC        failure_rate,
# MAGIC        sum(failed_records + passed_records)
# MAGIC        output_records
# MAGIC FROM   dlt_workshop_retail.dlt_dataquality 

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Number of Failed Records */
# MAGIC SELECT avg(failed_records)
# MAGIC FROM   dlt_workshop_retail.dlt_dataquality
# MAGIC WHERE  failed_records > 0 

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Failed and Passed Records Monthly */
# MAGIC SELECT Sum(passed_records) AS passed_records,
# MAGIC        Sum(failed_records) AS failed_records,
# MAGIC        Sum(passed_records + failed_records) AS output_records,
# MAGIC        Sum(failed_records) / Sum(passed_records + failed_records) * 100 AS failure_rate,
# MAGIC        NAME,
# MAGIC        dataset,
# MAGIC        Date(timestamp) AS date
# MAGIC FROM   dlt_workshop_retail.dlt_dataquality
# MAGIC GROUP BY date,
# MAGIC           dataset,
# MAGIC           NAME 

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Records by Dataset */
# MAGIC SELECT 'passed_records' AS type,
# MAGIC        Sum(passed_records) AS value,
# MAGIC        Sum(failed_records) / Sum(passed_records + failed_records) * 100 AS failure_rate,
# MAGIC        dataset
# MAGIC FROM   dlt_workshop_retail.dlt_dataquality
# MAGIC GROUP BY dataset
# MAGIC UNION
# MAGIC SELECT 'failed_records' AS type,
# MAGIC        Sum(failed_records) AS value,
# MAGIC        Sum(failed_records) / Sum(passed_records + failed_records) * 100 AS failure_rate,
# MAGIC        dataset
# MAGIC FROM   dlt_workshop_retail.dlt_dataquality
# MAGIC GROUP BY dataset 

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Lineage Table */
# MAGIC SELECT output_dataset AS dataset,
# MAGIC        input_datasets AS input
# MAGIC FROM   dlt_workshop_retail.dlt_lineage
# MAGIC WHERE  Date(timestamp) = "2022-12-09"
# MAGIC        AND flow_type = "incremental" 

# COMMAND ----------


