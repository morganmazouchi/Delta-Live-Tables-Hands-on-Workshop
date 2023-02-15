# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Structured Streaming with Databricks Delta Tables
# MAGIC 
# MAGIC One of the hallmark innovations of Databricks and the Lakehouse vision is the establishing of a unified method for writing and reading data in a data lake. This unification of batch and streaming jobs has been called the post-lambda architecture for data warehousing. The flexibility, simplicity, and scalability of the new delta lake architecture has been pivotal towards addressing big data needs and has been gifted to the Linux Foundation. Fundamental to the lakehouse view of ETL/ELT is the usage of a multi-hop data architecture known as the medallion architecture. 
# MAGIC Delta Lake, the pillar of lakehouse platform, is an open-source storage layer that brings ACID transactions and increased performance to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/02/telco-accel-blog-2-new.png" width=1012/>
# MAGIC 
# MAGIC See below links for more documentation:
# MAGIC * [How to Process IoT Device JSON Data Using Apache Spark Datasets and DataFrames](https://databricks.com/blog/2016/03/28/how-to-process-iot-device-json-data-using-apache-spark-datasets-and-dataframes.html)
# MAGIC * [Spark Structure Streaming](https://databricks.com/blog/2016/07/28/structured-streaming-in-apache-spark.html)
# MAGIC * [Beyond Lambda](https://databricks.com/discover/getting-started-with-delta-lake-tech-talks/beyond-lambda-introducing-delta-architecture)
# MAGIC * [Delta Lake Docs](https://docs.databricks.com/delta/index.html)
# MAGIC * [Medallion Architecture](https://databricks.com/solutions/data-pipelines)
# MAGIC * [Cost Savings with the Medallion Architecture](https://techcommunity.microsoft.com/t5/analytics-on-azure/how-to-reduce-infrastructure-costs-by-up-to-80-with-azure/ba-p/1820280)
# MAGIC * [Change Data Capture Streams with the Medallion Architecture](https://databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema

# COMMAND ----------

import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timezone
import uuid

# COMMAND ----------

file_schema = (spark
               .read
               .format("csv")
               .option("header", True)
               .option("inferSchema", True)
               .load("/databricks-datasets/iot-stream/data-user/userData.csv")
               .limit(10)
               .schema)

# COMMAND ----------

print(file_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark Structured Streaming                                                   
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/01/cloudtrail-structured-streaming-model.png"> 

# COMMAND ----------

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# Stream raw IOT Events from S3 bucket
iot_event_stream = (spark
                    .readStream
                    .option( "maxFilesPerTrigger", 1 )
                    .format("csv")
                    .option("header", True)
                    .schema(file_schema)
                    .load("/databricks-datasets/iot-stream/data-user/*.csv")
                    .withColumn( "id", uuidUdf() )
                    .withColumn( "timestamp", lit(datetime.now().timestamp()).cast("timestamp") )
                    .repartition(200)
                   )
display(iot_event_stream)

# COMMAND ----------

# DBTITLE 1,Housekeeping to make this idempotent
# Full username, e.g. "<first>.<last>@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# Database name
dbName = user+"_workshop_db"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dbName}")
spark.sql(f"USE {dbName}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Medallion Architecture

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Drop streaming tables if they exist
# MAGIC --
# MAGIC 
# MAGIC Drop TABLE IF EXISTS iot_event_bronze;
# MAGIC Drop TABLE IF EXISTS iot_event_silver;
# MAGIC Drop TABLE IF EXISTS iot_event_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Set up

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta With Checkpointing
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/checkpoint.png?raw=true"> 

# COMMAND ----------

######
##  Setup checkpoint directory for writing out streaming workloads
######

checkpoint_dir_bronze = f"/tmp/delta-stream-dltworkshop/{user}/bronze_check";
checkpoint_dir_silver = f"/tmp/delta-stream-dltworkshop/{user}/silver_check"
checkpoint_dir_gold = f"/tmp/delta-stream-dltworkshop/{user}/gold_check"

# COMMAND ----------

# MAGIC %md 
# MAGIC # Write IOT Events into a Bronze Delta Table

# COMMAND ----------

# Clear checkpoint location (for IDEMPOTENCY)
dbutils.fs.rm(checkpoint_dir_bronze, True)

iot_stream = iot_event_stream.writeStream\
                             .format("delta")\
                             .outputMode("append")\
                             .option("header", True)\
                             .option("checkpointLocation", checkpoint_dir_bronze)\
                             .trigger(processingTime='10 seconds')\
                             .table("iot_event_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED iot_event_bronze;

# COMMAND ----------

# DBTITLE 1,Take a peak at delta lake files created
display(dbutils.fs.ls(f"/user/hive/warehouse/{dbName}.db/iot_event_bronze/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_event_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Streaming ETL from Bronze to Silver
# MAGIC 
# MAGIC Perform data cleanup and augmentation as we transform the Bronze data to Silver

# COMMAND ----------

"""
Deduplicate Bronze level data
"""

# Clear checkpoint location (for IDEMPOTENCY)
dbutils.fs.rm(checkpoint_dir_silver, True)

# Drop terribly out-of-order events
bronzeCleanDF = iot_event_stream.withWatermark( "timestamp", "1 day" )

# Drop bad events
bronzeCleanDF = bronzeCleanDF.dropna()

silverStream = bronzeCleanDF.writeStream\
            .format("delta")\
            .outputMode("append")\
            .option( "checkpointLocation", checkpoint_dir_silver)\
            .trigger(processingTime='10 seconds')\
            .table("iot_event_silver")
silverStream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_event_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Streaming Aggregation from Silver to Gold

# COMMAND ----------

silver_streamDF = spark.readStream.option( "maxFilesPerTrigger", 1 ).format( "delta" ).table("iot_event_silver")

# Clear checkpoint location (for IDEMPOTENCY)
dbutils.fs.rm(checkpoint_dir_gold, True)

# def updateGold( batch, batchId ):
#   ( gold.alias("gold")
#         .merge( batch.alias("batch"),
#                 "gold.date = batch.date AND gold.miles_walked = batch.miles_walked"
#               )
#         .whenMatchedUpdateAll()
#         .whenNotMatchedInsertAll()
#         .execute()
#   )

( (silver_streamDF.withWatermark("timestamp", "1 hour").groupBy("gender").agg(avg("weight").alias("avg_weight")))
   .writeStream
   .trigger(processingTime='12 seconds')
   .outputMode("complete")\
   .option( "checkpointLocation", checkpoint_dir_gold)\
   .table("iot_event_gold")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_event_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data Skipping and ZORDER
# MAGIC 
# MAGIC Databricks Delta uses multiple mechanisms to speed up queries.
# MAGIC  
# MAGIC 
# MAGIC <b>Data Skipping</b> is a performance optimization that aims at speeding up queries that contain filters (WHERE clauses). 
# MAGIC 
# MAGIC As new data is inserted into a Databricks Delta table, file-level min/max statistics are collected for all columns (including nested ones) of supported types. Then, when there’s a lookup query against the table, Databricks Delta first consults these statistics in order to determine which files can safely be skipped.
# MAGIC 
# MAGIC <b>ZOrdering</b> Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to co-locate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Given a column that you want to perform ZORDER on, say `OrderColumn`, Delta
# MAGIC * Takes existing parquet files within a partition.
# MAGIC * Maps the rows within the parquet files according to `OrderColumn` using <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">this algorithm</a>.
# MAGIC * In the case of only one column, the mapping above becomes a linear sort.
# MAGIC * Rewrites the sorted data into new parquet files.
# MAGIC 
# MAGIC Note: In streaming, where incoming events are inherently ordered (more or less) by event time, use `ZORDER` to sort by a different column, say 'userID'.
# MAGIC 
# MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run a sample query
# MAGIC --
# MAGIC 
# MAGIC SELECT gender, avg(weight) as AVG_weight, avg(height) as AVG_height
# MAGIC FROM iot_event_silver
# MAGIC Group by gender
# MAGIC ORDER by gender DESC, AVG_weight ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Optimize and Z-order by 
# MAGIC --
# MAGIC 
# MAGIC OPTIMIZE iot_event_silver
# MAGIC ZORDER BY gender, height, weight;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run the same select query at higher performance
# MAGIC --
# MAGIC 
# MAGIC SELECT gender, avg(weight) as AVG_weight, avg(height) as AVG_height
# MAGIC FROM iot_event_silver
# MAGIC Group by gender
# MAGIC ORDER by gender DESC, AVG_weight ASC;

# COMMAND ----------

# DBTITLE 1,Stop all streams
for s in spark.streams.active:
  s.stop()

# COMMAND ----------


