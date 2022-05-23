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

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup a data set 
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.rand;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC 
# MAGIC // get the schema from the csv files
# MAGIC val file_schema : StructType = spark.read
# MAGIC                                 .format("csv")
# MAGIC                                 .option("header", true)
# MAGIC                                 .option("inferSchema", true)
# MAGIC                                 .load("/databricks-datasets/online_retail/data-001/data.csv")
# MAGIC                                 .limit(10)
# MAGIC                                 .schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark Structured Streaming                                                   
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/01/cloudtrail-structured-streaming-model.png"> 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.{to_timestamp};
# MAGIC 
# MAGIC /*
# MAGIC   Read Online Retail Data stream of data
# MAGIC */
# MAGIC 
# MAGIC // get the schema from the csv files
# MAGIC val InventoryData : DataFrame = spark.readStream
# MAGIC                                        .format("csv")
# MAGIC                                        .option("header", true)
# MAGIC                                        .schema(file_schema)
# MAGIC                                        .load("databricks-datasets/online_retail/data-001/*.csv")
# MAGIC                                        .withColumn("InvoiceDate", to_timestamp($"InvoiceDate", "MM-yyyy"));                  //parse timestamp
# MAGIC 
# MAGIC 
# MAGIC // See the dataframe
# MAGIC display(InventoryData);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta With Checkpointing
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/checkpoint.png?raw=true"> 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Drop streaming tables if they exist
# MAGIC --
# MAGIC 
# MAGIC drop table if exists InventoryData;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup checkpoint directory
# MAGIC */
# MAGIC 
# MAGIC val checkpointDir : String = "/tmp/delta-stream_dltworkshop/7";

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Write the stream to delta lake
# MAGIC */
# MAGIC 
# MAGIC val inventorystream = InventoryData.writeStream
# MAGIC                                    .format("delta")
# MAGIC                                    .outputMode("append")
# MAGIC                                    .option("header", true)
# MAGIC                                    .option("checkpointLocation", checkpointDir)
# MAGIC                                    .table("InventoryData")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe table extended InventoryData;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/inventorydata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventorydata;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Drop streaming tables if they exist
# MAGIC --
# MAGIC 
# MAGIC drop table if exists inventorydata_silver;
# MAGIC drop table if exists inventorydata_silver_updates;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Creating Bronze Tables

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import DataFrame
# MAGIC 
# MAGIC """
# MAGIC Do some data deduplication on ingestion streams
# MAGIC """
# MAGIC 
# MAGIC df_bronze : DataFrame = spark.readStream\
# MAGIC                              .format("delta")\
# MAGIC                              .table("inventorydata")
# MAGIC   
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tuning with Optimize and Zorder
# MAGIC 
# MAGIC Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to co-locate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC 
# MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run a sample query
# MAGIC --
# MAGIC 
# MAGIC SELECT Country, avg(UnitPrice) as AVG_unitprice
# MAGIC FROM inventorydata
# MAGIC WHERE Quantity >5 
# MAGIC Group by Country
# MAGIC ORDER by Country DESC, AVG_unitprice ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Optimize and Z-order by 
# MAGIC --
# MAGIC 
# MAGIC OPTIMIZE inventorydata
# MAGIC ZORDER BY Country, UnitPrice;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run the same select query at higher performance
# MAGIC --
# MAGIC 
# MAGIC SELECT Country, avg(UnitPrice) as AVG_unitprice
# MAGIC FROM inventorydata
# MAGIC WHERE Quantity >5 
# MAGIC Group by Country
# MAGIC ORDER by Country DESC, AVG_unitprice ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating Silver Tables

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Deduplicate Bronze level data
# MAGIC """
# MAGIC 
# MAGIC df_silver : DataFrame = df_bronze.distinct()
# MAGIC   
# MAGIC display(df_silver)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_1 : str = "/tmp/delta-stream_dltworkshop/silver_check_5"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write deduplicated silver streams 
# MAGIC """
# MAGIC 
# MAGIC df_silver.writeStream\
# MAGIC          .format("delta")\
# MAGIC          .option("checkpointLocation", checkpoint_dir_1)\
# MAGIC          .table("inventorydata_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED inventorydata_silver;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/inventorydata_silver

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Specify a checkpoint directory for writing out a stream
# MAGIC """
# MAGIC 
# MAGIC checkpoint_dir_2 : str = "/tmp/delta-stream_dltworkshop/silverupdate_check_6"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write deduplicated silver streams 
# MAGIC """
# MAGIC 
# MAGIC df_silver.na.drop()\
# MAGIC          .writeStream\
# MAGIC          .format("delta")\
# MAGIC          .option("checkpointLocation", checkpoint_dir_2)\
# MAGIC          .table("inventorydata_silver_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED inventorydata_silver_updates;

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/inventorydata_silver_updates
