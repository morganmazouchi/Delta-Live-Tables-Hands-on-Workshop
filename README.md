# Intorduction: Delta-Live-Tables-Hands-on-Workshop
Welcome to the repository for the Databricks 1:M Delta Live Tables Workshop!

This repository contains the notebooks that are used in the workshop to demonstrate the use of Delta Live Tables to build simple, scalable , production-ready pipelines that provides built-in data quality controls and monitoring, data pipeline logging, data lineage tracking, automated pipeline orchestration, automatic Error Handling, advanced auto-scaling, change data capture (CDC) and advanced data engineering concepts (window functions and meta-programming) into a simple pipeline.
<img width="1279" alt="cdc_flow_new" src="https://user-images.githubusercontent.com/85911675/178178051-820a22ae-4b9b-4394-9754-7b9f245d552a.png">



![Screen Shot 2022-07-10 at 7 18 23 PM](https://user-images.githubusercontent.com/85911675/178177635-249deecd-9261-4586-8032-565c534b5e9f.png)



# Reading Resources

See below links for more documentation:
* [How to Process IoT Device JSON Data Using Apache Spark Datasets and DataFrames](https://databricks.com/blog/2016/03/28/how-to-process-iot-device-json-data-using-apache-spark-datasets-and-dataframes.html)
* [Spark Structure Streaming](https://databricks.com/blog/2016/07/28/structured-streaming-in-apache-spark.html)
* [Beyond Lambda](https://databricks.com/discover/getting-started-with-delta-lake-tech-talks/beyond-lambda-introducing-delta-architecture)
* [Delta Lake Docs](https://docs.databricks.com/delta/index.html)
* [Medallion Architecture](https://databricks.com/solutions/data-pipelines)
* [Cost Savings with the Medallion Architecture](https://techcommunity.microsoft.com/t5/analytics-on-azure/how-to-reduce-infrastructure-costs-by-up-to-80-with-azure/ba-p/1820280)
* [Change Data Capture Streams with the Medallion Architecture](https://databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html)



# Workshop Flow

The workshop consists of 4 interactive sections that are separated by 4 notebooks located in the notebooks folder in this repository. Each is run sequentially as we explore the abilities of the lakehouse from data ingestion, data curation, and performance optimizations
|Notebook|Summary|
|--------|-------|
|`01-Structured Streaming with Databricks Delta Tables`|Processing and ingesting data at scale utilizing databricks tunables and the medallion architecture|
|`02-Orchestrating with Delta Live Tables`|Changing Spark Properties, Configuring Table Properties, Optimization of Tables, Combining Batch and Incremental Tables|
|`03. Implement CDC In DLT Pipeline: Change Data Capture (Python)`|Implementing Change Data Capture in DLT pipelines for accessing to fresh data|
|`04: Meta-programming`|Examples of metaprogramming in DLT When to use/problems is solved How to configure|
|`05: ML Models in DLT Pipelines`|Example of integratation of ML models with DLT pipelines|



# Setup / Requirements

This workshop requires a running Databricks workspace. If you are an existing Databricks customer, you can use your existing Databricks workspace. Otherwise, the notebooks in this workshop have been tested to run on [Databricks Community Edition](https://databricks.com/product/faq/community-edition) as well.

## DBR Version

The features used in this workshop require `DBR 9.1 LTS`+.

## Repos

If you have repos enabled on your Databricks workspace. You can directly import this repo and run the notebooks as is and avoid the DBC archive step.

## DBC Archive

Download the DBC archive from releases and import the archive into your Databricks workspace.
