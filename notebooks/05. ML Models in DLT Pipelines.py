# Databricks notebook source
# MAGIC %pip install mlflow
# MAGIC %pip install databricks
# MAGIC %pip install cffi==1.14.5
# MAGIC %pip install cloudpickle==1.6.0
# MAGIC %pip install databricks-automl-runtime==0.1.0
# MAGIC %pip install holidays==0.11.2
# MAGIC %pip install koalas==1.8.1
# MAGIC %pip install lightgbm==3.1.1
# MAGIC %pip install matplotlib==3.4.2
# MAGIC %pip install psutil==5.8.0
# MAGIC %pip install scikit-learn==0.24.1
# MAGIC %pip install simplejson==3.17.2
# MAGIC %pip install typing-extensions==3.7.4.3

# COMMAND ----------

import dlt
import mlflow
from pyspark.sql.functions import struct
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

# COMMAND ----------

@dlt.table(
name='sales_by_day_forecast'
)
def sales_by_day_forecast():
  import mlflow
  ## Get Model param
  model_name = "dlt_workshop_retail_forecast"
  model_uri = f"models:/{model_name}/2" ## New version developed on 9.1 LTS
  
  #logged_model = 'runs:/e4d61bc06e4b4b3fad5ce3709c457f6e/model'

  # Load model as a Spark UDF. Override result_type if the model does not return double values.
  loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type='double')
  
  source_table = dlt.read("sales_by_day")
  columns = list(source_table.columns)
  
  # Predict on a Spark DataFrame.
  output_df = source_table.withColumn('predictions', loaded_model(*columns))

  return output_df
