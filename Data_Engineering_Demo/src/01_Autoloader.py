# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from helpers import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialization

# COMMAND ----------

spark: SparkSession = spark

# COMMAND ----------

default_catalog = "ls_dbx_workshop"
default_schema = "day_one"

dbutils.widgets.text("catalog", default_catalog, "Provide real name of the used catalog.")
dbutils.widgets.text("schema", default_schema, "Provide real name of the used schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_ss_product_catalog;
# MAGIC DROP TABLE IF EXISTS bronze_ss_product_records;
# MAGIC DROP TABLE IF EXISTS bronze_ss_sales_records;

# COMMAND ----------

base_path = os.environ["DATA_LOCATION"]

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze product_catalog

# COMMAND ----------

data_location = os.path.join(base_path, "_product_catalog", "*")
table_name = "bronze_ss_product_catalog"
schema_location = f"{os.environ['SCHEMAPATH']}/{catalog}_{schema}_{table_name}"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

product_catalog_bronze_df = create_bronze_df(spark, data_location, schema_location)
product_catalog_bronze_query = (
    product_catalog_bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1)
    .queryName(table_name + "_query")
    .trigger(once=True)
    .toTable(table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze production_records

# COMMAND ----------

data_location = os.path.join(base_path, "_production_records", "*")
table_name = "bronze_ss_production_records"
schema_location = f"{os.environ['SCHEMAPATH']}/{catalog}_{schema}_{table_name}"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

production_records_bronze_df = create_bronze_df(spark, data_location, schema_location)
production_records_bronze_query = (
    production_records_bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1)
    .queryName(table_name + "_query")
    .toTable(table_name)
)

# COMMAND ----------

# production_records_silver_df.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze sales_records

# COMMAND ----------

data_location = f"{base_path}/_sales_records/*"
table_name = "bronze_ss_sales_records"
schema_location = f"{os.environ['SCHEMAPATH']}/{catalog}_{schema}_{table_name}"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

sales_records_bronze_df = create_bronze_df(spark, data_location, schema_location)
sales_records_bronze_query = (
    sales_records_bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1)
    .queryName(table_name + "_query")
    .trigger(once=True)
    .toTable(table_name)
)
