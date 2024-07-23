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

catalog = "ls_dbx_workshop"
schema = "day_one"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_ss_product_catalog;
# MAGIC DROP TABLE IF EXISTS silver_ss_product_catalog_invalid;
# MAGIC DROP TABLE IF EXISTS silver_ss_production_records;
# MAGIC DROP TABLE IF EXISTS silver_ss_production_records_invalid;
# MAGIC DROP TABLE IF EXISTS silver_ss_sales_records;
# MAGIC DROP TABLE IF EXISTS silver_ss_sales_records_invalid;

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver product_catalog

# COMMAND ----------

source_table = "bronze_ss_product_catalog"
table_name = "silver_ss_product_catalog"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

product_catalog_bronze_df = spark.readStream.table(source_table)
product_catalog_silver_df = create_silver_df(spark, product_catalog_bronze_df, ["product_id"])
product_catalog_silver_valid_query = (
    product_catalog_silver_df
    .where(col("is_quarantined") == False)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .queryName(table_name + "_query")

    .trigger(once=True)
    .toTable(table_name)
)

product_catalog_silver_invalid_query = (
    product_catalog_silver_df
    .where(col("is_quarantined") == True)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path + "_invalid")
    .queryName(table_name + "_invalid_query")
    .trigger(once=True)
    .toTable(table_name + "_invalid")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver production_records

# COMMAND ----------

source_table = "bronze_ss_production_records"
table_name = "silver_ss_production_records"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

production_records_bronze_df = spark.readStream.table(source_table)
production_records_silver_df = create_silver_df(spark, production_records_bronze_df, ["record_id", "product_id"])
production_records_silver_valid_query = (
    production_records_silver_df
    .where(col("is_quarantined") == False)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .queryName(table_name + "_query")
    .trigger(once=True)
    .toTable(table_name)
)

production_records_silver_invalid_query = (
    production_records_silver_df
    .where(col("is_quarantined") == True)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path + "_invalid")
    .queryName(table_name + "_invalid_query")
    .trigger(once=True)
    .toTable(table_name + "_invalid")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver sales_records

# COMMAND ----------

source_table = "bronze_ss_sales_records"
table_name = "silver_ss_sales_records"
checkpoint_path = f"{os.environ['CHECKPOINTPATH']}/{catalog}_{schema}_{table_name}"

sales_records_bronze_df = spark.readStream.table(source_table)
sales_records_silver_df = create_silver_df(spark, sales_records_bronze_df, ["sales_id", "product_id"])
sales_records_silver_valid_query = (
    sales_records_silver_df
    .where(col("is_quarantined") == False)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .queryName(table_name + "_query")
    .trigger(once=True)
    .toTable(table_name)
)

sales_records_silver_invalid_query(
    sales_records_silver_df
    .where(col("is_quarantined") == True)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path + "_invalid")
    .queryName(table_name + "_invalid_query")
    .trigger(once=True)
    .toTable(table_name + "_invalid")
)
