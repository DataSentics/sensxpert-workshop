# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

import os
import sys
sys.path.append(os.getcwd())

# COMMAND ----------

import dlt
from pyspark.sql import SparkSession
from helpers import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialization

# COMMAND ----------

spark: SparkSession = spark

# COMMAND ----------

account_key = dbutils.secrets.get("ls_workshop", "access_key")
spark.conf.set("fs.azure.account.key.lsworkshopdayone.dfs.core.windows.net", account_key)

# COMMAND ----------

base_path = spark.conf.get("base_path", "abfss://landingzone@lsworkshopdayone.dfs.core.windows.net/")
catalog = spark.conf.get("catalog", "ls_dbx_workshop")
schema = spark.conf.get("schema", "day_one")

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze product_catalog

# COMMAND ----------

@dlt.table
def bronze_dlt_product_catalog():
    data_location = os.path.join(base_path, "_product_catalog", "*")
    
    df_bronze = create_bronze_df(spark, data_location)
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze production_records

# COMMAND ----------

@dlt.table
def bronze_dlt_production_records():
    data_location = os.path.join(base_path, "_production_records", "*")

    df_bronze = create_bronze_df(spark, data_location)
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze sales_records

# COMMAND ----------

@dlt.table(temporary = True)
def bronze_dlt_sales_records():
    data_location = f"{base_path}/_sales_records/*"

    df_bronze = create_bronze_df(spark, data_location)
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver product_catalog

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_dlt_product_catalog",
    table_properties={"quality": "silver"}
)

# COMMAND ----------

dlt.apply_changes(
    target="silver_dlt_product_catalog",
    source="bronze_dlt_product_catalog",
    keys=["product_id"],
    sequence_by="ingest_timestamp",
)

# COMMAND ----------

@dlt.table(temporary=True)
def silver_dlt_product_catalog_tmp():
    source_table = "silver_dlt_product_catalog"

    product_catalog_silver_tmp_df = dlt.readStream(source_table)
    product_catalog_silver_df = create_silver_df(spark, product_catalog_silver_tmp_df, ["product_id"])
    return product_catalog_silver_df

# COMMAND ----------

@dlt.table
def silver_dlt_product_catalog_valid():
    source_table = "silver_dlt_product_catalog_tmp"

    product_catalog_silver_df = dlt.readStream(source_table)
    df_valid = product_catalog_silver_df.where(col("is_quarantined") == False)
    return df_valid

# COMMAND ----------

@dlt.table
def silver_dlt_product_catalog_invalid():
    source_table = "silver_dlt_product_catalog_tmp"

    product_catalog_silver_df = dlt.readStream(source_table)
    df_invalid = product_catalog_silver_df.where(col("is_quarantined") == True)
    return df_invalid

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver production_records

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_dlt_production_records",
    table_properties={"quality": "silver"}
)

# COMMAND ----------

dlt.apply_changes(
    target="silver_dlt_production_records",
    source="bronze_dlt_production_records",
    keys=["record_id", "product_id"],
    sequence_by="ingest_timestamp",
)

# COMMAND ----------

@dlt.table(temporary=True)
def silver_dlt_production_records_tmp():
    source_table = "silver_dlt_production_records"

    production_records_silver_tmp_df = dlt.readStream(source_table)
    production_records_silver_df = create_silver_df(spark, production_records_silver_tmp_df, ["record_id", "product_id"])
    return production_records_silver_df

# COMMAND ----------

@dlt.table
def silver_dlt_production_records_valid():
    source_table = "silver_dlt_production_records_tmp"

    production_records_silver_df = dlt.readStream(source_table)
    df_valid = production_records_silver_df.where(col("is_quarantined") == False)
    return df_valid

# COMMAND ----------

@dlt.table
def silver_dlt_production_records_invalid():
    source_table = "silver_dlt_production_records_tmp"

    production_records_silver_df = dlt.readStream(source_table)
    df_invalid = production_records_silver_df.where(col("is_quarantined") == True)
    return df_invalid

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver sales_records

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_dlt_sales_records",
    table_properties={"quality": "silver"}
)

# COMMAND ----------

dlt.apply_changes(
    target="silver_dlt_sales_records",
    source="bronze_dlt_sales_records",
    keys=["sales_id", "product_id"],
    sequence_by="ingest_timestamp",
)

# COMMAND ----------

@dlt.table(temporary=True)
def silver_dlt_sales_records_tmp():
    source_table = "silver_dlt_sales_records"

    sales_records_silver_tmp_df = dlt.readStream(source_table)
    sales_records_silver_df = create_silver_df(spark, sales_records_silver_tmp_df, ["sales_id", "product_id"])
    return sales_records_silver_df

# COMMAND ----------

@dlt.table
def silver_dlt_sales_records_valid():
    source_table = "silver_dlt_sales_records_tmp"

    sales_records_silver_df = dlt.readStream(source_table)
    df_valid = sales_records_silver_df.where(col("is_quarantined") == False)
    return df_valid

# COMMAND ----------

@dlt.table
def silver_dlt_sales_records_invalid():
    source_table = "silver_dlt_sales_records_tmp"

    sales_records_silver_df = dlt.readStream(source_table)
    df_invalid = sales_records_silver_df.where(col("is_quarantined") == True)
    return df_invalid

# COMMAND ----------

# MAGIC %md
# MAGIC ###gold sales_statistincs

# COMMAND ----------

@dlt.table
def gold_dlt_sales_statistics():
    source_table = "silver_dlt_sales_records_valid"

    sales_records_silver_df = dlt.read(source_table)
    df_sales_statistincs = create_sales_statistics_df(spark, sales_records_silver_df)
    return df_sales_statistincs
