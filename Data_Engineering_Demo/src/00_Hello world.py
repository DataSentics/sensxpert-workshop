# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

import os
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialization

# COMMAND ----------

spark: SparkSession = spark

# COMMAND ----------

data_path = "../_data/init_load"

# COMMAND ----------

product_catalog = "product_catalog_1.csv"
product_records = "product_records_1.csv"
sales_records = "sales_records_1.csv"

# COMMAND ----------


product_catalog_path = f"file:{os.getcwd()}/{data_path}/{product_catalog}"
product_records_path = f"file:{os.getcwd()}/{data_path}/{product_records}"
sales_records_path = f"file:{os.getcwd()}/{data_path}/{sales_records}"

# COMMAND ----------

print(product_catalog_path)
print(product_records_path)
print(sales_records_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load data

# COMMAND ----------

product_catalog_df = spark.read.format("csv").option("header", True).load(product_catalog_path)
product_records_df = spark.read.format("csv").option("header", True).load(product_catalog_path)
sales_records_df = spark.read.format("csv").option("header", True).load(sales_records_path)

# COMMAND ----------

product_catalog_df.show()
