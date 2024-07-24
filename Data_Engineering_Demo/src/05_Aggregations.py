# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

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

# MAGIC %md
# MAGIC ###gold sales_statistics

# COMMAND ----------

source_table = "silver_ss_sales_records"
table_name = "gold_ss_sales_statistincs"

sales_records_silver_df = spark.table(source_table)
df_sales_statistincs = create_sales_statistics_df(spark, sales_records_silver_df)
df_sales_statistincs.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

spark.table(table_name).display()

# COMMAND ----------


