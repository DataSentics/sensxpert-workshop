# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
username = username + "_db"

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {username} CASCADE")

spark.sql(f"CREATE DATABASE {username}")

spark.sql(f"USE DATABASE {username}")

# COMMAND ----------

import os

source_tips = f"file:{os.getcwd()}/../Data/tips.csv"
source_iris = f"file:{os.getcwd()}/../Data/iris.csv"
source_air_quality_no2 = f"file:{os.getcwd()}/../Data/air_quality_no2_long.csv"
source_air_quality_pm25 = f"file:{os.getcwd()}/../Data/air_quality_pm25_long.csv"

tips = spark.read.option("header", "true").csv(source_tips)
iris = spark.read.option("header", "true").csv(source_iris)
air_quality_no2 = spark.read.option("header", "true").csv(source_air_quality_no2)
air_quality_pm25 = spark.read.option("header", "true").csv(source_air_quality_pm25)

tips.write.mode("overwrite").saveAsTable("tips_table")
iris.write.mode("overwrite").saveAsTable("iris_table")
air_quality_no2.write.mode("overwrite").saveAsTable("air_quality_no2_table")
air_quality_pm25.write.mode("overwrite").saveAsTable("air_quality_pm25_table")
