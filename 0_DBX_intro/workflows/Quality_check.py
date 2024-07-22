# Databricks notebook source
df = spark.read.table("olostak.bank_data.user_product")
max_price = df.select("product_unit_price").rdd.max()[0]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'max_price', value = max_price)
