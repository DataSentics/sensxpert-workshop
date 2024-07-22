# Databricks notebook source
dbutils.widgets.text("catalog_name", "")

# COMMAND ----------

account = spark.read.format("csv").option("header",True).load("/Volumes/olostak/workflows_test/data/ETL_CORP_ACCOUNT.csv")
user = spark.read.format("csv").option("header",True).load("/Volumes/olostak/workflows_test/data/ETL_CORP_USER.csv")
product = spark.read.format("csv").option("header",True).load("/Volumes/olostak/workflows_test/data/ETL_PRODUCT.csv")
transaction = spark.read.format("csv").option("header",True).load("/Volumes/olostak/workflows_test/data/ETL_TRANSACTION.csv")
account.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog_name}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bank_data

# COMMAND ----------

account.write.mode("overwrite").saveAsTable("bank_data.account")
user.write.mode("overwrite").saveAsTable("bank_data.user")
product.write.mode("overwrite").saveAsTable("bank_data.product")
transaction.write.mode("overwrite").saveAsTable("bank_data.transaction")
