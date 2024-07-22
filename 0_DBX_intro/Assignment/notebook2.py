# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
path = f"/FileStore/dbx-workshop/{username}/titanic"

# COMMAND ----------

df_titanic =(spark
      .read
      .format("delta")
      .load(path)
    )

display(df_titanic)


# COMMAND ----------

import pyspark.sql.functions as sf
df_grouped = (df_titanic.groupBy("Sex").agg(
      sf.avg("MonthlyIncome").alias("avg_income"),
      sf.max("Age").alias("max_age")))

display(df_grouped)
