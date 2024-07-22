# Databricks notebook source
# DBTITLE 1,Generate SQL or Python code
# MAGIC %md
# MAGIC prompt:
# MAGIC show 10 most expensive products in table olostak.bank_data.transaction in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autocomplete code or queries
# MAGIC Type a comment, and press control + shift + space (Windows) or option + shift + space (Mac) to trigger an autocomplete suggestion.

# COMMAND ----------

# DBTITLE 0,Autocomplete code or queries
# Write code to reverse string. Than display the reversed string.


# COMMAND ----------

# DBTITLE 1,Transform code to another language
"""prompt: Transform code below in pandas to pyspark:
import numpy as np
import pandas as pd

df = pd.read_csv("/Volumes/olostak/workflows_test/data/ETL_TRANSACTION.csv")
df_quantity_sum = df.groupby('product_id')['product_quantity'].transform('sum')
df_quantity_sum.head()
"""

# COMMAND ----------

# DBTITLE 1,Explain code or query
# MAGIC %sql
# MAGIC SELECT product_id, product_quantity
# MAGIC FROM olostak.bank_data.transaction
# MAGIC ORDER BY product_quantity DESC
# MAGIC LIMIT 10
# MAGIC
# MAGIC --Prompt: Select query above

# COMMAND ----------

# DBTITLE 1,Fix issues
import pyspark.sql.functions as F

df = spark.range(5).select(F.col('id').alias('a'), F.col('id').alias('b'))

df.select('c').show()

# click Diagnose error
