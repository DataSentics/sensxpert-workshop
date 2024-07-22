# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark
# MAGIC
# MAGIC Python API for Spark - creates a logical plan on the **Driver** node and translates it to a physical plan in Scala for the **Worker** nodes.
# MAGIC
# MAGIC - Uses the **Builder design pattern** ([link](https://en.wikipedia.org/wiki/Builder_pattern))
# MAGIC - **Lazy evaluation** ([link](https://en.wikipedia.org/wiki/Lazy_evaluation#:~:text=In%20programming%20language%20theory%2C%20lazy,avoids%20repeated%20evaluations%20(sharing)) triggered by **Actions**
# MAGIC
# MAGIC - [Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
# MAGIC
# MAGIC <img src='https://spark.apache.org/docs/latest/img/cluster-overview.png'/>

# COMMAND ----------

# MAGIC %run ./Setup-datasets

# COMMAND ----------

print(f"Using database = '{username}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and display table

# COMMAND ----------

df_tips = spark.read.table(f"{username}.tips_table")

# COMMAND ----------

# MAGIC %md
# MAGIC **Display Triggers an action**

# COMMAND ----------

df = df_tips
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display schema

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate size
# MAGIC
# MAGIC **! Triggers an action**

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting columns

# COMMAND ----------

df_select = df.select("total_bill", "tip")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering rows
# MAGIC
# MAGIC #### SQL style

# COMMAND ----------

df_filter_sql = df_select.filter("total_bill > 20")

# COMMAND ----------

display(df_filter_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering rows
# MAGIC
# MAGIC #### PySpark Column style

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df_filter = df_select.filter(f.col("total_bill") > 20)

# COMMAND ----------

display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How did it find the column?

# COMMAND ----------

f.col("hello")

# COMMAND ----------

f.col("hello") > 10

# COMMAND ----------

f.col("hello") > (f.col("hi") * 2 - f.lit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering rows
# MAGIC
# MAGIC #### Multiple conditions
# MAGIC
# MAGIC Hello again `&` and `|`

# COMMAND ----------

condition = (f.col("total_bill") > 20) & (f.col("sex") == "Male") & ~(f.col("day") == "Sun")
print(condition)
df_complex_filter = df.filter(condition)
df_complex_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### TODO:
# MAGIC 1) Filter all rows where the `size` of the group is more than 2 and less than 6.
# MAGIC 2) Find out how many rows it is

# COMMAND ----------

# write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering NULLs

# COMMAND ----------

df_no_nulls = df.filter(f.col("total_bill").isNotNull())
df_no_nulls.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding columns

# COMMAND ----------

df = df.withColumn("group", f.col("size") > 2)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CASE WHEN

# COMMAND ----------

df = df.withColumn(
    "group",
    f.when(f.col("size") < 2, "alone")
    .when(f.col("size") == 2, "couple")
    .when(f.col("size") < 4, "small group")
    .otherwise("large group"),
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO:
# MAGIC Add a column called `weekend` which is `True` when `day` is `Sat` or `Sun` otherwise it is `False`.

# COMMAND ----------

# write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column

# COMMAND ----------

df = df.withColumnRenamed("group", "group_type")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop a column

# COMMAND ----------

df_dropped = df.drop("size", "smoker")
df_dropped.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining

# COMMAND ----------

df_air_quality_no2 = spark.read.table(f"{username}.air_quality_no2_table")
df_air_quality_no2.display()

# COMMAND ----------

df_air_quality_pm25 = spark.read.table(f"{username}.air_quality_pm25_table")
df_air_quality_pm25.display()

# COMMAND ----------

df_joined = df_air_quality_no2.join(
    df_air_quality_pm25.select("location", "date_utc", "parameter", "value"),
    on=["date_utc", "location"],
    how="inner",
)

# COMMAND ----------

df_joined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Aggregating

# COMMAND ----------

df.select(f.mean("tip"), f.max("tip"), f.min("tip"), f.percentile_approx("tip", 0.5).alias("median(tip)")).display()

# COMMAND ----------

df.groupby("time").mean().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO:
# MAGIC Groupby `sex`, calculate ration between average `tip` and average `total_bill` hint: `(avg(tip) / avg(total_bill))`

# COMMAND ----------

# write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complex queries using the builder syntax

# COMMAND ----------

# Everything is lazy here
df_agg = df_tips.filter(f.col("time") == "Dinner")
df_agg = df_agg.groupby("sex").agg(f.mean("tip").alias("average_tip"), f.mean("total_bill").alias("average_total_bill"))
df_agg = df_agg.withColumn("10_percent", 0.1 * f.col("average_total_bill"))
df_agg = df_agg.withColumn("tip_more_than_10_percent", f.col("average_tip") > f.col("10_percent"))

# COMMAND ----------

# Trigger computation
df_agg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect
# MAGIC
# MAGIC Getting data to the driver

# COMMAND ----------

df_agg.collect()[0][2]

# COMMAND ----------

for row in df_agg.collect():
    print(f"{row.sex} paid on average {row.average_total_bill} per dinner")

# COMMAND ----------

df_tips = spark.read.table(f"{username}.tips_table")
df_tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### toPandas

# COMMAND ----------

# pandas.core.frame.DataFrame
df_tips = spark.read.table(f"{username}.tips_table")
pdf = df_tips.toPandas()
pdf['new_column'] = "new column"
pdf['amount_doubled'] = pdf['total_bill']*2
pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### pandas API

# COMMAND ----------

# pyspark.pandas.frame.DataFrame
df_tips = spark.read.table(f"{username}.tips_table")
pdf = df_tips.pandas_api()
pdf['new_column'] = "new column"
pdf['amount_doubled'] = pdf['total_bill']*2
df = pdf.to_spark()
display(df)
