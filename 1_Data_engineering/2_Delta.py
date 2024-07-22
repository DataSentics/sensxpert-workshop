# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Delta format
# MAGIC - ACID Transactions
# MAGIC - time travel/historization
# MAGIC - schema evolution
# MAGIC - caching
# MAGIC - Incremental data processing
# MAGIC
# MAGIC [Introduction](https://docs.delta.io/latest/delta-intro.html)
# MAGIC <br/>
# MAGIC <img src='https://miro.medium.com/v2/resize:fit:1400/1*L1yM-xDaIUNMSIoanVr8BA.png' width=500 height=500/>
# MAGIC
# MAGIC Recammandation is that one partition should have around 1 GB.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la ../Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manually create Delta table from CSV

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
database = username + "_db"

# COMMAND ----------

import os

df = spark.read.option("header", "true").csv(
    f"file:{os.getcwd()}/../Data/tips.csv"
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Without partitioning

# COMMAND ----------

(
    df.write.mode("append")
    .format("delta")
    .saveAsTable(f"hive_metastore.demo.tips")
)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -a /dbfs/user/hive/warehouse/demo.db/tips

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/demo.db/diabetes/part-00000-01cd7cc5-0a27-4646-9723-7d20ce975ac9-c000.snappy.parquet")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -a /dbfs/user/hive/warehouse/demo.db/diabetes/_delta_log

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/demo.db/diabetes/_delta_log/00000000000000000001.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### With partitioning by day

# COMMAND ----------

(
    df.write.mode("overwrite")
    .partitionBy("day")
    .format("delta")
    .saveAsTable("hive_metastore.demo.tips_partitioned")
)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -a /dbfs/user/hive/warehouse/demo.db/tips_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select from Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now we can query the delta table as SQL table
# MAGIC SELECT *
# MAGIC FROM hive_metastore.demo.tips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tips WHERE size = 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED tips

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tips

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{username}.db/tips")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{username}.db/tips/_delta_log")

# COMMAND ----------

display(
    spark.read.format("json").load(
        f"dbfs:/user/hive/warehouse/{username}.db/tips/_delta_log/00000000000000000000.json"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE tips TO VERSION AS OF 0

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge

# COMMAND ----------

spark.createDataFrame(
    [
        [20.65, 3.35, "Male", "No", "Sat", "Dinner", 3, None, True],
        [9.78, 1.73, "Male", "No", "Thur", "Lunch", 2, 12.2, False],
        [1.11, 0.11, "Male", "No", "Thur", "Lunch", 2, None, False],
    ],
    schema="total_bill double,tip double,sex string,smoker string,day string,time string,size string, new_total_bill double, delete_row boolean",
).createOrReplaceTempView("tips_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tips as target USING tips_update as source ON source.total_bill = target.total_bill
# MAGIC and source.tip = target.tip
# MAGIC and source.sex = target.sex
# MAGIC and source.smoker = target.smoker
# MAGIC and source.time = target.time
# MAGIC and source.size = target.size
# MAGIC when matched
# MAGIC and source.delete_row then delete
# MAGIC when matched then
# MAGIC update
# MAGIC set
# MAGIC   target.total_bill = source.new_total_bill
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tips where total_bill = 20.65
