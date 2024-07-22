# Databricks notebook source
import pyspark.sql.functions as f
import os

# COMMAND ----------

catalog_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
catalog_name = catalog_name.replace("-", "_")
catalog_name

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.streaming_workshop")

# COMMAND ----------

initDF = (spark.readStream
                            .format("rate")
                            .option("rowsPerSecond", 10)
                            .option("numPartitions", 1)
                            .load())

resultDF = (initDF
                    .withColumn("restaurant_id", f.round(f.rand() * 100))
                    .withColumn("total_bill", f.rand()*30)
                    .withColumn("tip", (f.rand()*0.2)*f.col("total_bill"))
                    .withColumn(
                            "sex",
                            f.array(
                                f.lit("Female"),
                                f.lit("Male"),
                            ).getItem(
                                (f.rand()*2).cast("int")
                            )
                    )
                    .withColumn(
                            "smoker",
                            f.array(
                                f.lit("No"),
                                f.lit("Yes"),
                            ).getItem(
                                (f.rand()*2).cast("int")
                            )
                    )
                    .withColumn(
                            "day",
                            f.array(
                                f.lit("Mon"),
                                f.lit("Tue"),
                                f.lit("Wed"),
                                f.lit("Thu"),
                                f.lit("Fri"),
                                f.lit("Sat"),
                                f.lit("Sun"),
                            ).getItem(
                                (f.rand()*7).cast("int")
                            )
                    )
                    .withColumn(
                            "time",
                            f.array(
                                f.lit("Breakfast"),
                                f.lit("Lunch"),
                                f.lit("Dinner"),
                            ).getItem(
                                (f.rand()*3).cast("int")
                            )
                    )
                    .withColumn(
                        "size",
                        f.round(f.rand()*6,0)
                    )
                    .drop("value")
                    )

(
    resultDF.writeStream
                        .option("checkpointLocation", "/tmp/streaming_workshop/tips/_checkpoints/")
                        .toTable(f"{catalog_name}.streaming_workshop.tips")
)

# COMMAND ----------

user_folder = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
catalog_name = user_folder.split('@')[0].replace("-", "_")
restaurants =spark.read.format("csv").option("header","true").load(f"file:/Workspace/Repos/{user_folder}/odap-workshops/streaming_workshop/data/restaurant_names.csv")
restaurants.write.mode('overwrite').saveAsTable(f"{catalog_name}.streaming_workshop.restaurants")
