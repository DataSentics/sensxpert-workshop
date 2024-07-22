# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Structured Streaming Workshop
# MAGIC
# MAGIC [Guide in Spark docummentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
# MAGIC
# MAGIC Content:
# MAGIC 1) Stateless operations
# MAGIC 2) Checkpoints
# MAGIC 3) Triggers
# MAGIC 4) Statefull operations
# MAGIC 5) Watermarking
# MAGIC 6) Metrics

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as T
import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from datetime import datetime
from delta.tables import *
import json
from pyspark.sql.streaming import StreamingQueryListener

# get name of your catalog
catalog_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .split("@")[0]
)
catalog_name = catalog_name.replace("-", "_")
catalog_name

# COMMAND ----------

# read streaming table as static table
tips_static = spark.read.table(f"{catalog_name}.streaming_workshop.tips")
tips_static.count()

# COMMAND ----------

# read streaming table as stream
tips_stream = spark.readStream.table(f"{catalog_name}.streaming_workshop.tips")
display(tips_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stateless operations
# MAGIC
# MAGIC * filter
# MAGIC * projection/select
# MAGIC * udf
# MAGIC * forEachBatch
# MAGIC * join with static table

# COMMAND ----------

# filter
tips_stream_lunch = tips_stream.filter(f.col("time") == "Lunch")

# udf
# When you use an UDF, Spark has to serialize/deserialize the data representation from Spark to Python types and viceversa, so there is an additional cost, but it does not mean that you have to avoid them, sometimes they are very useful.
@udf
def to_upper(s):
    if s is not None:
        return s.upper()


# select
tips_stream_lunch = tips_stream_lunch.select(
    "timestamp", "restaurant_id", "total_bill", to_upper("day").alias("day"), "time"
)
display(tips_stream_lunch)

# COMMAND ----------

# join static table
restaurants = spark.read.table(f"{catalog_name}.streaming_workshop.restaurants")

restaurant_incomes = tips_stream_lunch.join(
    restaurants, tips_stream_lunch.restaurant_id == restaurants.id
).drop("id")

display(restaurant_incomes)

# COMMAND ----------

restaurants.persist()


def update_restaurants(batch_df, batch_id):
    batch_df.unpersist()
    batch_df = spark.read.table(f"{catalog_name}.streaming_workshop.restaurants")
    batch_df.persist()
    print(f"Refreshing static restaurants")


restaurants_clock = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
    .selectExpr("CAST(value as LONG) as trigger")
)

# Update table each 2 hours
(
    restaurants_clock.writeStream.outputMode("append")
    .foreachBatch(update_restaurants)
    .queryName("RefreshRestaurants")
    .trigger(processingTime="10 minutes")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write stream, trigger and checkpoints
# MAGIC #### Checkpoints
# MAGIC
# MAGIC A checkpoint helps build fault-tolerant and resilient Spark applications. In Spark Structured Streaming, it maintains intermediate state on HDFS compatible file systems to recover from failures. To specify the checkpoint in a streaming query, we use the checkpointLocation parameter.
# MAGIC
# MAGIC It is necessary to clean unneeded checkpoints, so it is not growing.
# MAGIC
# MAGIC `'spark.cleaner.referenceTracking.cleanCheckpoints', 'true'`

# COMMAND ----------

query = restaurant_incomes.writeStream.option(
    "checkpointLocation",
    f"/tmp/streaming_workshop/{catalog_name}/restaurant_lunch_incomess/_checkpoints/",
).toTable(f"{catalog_name}.streaming_workshop.restaurant_lunch_incomes")

# COMMAND ----------

# query.stop()

# COMMAND ----------

dbutils.fs.ls(
    f"/tmp/streaming_workshop/{catalog_name}/restaurant_lunch_incomess/_checkpoints/offsets/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trigger
# MAGIC
# MAGIC The trigger settings of a streaming query define the timing of streaming data processing, whether the query is going to be executed as micro-batch query with a fixed batch interval or as a continuous processing query. If no trigger setting is explicitly specified, then by default, the query will be executed in micro-batch mode, where micro-batches will be generated as soon as the previous micro-batch has completed processing.
# MAGIC
# MAGIC
# MAGIC ##### Fixed interval microbatches
# MAGIC
# MAGIC The query will be executed with micro-batches mode, where micro-batches will be kicked off at the user-specified intervals.
# MAGIC * If the previous micro-batch completes within the interval, then the engine will wait until the interval is over before kicking off the next micro-batch.
# MAGIC * If the previous micro-batch takes longer than the interval to complete (i.e. if an interval boundary is missed), then the next micro-batch will start as soon as the previous one completes (i.e., it will not wait for the next interval boundary).
# MAGIC * If no new data is available, then no micro-batch will be kicked off.
# MAGIC
# MAGIC ##### One-time microbatch
# MAGIC
# MAGIC The query will execute only one micro-batch to process all the available data and then stop on its own. This is useful in scenarios you want to periodically spin up a cluster, process everything that is available since the last period, and then shutdown the cluster. In some case, this may lead to significant cost savings. Note that this trigger is deprecated and users are encouraged to migrate to Available-now micro-batch, as it provides the better guarantee of processing, fine-grained scale of batches, and better gradual processing of watermark advancement including no-data batch.
# MAGIC
# MAGIC ##### Available-now microbatch
# MAGIC
# MAGIC Similar to queries one-time micro-batch trigger, the query will process all the available data and then stop on its own. The difference is that, it will process the data in (possibly) multiple micro-batches based on the source options (e.g. maxFilesPerTrigger for file source), which will result in better query scalability.
# MAGIC * This trigger provides a strong guarantee of processing: regardless of how many batches were left over in previous run, it ensures all available data at the time of execution gets processed before termination. All uncommitted batches will be processed first.
# MAGIC * Watermark gets advanced per each batch, and no-data batch gets executed before termination if the last batch advances the watermark. This helps to maintain smaller and predictable state size and smaller latency on the output of stateful operators.

# COMMAND ----------

# Default trigger (runs micro-batch as soon as it can)
(restaurant_incomes.writeStream.format("console").queryName("default").start())

# ProcessingTime trigger with two-seconds micro-batch interval
(
    restaurant_incomes.writeStream.format("console")
    .trigger(processingTime="5 minutes")
    .queryName("fixed_interval")
    .start()
)

# One-time trigger (Deprecated, encouraged to use Available-now trigger)
(
    restaurant_incomes.writeStream.format("console")
    .trigger(once=True)
    .queryName("once")
    .start()
)

# Available-now trigger
(
    restaurant_incomes.writeStream.format("console")
    .trigger(availableNow=True)
    .queryName("available_now")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge - aggregation over all data

# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("restaurant_id", T.IntegerType(), True),
        T.StructField("income", T.DoubleType(), True),
    ]
)

spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.streaming_workshop.incomes"
)


def func(batch_df, batch_id):
    batch_df = batch_df.groupBy("restaurant_id").agg(
        f.sum("total_bill").alias("income")
    )

    incomes = DeltaTable.forName(spark, f"{catalog_name}.streaming_workshop.incomes")
    (
        incomes.alias("source")
        .merge(
            source=batch_df.alias("updates"),
            condition="source.restaurant_id = updates.restaurant_id",
        )
        .whenMatchedUpdate(set={"source.income": "source.income + updates.income"})
        .whenNotMatchedInsertAll()
        .execute()
    )


(
    tips_stream_lunch.writeStream.foreachBatch(func)
    .outputMode("update")
    .option(
        "checkpointLocation",
        f"/tmp/streaming_workshop/{catalog_name}/income/_checkpoints",
    )
    .start()
)

# COMMAND ----------

incomes = spark.read.table(f"{catalog_name}.streaming_workshop.incomes")
display(incomes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statefull operations
# MAGIC * window operations
# MAGIC * dropDuplicates
# MAGIC * flatMapGroupWhiteState
# MAGIC * Stream-stream joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermarking
# MAGIC
# MAGIC Watermarking in structured streaming is a technique that ensures the correctness and consistency of event time processing by defining a threshold for late-arriving events.
# MAGIC
# MAGIC * The watermark specifies the point after which Spark assumes there will be no more data with earlier timestamps.
# MAGIC * Watermarking is typically used with window-based operations like aggregations or joins.
# MAGIC * It prevents issues such as unbounded memory usage and incorrect aggregations due to late data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows opearations
# MAGIC
# MAGIC Windows are a mechanism for calculating aggregations over subsets of data specified by a time range.
# MAGIC
# MAGIC "Don't use aggregation over all data!!! It is not computationally possible."
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ![image](https://raw.githubusercontent.com/DataSentics/odap-workshops/main/streaming_workshop/data/windows.PNG)
# MAGIC
# MAGIC Example with watermarking:
# MAGIC
# MAGIC ![image](https://raw.githubusercontent.com/DataSentics/odap-workshops/main/streaming_workshop/data/watermarking.PNG)
# MAGIC
# MAGIC window function parameters:
# MAGIC * timeColumn
# MAGIC * windowDuration
# MAGIC * slideDuration=None
# MAGIC * startTime=None

# COMMAND ----------

sum_of_bills = (
    tips_stream.withWatermark("timestamp", "10 minutes")
    .groupBy(
        f.window(tips_stream.timestamp, "10 minutes", "5 minutes"),
        tips_stream.restaurant_id,
    )
    .agg(f.sum(tips_stream.total_bill).alias("income"))
)

display(sum_of_bills)

# COMMAND ----------

# MAGIC %md
# MAGIC #### dropDuplicates
# MAGIC
# MAGIC The example below adds duplicates to the stream.
# MAGIC
# MAGIC 1) run it first with the duplicates
# MAGIC 2) clear checkpoints
# MAGIC 3) uncommend dropDuplicates and rerun the cell

# COMMAND ----------

# dropDuplicates
# Recommanded way is to implement dropDuplicates with MERGE to ensure that the duplicate record does not come out of the watermark.

# create artificial duplicates
duplicates = (
    tips_stream.withColumn("drop", f.when(f.rand() > 0.8, False).otherwise(True))
    .filter(f.col("drop"))
    .drop("drop")
)

tips_with_duplicates = tips_stream.unionByName(duplicates)

# tips_with_duplicates = tips_with_duplicates.withWatermark(
#    "timestamp", "10 minutes"
# ).dropDuplicates()

(
    tips_with_duplicates.writeStream.option(
        "checkpointLocation",
        f"/tmp/streaming_workshop/{catalog_name}/duplicites/_checkpoints/",
    ).toTable(f"{catalog_name}.streaming_workshop.duplicites")
)

# COMMAND ----------

df = spark.read.table(f"{catalog_name}.streaming_workshop.duplicites")
print(f"Duplicits: {(df.count() - df.dropDuplicates().count())}")

# COMMAND ----------

dbutils.fs.rm(f"/tmp/streaming_workshop/{catalog_name}/duplicites/_checkpoints/", True)
spark.sql(f"DROP TABLE {catalog_name}.streaming_workshop.duplicites")

# COMMAND ----------

# flatMapGroupWhiteState
from typing import Tuple, Iterator


def func(
    key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        (restaurant_id,) = key
        (count,) = state.get
        state.remove()
        yield pd.DataFrame({"restaurant": [restaurant_id], "count": [count]})
    else:
        # Aggregate the number of words.
        count = sum(map(lambda pdf: len(pdf), pdfs))
        if state.exists:
            (old_count,) = state.get
            count += old_count
        state.update((count,))
        # Set the timeout as 10 seconds.
        state.setTimeoutDuration(10000)
        yield pd.DataFrame()


# Group the data by word, and compute the count of each group
output_schema = "restaurant LONG, count LONG"
state_schema = "count LONG"
restaurants = (
    tips_stream.groupBy(f.col("restaurant_id"))
    .applyInPandasWithState(
        func,
        output_schema,
        state_schema,
        "append",
        GroupStateTimeout.ProcessingTimeTimeout,
    )
    .withColumn("timestamp", f.lit(datetime.now()))
)

display(restaurants)

# COMMAND ----------

tips_stream_withWatermark = tips_stream.withWatermark("timestamp", "10 seconds")
restaurants_withWatermark = restaurants.withWatermark("timestamp", "10 seconds")

tips_count = tips_stream.join(
    restaurants_withWatermark, f.expr("restaurant_id = restaurant")
)
display(tips_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring
# MAGIC
# MAGIC [Performance troubleshooting](https://www.databricks.com/blog/2020/07/29/a-look-at-the-new-structured-streaming-ui-in-apache-spark-3-0.html)

# COMMAND ----------

print(json.dumps(query.lastProgress, indent=4))
print("STATUS ################################################")
print(query.status)

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning

# COMMAND ----------

dbutils.fs.rm(f"/tmp/streaming_workshop/{catalog_name}", True)
spark.sql(f"DROP DATABASE IF EXISTS {catalog_name}.streaming_workshop CASCADE;")
