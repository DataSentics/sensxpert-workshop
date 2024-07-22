# Databricks notebook source
# MAGIC %md
# MAGIC ### Delta Live Tables(DLT) 
# MAGIC a framework for building reliable, maintainable, and testable data processing pipelines.  
# MAGIC
# MAGIC You define the transformations to perform on your data, and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.
# MAGIC
# MAGIC DLT pipelines are pipelines that are production ready and offer all the services you need from production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unit of execution 
# MAGIC DLT framework does not support interactive development. The smallest unit of development is pipeline which needs to be created under Workflows.  

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating a DLT Pipeline
# MAGIC 1. Go to workflows
# MAGIC 2. Select Delta Live Tables 
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/images/dlt-1.png?raw=true)
# MAGIC 3. Click Create a Pipeline
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/images/dlt-2.png?raw=true)
# MAGIC 4. Select [Product edition](https://www.databricks.com/product/delta-live-tables-pricing-azure). This determines how many functionalities from DLT will be available to you together with pricing.
# MAGIC 5. Enter Pipeline name
# MAGIC 6. Enter Notebook libraries(Paths to the DLT notebooks)
# MAGIC 7. Select pipeline mode. Triggered means batch, continous streaming.
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/images/dlt-3.png?raw=true)
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/images/dlt-4.png?raw=true)
# MAGIC 8. Select development mode if developing(resources will be available for longer) or production(like a job cluster)
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/images/dlt-5.png?raw=true)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
  return (spark.read.option('header', True).csv('dbfs:/FileStore/export_dlt.csv'))

# COMMAND ----------

@dlt.table(
  comment="Wikipedia clickstream data cleaned and prepared for analysis."
)
#@dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
#@dlt.expect_or_fail("valid_count", "click_count > 0")
def clickstream_prepared():
  return (
    dlt.read("clickstream_raw")
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_title", "click_count", "previous_page_title")
  )

# COMMAND ----------

@dlt.table(
  comment="A table containing the top pages linking to the Apache Spark page."
)
def top_spark_referrers():
  return (
    dlt.read("clickstream_prepared")
      .filter(expr("current_page_title == 'Apache_Spark'"))
      .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("click_count"))
      .select("referrer", "click_count")
      .limit(10)
  )
