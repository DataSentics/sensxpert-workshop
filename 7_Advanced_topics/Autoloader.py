# Databricks notebook source
# MAGIC %md
# MAGIC ## Autoloader in SQL

# COMMAND ----------

# Set parameters for isolation in workspace and reset demo

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
database = f"copyinto_{username}_db"
source = f"dbfs:/user/{username}/copy-into-demo"

spark.sql(f"SET c.username='{username}'")
spark.sql(f"SET c.database={database}")
spark.sql(f"SET c.source='{source}'")

spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
spark.sql("CREATE DATABASE ${c.database}")
spark.sql("USE ${c.database}")

dbutils.fs.rm(source, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Configure random data generator
# MAGIC
# MAGIC CREATE TABLE user_ping_raw
# MAGIC (user_id STRING, ping INTEGER, time TIMESTAMP)
# MAGIC USING json
# MAGIC LOCATION ${c.source};
# MAGIC
# MAGIC CREATE TABLE user_ids (user_id STRING);
# MAGIC
# MAGIC INSERT INTO user_ids VALUES
# MAGIC ("potato_luver"),
# MAGIC ("beanbag_lyfe"),
# MAGIC ("default_username"),
# MAGIC ("the_king"),
# MAGIC ("n00b"),
# MAGIC ("frodo"),
# MAGIC ("data_the_kid"),
# MAGIC ("el_matador"),
# MAGIC ("the_wiz");
# MAGIC
# MAGIC CREATE FUNCTION get_ping()
# MAGIC     RETURNS INT
# MAGIC     RETURN int(rand() * 250);
# MAGIC
# MAGIC CREATE FUNCTION is_active()
# MAGIC     RETURNS BOOLEAN
# MAGIC     RETURN CASE
# MAGIC         WHEN rand() > .25 THEN true
# MAGIC         ELSE false
# MAGIC         END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a new batch of data to the data source
# MAGIC
# MAGIC INSERT INTO user_ping_raw
# MAGIC SELECT *,
# MAGIC   get_ping() ping,
# MAGIC   current_timestamp() time
# MAGIC FROM user_ids
# MAGIC WHERE is_active()=true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table and load data
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS user_ping_target;
# MAGIC
# MAGIC COPY INTO user_ping_target
# MAGIC FROM ${c.source}
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ("mergeSchema" = "true")
# MAGIC COPY_OPTIONS ("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review updated table
# MAGIC
# MAGIC SELECT * FROM user_ping_target

# COMMAND ----------

# Drop database and tables and remove data

spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
dbutils.fs.rm(source, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoloader in Python

# COMMAND ----------

def autoload_bronze(filename):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option(
            "cloudFiles.schemaLocation",
            f"dbfs:/checkpoint_{username}/{filename}_schema",
        )
        .load(f"dbfs:/FileStore/python-workshop/{filename}*.csv")
        .writeStream.option(
            "checkpointLocation", f"dbfs:/checkpoint_{username}/{filename}"
        )
        .trigger(availableNow=True)
        .table(f"bronze_{filename}")
    )

    query.awaitTermination()

# COMMAND ----------

autoload_bronze("tips")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_tips
