# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialization

# COMMAND ----------

spark: SparkSession = spark

# COMMAND ----------

catalog = "ls_dbx_workshop"
schema = "day_one"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_ss_product_catalog ALTER COLUMN product_id DROP NOT NULL;
# MAGIC ALTER TABLE silver_ss_product_catalog DROP PRIMARY KEY IF EXISTS CASCADE;
# MAGIC ALTER TABLE silver_ss_production_records ALTER COLUMN record_id DROP NOT NULL;
# MAGIC ALTER TABLE silver_ss_production_records DROP PRIMARY KEY IF EXISTS CASCADE;
# MAGIC ALTER TABLE silver_ss_production_records DROP FOREIGN KEY IF EXISTS (product_id);
# MAGIC ALTER TABLE silver_ss_sales_records ALTER COLUMN sales_id DROP NOT NULL;
# MAGIC ALTER TABLE silver_ss_sales_records DROP PRIMARY KEY IF EXISTS CASCADE;
# MAGIC ALTER TABLE silver_ss_sales_records DROP FOREIGN KEY IF EXISTS (product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a primary key
# MAGIC ALTER TABLE silver_ss_product_catalog ALTER COLUMN product_id SET NOT NULL;
# MAGIC ALTER TABLE silver_ss_product_catalog ADD CONSTRAINT product_catalog_pk PRIMARY KEY(product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver production_records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a primary and foreign key
# MAGIC ALTER TABLE silver_ss_production_records ALTER COLUMN record_id SET NOT NULL;
# MAGIC ALTER TABLE silver_ss_production_records ADD CONSTRAINT production_records_pk PRIMARY KEY(record_id);
# MAGIC ALTER TABLE silver_ss_production_records ADD CONSTRAINT production_records_fk
# MAGIC     FOREIGN KEY(product_id) REFERENCES silver_ss_product_catalog NOT ENFORCED;

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver sales_records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a primary and foreign key
# MAGIC ALTER TABLE silver_ss_sales_records ALTER COLUMN sales_id SET NOT NULL;
# MAGIC ALTER TABLE silver_ss_sales_records ADD CONSTRAINT sales_records_pk PRIMARY KEY(sales_id);
# MAGIC ALTER TABLE silver_ss_sales_records ADD CONSTRAINT sales_records_fk
# MAGIC     FOREIGN KEY(product_id) REFERENCES silver_ss_product_catalog NOT ENFORCED;
