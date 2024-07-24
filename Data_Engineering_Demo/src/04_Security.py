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

default_catalog = "ls_dbx_workshop"
default_schema = "day_one"

dbutils.widgets.text("catalog", default_catalog, "Provide real name of the used catalog.")
dbutils.widgets.text("schema", default_schema, "Provide real name of the used schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dynamic View - Column level security

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sales_records AS
# MAGIC SELECT
# MAGIC   sales_id, product_id, sale_date, quantity_sold, sale_price_per_unit, customer_id, sales_region_id,
# MAGIC   CASE WHEN
# MAGIC     is_account_group_member('some_group') THEN salesperson_id
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS salesperson_id
# MAGIC FROM silver_ss_sales_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_records

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dynamic View - Row level security

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW production_records AS
# MAGIC SELECT
# MAGIC   product_id, production_date, quantity_produced
# MAGIC FROM silver_ss_production_records
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('some_group') THEN TRUE
# MAGIC     ELSE quantity_produced <= 100
# MAGIC   END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM production_records

# COMMAND ----------

# MAGIC %md
# MAGIC ###Row filter

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION row_filter(val DOUBLE)
# MAGIC RETURN IF(IS_ACCOUNT_GROUP_MEMBER('some_group'), true, val <= 500);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_ss_sales_records SET ROW FILTER row_filter ON (sale_price_per_unit);

# COMMAND ----------

# MAGIC %md
# MAGIC ###Column Mask

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION column_mask(val STRING)
# MAGIC   RETURN CASE WHEN is_member('some_group') THEN val ELSE '*********' END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_ss_sales_records ALTER COLUMN customer_id SET MASK column_mask;
