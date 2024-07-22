# Databricks notebook source
dbutils.widgets.text("catalog_name", "")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog_name}

# COMMAND ----------

account = spark.read.table("bank_data.account")
user = spark.read.table("bank_data.user")
product = spark.read.table("bank_data.product")
transaction = spark.read.table("bank_data.transaction")

# COMMAND ----------

user_transaction = transaction.join(user, transaction.user_id == user.user_id,"inner")
user_product = (
                    user_transaction.join(product, user_transaction.product_id == product.product_id, "inner")
                    .select(["first_name", "last_name", "email", "manufacturer", "product_quantity", "product_unit_price"])
)

user_product.write.mode("overwrite").saveAsTable("bank_data.user_product")
