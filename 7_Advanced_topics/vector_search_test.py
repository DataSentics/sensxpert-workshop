# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

client = VectorSearchClient()

# COMMAND ----------

index = client.get_index("vector-search-demo-endpoint", "dev.jaffle_shop.index_test")

# COMMAND ----------

results = index.similarity_search(
  query_text="sarah",
  columns=["ID", "FIRST_NAME"],
  num_results=2
)

results
