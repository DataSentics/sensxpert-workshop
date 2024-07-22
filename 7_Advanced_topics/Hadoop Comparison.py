# Databricks notebook source
# MAGIC %md # Comparison with Hadoop environments

# COMMAND ----------

# MAGIC %md ## Hadoop vs. databricks
# MAGIC - Databricks is optimized for cloud, hadoop for on-prem
# MAGIC - Delta format
# MAGIC - Databricks is unified platform
# MAGIC - Databricks is easier for individual teams to manage their own infrastructure

# COMMAND ----------

# MAGIC %md ## Clarification of terms
# MAGIC - Hadoop - framework for distributed processing of data.
# MAGIC   - HDFS - hadoop distributed storage
# MAGIC   - Native query framework over HDFS is MapReduce
# MAGIC - Hive - SQL abstraction over MapReduce, for this abstraction, it uses hive_metastore
# MAGIC - Impala - SQL abstraction + it's own compute framework (not MapReduce) - suitable for interactive queries
# MAGIC - BDSL - Cloudera - Offers those services (Hive, impala)
# MAGIC - CRANE - ?

# COMMAND ----------

# MAGIC %md ### Mapping of individual components
# MAGIC |X| Hadoop | Databricks |
# MAGIC |--|------- | -----------|
# MAGIC |__Storage__| HDFS, Kafka, HDBase | Cloud object storages (behind unity catalog) | 
# MAGIC |__Data processing__| MapReduce, HiveQL, Spark | Databricks delta Engine, optimized apache spark, DBSQL | 
# MAGIC | __Jobs__| Oozie | Databricks workflows |
# MAGIC |__Code development__| Zeppelin notebooks, Jupyter notebook | Databricks notebooks, Support for zeppelin, jupyter or any notebook or IDE (Pycharm, VS code..), python files |
# MAGIC |__Interactive query__| Hue, Impala / Hive LLAP | SQL Analytics workspace, Delta engine / Photon |

# COMMAND ----------

# MAGIC %md ## Practical
# MAGIC TODO: Rewrite some use case from hadoop env to databricks
