# Databricks notebook source
import os

# COMMAND ----------

checkpointpath = os.environ["CHECKPOINTPATH"]
schemapath = os.environ["SCHEMAPATH"]
storage_path = os.environ["DATA_LOCATION"]

# COMMAND ----------

print(checkpointpath)
print(schemapath)
print(storage_path)

# COMMAND ----------


dbutils.fs.rm(checkpointpath, recurse=True)
dbutils.fs.rm(schemapath, recurse=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Old tables 
# MAGIC Should old structures from previous run exist, these will be deleted as well. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS ls_dbx_workshop CASCADE;
# MAGIC CREATE CATALOG ls_dbx_workshop;
# MAGIC USE CATALOG ls_dbx_workshop;
# MAGIC CREATE SCHEMA day_one;

# COMMAND ----------

directory_path_init_load = f"file://{os.getcwd()}/_data/init_load"
directory_path_additional_load = f"file://{os.getcwd()}/_data/additional_load"

# COMMAND ----------

def load_raw_to_storage(load_type_path: str, storage_path: str):

    files = dbutils.fs.ls(load_type_path)

    for file in files:
        file_name = file.name
        file_name_without_extension = file_name.rsplit(".", 1)[0]
        file_name_without_suffix = file_name_without_extension.rsplit("_", 1)[0]

        file_path = f"{load_type_path}/{file_name}"
        final_folder_path = f"{storage_path}_{file_name_without_suffix}"
        final_storage_path = f"{final_folder_path}/{file_name}"

        dbutils.fs.rm(final_folder_path, recurse=True)

        print(f"Copying file {file_name}.")
        dbutils.fs.cp(file_path, final_storage_path)

# COMMAND ----------

load_raw_to_storage(directory_path_init_load, storage_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Additional load
# MAGIC
# MAGIC Should you wish to demonstrate the abilities of the autoloader further, you need to: 
# MAGIC
# MAGIC 1. Uncomment the cell below and run it.
# MAGIC 2. Run the raw layer jobs again which will ingest additional tables. 

# COMMAND ----------

# load_raw_to_storage(directory_path_additional_load, storage_path)

# COMMAND ----------


