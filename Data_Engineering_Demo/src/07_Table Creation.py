# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import os
import json
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialization

# COMMAND ----------

spark: SparkSession = spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_path", "../configs/table_schema.json")
dbutils.widgets.dropdown("reset", "False", ["False", "True"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")
reset = bool(dbutils.widgets.get("reset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialization

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

absolute_config_path = os.path.abspath(config_path)
absolute_config_path

# COMMAND ----------

def parse_columns(columns):
    parsed_columns = StructType()
    for column in columns:
        parsed_columns.add(
            column.get("name"),
            column.get("type"),
            column.get("nullable", True),
            column.get("metadata", {}),
        )
    return parsed_columns

# COMMAND ----------

def format_tags(tags):
    formated_tags = ", ".join([f"'{key}' = '{value}'" for key, value in tags.items()])
    return formated_tags

# COMMAND ----------

def add_table_tags(table_name, tags):
    table_tags = format_tags(tags)
    sql_query = f"ALTER TABLE {table_name} SET TAGS ({table_tags})"
    spark.sql(sql_query)

# COMMAND ----------

def add_column_tags(table_name, column_name, tags):    
    column_tags = format_tags(tags)
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET TAGS ({column_tags})"
    spark.sql(sql_query)

# COMMAND ----------

def create_table(schema_data, reset=False):
    table_name = schema_data.get("table_name")
    columns = schema_data.get("columns")
    comment = schema_data.get("comment", "")
    cluster_by = schema_data.get("cluster_by")
    properties = schema_data.get("properties")
    tags = schema_data.get("tags")
    
    full_table_name = f"{catalog}.{schema}.{table_name}"

    dp_table_builder = DeltaTable.createOrReplace() if reset else DeltaTable.createIfNotExists()
    dp_table_builder = (
        dp_table_builder
        .tableName(full_table_name)
        .addColumns(parse_columns(columns))
        .comment(comment)
    )

    if properties:
        for key, value in properties.items():
            dp_table_builder = dp_table_builder.property(key, value)

    if cluster_by:
        dp_table_builder = dp_table_builder.clusterBy(cluster_by)
        
    dp_table = dp_table_builder.execute()

    if tags:
        add_table_tags(full_table_name, tags)

    for column in columns:        
        column_tags = column.get("tags")
        if column_tags:
            add_column_tags(full_table_name, column.get("name"), column_tags)

    print(f"Table: {full_table_name} is created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create tables
# MAGIC

# COMMAND ----------

with open(absolute_config_path.removeprefix("file:"), "r") as read_file:
    schema_data = json.load(read_file)

create_table(schema_data)
