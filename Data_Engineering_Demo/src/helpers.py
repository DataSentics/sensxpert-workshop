import os
from pyspark.sql.functions import col
import pyspark.sql.functions as F


def create_bronze_df(spark, data_location, schema_location=None):

    cloud_files_options = {        
        "cloudFiles.format": "csv",
        "cloudFiles.inferColumnTypes": True,
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "header": True,
        "separator": ","
    }
    if schema_location:
        cloud_files_options["cloudFiles.schemaLocation"] = schema_location
        
    df_final = (
        spark
        .readStream
        .format("cloudFiles")
        .options(**cloud_files_options)
        .load(data_location)
        .withColumn("input_file", F.col("_metadata.file_path"))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )
    return df_final


def create_silver_df(spark, df_bronze, non_null_columns):
    rules = {f"{column}_null_check": f"{column} IS NOT NULL" for column in non_null_columns}
    quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))
    df_silver = df_bronze.withColumn("is_quarantined", F.expr(quarantine_rules))
    return df_silver


def create_sales_statistics_df(spark, df_silver_sales_records):
    sales_df = df_silver_sales_records.withColumn("total_sales", col("quantity_sold") * col("sale_price_per_unit"))
    df_sales_statistincs = (
        sales_df.groupBy("product_id")
        .agg(
            F.sum("total_sales").alias("total_sales_amount"),
            F.avg("sale_price_per_unit").alias("average_sale_price"),
            F.sum("quantity_sold").alias("total_quantity_sold"),
            F.max("sale_price_per_unit").alias("max_sale_price"),
            F.min("sale_price_per_unit").alias("min_sale_price"),
            F.count("sales_id").alias("number_of_sales")
        )
    )
    return df_sales_statistincs




