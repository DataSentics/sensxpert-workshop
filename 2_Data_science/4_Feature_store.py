# Databricks notebook source
# MAGIC %md # Feature store
# MAGIC - Databricks Feature Store is a centralized repository in Databricks that simplifies the management and organization of data for machine learning, allowing users to create, version, and share features across different ML pipelines.
# MAGIC - It enables data engineers and data scientists to easily access and use curated, feature-engineered datasets, promoting collaboration and reproducibility in the machine learning workflow.

# COMMAND ----------

from matplotlib import pyplot as plt
from matplotlib import image as mpimg
from sklearn.datasets import load_wine
import pandas as pd
from databricks import feature_store

# COMMAND ----------

# MAGIC %md <img src="https://www.databricks.com/wp-content/uploads/2021/05/fs-blog-img-2.png" width=1000 height=500/>

# COMMAND ----------

#load data
wine = load_wine()

# COMMAND ----------

# Convert the dataset to a DataFrame
wine_df = pd.DataFrame(data=wine.data, columns=wine.feature_names)
wine_df["target"] = wine.target

features_df = spark.createDataFrame(wine_df).drop('target')

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

try:
    fs.create_table(
        name="wine_data",
        primary_keys=["wine_id"],
        # df=features_df,
        # schema=features_df.schema,
        description="wine features"
    )
except:
    print('FS table already exists')

# COMMAND ----------

df = fs.read_table('wine_data')

# COMMAND ----------

display(df)
