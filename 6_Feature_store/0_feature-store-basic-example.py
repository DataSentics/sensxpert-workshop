# Databricks notebook source
# MAGIC %md # Basic feature store example
# MAGIC This notebook illustrates how you can use Databricks Feature Store to create, store, and manage features to train ML models and make batch predictions, including with features whose value is only available at the time of prediction. In this example, the goal is to predict the wine quality using a ML model with a variety of static wine features and a realtime input.
# MAGIC
# MAGIC This notebook shows how to:
# MAGIC - Create a feature store table and use it to build a training dataset for a machine learning model.
# MAGIC - Modify the feature table and use the updated table to create a new version of the model.
# MAGIC - Use the Databricks feature store UI to determine how features map to models.
# MAGIC - Perform batch scoring using automatic feature lookup.
# MAGIC
# MAGIC

# COMMAND ----------

from matplotlib import pyplot as plt
from matplotlib import image as mpimg
image = mpimg.imread("/dbfs/FileStore/imgs/fs.png")
plt.figure(figsize=(7, 7))
plt.imshow(image)
plt.show()

# COMMAND ----------

import pandas as pd

from pyspark.sql.functions import monotonically_increasing_id, expr, rand
import uuid

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn import datasets

# COMMAND ----------

# MAGIC %md ## Load dataset
# MAGIC The code in the following cell loads the dataset and does some minor data preparation: creates a unique ID for each observation and removes spaces from the column names. The unique ID column (`wine_id`) is the primary key of the feature table and is used to lookup features.

# COMMAND ----------

# raw_data = spark.read.load("/databricks-datasets/wine-quality/winequality-red.csv",format="csv",sep=";",inferSchema="true",header="true" )
raw_data = spark.createDataFrame(pd.DataFrame(data=datasets.load_wine().data, columns=datasets.load_wine().feature_names))
def addIdColumn(dataframe, id_column_name):
    """Add id column to dataframe"""
    columns = dataframe.columns
    new_df = dataframe.withColumn(id_column_name, monotonically_increasing_id())
    return new_df[[id_column_name] + columns]

def renameColumns(df):
    """Rename columns to be compatible with Feature Store"""
    renamed_df = df
    for column in df.columns:
        renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
        renamed_df = renamed_df.withColumnRenamed(column, column.replace('/', '_'))
    return renamed_df

# Run functions
renamed_df = renameColumns(raw_data)
df = addIdColumn(renamed_df, 'wine_id')

# Drop target column ('quality') as it is not included in the feature table
features_df = df.drop('alcohol')
display(features_df)


# COMMAND ----------

# MAGIC %md ## Create a new database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS wine_db")

# Create a unique table name for each run. This prevents errors if you run the notebook multiple times.
table_name = f"wine_db_" + str(uuid.uuid4())[:6]
print(table_name)

# COMMAND ----------

# MAGIC %md ## Create the feature table

# COMMAND ----------

# MAGIC %md The first step is to create a FeatureStoreClient.

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# You can get help in the notebook for feature store API functions:
# help(fs.<function_name>)

# For example:
# help(fs.create_table)

# COMMAND ----------

# MAGIC %md Create the feature table. For a complete API reference, see ([AWS](https://docs.databricks.com/machine-learning/feature-store/python-api.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/python-api)|[GCP](https://docs.gcp.databricks.com/machine-learning/feature-store/python-api.html)).

# COMMAND ----------

fs.create_table(
    name=table_name,
    primary_keys=["wine_id"],
    df=features_df,
    schema=features_df.schema,
    description="wine features"
)

# COMMAND ----------

# MAGIC %md You can also use `create_table` without providing a dataframe, and then later populate the feature table using `fs.write_table`. `fs.write_table` supports both `overwrite` and `merge` modes.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```
# MAGIC fs.create_table(
# MAGIC     name=table_name,
# MAGIC     primary_keys=["wine_id"],
# MAGIC     schema=features_df.schema,
# MAGIC     description="wine features"
# MAGIC )
# MAGIC
# MAGIC fs.write_table(
# MAGIC     name=table_name,
# MAGIC     df=features_df,
# MAGIC     mode="overwrite"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Feature Store UI
# MAGIC
# MAGIC To view the Feature Store UI, you must be in the Machine Learning persona ([AWS](https://docs.databricks.com/workspace/index.html#use-the-sidebar)|[Azure](https://learn.microsoft.com/azure/databricks/workspace/index#use-the-sidebar)|[GCP](https://docs.gcp.databricks.com/workspace/index.html#use-the-sidebar)). To access the Feature Store UI, click the Feature Store icon on the left navigation bar:  <img src="https://docs.databricks.com/_static/images/icons/feature-store-icon.png"/>  
# MAGIC
# MAGIC The Databricks Feature Store UI shows a list of all feature tables in the workspace and also displays information about the tables including the creator, data sources, online stores, scheduled jobs that update the table, and the time the table was most recently updated. 
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/basic-fs-nb-ui.png"/>  
# MAGIC
# MAGIC Find the table you just created in the list. You can enter text into the Search box to search based on the name of a feature table, a feature, or a data source.
# MAGIC
# MAGIC Click the table name to display details about the table. This page shows information about the table. The **Producers** section shows the name of the notebook that created the table and the last time the notebook was run. You can scroll down to the **Features** table for more details about the features in the table; an example is shown later in this notebook.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/basic-fs-nb-producers.png"/> 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## Train a model with feature store

# COMMAND ----------

# MAGIC %md The feature table does not include the prediction target. However, the training dataset needs the prediction target values. There may also be features that are not available until the time the model is used for inference. 
# MAGIC
# MAGIC This example uses the feature **`real_time_measurement`** to represent a characteristic of the wine that can only be observed at inference time. This feature is used in training and the feature value for a wine is provided at inference time.

# COMMAND ----------

## inference_data_df includes wine_id (primary key), alcohol (prediction target), and a real time feature
inference_data_df = df.select("wine_id", "alcohol", (10 * rand()).alias("real_time_measurement"))
display(inference_data_df)

# COMMAND ----------

# MAGIC %md Use a `FeatureLookup` to build a training dataset that uses the specified `lookup_key` to lookup features from the feature table and the online feature `real_time_measurement`. If you do not specify the `feature_names` parameter, all features except the primary key are returned.

# COMMAND ----------

def load_data(table_name, lookup_key):
    # In the FeatureLookup, if you do not provide the `feature_names` parameter, all features except primary keys are returned
    model_feature_lookups = [FeatureLookup(table_name=table_name, lookup_key=lookup_key)]

    # fs.create_training_set looks up features in model_feature_lookups that match the primary key from inference_data_df
    training_set = fs.create_training_set(inference_data_df, model_feature_lookups, label="alcohol", exclude_columns="wine_id")
    training_pd = training_set.load_df().toPandas()

    # Create train and test datasets
    X = training_pd.drop("alcohol", axis=1)
    y = training_pd["alcohol"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test, training_set

# Create the train and test datasets
X_train, X_test, y_train, y_test, training_set = load_data(table_name, "wine_id")
X_train.head()

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()

try:
    client.delete_registered_model("wine_model") # Delete the model if already created
except:
    None

# COMMAND ----------

# MAGIC %md 
# MAGIC The code in the next cell trains a scikit-learn RandomForestRegressor model and logs the model with the Feature Store.  
# MAGIC
# MAGIC The code starts an MLflow experiment to track training parameters and results. Note that model autologging is disabled (`mlflow.sklearn.autolog(log_models=False)`); this is because the model is logged using `fs.log_model`.

# COMMAND ----------

# Disable MLflow autologging and instead log the model using Feature Store
mlflow.sklearn.autolog(log_models=False)

def train_model(X_train, X_test, y_train, y_test, training_set, fs):
    ## fit and log model
    with mlflow.start_run() as run:

        rf = RandomForestRegressor(max_depth=3, n_estimators=20, random_state=42)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_test)
 
        mlflow.log_metric("test_mse", mean_squared_error(y_test, y_pred))
        mlflow.log_metric("test_r2_score", r2_score(y_test, y_pred))

        fs.log_model(
            model=rf,
            artifact_path="wine_alcohol_prediction",
            flavor=mlflow.sklearn,
            training_set=training_set,
            registered_model_name="wine_model",
        )

train_model(X_train, X_test, y_train, y_test, training_set, fs)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC To view the logged model, navigate to the MLflow Experiments page for this notebook. To access the Experiments page, click the Experiments icon on the left navigation bar:  <img src="https://docs.databricks.com/_static/images/icons/experiments-icon.png"/>  
# MAGIC
# MAGIC Find the notebook experiment in the list. It has the same name as the notebook, in this case, "Basic Feature Store Example".
# MAGIC
# MAGIC Click the experiment name to display the experiment page. The packaged feature store model, created when you called `fs.log_model` appears in the **Artifacts** section of this page. You can use this model for batch scoring.  
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/basic-fs-nb-artifact.png"/>  
# MAGIC
# MAGIC The model is also automatically registered in the Model Registry. The feature table in the Feature Store UI is also updated to show which features from the feature table were used to train the model. 

# COMMAND ----------

# MAGIC %md ## Batch scoring
# MAGIC Use `score_batch` to apply a packaged feature store model to new data for inference. The input data only needs the primary key column `wine_id` and the realtime feature `real_time_measurement`. The model automatically looks up all of the other feature values from the feature store.
# MAGIC

# COMMAND ----------

## For simplicity, this example uses inference_data_df as input data for prediction
batch_input_df = inference_data_df.drop("alcohol") # Drop the label column
# batch_input_df = inference_data_df.select('wine_id', 'real_time_measurement')
predictions_df = fs.score_batch("models:/wine_model/latest", batch_input_df)
                                  
display(predictions_df["wine_id", "prediction"])

# COMMAND ----------

# MAGIC %md ## Modify feature table
# MAGIC Suppose you modify the dataframe by adding a new feature. You can use `fs.write_table` with `mode="merge"` to update the feature table.

# COMMAND ----------

## Modify the dataframe containing the features
so2_cols = ["proline", "malic_acid"]
new_features_df = (features_df.withColumn("average_so2", expr("+".join(so2_cols)) / 2))

display(new_features_df)

# COMMAND ----------

# MAGIC %md Update the feature table using `fs.write_table` with `mode="merge"`.

# COMMAND ----------

fs.write_table(
    name=table_name,
    df=new_features_df,
    mode="merge"
)

# COMMAND ----------

# MAGIC %md ## Explore feature lineage in the UI

# COMMAND ----------

# MAGIC %md
# MAGIC After you update the feature table, the UI reflects the following:
# MAGIC - The new feature appears in the feature list.
# MAGIC - Deleted features still appear. When you read in the feature table, the deleted features have null values.
# MAGIC - The **Models** column displays model versions that use each feature.
# MAGIC - The **Notebooks** column displays notebooks that use each feature.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/basic-fs-nb-feature-table.png"/>  

# COMMAND ----------

# MAGIC %md To read feature data from the Feature Store, use `fs.read_table()`.

# COMMAND ----------

# Displays most recent version of the feature table
# Note that features that were deleted in the current version still appear in the table but with value = null.
display(fs.read_table(name=table_name))

# COMMAND ----------

# MAGIC %md ## Train a new model version using the updated feature table

# COMMAND ----------

def load_data(table_name, lookup_key):
    model_feature_lookups = [FeatureLookup(table_name=table_name, lookup_key=lookup_key)]
    
    # fs.create_training_set will look up features in model_feature_lookups with matched key from inference_data_df
    training_set = fs.create_training_set(inference_data_df, model_feature_lookups, label="alcohol", exclude_columns="wine_id")
    training_pd = training_set.load_df().toPandas()

    # Create train and test datasets
    X = training_pd.drop("alcohol", axis=1)
    y = training_pd["alcohol"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test, training_set

X_train, X_test, y_train, y_test, training_set = load_data(table_name, "wine_id")
X_train.head()

# COMMAND ----------

# MAGIC %md 
# MAGIC Build a training dataset that will use the indicated `key` to lookup features.

# COMMAND ----------

def train_model(X_train, X_test, y_train, y_test, training_set, fs):
    ## fit and log model
    with mlflow.start_run() as run:

        rf = RandomForestRegressor(max_depth=3, n_estimators=20, random_state=42)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_test)

        mlflow.log_metric("test_mse", mean_squared_error(y_test, y_pred))
        mlflow.log_metric("test_r2_score", r2_score(y_test, y_pred))

        fs.log_model(
            model=rf,
            artifact_path="feature-store-model",
            flavor=mlflow.sklearn,
            training_set=training_set,
            registered_model_name="wine_model",
        )

train_model(X_train, X_test, y_train, y_test, training_set, fs)

# COMMAND ----------

# MAGIC %md Apply the latest version of the feature store registered MLflow model to features using **`score_batch`**.

# COMMAND ----------

## For simplicity, this example uses inference_data_df as input data for prediction
batch_input_df = inference_data_df.drop("alcohol") # Drop the label column

# we need to provide wine_id as well as real_time_measurement, as this is not available in the feature store
batch_input_df = inference_data_df.select("wine_id", "real_time_measurement") 

predictions_df = fs.score_batch(f"models:/wine_model/latest", batch_input_df)
display(predictions_df["wine_id","prediction"])

# COMMAND ----------

# MAGIC %md The UI now reflects that version 2 of `wine_model` uses the newly created feature `average_so2`.
# MAGIC   
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/basic-fs-nb-new-feature.png"/>  

# COMMAND ----------

# MAGIC %md ## Control permissions for and delete feature tables
# MAGIC - To control who has access to a feature table, use the **Permissions** button in the UI.
# MAGIC - To delete a feature table, click the kebab menu next to **Permissions** in the UI and select **Delete**. When you delete a feature table using the UI, the corresponding Delta table is not deleted; you must delete that table separately. 
