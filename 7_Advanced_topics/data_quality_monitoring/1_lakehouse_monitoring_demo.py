# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse monitoring
# MAGIC
# MAGIC Lakehouse monitoring creates metric tables and dashboard, which is monitoring quality of data in a table and indicate changes of distribution in the data.
# MAGIC
# MAGIC Requirements:
# MAGIC * Your workspace must be enabled for Unity Catalog and you must have access to Databricks SQL.
# MAGIC * Only Delta managed tables, external tables, views, and materialized views are supported for monitoring.
# MAGIC * Serverless jobs compute.
# MAGIC
# MAGIC Create monitor:
# MAGIC
# MAGIC * [Databricks UI](https://docs.databricks.com/en/lakehouse-monitoring/create-monitor-ui.html)
# MAGIC * [API notebook example](https://learn.microsoft.com/en-gb/azure/databricks/_extras/notebooks/source/monitoring/snapshot-monitor.html)
# MAGIC
# MAGIC Profile types:
# MAGIC
# MAGIC | profile type   | description |
# MAGIC | -------- | ------- |
# MAGIC | Snapshot profile  | Any Delta managed table, external table, view, or materialized view.   |
# MAGIC | Time series profile | A table containing values measured over time. This table includes a timestamp column.    |
# MAGIC | Inference profile    | A table containing predicted values output by a machine learning classification or regression model. This table includes a timestamp, a model id, model inputs (features), a column containing model predictions, and optional columns containing unique observation IDs and ground truth labels. It can also contain metadata, such as demographic information, that is not used as input to the model but might be useful for fairness and bias investigations or other monitoring.   |
# MAGIC
# MAGIC Understand metrices:
# MAGIC
# MAGIC Metrices are always calculated for entire table and in addition for slices if there are specified. Slice can be specified by unique value in column or by expression (col1 > 10).
# MAGIC
# MAGIC * profile metrices
# MAGIC * drift metrices
# MAGIC
# MAGIC [List of metrices](https://docs.databricks.com/en/lakehouse-monitoring/monitor-output.html)

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo data and model

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble
import pyspark.sql.functions as f
 
from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

# Load and preprocess data
white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=';')
red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=';')
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)
 
# Define classification labels based on the wine quality
data_labels = data_df['quality'] >= 7
data_df = data_df.drop(['quality'], axis=1)
 
# Split 80/20 train-test
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
  data_df,
  data_labels,
  test_size=0.2,
  random_state=1
)

# COMMAND ----------

data_df.head()

# COMMAND ----------

data_df = data_df.rename(columns={col: col.replace(" ", "_") for col in data_df.columns})

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog olostak;
# MAGIC CREATE DATABASE IF NOT EXISTS demo_monitoring;

# COMMAND ----------

datetime_value1 = pd.Timestamp(year=2024, month=2, day=29, hour=0, minute=0, second=0)
datetime_value2 = pd.Timestamp(year=2024, month=3, day=31, hour=0, minute=0, second=0)
pdf1 = data_df.sample(frac = 0.5, random_state=1)
pdf1["timestamp"] = datetime_value1
pdf2 = data_df.drop(pdf1.index)
pdf2["is_red"] = (pdf2["is_red"] + 1) % 2
pdf2["citric_acid"] = pdf2["citric_acid"] / 4
pdf2["alcohol"] = pdf2["alcohol"].map(lambda x: None if x < 10 else x)
pdf2["timestamp"] = datetime_value2
modified_data_df = pd.concat([pdf1, pdf2], ignore_index=True)

# COMMAND ----------

spark.createDataFrame(pdf1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olostak.demo_monitoring.features_baseline")

# COMMAND ----------

spark.createDataFrame(modified_data_df).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olostak.demo_monitoring.features")

# COMMAND ----------

df = spark.read.table("demo_monitoring.features")
display(df)

# COMMAND ----------

mlflow.autolog()

# COMMAND ----------

with mlflow.start_run(run_name='gradient_boost') as run:
  model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)
  
  # Models, parameters, and training metrics are tracked automatically
  model.fit(X_train, y_train)
 
  predicted_probs = model.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  
  # The AUC score on test data is not automatically logged, so log it manually
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# Start a new run and assign a run_name for future reference
with mlflow.start_run(run_name='gradient_boost') as run:
  model_2 = sklearn.ensemble.GradientBoostingClassifier(
    random_state=0, 
    
    # Try a new parameter setting for n_estimators
    n_estimators=200,
  )
  model_2.fit(X_train, y_train)
 
  predicted_probs = model_2.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

mlflow.pyfunc.save_model(
    model=model_2,
    python_model=None,
    artifacts={"model": "model-artifacts"}
)

# COMMAND ----------

# After a model has been logged, you can load it in different notebooks or jobs
# mlflow.pyfunc.load_model makes model prediction available under a common API
model_loaded = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=run.info.run_id
  )
)
 
predictions_loaded = model_loaded.predict(X_test)
predictions_original = model_2.predict(X_test)
 
# The loaded model should match the original
assert(np.array_equal(predictions_loaded, predictions_original))

# COMMAND ----------

# Define the search space to explore
search_space = {
  'n_estimators': scope.int(hp.quniform('n_estimators', 20, 1000, 1)),
  'learning_rate': hp.loguniform('learning_rate', -3, 0),
  'max_depth': scope.int(hp.quniform('max_depth', 2, 5, 1)),
}
 
def train_model(params):
  # Enable autologging on each worker
  mlflow.autolog()
  with mlflow.start_run(nested=True):
    model_hp = sklearn.ensemble.GradientBoostingClassifier(
      random_state=0,
      **params
    )
    model_hp.fit(X_train, y_train)
    predicted_probs = model_hp.predict_proba(X_test)
    # Tune based on the test AUC
    # In production settings, you could use a separate validation set instead
    roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
    mlflow.log_metric('test_auc', roc_auc)
    
    # Set the loss to -1*auc_score so fmin maximizes the auc_score
    return {'status': STATUS_OK, 'loss': -1*roc_auc}
 
# SparkTrials distributes the tuning using Spark workers
# Greater parallelism speeds processing, but each hyperparameter trial has less information from other trials
# On smaller clusters or Databricks Community Edition try setting parallelism=2
spark_trials = SparkTrials(
  parallelism=8
)
 
with mlflow.start_run(run_name='gb_hyperopt') as run:
  # Use hyperopt to find the parameters yielding the highest AUC
  best_params = fmin(
    fn=train_model, 
    space=search_space, 
    algo=tpe.suggest, 
    max_evals=32,
    trials=spark_trials)

# COMMAND ----------

# Sort runs by their test auc; in case of ties, use the most recent run
best_run = mlflow.search_runs(
  order_by=['metrics.test_auc DESC', 'start_time DESC'],
  max_results=10,
).iloc[0]
print('Best Run')
print('AUC: {}'.format(best_run["metrics.test_auc"]))
print('Num Estimators: {}'.format(best_run["params.n_estimators"]))
print('Max Depth: {}'.format(best_run["params.max_depth"]))
print('Learning Rate: {}'.format(best_run["params.learning_rate"]))
 
best_model_pyfunc = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
)
best_model_predictions = best_model_pyfunc.predict(X_test[:5])
print("Test Predictions: {}".format(best_model_predictions))

# COMMAND ----------

best_run.info

# COMMAND ----------

mlflow.log_artifact(best_run.info.artifact_uri, "model-artifacts")

# Save the artifacts as an MLflow model
model_path = "my-mlflow-model"
mlflow.pyfunc.save_model(
    path=model_path,
    python_model=None,
    artifacts={"model": "model-artifacts"}
)

# COMMAND ----------

new_model_version = mlflow.register_model(f"runs:/{best_run.run_id}/model", "monitoring_demo")

# COMMAND ----------

import os
DBX_TOKEN = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
os.environ["DATABRICKS_TOKEN"] = DBX_TOKEN

# COMMAND ----------

import requests
import pandas as pd
 
def score_model(dataset: pd.DataFrame):
  url = 'https://<DATABRICKS_URL>/model/wine_quality/Production/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='records')
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

num_predictions = 5
served_predictions = score_model(X_test[:num_predictions])
model_evaluations = model.predict(X_test[:num_predictions])
# Compare the results from the deployed model and the trained model
pd.DataFrame({
  "Model Prediction": model_evaluations,
  "Served Model Prediction": served_predictions,
})

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.table("olostak.demo_monitoring.features")
df_red = df.filter(F.col("is_red") == 1.0)
json_strings = df_red.toJSON().collect()
for s in json_strings:
  print(s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the monitor

# COMMAND ----------

import os

assert float(os.environ.get("DATABRICKS_RUNTIME_VERSION", 0)) >= 12.2, "Please configure your cluster to use Databricks Runtime 12.2 LTS or above."

# COMMAND ----------

# MAGIC %pip install "https://ml-team-public-read.s3.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_lakehouse_monitoring-0.4.4-py3-none-any.whl"

# COMMAND ----------

# This step is necessary to reset the environment with our newly installed wheel.
dbutils.library.restartPython()

# COMMAND ----------

SLICING_EXPRS = ["timestamp"]
TABLE_NAME = "olostak.demo_monitoring.features"
BASELINE_TABLE = "olostak.demo_monitoring.features_baseline"
TIMESTAMP_COL = "timestamp"
GRANULARITIES = ["1 month"]

# COMMAND ----------

from databricks import lakehouse_monitoring as lm

info = lm.create_monitor(
    table_name=TABLE_NAME,
    profile_type=lm.Snapshot(),
    slicing_exprs=SLICING_EXPRS,
    baseline_table_name=BASELINE_TABLE,
    output_schema_name=f"olostak.demo_monitoring"
)

# COMMAND ----------

import time


# Wait for monitor to be created
while info.status == lm.MonitorStatus.PENDING:
  info = lm.get_monitor(table_name=TABLE_NAME)
  time.sleep(10)

assert(info.status == lm.MonitorStatus.ACTIVE)

# COMMAND ----------

# A metric refresh will automatically be triggered on creation
refreshes = lm.list_refreshes(table_name=TABLE_NAME)
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (lm.RefreshState.PENDING, lm.RefreshState.RUNNING):
  run_info = lm.get_refresh(table_name=TABLE_NAME, refresh_id=run_info.refresh_id)
  time.sleep(10)

assert(run_info.state == lm.RefreshState.SUCCESS)

# COMMAND ----------

# Display profile metrics table
profile_table = f"{TABLE_NAME}_profile_metrics"
display(spark.sql(f"SELECT * FROM {profile_table}"))

# COMMAND ----------

# Display the drift metrics table
drift_table = f"{TABLE_NAME}_drift_metrics"
display(spark.sql(f"SELECT * FROM {drift_table}"))

# COMMAND ----------

# Delete the monitor

lm.delete_monitor(table_name=TABLE_NAME)
spark.sql("DROP TABLE demo_monitoring.features_profile_metrics")
spark.sql("DROP TABLE demo_monitoring.features_profile_metrics")
