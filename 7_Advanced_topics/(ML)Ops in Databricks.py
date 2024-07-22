# Databricks notebook source
# MAGIC %md ## Basic (ML)Ops in Databricks

# COMMAND ----------

# MAGIC %md ## TODO: Create a simple model and get it into production
# MAGIC Flow: Do the experiments and once satisfied, register the best model from experiments to model registry.

# COMMAND ----------

from sklearn import datasets
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow

# COMMAND ----------

dataset = datasets.load_wine()

# COMMAND ----------

df = pd.DataFrame(data=dataset.data, columns=dataset.feature_names)

# COMMAND ----------

exp_id = mlflow.set_experiment('/Shared/Experiments/wine')
with mlflow.start_run(run_name = 'advanced_forrest') as run:
    mlflow.autolog()
    X_train, X_test, y_train, y_test = train_test_split(df, dataset.target)
    clf = RandomForestClassifier(max_depth=2)
    clf.fit(X_train, y_train)
    preds = clf.predict(X_test)
    mlflow.log_metric('some_custom_metric', 5)

# COMMAND ----------

# MAGIC %md ## Use the registered model

# COMMAND ----------

model = mlflow.pyfunc.load_model('models:/wine_model_september/Production')

# COMMAND ----------

model.predict(X_test)

# COMMAND ----------

X_test[:2].to_json(orient='split')

# COMMAND ----------

# MAGIC %md {
# MAGIC "dataframe_split":
# MAGIC {"index":[0],
# MAGIC "columns":["alcohol","malic_acid","ash","alcalinity_of_ash","magnesium","total_phenols","flavanoids","nonflavanoid_phenols","proanthocyanins","color_intensity","hue","od280/od315_of_diluted_wines","proline","target"],"index":[132, 38],"data":[[12.81,2.31,2.4,24.0,98.0,1.15,1.09,0.27,0.83,5.7,0.66,1.36,560.0,2],[13.07,1.5,2.1,15.5,98.0,2.4,2.64,0.28,1.37,3.7,1.18,2.69,1020.0,0]]}
# MAGIC }

# COMMAND ----------

# MAGIC %md ## Use the model for inference

# COMMAND ----------

# MAGIC %md ## Re-train the model

# COMMAND ----------

exp_id = mlflow.set_experiment('/Shared/Experiments/wine')
with mlflow.start_run(run_name = 'advanced_random_forrest') as run:
    mlflow.autolog()
    X_train, X_test, y_train, y_test = train_test_split(df, dataset.target)
    clf = RandomForestClassifier(max_depth=5)
    clf.fit(X_train, y_train)
    preds = clf.predict(X_test)
    mlflow.log_metric('some_custom_metric', 5)

# COMMAND ----------

# MAGIC %md ## Re-deploy
