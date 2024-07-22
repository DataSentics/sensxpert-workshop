# Databricks notebook source
# MAGIC %md 
# MAGIC ## Distributed Hyperopt and automated MLflow tracking
# MAGIC
# MAGIC [Hyperopt](https://github.com/hyperopt/hyperopt) is a Python library for hyperparameter tuning. Databricks Runtime for Machine Learning includes an optimized and enhanced version of Hyperopt, including automated MLflow tracking and the `SparkTrials` class for distributed tuning.  
# MAGIC
# MAGIC This notebook illustrates how to scale up hyperparameter tuning for a single-machine Python ML algorithm and track the results using MLflow. In part 1, you create a single-machine Hyperopt workflow. In part 2, you learn to use the `SparkTrials` class to distribute the workflow calculations across the Spark cluster. 

# COMMAND ----------

# MAGIC %md ## Import required packages and load dataset

# COMMAND ----------

from sklearn.datasets import load_iris
from sklearn.model_selection import cross_val_score
from sklearn.svm import SVC

from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK, Trials

# If you are running Databricks Runtime for Machine Learning, `mlflow` is already installed and you can skip the following line. 
import mlflow

# COMMAND ----------

# Load the iris dataset from scikit-learn
iris = iris = load_iris()
X = iris.data
y = iris.target

# COMMAND ----------

# MAGIC %md ## Part 1. Single-machine Hyperopt workflow
# MAGIC
# MAGIC Here are the steps in a Hyperopt workflow:  
# MAGIC 1. Define a function to minimize.  
# MAGIC 2. Define a search space over hyperparameters.  
# MAGIC 3. Select a search algorithm.  
# MAGIC 4. Run the tuning algorithm with Hyperopt `fmin()`.
# MAGIC
# MAGIC For more information, see the [Hyperopt documentation](https://github.com/hyperopt/hyperopt/wiki/FMin).

# COMMAND ----------

# MAGIC %md ### Define a function to minimize
# MAGIC In this example, we use a support vector machine classifier. The objective is to find the best value for the regularization parameter `C`.  
# MAGIC
# MAGIC Most of the code for a Hyperopt workflow is in the objective function. This example uses the [support vector classifier from scikit-learn](https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html).
# MAGIC
# MAGIC If your cluster uses Databricks Runtime 11.3 ML, edit the support vector classifier to take a positional argument, `clf = SVC(C)`. 

# COMMAND ----------

def objective(C):
    # Create a support vector classifier model
    clf = SVC(C=C)
    
    # Use the cross-validation accuracy to compare the models' performance
    accuracy = cross_val_score(clf, X, y).mean()
    
    # Hyperopt tries to minimize the objective function. A higher accuracy value means a better model, so you must return the negative accuracy.
    return {'loss': -accuracy, 'status': STATUS_OK}

# COMMAND ----------

# MAGIC %md ### Define the search space over hyperparameters
# MAGIC
# MAGIC See the [Hyperopt docs](https://github.com/hyperopt/hyperopt/wiki/FMin#21-parameter-expressions) for details on defining a search space and parameter expressions.

# COMMAND ----------

search_space = hp.lognormal('C', 0, 1.0)

# COMMAND ----------

# MAGIC %md ### Select a search algorithm
# MAGIC
# MAGIC The two main choices are:
# MAGIC * `hyperopt.tpe.suggest`: Tree of Parzen Estimators, a Bayesian approach which iteratively and adaptively selects new hyperparameter settings to explore based on past results
# MAGIC * `hyperopt.rand.suggest`: Random search, a non-adaptive approach that samples over the search space

# COMMAND ----------

algo=tpe.suggest

# COMMAND ----------

# MAGIC %md Run the tuning algorithm with Hyperopt `fmin()`
# MAGIC
# MAGIC Set `max_evals` to the maximum number of points in hyperparameter space to test, that is, the maximum number of models to fit and evaluate.

# COMMAND ----------

argmin = fmin(
  fn=objective,
  space=search_space,
  algo=algo,
  max_evals=16)

# COMMAND ----------

# Print the best value found for C
print("Best value found: ", argmin)

# COMMAND ----------

# MAGIC %md ## Part 2. Distributed tuning using Apache Spark and MLflow
# MAGIC
# MAGIC To distribute tuning, add one more argument to `fmin()`: a `Trials` class called `SparkTrials`. 
# MAGIC
# MAGIC `SparkTrials` takes 2 optional arguments:  
# MAGIC * `parallelism`: Number of models to fit and evaluate concurrently. The default is the number of available Spark task slots.
# MAGIC * `timeout`: Maximum time (in seconds) that `fmin()` can run. The default is no maximum time limit.
# MAGIC
# MAGIC This example uses the very simple objective function defined in Cmd 7. In this case, the function runs quickly and the overhead of starting the Spark jobs dominates the calculation time, so the calculations for the distributed case take more time. For typical real-world problems, the objective function is more complex, and using `SparkTrails` to distribute the calculations will be faster than single-machine tuning.
# MAGIC
# MAGIC Automated MLflow tracking is enabled by default. To use it, call `mlflow.start_run()` before calling `fmin()` as shown in the example.

# COMMAND ----------

from hyperopt import SparkTrials

# To display the API documentation for the SparkTrials class, uncomment the following line.
# help(SparkTrials)

# COMMAND ----------

spark_trials = SparkTrials(4)

with mlflow.start_run():
  argmin = fmin(
    fn=objective,
    space=search_space,
    algo=algo,
    max_evals=16,
    trials=spark_trials)

# COMMAND ----------

# Print the best value found for C
print("Best value found: ", argmin)

# COMMAND ----------

# MAGIC %md To view the MLflow experiment associated with the notebook, click the **Experiment** icon in the notebook context bar on the upper right.  There, you can view all runs. To view runs in the MLflow UI, click the icon at the far right next to **Experiment Runs**. 
# MAGIC
# MAGIC To examine the effect of tuning `C`:
# MAGIC
# MAGIC 1. Select the resulting runs and click **Compare**.
# MAGIC 1. In the Scatter Plot, select **C** for X-axis and **loss** for Y-axis.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train models for different groups in parallel

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, FloatType, DoubleType, StringType

# Define the schema
schema = StructType([
    StructField("SepalLength", DoubleType()),
    StructField("SepalWidth", DoubleType()),
    StructField("PetalLength", DoubleType()),
    StructField("PetalWidth", DoubleType()),
    StructField("Name", StringType())
])

df = spark.read.schema(schema).option("header", "true").csv("dbfs:/FileStore/python-workshop/iris.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
from sklearn.linear_model import LinearRegression
import mlflow
import mlflow.sklearn
import os


def train_model(df: pd.DataFrame) -> pd.DataFrame:
    group = df['Name'].iloc[0]
    X = df[['SepalLength', 'SepalWidth', 'PetalLength']]
    y = df['PetalWidth']
    
    model = LinearRegression()
    
    with mlflow.start_run() as run:
        model.fit(X, y)
        
        # Log the model
        mlflow.sklearn.log_model(model, "model")
        
        # Get the run ID and artifact URI
        run_id = run.info.run_id
        artifact_uri = mlflow.get_artifact_uri("model")
    
    return pd.DataFrame({'group': [group], 'run_id': [run_id], 'artifact_uri': [artifact_uri]})

# Apply the function to each group
result_schema = StructType([
    StructField("group", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("artifact_uri", StringType(), True)
])

models_df = df.groupby('Name').applyInPandas(train_model, schema=result_schema)

# Show the resulting dataframe
display(models_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paralelized inference

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/d6b19b9aaa0f4244a4a5d4eae36a9719/iris_model'

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Predict on a Spark DataFrame.
df_predictions = df.withColumn('predictions', loaded_model(struct(*map(col, ['SepalLength', 'SepalWidth', 'PetalLength', 'PetalWidth']))))
display(df_predictions)

# COMMAND ----------

import mlflow
logged_model = 'runs:/d6b19b9aaa0f4244a4a5d4eae36a9719/iris_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd

data = df.drop("Name").toPandas()
loaded_model.predict(pd.DataFrame(data))
