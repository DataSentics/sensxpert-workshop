# Databricks notebook source
# MAGIC %md
# MAGIC # Features monitoring
# MAGIC
# MAGIC This is a demo notebook demonstrating how to use odap features monitoring tool. First, let's import all the important functions and notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC Imports

# COMMAND ----------

import sys
sys.path.append('/Workspace/Repos/ondrej.lostak@datasentics.com/odap-widgets/src')

# COMMAND ----------

from datetime import datetime, timedelta, date
from pyspark.sql import functions as F

from odap.api import QiFeaturesMonitoring, print_binning_overview, figures_binning_overview, QiApiPreprocess

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS olostak.qi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring process
# MAGIC
# MAGIC First, we reduce the number of categories of the categorical variables so that there are only a few categories with high occurrence and the less common categories are merged into one category named "other". We also use binning of continuous (numerical) variables. Then, we run a basic analysis of the features (share of the most common category, number of missing values, number of nulls, etc.). This stage of the monitoring process is called **preprocessing**.
# MAGIC
# MAGIC We may also perform **sampling** of the feature store if there was too much data and the monitoring would take too long to process. As for this moment, it is not necessary to perform sampling.
# MAGIC
# MAGIC We choose two dates (`obs_date`, `base_date`) to compare if there was any significant change in the features' distributions. The change in the distributions is evaluated by **JLH**, which is calculated as a maximum value of differences (in absolute values) between shares for individual categories, **distance**, which is the same as JLH only we do not take the maximum value but we sum the differences between shares. And then there is **PSI** (Population stability index, the explanation of its calculation is [here](https://mwburke.github.io/data%20science/2018/04/29/population-stability-index.html)). We can interpret the results of PSI as follows:
# MAGIC - PSI \\( \\lt \\) 0.1 ... no significant population change
# MAGIC - PSI \\( \\lt \\) 0.2 ... moderate population change
# MAGIC - PSI \\( \\geq \\) 0.2 ... significant population change

# COMMAND ----------

# MAGIC %md 
# MAGIC #### QI features monitoring function
# MAGIC
# MAGIC Parameters of the function are as follows:
# MAGIC - date_column ... date column name in the feature store table
# MAGIC - obs_date, base_date ... first and second chosen date to compare
# MAGIC - feature store tables
# MAGIC   - feature_store ... path of the feature store table
# MAGIC   - feature_store_sampled ... path of a sample of the feature store table (if needed)
# MAGIC   - feature_store_processed ... path of the processed feature store table to be created
# MAGIC   - feature_store_stats ... path of the stats feature store table to be created
# MAGIC - perform_sampling ... boolean parameter whether to perform sampling of the feature store
# MAGIC - perform_preprocessing .. boolean parameter whehter to perform preprocessing of the features
# MAGIC - features_list ... names of feature columns to monitor
# MAGIC - sample_rate ... rate of sampling
# MAGIC - run_logistic_regression ... boolean parameter whether to perform logistic regression to get more metrics (R squared etc.)
# MAGIC - regression_estimator ... which library to use to run the logistic regression
# MAGIC - run_analysis_without_missings ... boolean, useful if the feature store table contains high amount of nulls
# MAGIC - run_missings_rate ... boolean parameter whether to calculate missing rate of the two dates separately
# MAGIC - analyse_all_features ... boolean parameter whether to analyse all features or whether we analyse only those with "ok" variable status

# COMMAND ----------

df = spark.read.table("olostak.demo_monitoring.features")
df.columns

# COMMAND ----------

dbutils.widgets.text("obs_date", "")
dbutils.widgets.text("days_before", "")

# COMMAND ----------

obs_date = dbutils.widgets.get("obs_date")
days_before = dbutils.widgets.get("days_before")

base_date = (datetime.strptime(obs_date, '%Y-%m-%d') - timedelta(days = int(days_before))).strftime('%Y-%m-%d')
print(base_date)

# COMMAND ----------

# MAGIC %md 
# MAGIC Select features you want to monitor.

# COMMAND ----------

features_names = [
 'fixed_acidity',
 'volatile_acidity',
 'citric_acid',
 'residual_sugar',
 'chlorides',
 'free_sulfur_dioxide',
 'total_sulfur_dioxide',
 'density',
 'pH',
 'sulphates',
 'alcohol',
 'is_red',
]

# COMMAND ----------

df = spark.read.table("olostak.demo_monitoring.features")
features_names = df.columns[2:]

# COMMAND ----------

# MAGIC %md
# MAGIC Define the QI features monitoring function.

# COMMAND ----------

qi_feature_monitoring = QiFeaturesMonitoring(
    date_column="timestamp",
    obs_date=obs_date,
    base_date=base_date,
    feature_store="olostak.demo_monitoring.features",
    feature_store_sampled="olostak.demo_monitoring.features",
    feature_store_processed="olostak.qi.feature_store_processed",
    feature_store_stats="olostak.qi.feature_store_stats",
    perform_sampling=False,
    perform_preprocessing=True,
    features_list=features_names,
    sample_rate=0.1,
    run_logistic_regression=True,
    run_analysis_without_missings=False,
    run_missings_rate=False,
    regression_estimator="spark",
    analyse_all_features=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Run feature monitoring. This may take a few minutes to run.

# COMMAND ----------

qi_feature_monitoring_res = qi_feature_monitoring.runner()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC
# MAGIC Results of features monitoring may be seen by calling tables with functions `qi_feature_monitoring_res["features analysis"]` and `qi_feature_monitoring_res["features analysis missings excluded"]`. Along with the JLH and PSI, we get the information about the missing rate, the percentage of the most common category, or the feature type and status.
# MAGIC
# MAGIC We can also see the results by printing graphs (histograms of bins) with the function `figures_binning_overview(qi_feature_monitoring_res["binning plots input"])`. Here we can observe the percentage of the observations between the bins and compare it with the two dates of interest.

# COMMAND ----------

display(qi_feature_monitoring_res["features analysis missings excluded"])

# COMMAND ----------

display(qi_feature_monitoring_res["features analysis"])

# COMMAND ----------

figures_binning_overview(qi_feature_monitoring_res["binning plots input"], keep_missings=False)
# keep_missings=False to exclude the missings

# COMMAND ----------

print_binning_overview(qi_feature_monitoring_res["binning overview input"])
