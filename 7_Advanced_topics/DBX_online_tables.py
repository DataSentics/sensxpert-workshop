# Databricks notebook source
# MAGIC %md
# MAGIC # Online Feature Store example notebook
# MAGIC
# MAGIC This notebook illustrates the use of Databricks Feature Store to publish features to Databricks Online Tables for real-time 
# MAGIC serving and automated feature lookup. The problem is to predict the wine quality using a ML model
# MAGIC with a variety of static wine features and a realtime input.
# MAGIC
# MAGIC This notebook creates an endpoint to predict the quality of a bottle of wine, given an ID and the realtime feature alcohol by volume (ABV).
# MAGIC
# MAGIC The notebook is structured as follows:
# MAGIC  
# MAGIC 1. Prepare the feature table.
# MAGIC 2. Set up Databricks Online Table.
# MAGIC     * This notebook uses Databricks Online Tables. For a list of supported functionality, see the Databricks documentation ([AWS](https://docs.databricks.com/machine-learning/feature-store/online-tables.html)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables)).  
# MAGIC 3. Train and deploy the model.
# MAGIC 4. Serve realtime queries with automatic feature lookup.
# MAGIC 5. Clean up.
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC * Databricks Runtime 14.2 for Machine Learning or above.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/online-tables-nb-diagram.png"/>

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC %pip install mlflow>=2.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import requests
import pandas as pd
import statistics
import random
import time
import pyspark.sql.functions as F
import json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare the feature table
# MAGIC
# MAGIC Suppose you need to build an endpoint to predict wine quality with just the `wine_id`. This requires a feature table saved in Feature Store where the endpoint can look up features of the wine by the `wine_id`. For the purpose of this demo, we need to prepare this feature table ourselves first. The steps are:
# MAGIC
# MAGIC 1. Load and clean the raw data.
# MAGIC 2. Separate features and labels.
# MAGIC 3. Save features into a feature table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and clean the raw data 
# MAGIC
# MAGIC The raw data contains 12 columns including 11 features and the `quality` column. The `quality` column is an integer that ranges from 3 to 8. The goal is to build a model that predicts the `quality` value.

# COMMAND ----------

raw_data_frame = spark.read.table("olo_online_tables_demo.olo_online_tables.customer_features")
display(raw_data_frame.limit(10))

# COMMAND ----------

# Have a look at the size of the raw data.
raw_data_frame.count()

# COMMAND ----------

# MAGIC %md
# MAGIC There are some problems with the raw data:
# MAGIC 1. The column names contain space (' '), which is not compatible with Feature Store. 
# MAGIC 2. We need to add ID to the raw data so they can be looked up later by Feature Store.
# MAGIC
# MAGIC The following cell addresses these issues.

# COMMAND ----------

# DBTITLE 1,Spark Dataframe Preprocessing
from sklearn.preprocessing import MinMaxScaler
from pyspark.sql.functions import monotonically_increasing_id


def addIdColumn(dataframe, id_column_name):
    columns = dataframe.columns
    new_df = dataframe.withColumn(id_column_name, monotonically_increasing_id())
    return new_df[[id_column_name] + columns]

def renameColumns(df):
    renamed_df = df
    for column in df.columns:
        renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
    return renamed_df


# Rename columns so that they are compatible with Feature Store
renamed_df = renameColumns(raw_data_frame)

# Add id column
id_and_data = addIdColumn(renamed_df, 'id')

display(id_and_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's assume that the alcohol by volume (ABV) is a variable that changes over time after the wine is opened. The value will be provided as a real-time input for online inference. 
# MAGIC
# MAGIC Now, split the data into two parts and store only the part with static features to Feature Store. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a feature table
# MAGIC
# MAGIC Save the feature data `id_static_features` into a feature table.

# COMMAND ----------

# You must have `CREATE CATALOG` privileges on the catalog.
# If necessary, change the catalog and schema name here.
username = spark.sql("SELECT current_user()").first()["current_user()"]
username = username.split(".")[0]
catalog_name = 'olo_online_tables_demo'

# Fetch the username to use as the schema name.
schema_name = "olo_online_tables"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

customer_table = f"{catalog_name}.{schema_name}.customer_features_static"
online_table_name = f"{catalog_name}.{schema_name}.customer_features_online"
fe = FeatureEngineeringClient()
fe.create_table(
    name=customer_table,
    primary_keys=["id"],
    df=id_and_data,
    description="id and features of all customers",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE olo_online_tables_demo.olo_online_tables.customer_features_static SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC The feature data has now been stored into the feature table. The next step is to set up a Databricks Online Table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Databricks Online Tables
# MAGIC
# MAGIC You can create an online table from the Catalog Explorer UI, Databricks SDK or Rest API. The steps to use Databricks python SDK are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#create)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#create)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).

# COMMAND ----------

# DBTITLE 1,Databricks Online Table Creation
from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

w = WorkspaceClient()

# Create an online table
spec = OnlineTableSpec(
  primary_key_columns=["id"],
  source_table_full_name="olo_online_tables_demo.olo_online_tables.customer_features_static",
  run_continuously=OnlineTableSpecContinuousSchedulingPolicy.from_dict({'continuous': 'true'})
)

w.online_tables.create(name='olo_online_tables_demo.olo_online_tables.customer_features_online', spec=spec)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION olo_online_tables_demo.olo_online_tables.test_function(transactions_sum_amount_in_last_30d DOUBLE, transactions_sum_amount_in_last_60d DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON AS
$$

def difference_60_30days(transactions_sum_amount_in_last_30d, transactions_sum_amount_in_last_60d):
    # Calculate price with tax
    difference = transactions_sum_amount_in_last_60d - transactions_sum_amount_in_last_30d

    return difference

return difference_60_30days(transactions_sum_amount_in_last_30d, transactions_sum_amount_in_last_60d)
$$""")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup, FeatureFunction

fe = FeatureEngineeringClient()
fe.create_feature_spec(
  name="olo_online_tables_demo.olo_online_tables.customer_features_spec",
  features=[
    FeatureLookup(
      table_name="olo_online_tables_demo.olo_online_tables.customer_features_static",
      lookup_key='id'
    ),
    FeatureFunction(
      udf_name="olo_online_tables_demo.olo_online_tables.test_function",
      input_bindings={
        "transactions_sum_amount_in_last_30d": "transactions_sum_amount_in_last_30d",
        "transactions_sum_amount_in_last_60d": "transactions_sum_amount_in_last_60d"
      },
      output_name="difference_60_30days"
  )
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create endpoint - small cluster

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

workspace = WorkspaceClient()

# Create endpoint
endpoint_name = "olo-customer-features-small"

workspace.serving_endpoints.create_and_wait(
  name=endpoint_name,
  config=EndpointCoreConfigInput(
    served_entities=[
      ServedEntityInput(
        entity_name='olo_online_tables_demo.olo_online_tables.customer_features_spec',
        scale_to_zero_enabled=True,
        workload_size="Small"
      )
    ]
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create endpoint - large cluster

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

workspace = WorkspaceClient()

# Create endpoint
endpoint_name = "olo-customer-features-large"

workspace.serving_endpoints.create_and_wait(
  name=endpoint_name,
  config=EndpointCoreConfigInput(
    served_entities=[
      ServedEntityInput(
        entity_name='olo_online_tables_demo.olo_online_tables.customer_features_spec',
        scale_to_zero_enabled=True,
        workload_size="Large"
      )
    ]
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the endpoint

# COMMAND ----------

DATABRICKS_TOKEN =  dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
url = "https://adb-8406481409439422.2.azuredatabricks.net/serving-endpoints/olo-customer-features-small/invocations"
headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}

customer_data = [{'id': random. randint(1, 20000000)} for _ in range(10)]
data = {
    "dataframe_records": customer_data
}
data_json = json.dumps(data, allow_nan=True)
start_time = time.perf_counter()
response = requests.request(method='POST', headers=headers, url=url, data=data_json)
if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')

result_df = pd.DataFrame(response.json()['outputs'])

# Convert the Pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(result_df)
display(spark_df)

# COMMAND ----------

# specify values of the function parameters
DATABRICKS_TOKEN =  dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
url = "https://adb-8406481409439422.2.azuredatabricks.net/serving-endpoints/olo-customer-features-small/invocations"
headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}

customer_data = [{
                  'id': 0, 
                  'transactions_sum_amount_in_last_30d': 3, 
                  'transactions_sum_amount_in_last_60d': 1
                  }]
data = {
    "dataframe_records": customer_data
}
data_json = json.dumps(data, allow_nan=True)
start_time = time.perf_counter()
response = requests.request(method='POST', headers=headers, url=url, data=data_json)
if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')

result_df = pd.DataFrame(response.json()['outputs'])

# Convert the Pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(result_df)
display(spark_df)

# COMMAND ----------

def measure_extract_rows_by_ids(url, num_records):
    DATABRICKS_TOKEN =  dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}

    durations = []
    for _ in range(30):
        customer_data = [{'id': random. randint(1, 20000000)} for _ in range(num_records)]
        data = {
          "dataframe_records": customer_data
        }
        data_json = json.dumps(data, allow_nan=True)
        start_time = time.perf_counter()
        response = requests.request(method='POST', headers=headers, url=url, data=data_json)
        if response.status_code != 200:
          raise Exception(f'Request failed with status {response.status_code}, {response.text}')

        result_df = pd.DataFrame(response.json()['outputs'])
        end_time = time.perf_counter()
        duration = end_time - start_time
        durations.append(duration)

    print(durations)
    print(f"Average time {statistics.mean(durations)} s")

# COMMAND ----------

url = "https://adb-8406481409439422.2.azuredatabricks.net/serving-endpoints/olo-customer-features-small/invocations"
num_records = 50000
measure_extract_rows_by_ids(url, num_records)

# COMMAND ----------

url = "https://adb-8406481409439422.2.azuredatabricks.net/serving-endpoints/olo-customer-features-large/invocations"
num_records = 50000
measure_extract_rows_by_ids(url, num_records)

# COMMAND ----------

df = spark.read.table(f"{catalog_name}.{schema_name}.customer_features_static")
df = df.limit(1).withColumn("id", F.lit(30000000).cast("bigint"))
df.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.customer_features_static")

url = "https://adb-8406481409439422.2.azuredatabricks.net/serving-endpoints/olo-customer-features-large/invocations"

DATABRICKS_TOKEN =  dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}

customer_data = [{'id': 30000000, "transactions_sum_amount_in_last_30d": 1, "transactions_sum_amount_in_last_60d": 1}]
data = {
"dataframe_records": customer_data
}
data_json = json.dumps(data, allow_nan=True)
start_time = time.perf_counter()
response = requests.request(method='POST', headers=headers, url=url, data=data_json)
if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
print(response.json()['outputs'])

# COMMAND ----------

# Small cluster
# 500

[0.20818358399992576, 0.19213764500000252, 0.28626536299998406, 0.2122683400002643, 0.19490068800041627, 0.1808153450001555, 0.17778533599994262, 0.19681339399994613, 0.18804636699951516, 0.20430821599984483, 0.18459765700026765, 0.19523138900058257, 0.19301698300023418, 0.19716109499950107, 0.187031365000621, 0.19442538600014814, 0.19588369099983538, 0.18219644999953744, 0.19005067299985967, 0.21491754799990304, 0.19652909299929888, 0.1922510800004602, 0.2007216050005809, 0.18601326099997095, 0.18090184500033502, 0.1803624439999112, 0.19072057500034134, 0.19827479899959144, 0.2530728639994777, 0.18548485900009837]
# Average time 0.19801229800001843 s

# 5000

[0.7639402140002858, 0.7772703600003297, 0.7166262460004873, 0.8435503199998493, 0.8034895629998573, 0.7540716450002947, 0.7485500090006099, 0.7322692049992838, 0.7328363090000494, 0.7380514429996765, 0.7397622530006629, 0.8006747449999239, 0.7176366300000154, 0.7644519770001352, 0.7361969410003439, 0.7580500460007897, 0.849081784000191, 0.7474505949994636, 0.6982865589998255, 0.7482227989994499, 0.6847698929996113, 0.7701139040000271, 0.7458542869999292, 0.8124259119995259, 0.7621404110004733, 0.7184362500001953, 0.7824671869993836, 0.730181494000135, 0.7376728210001602, 0.7038448969997262]
# Average time 0.7539458899666898 s

# 50000

[11.33236265800042, 10.800320047999776, 10.691010404000735, 10.454801978999967, 10.702199135999763, 10.36688101100026, 10.5232483890004, 10.531852155999331, 10.319639500999983, 10.485092019000149, 10.375545568999769, 10.236694222000551, 10.320291766999617, 10.585576842000592, 10.253031048000594, 10.62366902300073, 10.525437551000323, 10.251684333999947, 10.103073870000117, 9.941999036999732, 9.672092349000195, 10.14344805699966, 9.973497328000121, 9.929823185000714, 9.823418695000328, 9.795475624000574, 9.849389559999508, 9.60104374399998, 9.958070542000314, 9.737218361000487]
# Average time 10.263596266966822 s

# COMMAND ----------


# Large cluster
# 500

[0.194370505999359, 0.21945317099925887, 0.21120495000013761, 0.2117154510006003, 0.19408120499974757, 0.19432950600003096, 0.18455598000036844, 0.1947614059999978, 0.20864794299995992, 0.1867747859996598, 0.22873499599973002, 0.16934804000084114, 0.17473155400057294, 0.19610911100062367, 0.19027229499988607, 0.18575328299993998, 0.17924616599975707, 0.1844186799999079, 0.1802457690000665, 0.20578403500076092, 0.1801765690006505, 0.19152539799961232, 0.2058894350002447, 0.19116429800033075, 0.18893369199940935, 0.2011286229999314, 0.18969139299952076, 0.18972799299990584, 0.1916509419997965, 0.20212528499996552]
# Average time 0.1942184153666858 s

# large cluster 5000


[0.7504661630000555, 0.7432290229999126, 0.7740191869997943, 0.7914505299995653, 0.7423116100007974, 0.7394557030002034, 0.828917120999904, 0.7691812749999372, 0.7100559309992605, 0.7777924969996093, 0.7565743440000006, 0.7240245739994862, 0.7216014880004877, 0.7314393130000099, 0.758555680000427, 0.7354872220003017, 0.7474277519995667, 0.7945808680005939, 0.7816458369998145, 0.7678369020004538, 0.7197166830001152, 0.7212655870007438, 0.774827472000652, 0.7372243290001279, 0.728338304999852, 0.7637209020003866, 0.7313415130001886, 0.7513655680004376, 0.7974441950000255, 0.7626250989997061]
# Average time 0.7544640891000806 s


# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
fe.delete_feature_spec(name="olo_online_tables_demo.olo_online_tables.customer_features_spec")
workspace.serving_endpoints.delete("olo-customer-features-small")
workspace.serving_endpoints.delete("olo-customer-features-large")
