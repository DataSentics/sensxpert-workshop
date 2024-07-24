# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Spark ML
# MAGIC
# MAGIC [Spark Machine Learning Library Guide](https://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC
# MAGIC Spark ML is machine learning framework leveraging paralelization inherent to Spark. There are a lot of building blocks all paralelized that can be composed into pipelines.
# MAGIC
# MAGIC **Preprocessing**:
# MAGIC VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler, MinMaxScaler, Normalizer, PCA, ... 
# MAGIC
# MAGIC **Models**: 
# MAGIC - regression: LogisticRegression, RandomForestRegressor, 
# MAGIC - classifiers: DecisionTreeClassifier, GBTClassifier, MultilayerPerceptronClassifier, LinearSVC, ...
# MAGIC
# MAGIC
# MAGIC **Evaluation**: MulticlassClassificationEvaluator, RegressionEvaluator ... 

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import pyspark.sql.functions as F 

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
database = username + "_db"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load data

# COMMAND ----------

df = (spark.table(f"{database}.iris_table")
        .withColumn("SepalLength", F.col("SepalLength").cast("float"))
        .withColumn("SepalWidth", F.col("SepalWidth").cast("float"))
        .withColumn("PetalLength", F.col("PetalLength").cast("float"))
        .withColumn("PetalWidth", F.col("PetalWidth").cast("float"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare data for training 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vector assembler
# MAGIC
# MAGIC [documentation](convert sepal and petal columns to floats, thera name SepalLength, SepalWidth etc.)
# MAGIC
# MAGIC The vector assembler will express the features efficiently using techniques like spark vector, which allow a larger amount of data to be handled with less memory. This helps the modeling algorithms run efficiently even on large data columns.

# COMMAND ----------

feature_cols = df.columns[:-1]

# COMMAND ----------

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
data = assembler.transform(df)

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Labels
# MAGIC
# MAGIC

# COMMAND ----------

data = data.select(['features', 'Name'])
label_indexer = StringIndexer(inputCol='Name', outputCol='label').fit(data)
data = label_indexer.transform(data)
display(data)

# COMMAND ----------

data = data.select(["features", "label"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train/test split

# COMMAND ----------

splits = data.randomSplit([0.7, 0.3])
train = splits[0]
test = splits[1]
train_rows = train.count()
test_rows = test.count()
print("Training Rows:", train_rows, " Testing Rows:", test_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train

# COMMAND ----------

lr = LogisticRegression(labelCol="label",featuresCol="features", maxIter=10,regParam=0.3)

# COMMAND ----------

model = lr.fit(train)

# COMMAND ----------

m = model.coefficientMatrix
b = model.interceptVector

# COMMAND ----------

predict_train = model.transform(train)
display(predict_train)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use pipeline

# COMMAND ----------

pipe = Pipeline(stages=[assembler, label_indexer, lr])

# COMMAND ----------

pipe = pipe.fit(df)
pipe.transform(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test

# COMMAND ----------

predict_test = model.transform(test)
display(predict_test)

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator()
for m in ["f1", "accuracy", "weightedPrecision", "weightedRecall"]:
    print(f"{m} = {evaluator.evaluate(predict_test, {evaluator.metricName: m})}")
