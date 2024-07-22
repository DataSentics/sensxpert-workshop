# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
path = f"/FileStore/dbx-workshop/{username}/titanic"

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, BooleanType, StringType, IntegerType, DoubleType

schema_titanic = StructType() \
      .add("PassengerId",IntegerType(),True) \
      .add("Survived",BooleanType(),True) \
      .add("Pclass",StringType(),True) \
      .add("Name",StringType(),True) \
      .add("Sex",StringType(),True) \
      .add("Age",IntegerType(),True) \
      .add("SibSp",IntegerType(),True) \
      .add("Parch",StringType(),True) \
      .add("Ticket",StringType(),True) \
      .add("Fare",DoubleType(),True) \
      .add("Cabin",StringType(),True) \
      .add("Embarked",StringType(),True)

schema_income = StructType() \
      .add("PassengerId",IntegerType(),True) \
      .add("MonthlyIncome",DoubleType(),True) \
      .add("Savings",DoubleType(),True)


df_titanic = (spark.read
              .option("header", "true")
              .schema(schema_titanic)
              .csv('file:/dbfs/FileStore/python-workshop/titanic.csv')
             )
df_titanic_income = (spark.read
                     .option("header", "true")
                     .schema(schema_income)
                     .csv('file:/dbfs/FileStore/python-workshop/titanic_income_savings.csv')
                    )
df_joined = df_titanic.join(df_titanic_income, "PassengerId", "left")
df_filtered = df_joined.filter(col("Age").isNotNull())

display(df_filtered)

(
df_filtered.write
  .mode("overwrite")
  .partitionBy("Sex")
  .format("delta")
  .save(path)
)
