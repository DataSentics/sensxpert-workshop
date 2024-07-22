# Databricks notebook source
# MAGIC %md
# MAGIC # Pandas
# MAGIC * [Documentation](https://pandas.pydata.org/docs/)
# MAGIC * Library for working with tabular data and perform all parts of the analysis from collection and manipulation through aggregation and visualization.
# MAGIC * The name is derived from "panel data", term from the econometrics
# MAGIC * It can be used with other common Python packages, such as NumPy, matplotlib and scikit-learn

# COMMAND ----------

# DBTITLE 1,Create SQL tables
# MAGIC %run ./Setup-datasets

# COMMAND ----------

import pandas as pd

# COMMAND ----------

source_tips = "file:/dbfs/FileStore/python-workshop/tips.csv"
source_iris = "file:/dbfs/FileStore/python-workshop/iris.csv"
source_air_quality_no2 = "file:/dbfs/FileStore/python-workshop/air_quality_pm25_long.csv"
source_air_quality_pm25 = "file:/dbfs/FileStore/python-workshop/air_quality_no2_long.csv"

df_iris = pd.read_csv(source_iris)
df_air_quality_pm25 = pd.read_csv(source_air_quality_pm25)
df_air_quality_no2 = pd.read_csv(source_air_quality_no2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame
# MAGIC - Data in table representation
# MAGIC - Pandas supports many formats (csv, excel, sql, json, parquet,…)
# MAGIC - A Series is essentially a column
# MAGIC - DataFrame is a multi-dimensional table made up of a collection of Series

# COMMAND ----------

# DBTITLE 1,Read CSV to DataFrame
df_tips = pd.read_csv("file:/dbfs/FileStore/python-workshop/tips.csv")
df_tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display first N rows

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tips_table LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display schema

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display size

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM tips_table;

# COMMAND ----------

# DBTITLE 1,Pandas
# how to find table size (nrows, ncolumns)
df_tips.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get one column

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT total_bill 
# MAGIC FROM tips_table;

# COMMAND ----------

# DBTITLE 1,Pandas
total = df_tips["total_bill"]
total

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get subset of columns

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT tip, day, time 
# MAGIC FROM tips_table;

# COMMAND ----------

# DBTITLE 1,Pandas
# get new DataFrame with subset of columns
tips_daytime = df_tips[["tip", "day", "time"]]
tips_daytime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter rows

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM tips_table 
# MAGIC WHERE tip > 3 AND time == "Lunch";

# COMMAND ----------

# DBTITLE 1,Pandas
# select files where is condition True
# allowed operations (>, <, ==, >=, <=, !=, .isin([]), .notna()...), logical operators (|, &)
filtered_tips = df_tips[(df_tips["tip"] > 3) & (df_tips["time"] == "Lunch")]
filtered_tips

# COMMAND ----------

# DBTITLE 1,TODO
# TODO
# Filter rows in dataframe df_tips where total_bill is higher or equal than 20 and smoker is "No".
# Select columns "total_bill", "tip", "day" and "time". Display 20 first rows.
# Find the number of rows in the filtered dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter NaN values and IN

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM tips_table 
# MAGIC WHERE total_bill IS NOT NULL AND day IN ("Sun", "Sat");

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[(df_tips["total_bill"].notna()) & (df_tips["day"].isin(["Sun", "Sat"]))]

# COMMAND ----------

# DBTITLE 0,Pandas Drop NaN
# add NaN value
df = pd.read_csv(source_tips)
df.iloc[0, 0] = None

# COMMAND ----------

# DBTITLE 0,Check null values
# check NaN values
df.isnull().any()

# COMMAND ----------

# drop NaN values / fill NaN values
df.dropna()
#df.fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select specific rows and columns by condition

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT day, time, size 
# MAGIC FROM tips_table 
# MAGIC WHERE size > 2;

# COMMAND ----------

# DBTITLE 1,Pandas
df_families = df_tips.loc[df_tips["size"] > 2, ["day", "time", "size"]]
df_families

# COMMAND ----------

# DBTITLE 1,Pandas
# Access specific rows and columns by index
df_tips.iloc[0:25, 0:4]

# COMMAND ----------

# DBTITLE 1,TODO
# TODO
# Create two new dataframes:
# First: Select from tips rows where "sex" is "Female" and select columns "total_bill", "tip", "day" and "time". Hint: Use method loc.
# Second: Select from tips columns "tip", "sex" and "smoker" and first 30 rows. Hint: Use method iloc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new column

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT total_bill, tip, sex, smoker, day, time, size, CASE
# MAGIC       WHEN size > 2 THEN true
# MAGIC       ELSE false
# MAGIC    END AS family
# MAGIC    FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["family"] = df_tips["size"] > 2
df_tips.head(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Map

# COMMAND ----------

# Map is defined only for Series(a column) and is optimized for mapping values from one domain to another
# Map accepts dict, Series or callable(function) as a argument
df_tips["family"] = (df_tips["size"] > 2).map({True: "Yes", False: "No"})
df_tips.head(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply

# COMMAND ----------

# Apply is defined both on Dataframe and Series, and is designated to apply a function on the data
# Apply accepts callable only
df_tips["family"] = (df_tips["family"]).apply(lambda x: "Is family: " + x)
df_tips.head(30)

# COMMAND ----------

# DBTITLE 1,TODO
# TODO
# Create a new column as how much percent was the tip from total_bill

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column

# COMMAND ----------

# DBTITLE 1,Pandas
df_renamed = df_tips.rename(columns={"time": "meal_type"})
df_renamed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join two tables

# COMMAND ----------

df_air_quality_no2

# COMMAND ----------

df_air_quality_pm25

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM air_quality_pm25_table LEFT JOIN air_quality_no2_table 
# MAGIC ON air_quality_pm25_table.date_utc=air_quality_no2_table.date_utc AND air_quality_pm25_table.location=air_quality_no2_table.location;

# COMMAND ----------

# DBTITLE 1,Pandas
pd.merge(df_air_quality_no2, df_air_quality_pm25, how="left", on=["date_utc", "location"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine data from multiple tables

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC         *
# MAGIC     FROM
# MAGIC        air_quality_pm25_table
# MAGIC UNION
# MAGIC     SELECT
# MAGIC         *
# MAGIC     FROM
# MAGIC         air_quality_no2_table

# COMMAND ----------

# DBTITLE 1,Pandas
pd.concat([df_air_quality_pm25, df_air_quality_no2])

# COMMAND ----------

# DBTITLE 1,TODO
# TODO
# Join two tables df_air_quality_no2, df_air_quality_pm25 (how: "full", on: "city", "country", "date_utc", "location".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregating statistics

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT MEAN(tip) 
# MAGIC FROM tips_table;

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["tip"].mean()

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT MEAN(size), MEAN(tip) 
# MAGIC FROM tips_table;

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["size", "tip"]].mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["size", "tip"]].describe()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.agg(
  {
    "size": ["min", "max", "median"],
    "tip": ["min", "max", "median", "mean"],
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group by

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT time, MEAN(tip)
# MAGIC FROM tips_table
# MAGIC GROUP BY time

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["time", "tip"]].groupby("time").mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.groupby("time").mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.groupby(["day", "time"]).mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["day"].value_counts()

# COMMAND ----------

# TODO
# Group tips by sex and find count of values and tip mean.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic visualizations

# COMMAND ----------

# DBTITLE 1,Pandas
# Plot
df_iris.plot(figsize=(10, 6))

# COMMAND ----------

# DBTITLE 1,Pandas
# Box-plot
df_iris.plot.box(figsize=(10, 6))

# COMMAND ----------

# DBTITLE 1,Pandas
# Scatter-plot
colors = {"Iris-setosa": "red", "Iris-versicolor": "green", "Iris-virginica": "blue"}
df_iris.plot.scatter(
  "PetalLength", "PetalWidth", c=df_iris["Name"].map(colors), figsize=(10, 6)
)
