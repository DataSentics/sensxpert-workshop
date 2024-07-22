# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment
# MAGIC * This is assignment for practice Pandas, Scikit-learn and Decorators.
# MAGIC * Feel free to use all notebooks with examples and documentations.

# COMMAND ----------

# Here you can add imports
import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import plot_confusion_matrix
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# DBTITLE 1,Paths to datasets
# run
source_titanic = "file:/dbfs/FileStore/python-workshop/titanic.csv"
source_titanic_income = "file:/dbfs/FileStore/python-workshop/titanic_income_savings.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1
# MAGIC * Read datasets titanc and titanic_income as pandas dataframes

# COMMAND ----------

# TODO write your solution here 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2
# MAGIC * For the first dataset (titanic) answer questions below:
# MAGIC   1) According to Wikipedia, there was an estimated 2,224 passengers and crew onboard the Titanic when it sank. How many of them do we have information for in this dataset?
# MAGIC   2) Of the people we have data for, how many of them survived and how many did not? (Visualize the result as barchart.)
# MAGIC   3) What is the overall survival rate?
# MAGIC   4) How many passengers on the Titanic were males and how many were females in each ticket class?

# COMMAND ----------

# TODO write your solution here for 2.1

# COMMAND ----------

# TODO write your solution here for 2.2

# COMMAND ----------

# TODO write your solution here for 2.3

# COMMAND ----------

# TODO write your solution here for 2.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3
# MAGIC * Join the two datasets on "id" and answer questions below:
# MAGIC   1) For how many people we have income information about?
# MAGIC   2) Which male and female who survived has the highest income?
# MAGIC   3) What is the average savings of people that did not survive?

# COMMAND ----------

# TODO write your solution here for 3.1

# COMMAND ----------

# TODO write your solution here for 3.2

# COMMAND ----------

# TODO write your solution here for 3.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4
# MAGIC * define simple function with some parameters which will by decorated by log

# COMMAND ----------

# This is already made decorator for you, you just need to use it on your function
def log(func):
    def inner(*args, **kwargs):
        print("Accessed the function '{}' with arguments {}".format(func.__name__, args, kwargs))
        return func(*args, **kwargs)
    return inner

# TODO here define and decorate your function it can be a really simple function with a parameter. Just returning the parameter is enough. 

# TODO call your decorated function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5
# MAGIC * From the first dataset remove features (PassengerId, Name, Cabine)
# MAGIC * Train classifier of your choice (from Scikit-learn library) on the new dataset - with the decorate function measure training time
# MAGIC * Find model accurancy
# MAGIC * Visualize confusion matrix
# MAGIC * Visualize feature importance

# COMMAND ----------

# First we need to drop or replace null values.
df_titanic = df_titanic.dropna()
# Then we need to encode the labels because it is in string and it have to be numerical.
df_titanic_label = df_titanic["Survived"]
label_encoder = preprocessing.LabelEncoder()
label_encoder.fit(df_titanic['Survived'])
df_titanic['Survived']=label_encoder.transform(df_titanic['Survived'])
# Than we need to drop "PassengerId", "Name", "Cabin", "Ticket" because it is unique for every passenger
# and "Survived" because it is target variable.
df_titanic_features = df_titanic.drop(columns=["PassengerId", "Name", "Cabin", "Survived", "Ticket"])
df_titanic_features = pd.get_dummies(data=df_titanic_features, drop_first=False)

# TODO Perform the train test split on df_titanc_features and df_titanc_label
x_train,x_test,y_train,y_test = 
# TODO Create RandomForestClassifier from the imported module RandomForestClassifier. It is already imported for you
clf_forest = 
# TODO Fit the  RandomForestClassifier

# TODO Make a prediction on RandomForestClassifier

