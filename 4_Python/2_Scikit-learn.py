# Databricks notebook source
# MAGIC %md
# MAGIC # Scikit-learn
# MAGIC * [Documentation](https://scikit-learn.org/stable/)
# MAGIC Scikit-learn (Sklearn) is the most useful and robust library for machine learning in Python. It provides a selection of efficient tools for machine learning and statistical modeling including classification, regression, clustering and dimensionality reduction via a consistence interface in Python.

# COMMAND ----------

import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import plot_confusion_matrix
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Iris classification

# COMMAND ----------

df_iris = pd.read_csv("./data/iris.csv")
df_iris

# COMMAND ----------

# DBTITLE 1,Convert target variable to numbers
# Encode target variable to numerical values
label_encoder = preprocessing.LabelEncoder()
label_encoder.fit(df_iris['Name'])
df_iris['Name']=label_encoder.transform(df_iris['Name'])
df_iris

# COMMAND ----------

# DBTITLE 1,Remove target variable from training data
# Separate target variable features
df_iris_label = df_iris["Name"]
df_iris_data = df_iris.drop(columns=["Name"])

# COMMAND ----------

# DBTITLE 1,Split dataset to train and test data
# test_size = fraction of data which is used for testing
x_train,x_test,y_train,y_test = train_test_split(df_iris_data, df_iris_label, test_size=0.3, random_state=123)

# COMMAND ----------

# DBTITLE 1,Train RandomForest
# Create and train model
clf_forest = RandomForestClassifier(random_state=0)
clf_forest.fit(x_train, y_train)

# COMMAND ----------

# DBTITLE 1,Prediction on test data
# Try predict test data
predicted = clf_forest.predict(x_test)
predicted

# COMMAND ----------

# DBTITLE 1,Model accurancy
# Measure model accurancy
accuracy_score(y_test, predicted, normalize=True)

# COMMAND ----------

# DBTITLE 1,Visualize confusion matrix
# Visualize true positives and negative positives
plot_confusion_matrix(clf_forest, x_test, y_test)
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize feature importance
# Visualize feature importance
importances = clf_forest.feature_importances_
feature_importances = pd.Series(importances, index=df_iris_data.columns)
feature_importances.plot.bar()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tips regression

# COMMAND ----------

df_tips = pd.read_csv("./data/tips.csv")
df_tips

# COMMAND ----------

df_tip_target = df_tips["tip"]
df_tip_features = df_tips.drop(columns=["tip"])

# COMMAND ----------

x_train,x_test,y_train,y_test = train_test_split(df_tip_features, df_tip_target, test_size=0.2, random_state=123)

# COMMAND ----------

# convert categorical variables into indicator variables (booleans)
X = pd.get_dummies(data=x_train, drop_first=False)
# fit linear regression model
reg = LinearRegression().fit(X, y_train)
reg.score(X, y_train)

# COMMAND ----------

# try to predict test data
predicted = reg.predict(pd.get_dummies(data=x_test, drop_first=False))

# COMMAND ----------

# visualize differences between predicted and real target variable
df = pd.DataFrame({'test': y_test, 'predicted': predicted})
df = df.reset_index()
df = df.drop(columns=["index"])
df.plot(figsize=([12, 8]))

# COMMAND ----------

mean_squared_error(y_test, predicted)
