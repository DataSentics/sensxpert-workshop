# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks introduction
# MAGIC
# MAGIC * [Documentation](https://docs.databricks.com/index.html)
# MAGIC
# MAGIC #### Databricks is an ultimate platform to handle all your data, analytics and AI use cases. It offers effective cooperation between Data Engineers and Data Scientists.

# COMMAND ----------

# MAGIC %run ./Setup-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL
# MAGIC * [Documentation](https://www.databricks.com/product/databricks-sql)
# MAGIC
# MAGIC SQL workspace provides a native SQL interface and query editor, integrates well with existing BI tools, supports the querying of data in Delta Lake using SQL queries, and offers the ability to create and share visualizations. With SQL Analytics, administrators can granularly access and gain insights into how data in being accessed within the Lakehouse through usage and phases of the query's execution process. With its tightly coupled Integration with Delta Lake, SQL Analytics offers reliable governance of data for audit and compliance needs. Users will have the ability to easily create visualizations in the form of dashboards, and will not have to worry about creating and managing complex Apache Spark based compute clusters with SQL serverless endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine Learning
# MAGIC * [Documentation](https://www.databricks.com/product/machine-learning)
# MAGIC
# MAGIC Machine Learning workspace is very similar to Data Science and Data Engineering workspace, but in addition, this Machine Learning workspace offers added components for Experiments, Feature Stores, and ML Models. This workspace supports an end-to-end machine learning environments including robust components for managing feature development, model training, model serving, and experiment tracking. Models can be trained manually or through AutoML. They can be tracked with MLFlow tracking and support the creation of feature tables for model training and inferencing. Models can then be stored, shared and served in the Model Registry.
# MAGIC
# MAGIC Experiment = The main unit of organization for tracking machine learning model development. Experiments organize, display, and control access to individual logged runs of model training code.
# MAGIC
# MAGIC Feature Store = a centralized repository of features. Databricks Feature Store enables feature sharing and discovery across your organization and also ensures that the same feature computation code is used for model training and inference.
# MAGIC
# MAGIC Model Registry = provides a Model Registry for managing the lifecycle of MLFlow Models. It provides model versioning, stage promotion details, model lineage, and supports email notifications.
# MAGIC
# MAGIC Models = a trained machine learning or deep learning model that has been registered in Model Registry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science & Data Engineering
# MAGIC
# MAGIC Data Science & Data Engineering workspace is the most common workspace used by Data Engineering and Data Science professionals. Within this workspace, you will be able to create notebooks for writing code in either Python, Scala, SQL, or R languages. Notebooks can be shared, secured with visibility and access control policies, organized in hierarchical folder structures, and attached to a variety of high-powered compute clusters. These compute clusters can be attached at either the workspace or notebook level. It is within these notebooks where you will also be able to render your code execution results in either tabular, chart, or graph format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks
# MAGIC
# MAGIC * Revision history
# MAGIC * Comments for collaboration
# MAGIC * Markdown + 4 languages (default python):

# COMMAND ----------

# for loop in Python
my_list = [2, 5, "apple", 78]
for i in my_list:
    print(i)

# COMMAND ----------

# MAGIC %scala
# MAGIC // for loop in Scala
# MAGIC var myArray = Array(2, 5, "apple", 78)
# MAGIC for (i <- myArray){
# MAGIC     println(i)
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC x <- c(2, 5, "apple", 78)
# MAGIC for (val in x) {
# MAGIC  print(val)
# MAGIC }

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 2, 5, "apple", 78

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Magic commands

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install package
# MAGIC %pip
# MAGIC
# MAGIC **!!! Restarts the environment**

# COMMAND ----------

# MAGIC %pip install matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run notebook in current scope
# MAGIC % run

# COMMAND ----------

# MAGIC %run ./test-notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import custom code from another file

# COMMAND ----------

from functions import multiplier

print(multiplier(5, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC # Global objects

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display

# COMMAND ----------

display(spark.read.table("iris_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display HTML

# COMMAND ----------

displayHTML("<h1 style=\"color: green\">Hello HTML world!</h1>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Utils
# MAGIC - fs - Manipulating the File system
# MAGIC - widgets - Creating and accessing widgets
# MAGIC - secrets - Accessing keys and tokens
# MAGIC - notebook - Accessing metadata information about the current notebook (user, path, Databricks instance, ...)
# MAGIC
# MAGIC * [Documentation](https://docs.databricks.com/dev-tools/databricks-utils.html)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/python-workshop")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/python-workshop/iris.csv", "file:/tmp/iris.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC Widgets are persistent after closing and opening of a notebook

# COMMAND ----------

# DBTITLE 1,Dropdown
dbutils.widgets.dropdown("state", "CZ", ["CZ", "SK", "PL", "D"])

# COMMAND ----------

state = dbutils.widgets.get("state")
print(f"I am from {state}.")

# COMMAND ----------

dbutils.widgets.remove("state")

# COMMAND ----------

# DBTITLE 1,Text
dbutils.widgets.text("name", "")

# COMMAND ----------

name = dbutils.widgets.get("name")
print(f"Hello {name}!")

# COMMAND ----------

# DBTITLE 1,Combobox
dbutils.widgets.combobox("fruit", "apple", ["apple", "orange", "banana", "strawberry"])

# COMMAND ----------

fruit = dbutils.widgets.get("fruit")
print(f"I like {fruit}.")

# COMMAND ----------

# DBTITLE 1,Multiselect
dbutils.widgets.multiselect("genre", "comedy", ["action", "adventure", "comedy", "crime", "fantasy", "historical", "horror", "romance", "satira"])

# COMMAND ----------

# Attention: multiselect output is a string with elements separated by commas. NOT list as you probably would expect.
genre = dbutils.widgets.get("genre")
print(f"My favorite movie is {genre}.")

# COMMAND ----------

# Python revision TODO: try converting the output of the `genre` widget to a list

# COMMAND ----------

# Remove all widgets.
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utils + Widgets example

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

username = username.split("@")[0]
username = username.replace('.', '_')
print("Username:", username)
database_name = username + "_db"

print("DB name:", database_name)

dbutils.widgets.dropdown("table", "iris_table", [database[0] for database in spark.catalog.listTables(database_name)])
dbutils.widgets.text("filter_param", "1.0")

# COMMAND ----------

filter_param = float(dbutils.widgets.get("filter_param"))
spark.read.table("iris_table").filter(f"SepalWidth > {filter_param}").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lukas_langr_db.${table}
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run another notebook

# COMMAND ----------

dbutils.notebook.run("./test-notebook", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO

# COMMAND ----------

# TODO create a simple notebook in the current folder and run it in this cell

# COMMAND ----------

# TODO create new dropdown widget called `name` with the types of iris {"Iris-setosa", "Iris-versicolor", "Iris-virginica"} from table iris_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO write SQL query which will select all rows with Name = {value in widget} from table iris_table

# COMMAND ----------

# TODO remove all widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data explorer
# MAGIC
# MAGIC * ### Databricks File System (DBFS)
# MAGIC A filesystem abstraction layer over a blob store. It contains directories, which can contain files (data files, libraries, and images), and other directories.
# MAGIC
# MAGIC ![DBFS](https://github.com/DataSentics/odap-workshops/blob/main/DBX/images/data_file_system.png?raw=true)
# MAGIC
# MAGIC * ### Database
# MAGIC A collection of information that is organized so that it can be easily accessed, managed, and updated.
# MAGIC
# MAGIC * #### Table
# MAGIC A representation of structured data. You query tables with Apache Spark SQL and Apache Spark APIs.
# MAGIC
# MAGIC * ### Metastore
# MAGIC The component that stores all the structure information of the various tables and partitions in the data warehouse including column and column type information, the serializers and deserializers necessary to read and write data, and the corresponding files where the data is stored.
# MAGIC
# MAGIC ![Metastore](https://github.com/DataSentics/odap-workshops/blob/main/DBX/images/metastore.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO

# COMMAND ----------

# TODO: Just run this cell. And fill in your first name to the widget.
dbutils.widgets.text("your_name", "")

# COMMAND ----------

# TODO:
# 1) read csv file file:/dbfs/FileStore/python-workshop/iris.csv as spark dataframe
# 2) write dataframe as delta table partitioned by 'Name' - as path use destination_iris
# 3) find the delta table in Data explorer -> hive_metastore -> default -> {your table}
# 4) find the delta table in DBFS by path /FileStore/dbx-workshop/iris_{your name}
source_iris = 'file:/dbfs/FileStore/python-workshop/iris.csv'
your_name = dbutils.widgets.get("your_name")
destination_iris = f'/FileStore/dbx-workshop/iris_{your_name}'

# COMMAND ----------

# TODO: Just run this cell.
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clusters
# MAGIC
# MAGIC An Azure Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning.
# MAGIC
# MAGIC ![cluster](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/cluster.png?raw=true)
# MAGIC
# MAGIC ### Interactive clusters
# MAGIC
# MAGIC Interactive clusters are used to analyze data collaboratively with interactive notebooks and are much more expensive. --> Mostly used in development phase.
# MAGIC
# MAGIC ![cluster_interactive](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/all_purpous_clusters.png?raw=true)
# MAGIC
# MAGIC ### Job clusters
# MAGIC
# MAGIC Job clusters are used to run fast and robust automated workflows using the UI or API and are cheaper. --> Mostly used in production.
# MAGIC
# MAGIC ![cluster_interactive](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/job_clusters.png?raw=true)
# MAGIC
# MAGIC ### DBR (Databricks Runtime) version
# MAGIC You can find there included packages. For example [10.4](https://docs.microsoft.com/en-us/azure/databricks/release-notes/runtime/10.4)
# MAGIC
# MAGIC There are 3 versions:
# MAGIC * Standard
# MAGIC * ML
# MAGIC * GBU
# MAGIC
# MAGIC ### Databricks pricing
# MAGIC
# MAGIC [Pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/)
# MAGIC
# MAGIC ### Azure VM pricing
# MAGIC
# MAGIC [Pricing](https://docs.microsoft.com/en-us/azure/virtual-machines/dv4-dsv4-series)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO

# COMMAND ----------

# TODO
# 1) View what notebooks are running on our cluster and cluster history in "Event log".
# 2) Create a cluster with parameters Runtime: "10.4 ML without GPU", Worker: Standard_D4s_v3, enable autoscaling "min 1" and "max 3", Driver: "Standard_f8"
# 3) Find in the documentation what version of scikit-learn is included in Databricks Runtime version 10.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflows
# MAGIC
# MAGIC Frameworks to develop and run data processing pipelines:
# MAGIC
# MAGIC ### Workflows with jobs:
# MAGIC A non-interactive mechanism for running a notebook or library either immediately or on a scheduled basis.
# MAGIC
# MAGIC ![Schedules](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/workflow.png?raw=true)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC   ![Schedules](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/Schedule.png?raw=true)          ![Notification](https://github.com/DataSentics/odap-workshops/blob/olo-dbx-workshop/DBX/images/notifications.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow
# MAGIC
# MAGIC [Documentation](https://docs.microsoft.com/en-us/azure/databricks/applications/mlflow/)
# MAGIC
# MAGIC MLflow is an open source platform for managing the end-to-end machine learning lifecycle. It has the following primary components:
# MAGIC
# MAGIC * Tracking: Allows you to track experiments to record and compare parameters and results.
# MAGIC * Models: Allow you to manage and deploy models from a variety of ML libraries to a variety of model serving and inference platforms.
# MAGIC * Projects: Allow you to package ML code in a reusable, reproducible form to share with other data scientists or transfer to production.
# MAGIC * Model Registry: Allows you to centralize a model store for managing models’ full lifecycle stage transitions: from staging to production, with capabilities for versioning and annotating.
# MAGIC * Model Serving: Allows you to host MLflow Models as REST endpoints.
# MAGIC
# MAGIC example in ./MLflow.py

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature store
# MAGIC
# MAGIC [Documentation](https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/feature-store/)
# MAGIC
# MAGIC A feature store is a centralized repository that enables data scientists to find and share features and also ensures that the same code used to compute the feature values is used for model training and inference.
