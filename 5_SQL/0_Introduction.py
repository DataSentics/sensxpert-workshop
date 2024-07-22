# Databricks notebook source
# MAGIC %md 
# MAGIC # Hello in Databricks 😺
# MAGIC * [Documentation](https://docs.databricks.com/index.html)
# MAGIC
# MAGIC #### Databricks is an ultimate platform to handle all your data, analytics and AI use cases. It offers effective cooperation between Data Engineers and Data Scientists.

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Lakehouse platform
# MAGIC
# MAGIC A merge of **Data Warehouse** and **Data Lake**
# MAGIC
# MAGIC ![](https://www.databricks.com/wp-content/uploads/2020/01/data-lakehouse-new.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC
# MAGIC Combines **Delta** format for file storage, **Spark** for distributed compute and **MLFlow** for AI models registry
# MAGIC
# MAGIC Delta Lake uses versioned Parquet files to store your data in your cloud storage. Apart from the versions, Delta Lake also stores a transaction log to keep track of all the commits made to the table or blob store directory to provide ACID transactions. Delta allows easy schema managment.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Medallion Architecture
# MAGIC ### Bronze, silver, gold
# MAGIC
# MAGIC ![](https://www.databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg)
# MAGIC
# MAGIC
# MAGIC **Bronze layer (raw data)**
# MAGIC The Bronze layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc. The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage), data lineage, auditability, reprocessing if needed without rereading the data from the source system.
# MAGIC Silver layer (cleansed and conformed data)
# MAGIC In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. (e.g. master customers, stores, non-duplicated transactions and cross-reference tables).
# MAGIC
# MAGIC The **Silver layer** brings the data from different sources into an Enterprise view and enables self-service analytics for ad-hoc reporting, advanced analytics and ML. It serves as a source for Departmental Analysts, Data Engineers and Data Scientists to further create projects and analysis to answer business problems via enterprise and departmental data projects in the Gold Layer.
# MAGIC
# MAGIC In the lakehouse data engineering paradigm, typically the ELT methodology is followed vs. ETL - which means only minimal or "just-enough" transformations and data cleansing rules are applied while loading the Silver layer. Speed and agility to ingest and deliver the data in the data lake is prioritized, and a lot of project-specific complex transformations and business rules are applied while loading the data from the Silver to Gold layer. From a data modeling perspective, the Silver Layer has more 3rd-Normal Form like data models. Data Vault-like, write-performant data models 76can be used in this layer.
# MAGIC
# MAGIC **Gold layer (curated business-level tables)**
# MAGIC Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules are applied here. Final presentation layer of projects such as Customer Analytics, Product Quality Analytics, Inventory Analytics, Customer Segmentation, Product Recommendations, Marking/Sales Analytics etc. fit in this layer. We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse.
# MAGIC
# MAGIC So you can see that the data is curated as it moves through the different layers of a lakehouse. In some cases, we also see that lot of Data Marts and EDWs from the traditional RDBMS technology stack are ingested into the lakehouse, so that for the first time Enterprises can do "pan-EDW" advanced analytics and ML - which was just not possible or too cost prohibitive to do on a traditional stack. (e.g. IoT/Manufacturing data is tied with Sales and Marketing data for defect analysis or health care genomics, EMR/HL7 clinical data markets are tied with financial claims data to create a Healthcare Data Lake for timely and improved patient care analytics.)
# MAGIC
# MAGIC
# MAGIC **TLDR;**
# MAGIC
# MAGIC Bronze = **raw**
# MAGIC
# MAGIC Silver = **parsed, cleansed, SDM (Smart Data Model**
# MAGIC
# MAGIC Gold = **aggregated features, Feature store**

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Databricks architecture(Control plane vs Data plane)
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/DE/Images/databricks_architecture.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science & Data Engineering
# MAGIC
# MAGIC Data Science & Data Engineering workspace is the most common workspace used by Data Engineering and Data Science professionals. Within this workspace, you will be able to create notebooks for writing code in either Python, Scala, SQL, or R languages. Notebooks can be shared, secured with visibility and access control policies, organized in hierarchical folder structures, and attached to a variety of high-powered compute clusters. These compute clusters can be attached at either the workspace or notebook level. It is within these notebooks where you will also be able to render your code execution results in either tabular, chart, or graph format.

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
# MAGIC ## SQL
# MAGIC * [Ducumentation](https://www.databricks.com/product/databricks-sql)
# MAGIC
# MAGIC SQL workspace provides a native SQL interface and query editor, integrates well with existing BI tools, supports the querying of data in Delta Lake using SQL queries, and offers the ability to create and share visualizations. With SQL Analytics, administrators can granularly access and gain insights into how data in being accessed within the Lakehouse through usage and phases of the query's execution process. With its tightly coupled Integration with Delta Lake, SQL Analytics offers reliable governance of data for audit and compliance needs. Users will have the ability to easily create visualizations in the form of dashboards, and will not have to worry about creating and managing complex Apache Spark based compute clusters with SQL serverless endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Difference between SQL workspace and Machine Learning workspace
# MAGIC **SQL workspace** is used by Data Analysts to create interactive dashboards and SQL queries for Ad-hoc analysis. Does not have support for ETL and ML. SQL workspace uses different warehouse cluster, called SQL endpoint, with just a few possibilities of configuration. 
# MAGIC
# MAGIC **Machine Learning workspace** is used by Data Engineers and Scientists to create production ready ML models and ETL pipelines. Machine Learning uses different cluster 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running a notebook
# MAGIC When we want to run a notebook like the one you currently see, you need to attach it to a cluster. How to attach to a cluster can be seen on on a picture below
# MAGIC [![Screen-Shot-2022-08-04-at-15-03-53.png](https://i.postimg.cc/cLHsJBDk/Screen-Shot-2022-08-04-at-15-03-53.png)](https://postimg.cc/Yh5cnYBQ)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Running a cell in the notebook
# MAGIC To run a single cell use **shift+enter**

# COMMAND ----------

print("Hello world")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Default language
# MAGIC The default language for the notebooks is Python. If you would like to use sql you need to insert %sql into the first line of each cell in which you want to use SQL. Example can be seen below

# COMMAND ----------

# MAGIC %md
# MAGIC Example of markdown

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS my_first_table;
# MAGIC CREATE TABLE IF NOT EXISTS my_first_table (name String, email String, age Int);
# MAGIC INSERT INTO my_first_table VALUES ("Tom", "email1@me.com", 23);
# MAGIC INSERT INTO my_first_table VALUES ("Som", "email2@me.com", 24);
# MAGIC INSERT INTO my_first_table VALUES ("Dom", "email3@me.com", 25);
# MAGIC INSERT INTO my_first_table VALUES ("Lom", "email3@me.com", 26);
# MAGIC SELECT * FROM my_first_table;

# COMMAND ----------

display(spark.read.table('my_first_table'))

# COMMAND ----------

# MAGIC %scala
# MAGIC val x = 1

# COMMAND ----------

# MAGIC %r
