# Databricks notebook source
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
