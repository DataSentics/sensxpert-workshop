# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Persona is not just an app!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persona product explanation
# MAGIC
# MAGIC Backend + frontend
# MAGIC
# MAGIC ### Backend
# MAGIC What lives in Databricks
# MAGIC
# MAGIC - **ODAP Framework** for writing Data pipelines (previously called Daipe)
# MAGIC - **ODAP Feature store**
# MAGIC - **Google Analytics downloader** + transformations into **Digital Smart Data Model** (digisdm)
# MAGIC - **AdForm metadata transformations into Digital Smart Data Model** (digisdm)
# MAGIC - Ready to use **Attributes/Features** for multiple domains:
# MAGIC   - Interests (Digital & Transactions)
# MAGIC   - Favourite merchants
# MAGIC   - Campaign interactions
# MAGIC   - Digital devices
# MAGIC   - Geo behaviour
# MAGIC   - Lifestyle & Lifestage
# MAGIC   - Lifestage
# MAGIC   - Digital behaviour
# MAGIC   - Digital product PTB
# MAGIC   - Transactional behaviour
# MAGIC - **ML Library and Templates** for exploration, training and evaluation of models (next Workshop)
# MAGIC
# MAGIC
# MAGIC ### Frontend
# MAGIC What does the **Persona 360** app do
# MAGIC
# MAGIC - **Quick insights** - comparison of two groups of clients based on a combination of existing attributes
# MAGIC - **Personas** - rule based definition of a group of clients for comparison and exporting
# MAGIC - **Attribute manager** - allows adding and filtering of attributes based on categories, subcategories and tags
# MAGIC - **Export manager** – allows exports of a defined Persona into an export destination (e.g Salesforce Marketing Cloud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ODAP Feature store vs Databricks Feature store
# MAGIC
# MAGIC ### What is a Feature store?
# MAGIC > A feature store is a centralized repository that enables data scientists to find and share features and also ensures that the same code used to compute the feature values is used for model training and inference.
# MAGIC
# MAGIC It's a big **historized** table for a specific entity (e.g. client) where rows are [ID, Timestamp] and columns are **features**.
# MAGIC
# MAGIC #### Use cases
# MAGIC - Data democratization - What features are used where, Documentation, Single source of truth
# MAGIC - Publishing features inside of an organization (API)
# MAGIC - Model training and scoring
# MAGIC
# MAGIC ### The Difference
# MAGIC
# MAGIC > ODAP and Databricks feature stores are NOT competing, they are in synergy
# MAGIC
# MAGIC **Databricks Feature store** is a platform enabling browsing of features in a UI, serving through API, linking of models/features/notebooks etc.
# MAGIC
# MAGIC **ODAP Feature store** is a nicer interface and an extension of Databricks Feature store
# MAGIC
# MAGIC | ODAP Feature store | Databricks Feature store |
# MAGIC |----|----|
# MAGIC | **Simple, well defined interface** for writing features | **General interface** for saving DataFrames |
# MAGIC | **Optimized** for reading performance | User is responsible for optimization |
# MAGIC | **Optimized** for writing multiple features at once | User is responsible for optimization |
# MAGIC | **Optimized** for writing features with time windows | User is responsible for optimization |
# MAGIC | Special interface for generating **target based** training sets | User is responsible for generating targets |
# MAGIC | Fully customizable per feature **metadata** | Only models/features/notebook links as metadata |
# MAGIC | **Outsources** online serving to Databricks FS | Handles online serving **out of the box** |

# COMMAND ----------

# MAGIC %md
# MAGIC ### ODAP Framework General Advantages
# MAGIC
# MAGIC > Without a framework any coding project eventually leads to a mess. We have a framework which works.
# MAGIC
# MAGIC 1) Best practices, which greatly simplify project structuring on all levels
# MAGIC 2) Decorated functions code is **self-documenting**
# MAGIC 3) **YAML** configuration files
# MAGIC 4) Production ready **dependency management** allowing for reproducible runs on any environment
# MAGIC 5) Standardized **logging**
# MAGIC 6) Decorated functions allow for simple and readable **Unit testing**
