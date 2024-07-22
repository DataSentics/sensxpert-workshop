# Databricks notebook source
# MAGIC %md # Library and environment management

# COMMAND ----------

# MAGIC %md Libraries can be either installed on a cluster or within the notebooks context. Installing in a notebooks context is recommended not to potentialy interfere with other users. <br/>
# MAGIC One can use for example poetry or simple requirements.txt. Many libraries are pre-installed on the databricks runtime. E.g. here https://docs.databricks.com/en/release-notes/runtime/13.2ml.html

# COMMAND ----------

import sys
import IPython

def resolve_repository_root():
    return min([path for path in sys.path if path.startswith("/Workspace")], key=len)

IPython.get_ipython().run_line_magic(
    "pip",
    f"install -r {resolve_repository_root()}/requirements.txt"
)

# COMMAND ----------


