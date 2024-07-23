# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Widgets

# COMMAND ----------

# Secret scope. It can be either key vault backed or databricks backed.
dbutils.widgets.text('scope', '')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

scope = dbutils.widgets.get('scope')

# COMMAND ----------

workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

headers = {'Authorization': f'Bearer {token}'}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Scope

# COMMAND ----------

request = f'{workspace_url}/api/2.0/secrets/scopes/create'
params = {'scope': scope,
          'scope_backend_type': 'DATABRICKS'}

# COMMAND ----------

response = requests.post(f'{request}', headers=headers, params=params)
print(f'response status code: {response.status_code}')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Secret

# COMMAND ----------

# Specification of the secret key and value.
secret_key = ""
secret_string_value = ""

# COMMAND ----------

request = f'{workspace_url}/api/2.0/secrets/put'
params = {'scope': scope,
          'key': secret_key,
          'string_value': secret_string_value}

# COMMAND ----------

response = requests.post(f'{request}', headers=headers, params=params)
print(f'response status code: {response.status_code}')

# COMMAND ----------

dbutils.secrets.list(scope)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete Scope

# COMMAND ----------

request = f'{workspace_url}/api/2.0/secrets/scopes/delete'
params = {'scope': scope}

# COMMAND ----------

response = requests.post(f'{request}', headers=headers, params=params)
print(f'response status code: {response.status_code}')

# COMMAND ----------

dbutils.secrets.listScopes()
