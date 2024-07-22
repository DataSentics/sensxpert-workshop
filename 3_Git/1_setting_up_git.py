# Databricks notebook source
# MAGIC %md
# MAGIC # Oh no my Git is not working in Databricks :(

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Enable "Files in Repos"
# MAGIC Enable 'Files in Repos' in your Databricks workspace at Settings -> Admin Console -> Workspace Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ![admin_console](https://github.com/DataSentics/odap-workshops/blob/tbo-git/Git/images/settings-adming-console.png?raw=true)
# MAGIC
# MAGIC
# MAGIC ![files_in_repos](https://github.com/DataSentics/odap-workshops/blob/tbo-git/Git/images/enable-files-in-repos.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up a GitHub personal access token
# MAGIC   1. In [GitHub](https://github.com), follow these steps to create a personal access token that allows access to your repositories: [more info](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
# MAGIC   2. In the upper-right corner of any page, click your profile photo, then click Settings.
# MAGIC   3. Click Developer settings.
# MAGIC   4. Click the Personal access tokens tab.
# MAGIC   5. Click the Generate new token button.
# MAGIC   6. Enter a token description.
# MAGIC   7. Select the repo permission, and click the Generate token button.
# MAGIC 2. In your Databricks workspace at Settings -> User Settings -> Git Integration select GitHub as a provider and use your new token here

# COMMAND ----------

# MAGIC %md
# MAGIC ![token-permissions](https://github.com/DataSentics/odap-workshops/blob/tbo-git/Git/images/github-newtoken.png?raw=true)
# MAGIC
# MAGIC
# MAGIC ![user_settings_token_1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/Screen%20Shot%202022-08-19%20at%2010.19.42.png?raw=true)
# MAGIC ![user_settings_token_2](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/Screen%20Shot%202022-08-19%20at%2010.20.20.png?raw=true)
