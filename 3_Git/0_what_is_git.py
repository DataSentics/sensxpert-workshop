# Databricks notebook source
# MAGIC %md
# MAGIC # A little bit of motivation
# MAGIC **Typical situations when Toso and Anet are working on a project together:**
# MAGIC - Hey Anet I saw that you were working on the new function, could you send me the file with the function? *Anet sends an email*
# MAGIC - Hmm I might need this old functionality later let me make a backup real quick
# MAGIC - Hey Toso I saw that we were both editing the same file, how should we merge the file together?
# MAGIC - What the hell is this function doing? Who introduced and when this function? Toso was it you?
# MAGIC - Hey Anet I developed this new functionality
# MAGIC - Hmm Toso once we are done with development how will we be tracking all the versions?
# MAGIC
# MAGIC **A typical folder structure for Toso when working on a project:**
# MAGIC - Toso_project
# MAGIC - Toso_project_1
# MAGIC - Toso_project_2
# MAGIC - Toso_project_copy
# MAGIC - Toso_project_final
# MAGIC - Toso_project_super_final
# MAGIC
# MAGIC **A typical folder structure for Anet when working on a project:**
# MAGIC - Anet_project
# MAGIC - Anet_project_1
# MAGIC - Anet_project_2
# MAGIC - Anet_project_copy
# MAGIC - Anet_project_Toso
# MAGIC - Anet_project_final_Toso_copy

# COMMAND ----------

# MAGIC %md
# MAGIC # Git
# MAGIC - Git is a free and open source distributed version control system designed to handle everything from small to very large projects with speed and efficiency.
# MAGIC - Git is easy to learn and has a tiny footprint with lightning fast performance.
# MAGIC - Records the changes made over time to files in a special repository
# MAGIC - Most popular version control system
# MAGIC - Git stores all the history in a hidden folder called **.git** in every repository. It maintains a snapshots of file-system as blobs, trees and branches. [More info](https://www.youtube.com/watch?v=MyvyqdQ3OjI)
# MAGIC - [Documentation to Git](https://git-scm.com/doc)

# COMMAND ----------

# MAGIC %md
# MAGIC ![what_is_git](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branches-merge.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GitHub
# MAGIC GitHub, Inc., is an Internet hosting service for software development and version control using Git.
# MAGIC
# MAGIC It provides the distributed version control of Git plus access control, bug tracking, software feature requests, task management, continuous integration, and wikis for every project. It is the most popular github provider. It is also the provider that Ceska Sporitelna is using. Alternatives are:
# MAGIC - Azure DevOps
# MAGIC - GitLab
# MAGIC - ...

# COMMAND ----------

# MAGIC %md
# MAGIC ![github_logo](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/github_logo.png?raw=true)
# MAGIC ![repo_image](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/repo_image.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advantages of git and GitHub:
# MAGIC - Distributed development.
# MAGIC - Easy to share your work.
# MAGIC - Easy to make code review.
# MAGIC - Easy to keep track of different versions
# MAGIC - When was the last time a line was edited?
# MAGIC - Who was the person that made the change?
# MAGIC - What changes were made?
# MAGIC - Possibility to return to the past. 
# MAGIC - Possibility of CI/CD processes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interacting with Git:
# MAGIC - Using command line interface(CLI)
# MAGIC - Using 3rd party GUI on your PC
# MAGIC - Using Databricks Repos
