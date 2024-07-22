# Databricks notebook source
# MAGIC %md 
# MAGIC # Hello in Databricks 😺
# MAGIC Deeper introduction into working with databricks will be in the next workshop, now we will cover just the basics

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

# MAGIC %scala
# MAGIC val x = 1

# COMMAND ----------

# MAGIC %r
