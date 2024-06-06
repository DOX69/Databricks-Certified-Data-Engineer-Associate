# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %md
# MAGIC let's see the content of this file

# COMMAND ----------

# %sql
# select * from json.`${dataset.bookstore}/books-cdc/02.json`

# COMMAND ----------

# MAGIC %md
# MAGIC The operational field is row_status
# MAGIC
# MAGIC When we DELETE, we only have one field filled : book_id
# MAGIC
