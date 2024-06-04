# Databricks notebook source
files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC **System directory** captures all events associated with the pipeline

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC These event log are store as a delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC We have our 5 DLT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.cn_daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC The table is indeed existing in the metastore and we can see the 123 records in this gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.fr_daily_customer_books
