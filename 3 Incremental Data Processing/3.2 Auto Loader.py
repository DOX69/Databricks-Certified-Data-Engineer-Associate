# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

datasetLocation = f"{dataset_bookstore}/orders-raw"

# COMMAND ----------

files = dbutils.fs.ls(datasetLocation)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC > **"cloudFiles"** indicate that it's an auto loader stream
# MAGIC > for the schema location "cloudFiles.shemaLocation", we can specify our orders checkpoint to have the same, so we don't have to compute an infer schema

# COMMAND ----------

checkpointLocation = "dbfs:/mnt/demo/orders_checkpoint"

# COMMAND ----------

spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format","parquet")\
  .option("cloudFiles.schemaLocation",checkpointLocation)\
  .load(datasetLocation)\
      .writeStream\
   .option("checkpointLocation",checkpointLocation)\
   .table("orders_updates") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(datasetLocation)
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
