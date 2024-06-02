# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

books_streaming = spark.readStream\
      .table("books")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# books_streaming.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

agg_books_streaming = books_streaming.groupBy("author")\
    .agg(count("book_id").alias("total_books"))

# COMMAND ----------

# agg_books_streaming.display()

# COMMAND ----------

books_streaming.createOrReplaceTempView("books_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC Error because sorting is not supporting

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

agg_books_streaming.writeStream\
      .trigger(processingTime='4 seconds')\
      .outputMode("complete")\
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")\
            .table("author_counts_streaming")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts_streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC The query will process only new available data

# COMMAND ----------

# MAGIC %md
# MAGIC In this case, we can use the awaitTermination method to block the execution of any cell in this notebook until the incremental batch's write has succeeded.

# COMMAND ----------

agg_books_streaming.writeStream\
      .trigger(availableNow=True)\
      .outputMode("complete")\
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")\
            .table("author_counts_streaming").awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts_streaming
