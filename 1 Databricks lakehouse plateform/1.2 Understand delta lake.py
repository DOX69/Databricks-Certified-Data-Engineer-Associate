# Databricks notebook source
# MAGIC %sql
# MAGIC select current_catalog()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Create table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC #Add value

# COMMAND ----------

# MAGIC %md
# MAGIC Note : we perform 4 INSERT in order to create 4 files

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (5, "Anna", 2500.0);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (6, "Kim", 6200.3)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %md
# MAGIC 4 files because there are 4 cores. Spark work in paralelle

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC # Make an update

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees
# MAGIC SET salary = salary + 100
# MAGIC WHERE name like 'A%'

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

# COMMAND ----------

print(dbutils.fs.head('dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC We have 2 more files because 2 records in 2 differente file had been updated.
# MAGIC
# MAGIC However, if we see the decription of the table, it specify 4 files (4 current file of the last version)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees
