-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  Try to delete

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### We can restore

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It's considered as a transaction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4 files > not optimize
-- MAGIC
-- MAGIC we optimize to compact these files

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ZORDER no benefit in a small data or a smal cardinality

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS
--- this do not work

-- COMMAND ----------

-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- This enable us to vacuum less than 7 days
-- do not use this in production !

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees
--- successfully deleted

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
