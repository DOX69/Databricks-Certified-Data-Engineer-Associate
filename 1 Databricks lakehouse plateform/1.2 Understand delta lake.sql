-- Databricks notebook source
select current_catalog()

-- COMMAND ----------

select current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS employees
(id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Add value

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note : we perform 4 INSERT in order to create 4 files

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4 files because there are 4 cores. Spark work in paralelle

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Make an update

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name like 'A%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(dbutils.fs.head('dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We have 2 more files because 2 records in 2 differente file had been updated.
-- MAGIC
-- MAGIC However, if we see the decription of the table, it specify 4 files (4 current file of the last version)

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees
