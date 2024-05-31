-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Temporary view from the previous session doe not exist
-- MAGIC but the stored view is

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We still hae the global_temp view

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


