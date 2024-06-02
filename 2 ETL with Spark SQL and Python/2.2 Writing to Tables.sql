-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can time travel, and thanks to ACID, if it fails, the table will be fully retrieve 

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Version 0 is when we create the table 
-- MAGIC Version 1 is when we CREATE OR REPLACE our table

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC if we change the schema by adding a field, it ends with a error message

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED 
AND c.email IS NULL AND u.email IS NOT NULL -- additionnal condition
THEN 
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Other example

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> We are only interested in updating the computer Science category
-- MAGIC
-- MAGIC >> And we do something only if the ids not match

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED 
AND u.category = 'Computer Science' 
THEN 
  INSERT *
