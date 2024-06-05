-- Databricks notebook source
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingest book CDC feed in a bronze table

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We use the auto loader to ingest file automatically as it arrive

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_silver; --target table where the CDC changes will be apply. We start by declaring the table since Apply Changes Into requires the target table to be declared in a separate statement.

APPLY CHANGES INTO LIVE.books_silver --target table
  FROM STREAM(LIVE.books_bronze) -- streaming source of aour CDC feed
  KEYS (book_id) --if exist the uspdate else insert
  APPLY AS DELETE WHEN row_status = "DELETE" -- Delete status 
  SEQUENCE BY row_time -- ospecify the row_time field for ordering the operations.
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is NOT a streaming table because datasource is updated, not append only

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver -- Can't process a streaming source because it's not an append only
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views

-- COMMAND ----------

CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;
