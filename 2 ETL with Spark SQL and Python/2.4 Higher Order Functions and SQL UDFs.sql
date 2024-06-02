-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_books_copies
FROM orders
-- they bought multiple book copies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In order to remove empty arry, we can use size function in a WHERE clause

-- COMMAND ----------

WITH multiple_books AS (
  
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_books_copies
  FROM orders
)

SELECT order_id, multiple_books_copies
FROM multiple_books

WHERE size(multiple_books_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We want to apply a 20% discount 

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

-- MAGIC %md
-- MAGIC UDFs can persist in database and can be reuse in different notebooks

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

-- MAGIC %md
-- MAGIC More examples

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
