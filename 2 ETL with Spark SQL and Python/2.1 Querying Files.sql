-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Data schemas

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Display files downloaded

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can query all json files begin with export_

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json` limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC we can specify only the path

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json` limit 100

-- COMMAND ----------

-- number of customer that we have : 1700 customer
SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> When reading files, it's useful to add `input_file_name()` function to have the datasource directory for each record

-- COMMAND ----------

 SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.bookstore}/customers-json` limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM TEXT.`${dataset.bookstore}/customers-json` limit 100

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `binatFile.`` ` Extract data and metadata of some files

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json` LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> we have 
-- MAGIC >> * The path of the file
-- MAGIC >> * modification time
-- MAGIC >> * length
-- MAGIC >> * And content the binary representation of the file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv` limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We manage to reade the data, but it is not well parsed 
-- MAGIC
-- MAGIC The header row is extracted as a row and only one column is return because of the delimiter
-- MAGIC
-- MAGIC Unlike parquet JSON files, **CSV do not have a self describe schema/format**
-- MAGIC
-- MAGIC We need additional schema decalaration : **CT USING OPTIONS LOCATION**

-- COMMAND ----------

CREATE TABLE  books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv
--- We have 12 lines (it will be important to know that to see flaws of reading an external csv table that is not a delta)

-- COMMAND ----------

-- DROP TABLE books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can see that no data move during the dable creation. We are just pointing to the externat table.
-- MAGIC
-- MAGIC The fact that we can't create a delta table here, we don't have the benefice of time travel, get the last version of data ...

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Lest' check how many files we have

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)
-- MAGIC # 4 files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC we rewrite the table in the same csv in the external path source 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We have indeed more files

-- COMMAND ----------

SELECT * FROM books_csv
-- We still have 12 lines event if we had more data in the external location previously
-- This is because databricsk automaticly store data in a local cache  
-- Spark we after use only this cache to perform tranformation for more efficiency 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### REFRESH
-- MAGIC We have the latest data only if we refresh the cache data using REFRESH TABLE statement
-- MAGIC
-- MAGIC || But be carefull, refreshing table will invalidate its cache >> we need to rescan all that source and reput that into memory
-- MAGIC || This can take a significant amount of time if we have huge volume of data

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv
--- we have all row thanks to REFRESH TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

SELECT * FROM customers LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can see that we are indeed creating a (managed)delta table  

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC But if we want to ingest non self define format like csv, the table is not parsed well 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSTA and CT UOLO capability combined thanks to view

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Now we have well parsed managed delta table !***
