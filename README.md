# Databricks-Certified-Data-Engineer-Associate-Preparation
Preparation course for Databricks Data Engineer Associate certification exam with hands-on training
## Introduction
***What is databricks ?*** It's a lakehouse plateteform base on Apache Spark. Here is the architecture of databricks lakehouse:

<h3 align="center" class="heading-element" dir="auto">
<td><a target="_blank" rel="noopener noreferrer" href="https://www.databricks.com/en-website-assets/static/bc1b13843b6ac6cc8acf38eb5f28d09c/22595.png"><img src="https://www.databricks.com/en-website-assets/static/bc1b13843b6ac6cc8acf38eb5f28d09c/22595.png" title="Databricks" alt="Databricks" height=300 "></a></td>
<td><a target="_blank" rel="noopener noreferrer" href="https://www.databricks.com/en-website-assets/static/8d7550df77b7f3adc86525f1321535d5/22598.pngg"><img src="https://www.databricks.com/en-website-assets/static/8d7550df77b7f3adc86525f1321535d5/22598.png" title="Databricks" alt="Databricks" height=300" ;"></a></td>
</h3>

***What is a lakehouse ?*** An unified plateform that combine benefits of datalake and data warehouse.


|| `DATALAKE`| `DATA WAREHOUSE` |
----------- |----------- | ----------- | 
| **`LAKEHOUSE`** | <li> Open </li> <li> Flexible </li> <li> ML support </li>  | <li> Reliable </li>  <li> Strong gouvernance </li> <li> Performance </li>| 

***How databricks ressources are deploy?*** 2 Layers :
| `Control plan` | `Data plane` |
----------- | ----------- | 
 |<li> Notebook </li> <li> Workflow </li> <li> Cluster managment </li> <li> Web UI </li>  | <li> Cluster VMs </li>  <li> DBFS </li>| 

 ### Cluster
 It's a set of computeur organized by driver and workers. Driver is the one who will orchestrate the jobs, transformation and storage amoung workers.
 In compute configuration in databricks, we can specify:
 * policy (choose unrestricted to create a free configurable cluster)
 * Access mode : sigle user, shared (only in Python and SQL) or no isolation shared (All languages)
 * number of node (worker)
 * Running time version + option using photon (a compute vector create in C++ to improve runtime performance)
 * Type of node
 * terminate time when the cluster is terminate automaticly 
 
### Notebooks
It's the coding environment where we collaborate. 
Multiple language thanks to magic command %. We can also use %run to run an other notebook. We also have %fs to deal with file system operation. Ex : `%fs ls '/databricks-datasets'`

Other option to explore and interact with file (and more), we have `dbutils.help()` .A module provide various utilities. We prefere to use this because it can be a part of python code.

-------------------

## Databricks lakehouse plateform

### Delta lake
it's an open source storage framework that brings reliability to data lakes(data inconsistency and performance issues. Enabling build lakehouse. Perform ACID


We have **transaction log or delta log**. A single source of truth. To get the version, commit etc.
Advance featured : 
* Time travel : version > `VERSION AS OF` or table_name`@v`version_number or `TIMESTAMP AS OF` ; rollback > RESTORE TABLE table_name TO VERSION AS OF/TO TIMESTAMP AS OF
* Compaction : OPTIMIZE + ZORDER BY (data skipping)
<p align="center" >
 <img src="Assets/ZORDER BY.png" alt="image" height=200 >
</p>

* Vacuum : VACUUM table_name [retention period] . Definitly delete files older than a threshold (no time travel). Delfault is 7 days

### Relational entities
Database = schema in Hive metastore: `CREATE DATABASE|SCHEMA db_name` 
**Hive metastore** :repository of metadata that stores information for data structure, such as databases,tables and partitions,metadata, the format of the delta and where this data is actually stored in the underlying storage.

Depend on the location of the underline storage, we have 2 type table
| `Managed table` | `External table` |
----------- | ----------- | 
 |<li> Create in the storage under db directory of dbx </li> <li> Drop table drop the underline data </li>  | <li> underline data created outsite dbx</li>  <li> `CREATE TABLE table_name LOCATION 'path'` </li> <li> drop table will **NOT** drop underline data</li>| 

 ### CTAS
```
CREATE TABLE table_new
COMMENT 'this is a comment'
PARTITION BY (city,birth_date) --for data skipping -- not usable for a small data
LOCATION '/some/path' -- if external
AS select * from table_from
```
### CONSTRAINTS
Constraints fall into two categories:
* Enforced contraints ensure that the quality and integrity of data added to a table is automatically verified.
  Specify with ALTER COLUMN to drop or add NOT NULL constraints
  ```
  ALTER TABLE people10m ALTER COLUMN middleName DROP NOT NULL;
  ALTER TABLE people10m ALTER COLUMN ssn SET NOT NULL;
  ```
  We can also add constraint, name it, with a condition, or drop it
  ```
  ALTER TABLE table_name ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');
  ALTER TABLE table_name DROP CONSTRAINT dateWithinRange;
  ```

* Informational primary key and foreign key constraints encode relationships between fields in tables and are not enforced.
  ```
  CREATE TABLE T(pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL,
                CONSTRAINT t_pk PRIMARY KEY(pk1, pk2));
  CREATE TABLE S(pk INTEGER NOT NULL PRIMARY KEY,
                fk1 INTEGER, fk2 INTEGER,
                CONSTRAINT s_t_fk FOREIGN KEY(fk1, fk2) REFERENCES T);
  ``` 
  ### CLONE
  * **DEEP CLONE** `CREATE TABLE table_cloned DEEP CLONE source_table` during cloning syncronize changes from the target to the source,and copy all data, can take a while
  * **SHALLOW CLONE** only clone transaction log and don't infer changed, **not data moving during shallow clone**
 
  ### View
  Not the data but just a query
  * (Stored) VIEW
  * TEMP VIEW : persiste during spark session
    > When as spark session is created in DBX ? Opening a new note book, attaching or detaching a cluster to a notebook, install package, restart cluster
  * GLOBAL VIEW : cluster scope view. You need to specify **global_temp** `SELECT * FROM global_temp.view_name`
    <p align="center" >
     <img src="Assets/view comparision.png" alt="image" height=200 >
    </p>

## ETL with Spark SQL and Python
### Query files 
```
SELECT * FROM file_format.`some/path`
--Can be JSON, TXT, CSV , TSV, parquet
```
Use CTAS statement to **create a delta table** 
```
CREATE TABLE table_name AS
SELECT * FROM file_format.`some/path`
```
**Limitation** : do not support file options;  CTAS do not support manual schema declaration

**Solution** : CT USING OPTIONS LOCATION (external table) =>**Non-Delta table**
```
CREATE TABLE table_name
USING data_source
OPTIONS (key1 = val1, key2 = val2, ...)
LOCATION = path
```
***Table with external data source is NOT a delta table*** 


