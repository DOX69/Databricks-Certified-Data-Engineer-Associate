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
* Vacuum : VACUUM table_name [retention period] . Definitly delete files older than a threshold (no time travel). Delfault is 7 days

### Relational entities
Database = schema in Hive metastore: `CREATE DATABASE|SCHEMA db_name` 
**Hive metastore** :repository of metadata that stores information for data structure, such as databases,tables and partitions,metadata, the format of the delta and where this data is actually stored in the underlying storage.

Depend on the location of the underline storage, we have 2 type table
| `Managed table` | `External table` |
----------- | ----------- | 
 |<li> Create in the storage under db directory of dbx </li> <li> Drop table drop the underline data </li>  | <li> underline data created outsite dbx</li>  <li> `CREATE TABLE table_name LOCATION 'path'` </li> <li> drop table will **NOT** drop underline data</li>| 
