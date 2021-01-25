# Analytical use case with SQL 2019 Big Data Cluster - POC 

The ultimate aim of this proof-of-concept project is to understand various features of SQL 2019 BDC and evaluate it for Data Lake purposes. 

**POC SCOPE:**

We are scoring each vendors based on their characteristics such as quality, delivery and price competitiveness and sentiment of news articles about those vendors.
The goal is to understand the followings. 
  

1. How to ingest local Sql Server Database tables into HDFS of SQL 2019 Big Data Cluster? 
2. How to query the data from HDFS in SQL Server Master Instance (a.k.a SQL Data Pools) using **Polybase** or **PySpark**?
3. How to write a SQL Stored Procedure in SQL Master Instance to do data aggregation or denormalization using **T-SQL**?
4. How to develop/train/predict machine learning models using SQL Server ML Services? 
5. How to ingest a local file into HDFS? 
6. How to write PySpark queries to read and pre-process data from HDFS and prepare them for ML Models?
7. How to write user defined functions (UDF) using Python and register them in PySpark and how to consume externally developed and deployed models for inferences?

## What is SQL 2019 BDC?

SQL Server Big Data Cluster is a scalable platform that provides flexibility in how to store and interact with your big data. This runs on a Kubernetes cluster and allows you deploy clusters of SQL Server, Spark and HDFS Containers. To read more about [What are SQL Server Big Data Clusters?](https://docs.microsoft.com/en-us/sql/big-data-cluster/big-data-cluster-overview?view=sql-server-ver15)


## What is Polybase?

PolyBase enables your SQL Server instance to process Transact-SQL queries that read data from external data sources.

## Why use PolyBase?

PolyBase allows you to join data from a SQL Server instance with external data. Prior to PolyBase to join data to external data sources you could either:

- Transfer half your data so that all the data was in one location. 
- Query both sources of data, then write custom query logic to join and integrate the data at the client level.

PolyBase allows you to simply use Transact-SQL to join the data.

PolyBase does not require you to install additional software to your Hadoop environment. You query external data by using the same T-SQL syntax used to query a database table. The support actions implemented by PolyBase all happen transparently. The query author does not need any knowledge about the external source.

## Notebooks

|Notebook|Description|Kernel|Environment|
|---|---|---|---|
|[Import Data from SQL Server](step1_poc_sqltables_to_hdfs_pyspark.ipynb)|A notebook using PySpark to import data from local SQL Server Database to HDFS in SQL 2019 BDC|PySpark|SQL 2019 BDC|
|[Query HDFS Data using Polybase](step2_poc_query-hdfs-data-sqlserver-using-polybase.ipynb)|A notebook contains steps involved in querying HDFS data and store them in SQL Data Pool|SQL|SQL 2019 BDC|
|[Stored Procedure - SQL Server]()|A noteboook shows stored procedure that aggregates external data stored in SQL data pool|T-SQL|SQL 2019 BDC|
|[SQL Server ML Services](step4_poc_ml_wt_r_sql2019.ipynb)|A notebook explains how to embed an external R script within a SQL stored proc using ML services|T-SQL & R|SQL 2019 BDC|
|[Write PySpark queries, Register UDFs, Consuming External model end points](step5_poc_ml_wt_r_sql2019.ipynb)|A notebook shows how to write PySpark Queries to read data from HDFS and convert them into Spark DataFrame and Pandas DataFrames. Also showing how to write and register UDFs with Spark and consume external model APIs for predictions|Python & Spark|SQL 2019 BDC|

## References

**SQL 2019 BDC:**
1. https://docs.microsoft.com/en-us/sql/big-data-cluster/connect-to-big-data-cluster?view=sql-server-ver15
2. https://github.com/microsoft/sql-server-samples/tree/master/samples/features/sql-big-data-cluster

**Spark Overview:** https://spark.apache.org/docs/preview/programming-guide.html

**SparkSQL and Spark DataFrames:** https://spark.apache.org/docs/preview/sql-programming-guide.html

**PySpark:** https://spark.apache.org/docs/preview/api/python/index.html

**SparkR:** https://spark.apache.org/docs/preview/sparkr.html

**Spark MLLib:** https://spark.apache.org/docs/preview/mllib-guide.htm

