# Data Lake Bootcamp: Building Reliable Data Lakes in 3 Weeks

https://learning.oreilly.com/live-events/data-lake-bootcamp-building-reliable-data-lakes-in-3-weeks/0636920080624/0636920080623/

### Week 1: Learning Data Lake and Ingesting Data

In this session, you’ll learn what a data lake is and how it can be used to store and organize your data, and you’ll explore related reference architectures like the data lakehouse and medallion architecture. You’ll learn how to set up a data lake in Microsoft Azure and examine the options it offers related to scalability, security, performance, and replication. Finally, you’ll use Azure Data Factory to ingest the raw data into your data lake.

### Week 2: Processing Data in Data Lake with Azure Databricks

In this session you’ll learn how to use Apache Spark and Azure Databricks to process large volumes of data. You’ll extract data from multiple file formats like CSV and JSON, clean and transform this data using various options in Spark, and write the processed data back to the data lake. You’ll also learn how to work with Spark tables and run SQL queries on data in your data lake.

### Week 3: Building Reliable Data Lake with Delta Lake

In this session, you’ll learn what Delta Lake is, how it handles metadata, and how it provides ACID guarantees to a data lake. You’ll work on storing data in Delta format, understand its transaction log, and learn about important Delta features including performing CRUD operations, schema enforcement, table constraints, and time travel. You’ll use these features to build an end-to-end medallion architecture—a data design pattern used to logically organize data in a data lakehouse.


## Schedule
The timeframes are only estimates and may vary according to how the class is progressing.

### Week 1: Learning Data Lake and Ingesting Data
#### Introduction to data lake (50 minutes)

Presentation: Understanding the data lake concept; challenges with data warehouses; data lake objectives; reference architecture to use data lake (data lakehouse and medallion); organizing data (raw, enriched, curated)
Group discussion: How organizations are handling data
Q&A
Break

#### Using Azure Data Lake Gen2 to build data lake (60 minutes)

Presentation and demos: Differences between Azure Storage, Data Lake Gen1, and Data Lake Gen2; scalability, performance and replication; setting up Data Lake Gen2 account; security options—access keys, SAS, Azure AD (RBAC); managing the data lifecycle
Hands-on exercises: Create an ADLS Gen2 account; set up permissions on storage; configure lifecycle
Q&A
Break

#### Ingesting data to data lake (70 minutes)

Presentation and demo: Understanding components of Azure Data Factory; ingesting data using Azure Data Factory
Hands-on exercise: Copy data from Azure SQL to Azure Data Lake (bulk and incremental)
Q&A

### Week 2: Processing Data in Data Lake with Azure Databricks
#### Introduction to Azure Databricks (40 minutes)

Presentation and demo: Introduction to Apache Spark and its architecture; overview of Azure Databricks platform
Hands-on exercise: Set up Databricks workspace and cluster
Q&A

#### Mounting data lake to Databricks (25 minutes)

Presentation and demo: Understanding Databricks File System (DBFS) and mount process; mounting Azure Data Lake Gen2 to DBFS
Hands-on exercise: Mount and access Azure Data Lake Gen2 account
Q&A
Break

#### Extracting and transforming data from multiple file formats (55 minutes)

Demonstration: Working with multiple file formats like CSV, JSON, and Parquet; reading data from data lake
Hands-on exercise: Apply operations to clean and transform data
Q&A
Break

#### Loading processed data to Data Lake (55 minutes)

Presentation: Understanding Spark SQL and Spark tables
Hands-on exercises: Write processed data to data lake; store dataset as Spark tables; write SQL queries on data in data lake
Q&A

### Week 3: Building a Reliable Data Lake with Delta Lake
#### Introduction to Delta Lake (40 minutes)

Presentation: Challenges with data lake; Delta format and transaction log; ACID guarantees on data lake; competitors
Group discussion: How Delta Lake can help build a data warehouse on data lake
Q&A

#### Storing data in Delta format (35 minutes)

Demonstration: Working with Delta format
Hands-on exercises: Creating and managing Delta tables; audit history; table constraints
Q&A
Break

#### Using Delta Lake features (60 minutes)

Demonstration: Working with Delta Lake features
Hands-on exercises: Perform CRUD operations, schema enforcement and evolution, time travel, and optimization; compare performance with Parquet
Q&A
Break

#### Building medallion architecture on data lake (45 minutes)

Presentation: Understanding multihop architecture (bronze, silver, and gold layers)
Hands-on exercise: Use the change data feed feature to build medallion architecture
Q&A
