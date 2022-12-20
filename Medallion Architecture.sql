-- Databricks notebook source
-- MAGIC %md ### Bronze Table

-- COMMAND ----------

CREATE TABLE TaxiDB.GreenTaxi_Bronze
(
    VendorId                INT,
    lpep_pickup_datetime    TIMESTAMP,
    lpep_dropoff_datetime   TIMESTAMP,
    store_and_fwd_flag      STRING,
    RatecodeId              INT,    
    PULocationID            INT,
    DOLocationID            INT,    
    
    passenger_count         INT,
    trip_distance           DOUBLE,
    
    fare_amount             DOUBLE,    
    extra                   DOUBLE,
    mta_tax                 DOUBLE,
    tip_amount              DOUBLE,
    tolls_amount            DOUBLE,         
    ehail_fee               DOUBLE,
    improvement_surcharge   DOUBLE,
    total_amount            DOUBLE,
    
    payment_type            INT,
    trip_type               INT
)
USING DELTA

PARTITIONED BY (VendorId)
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md ### Silver Table

-- COMMAND ----------

CREATE TABLE TaxiDB.GreenTaxi_Silver
(
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,
    PassengerCount          INT,
    TripDistance            DOUBLE,
    PaymentType             INT,
    TotalAmount             DOUBLE,
    
    PickupYear              INT     GENERATED ALWAYS AS (YEAR  (PickupTime)),
    PickupMonth             INT     GENERATED ALWAYS AS (MONTH (PickupTime)),
    PickupDay               INT     GENERATED ALWAYS AS (DAY (PickupTime))
    
)
USING DELTA
PARTITIONED BY (VendorId)

-- COMMAND ----------

-- MAGIC %md ### Gold Table

-- COMMAND ----------

CREATE TABLE TaxiDB.GreenTaxi_Gold
(
    PickupLocationId        INT,
    DropLocationId          INT,
    PickupYear              INT,
    PickupMonth             INT,
    PickupDay               INT,   
    
    TotalDistance           DOUBLE,    
    TotalAmount             DOUBLE
    
)
USING DELTA

-- COMMAND ----------

-- MAGIC %md ### 1. First Run

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Set the values to add Data Lake Gen2 Access Key to Spark config
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.***DataLakeName***.dfs.core.windows.net",
-- MAGIC   
-- MAGIC     "***Access Key***")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Define paths
-- MAGIC greenTaxisInputPath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Create schema for Green Taxi Data
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC   
-- MAGIC greenTaxiSchema = (
-- MAGIC             StructType()               
-- MAGIC                .add("VendorId", "integer")
-- MAGIC                .add("lpep_pickup_datetime", "timestamp")
-- MAGIC                .add("lpep_dropoff_datetime", "timestamp")
-- MAGIC                .add("store_and_fwd_flag", "string")
-- MAGIC                .add("RatecodeID", "integer")
-- MAGIC                .add("PULocationID", "integer")
-- MAGIC                .add("DOLocationID", "integer")
-- MAGIC   
-- MAGIC               .add("passenger_count", "integer")
-- MAGIC               .add("trip_distance", "double")
-- MAGIC               .add("fare_amount", "double")
-- MAGIC               .add("extra", "double")
-- MAGIC               .add("mta_tax", "double")
-- MAGIC               .add("tip_amount", "double")
-- MAGIC   
-- MAGIC               .add("tolls_amount", "double")
-- MAGIC               .add("ehail_fee", "double")
-- MAGIC               .add("improvement_surcharge", "double")
-- MAGIC               .add("total_amount", "double")
-- MAGIC               .add("payment_type", "integer")
-- MAGIC               .add("trip_type", "integer")
-- MAGIC          )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Read csv file
-- MAGIC greenTaxiDF = (
-- MAGIC                   spark
-- MAGIC                     .read                     
-- MAGIC                     .option("header", "true")
-- MAGIC                     .schema(greenTaxiSchema)
-- MAGIC                     .csv(greenTaxisInputPath + "GreenTaxis_201911.csv")
-- MAGIC               )
-- MAGIC 
-- MAGIC display(greenTaxiDF)

-- COMMAND ----------

-- MAGIC %md ####(1.a) Load Bronze Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Append to Delta table
-- MAGIC (
-- MAGIC     greenTaxiDF
-- MAGIC         .write
-- MAGIC         .mode("append")
-- MAGIC         .partitionBy("VendorId")
-- MAGIC         .format("delta")
-- MAGIC         .saveAsTable("TaxiDB.GreenTaxi_Bronze")
-- MAGIC )

-- COMMAND ----------

DESCRIBE HISTORY TaxiDB.GreenTaxi_Bronze

-- COMMAND ----------

-- MAGIC %md ####(1.b) Check Change Data Feed for Bronze table

-- COMMAND ----------

SELECT *
FROM table_changes('TaxiDB.GreenTaxi_Bronze', 1)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS

SELECT *
FROM table_changes('TaxiDB.GreenTaxi_Bronze', 1)

WHERE passenger_count <= 5

-- COMMAND ----------

-- MAGIC %md ####(1.c) Merge changes from Bronze to Silver table

-- COMMAND ----------

MERGE INTO TaxiDB.GreenTaxi_Silver  target

  USING  BronzeChanges            source
    
ON target.VendorId = source.VendorId
  AND target.PickupTime = source.lpep_pickup_datetime
  AND target.PickupLocationId = source.PULocationID
  AND target.DropLocationId = source.DOLocationID
  
-- Update row if join conditions match
WHEN MATCHED
      THEN  
         UPDATE SET
              target.TotalAmount = source.total_amount

-- Insert row if row is not present in target table
WHEN NOT MATCHED
      THEN
          INSERT (VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, 
                  PassengerCount, TripDistance, PaymentType, TotalAmount)  
                  
          VALUES (VendorId, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID,
                  passenger_count, trip_distance, payment_type, total_amount)

-- COMMAND ----------

-- MAGIC %md ####(1.d) Overwrite Gold table with all data from Silver table

-- COMMAND ----------

INSERT OVERWRITE TABLE TaxiDB.GreenTaxi_Gold

    SELECT PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

         , SUM(TripDistance)  AS TotalDistance
         , SUM(TotalAmount)   AS TotalAmount

    FROM TaxiDB.GreenTaxi_Silver
    
    GROUP BY PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

-- COMMAND ----------

-- MAGIC %md ###2. Second Run

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Read csv file
-- MAGIC greenTaxiAppendDF = (
-- MAGIC                         spark
-- MAGIC                           .read                     
-- MAGIC                           .option("header", "true")
-- MAGIC                           .schema(greenTaxiSchema)
-- MAGIC                           .csv(greenTaxisInputPath + "GreenTaxis_201911_changes.csv")
-- MAGIC                     )
-- MAGIC 
-- MAGIC display(greenTaxiAppendDF)

-- COMMAND ----------

-- MAGIC %md ####(2.a) Load incremental data to Bronze Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Append to Delta table
-- MAGIC (
-- MAGIC     greenTaxiAppendDF
-- MAGIC         .write
-- MAGIC         .mode("append")
-- MAGIC         .partitionBy("VendorId")
-- MAGIC         .format("delta")
-- MAGIC         .saveAsTable("TaxiDB.GreenTaxi_Bronze")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md ####(2.b) Check Change Data Feed for Bronze table
-- MAGIC   - Note the version number being read

-- COMMAND ----------

DESCRIBE HISTORY TaxiDB.GreenTaxi_Bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS

SELECT *
FROM table_changes('TaxiDB.GreenTaxi_Bronze', 2)

WHERE passenger_count <= 5

-- COMMAND ----------

-- MAGIC %md ####(2.c) Merge changes from Bronze to Silver table

-- COMMAND ----------

MERGE INTO TaxiDB.GreenTaxi_Silver  target

  USING  BronzeChanges            source
    
ON target.VendorId = source.VendorId
  AND target.PickupTime = source.lpep_pickup_datetime
  AND target.PickupLocationId = source.PULocationID
  AND target.DropLocationId = source.DOLocationID
  
-- Update row if join conditions match
WHEN MATCHED
      THEN  
         UPDATE SET
              target.TotalAmount = source.total_amount

-- Insert row if row is not present in target table
WHEN NOT MATCHED
      THEN
          INSERT (VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, 
                  PassengerCount, TripDistance, PaymentType, TotalAmount)  
                  
          VALUES (VendorId, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID,
                  passenger_count, trip_distance, payment_type, total_amount)

-- COMMAND ----------

-- MAGIC %md ####(2.d) Overwrite Gold table with all data from Silver table

-- COMMAND ----------

INSERT OVERWRITE TABLE TaxiDB.GreenTaxi_Gold

    SELECT PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

         , SUM(TripDistance)  AS TotalDistance
         , SUM(TotalAmount)   AS TotalAmount

    FROM TaxiDB.GreenTaxi_Silver
    
    GROUP BY PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

-- COMMAND ----------


