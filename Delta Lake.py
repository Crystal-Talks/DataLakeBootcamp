# Databricks notebook source
#Set the values to add Data Lake Gen2 Access Key to Spark config

spark.conf.set(
    "fs.azure.account.key.***DataLakeName***.dfs.core.windows.net",
  
    "***Access Key***")

# COMMAND ----------

# Define input & output paths

greenTaxisInputPath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/"

greenTaxisOutputPath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Output/OReilly/"

# COMMAND ----------

# Create schema for Green Taxi Data

from pyspark.sql.functions import *
from pyspark.sql.types import *
  
greenTaxiSchema = (
            StructType()               
               .add("VendorId", "integer")
               .add("lpep_pickup_datetime", "timestamp")
               .add("lpep_dropoff_datetime", "timestamp")
               .add("store_and_fwd_flag", "string")
               .add("RatecodeID", "integer")
               .add("PULocationID", "integer")
               .add("DOLocationID", "integer")
  
              .add("passenger_count", "integer")
              .add("trip_distance", "double")
              .add("fare_amount", "double")
              .add("extra", "double")
              .add("mta_tax", "double")
              .add("tip_amount", "double")
  
              .add("tolls_amount", "double")
              .add("ehail_fee", "double")
              .add("improvement_surcharge", "double")
              .add("total_amount", "double")
              .add("payment_type", "integer")
              .add("trip_type", "integer")
         )

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv(greenTaxisInputPath + "GreenTaxis_201911.csv")
              )

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md ### Save DataFrame in Parquet and Delta formats

# COMMAND ----------

# Write in parquet format
(
    greenTaxiDF
        .write
        .mode("overwrite")        
  
        .partitionBy("VendorId")
  
        .format("parquet")
  
        .save(greenTaxisOutputPath + "GreenTaxis.parquet")
)

# COMMAND ----------

# Write in delta format
(
    greenTaxiDF
        .write
        .mode("overwrite")        
  
        .partitionBy("VendorId")
  
        .format("delta")
  
        .save(greenTaxisOutputPath + "GreenTaxis.delta")
)

# COMMAND ----------

# MAGIC %md #####Check output folder to see differences between parquet & delta outputs
# MAGIC   - Do you see _delta_log folder in delta directory?

# COMMAND ----------

# MAGIC %md ### Create Parquet Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop tables if they exist
# MAGIC DROP TABLE IF EXISTS TaxiDB.GreenTaxisParquet;
# MAGIC DROP TABLE IF EXISTS TaxiDB.GreenTaxis;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS TaxiDB;
# MAGIC 
# MAGIC CREATE DATABASE TaxiDB;

# COMMAND ----------

filepath = greenTaxisOutputPath + "GreenTaxis.parquet"

spark.sql(f"""

    CREATE TABLE TaxiDB.GreenTaxisParquet
      USING PARQUET
      OPTIONS (path = '{filepath}')
      
""")

# COMMAND ----------

filepath = greenTaxisOutputPath + "GreenTaxis.delta"

spark.sql(f"""

    CREATE TABLE TaxiDB.GreenTaxis
      USING DELTA
      OPTIONS (path = '{filepath}')
      
""")

# COMMAND ----------

# MAGIC %md #####From left pane, navigate to Data tab and verify the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ### Check Audit History of Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ### Overwrite Parquet folder

# COMMAND ----------

# Overwrite data in parquet format
(
    greenTaxiDF
        .write
        .mode("overwrite")        
  
        .partitionBy("VendorId")
  
        .format("parquet")
  
        .save(greenTaxisOutputPath + "GreenTaxis.parquet")
)

# COMMAND ----------

# MAGIC %md ### Overwrite Delta folder

# COMMAND ----------

# Overwrite data in delta format
(
    greenTaxiDF
        .write
        .mode("overwrite")        
  
        .partitionBy("VendorId")
  
        .format("delta")
  
        .save(greenTaxisOutputPath + "GreenTaxis.delta")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md #####Notice number of records have not changed (they are overwritten), but log maintains the overwrite operation

# COMMAND ----------

# MAGIC %md ### Schema Enforcement

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Schema enforcement
# MAGIC INSERT INTO TaxiDB.GreenTaxis
# MAGIC (VendorId, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type)
# MAGIC 
# MAGIC -- VendorId should be integer, adding it as string
# MAGIC VALUES ('JUNK', '2019-12-01T00:00:00.000Z', '2019-12-01T00:15:34.000Z', 'N', 1, 145, 148, 1, 2.9, 100.0, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 140.0, 1, 1)

# COMMAND ----------

# MAGIC %md #####Previous command will fail since it does not match the schema

# COMMAND ----------

# MAGIC %md ### Insert Data to Delta Table: Insert Command

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO TaxiDB.GreenTaxis
# MAGIC (VendorId, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type)
# MAGIC 
# MAGIC VALUES (4, '2019-12-01T00:00:00.000Z', '2019-12-01T00:15:34.000Z', 'N', 1, 145, 148, 1, 2.9, 100.0, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 140.0, 1, 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ### Insert Data to Delta Table: Append DataFrame

# COMMAND ----------

# Extract new records from Data Lake
# Read csv file to append - this file only has one record for VendorId 3

greenTaxiAppendDF = (
                        spark
                          .read                     
                          .option("header", "true")
                          .schema(greenTaxiSchema)
                          .csv(greenTaxisInputPath + "GreenTaxis_201911_append.csv")
                    )

display(greenTaxiAppendDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM TaxiDB.GreenTaxis

# COMMAND ----------

# Append to Delta table
(
    greenTaxiAppendDF
        .write
        .mode("append")        
  
        .partitionBy("VendorId")
  
        .format("delta")
  
        .save(greenTaxisOutputPath + "GreenTaxis.delta")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ### Update Data in Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT RateCodeID
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE TaxiDB.GreenTaxis
# MAGIC 
# MAGIC SET RateCodeID = 2
# MAGIC 
# MAGIC WHERE VendorId = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT RateCodeID
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md #####Check how update operation has removed and added a new file in delta transaction log

# COMMAND ----------

# MAGIC %md ### Merge Data to Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_and_fwd_flag
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 3

# COMMAND ----------

# Extract new records from Data Lake
# Read csv file to append - this file only has one record for VendorId 3

greenTaxiChangesDF = (
                        spark
                          .read                     
                          .option("header", "true")
                          .schema(greenTaxiSchema)
                          .csv(greenTaxisInputPath + "GreenTaxis_201911_changes.csv")
                    )

display(greenTaxiChangesDF)

# COMMAND ----------

# Create a temporary view on top of DataFrame

greenTaxiChangesDF.createOrReplaceTempView("GreenTaxiChanges")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GreenTaxiChanges

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO TaxiDB.GreenTaxis AS target
# MAGIC 
# MAGIC   USING GreenTaxiChanges     AS source
# MAGIC   
# MAGIC ON target.VendorID = source.VendorId
# MAGIC   AND target.lpep_pickup_datetime = source.lpep_pickup_datetime
# MAGIC   AND target.PULocationID = source.PULocationID
# MAGIC   AND target.DOLocationID = source.DOLocationID
# MAGIC   
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.store_and_fwd_flag = source.store_and_fwd_flag
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_and_fwd_flag
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ### Time Travel in Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_and_fwd_flag
# MAGIC FROM TaxiDB.GreenTaxis
# MAGIC WHERE VendorId = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxiDB.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_and_fwd_flag
# MAGIC FROM TaxiDB.GreenTaxis    VERSION AS OF 4
# MAGIC WHERE VendorId = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_and_fwd_flag
# MAGIC FROM TaxiDB.GreenTaxis    TIMESTAMP AS OF '<add timestamp>'
# MAGIC WHERE VendorId = 3

# COMMAND ----------

# MAGIC %md ### Table Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table constraints
# MAGIC 
# MAGIC ALTER TABLE TaxiDB.GreenTaxis
# MAGIC 
# MAGIC ADD CONSTRAINT PassengerCountCheck CHECK (passenger_count IS NULL OR passenger_count <= 6)

# COMMAND ----------

# MAGIC %md #####Previous statement will fail since table already has records that does not satisfy constraint conditions

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Table constraints
# MAGIC 
# MAGIC ALTER TABLE TaxiDB.GreenTaxis
# MAGIC 
# MAGIC ADD CONSTRAINT PassengerCountCheck CHECK (passenger_count IS NULL OR passenger_count <= 9)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO TaxiDB.GreenTaxis
# MAGIC (VendorId, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type)
# MAGIC 
# MAGIC VALUES (1, '2019-12-01T00:00:00.000Z', '2019-12-01T00:15:34.000Z', 'N', 1, 145, 148, 
# MAGIC 
# MAGIC 10,  -- passenger_count
# MAGIC 
# MAGIC 2.9, 100.0, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 140.0, 1, 1)
