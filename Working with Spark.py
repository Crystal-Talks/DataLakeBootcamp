# Databricks notebook source
# Create multi column DataFrame from RDD
employees = sc.parallelize(
                            [
                                (1, "John", 10000),
                                (2, "Fred", 20000),
                                (3, "Anna", 30000),
                                (4, "James", 40000),
                                (5, "Mohit", 50000)
                            ]
                          )

employeesDF = employees.toDF()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF = employeesDF.toDF("id", "name", "salary")

employeesDF.show()

# COMMAND ----------

display(employeesDF)

# COMMAND ----------

# Filter data

newdf = (
            employeesDF
                .where("salary > 20000")
                .where("ID = 4")
                
        )

display(newdf)

# COMMAND ----------

# MAGIC %md ###Databricks Utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Go to Data menu from left pane, click on DBFS, go to /FileStore. Create a subfolder, TaxiFiles. Upload sample files
# MAGIC 
# MAGIC If DBFS is not showing up >> from right-top location, click on mail ID >> Admin Console >> Workspace Settings >> Advanced >> DBFS File Browser >> Enable >> Refresh browser window if required

# COMMAND ----------

dbutils.fs.ls("/FileStore/TaxiFiles")

# COMMAND ----------

dbutils.fs.head("/FileStore/TaxiFiles/GreenTaxis_201911.csv")

# COMMAND ----------

# MAGIC %md ###Option 1: Directly Accessing Data Lake with Access Key

# COMMAND ----------

# MAGIC %md ####Step 1: Get Access Key of Data Lake
# MAGIC 1. Go to Data Lake account
# MAGIC 2. From left pane, go to Access Keys
# MAGIC 3. Copy Key1
# MAGIC 
# MAGIC ####Step 2: Replace values
# MAGIC 1. In below 2 cells, replace the values of Data Lake name (twice), Access Key and Container Name

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.***DataLakeName***.dfs.core.windows.net",
  
    "***Access Key***")

# COMMAND ----------

display(dbutils.fs.ls("abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/"))

# COMMAND ----------

# MAGIC %md ###Option 2: Mount Data Lake

# COMMAND ----------

#Unmount if already mounted (otherwise this command will throw an error)
dbutils.fs.unmount("/mnt/mstrainingdatalake")

# COMMAND ----------

# MAGIC %md ###Step 1: Create Service Principal
# MAGIC 
# MAGIC 1. Go to Azure Active Directory
# MAGIC 2. Select App Registrations
# MAGIC 3. Click on Register new Application
# MAGIC 4. Add the name for application, and click Register
# MAGIC 5. Once created, go to App. And copy ApplicationId (ClientId) and TenantId (DirectoryId)
# MAGIC 6. In the app, go to Certificates and Secrets, and create a new client secret.
# MAGIC 7. Add a description and expiry date.
# MAGIC 8. Copy the Value of Secret
# MAGIC 
# MAGIC => At the end, you must have ApplicationId (ClientId), TenantId (DirectoryId), and ClientSecret (SecretValue).
# MAGIC 
# MAGIC ###Step 2: Give access to Service Principal on Data Lake
# MAGIC 
# MAGIC 1. Go to Data Lake account
# MAGIC 2. From left pane , click Access Control (IAM)
# MAGIC 3. Click Add >> Add Role Assignment
# MAGIC 4. Select role as "Storage Blob Data Contributor". Click Next
# MAGIC 5. Click on Select Members >> Search name of Service Principal >> Select. Click Review+Assign
# MAGIC 6. Click Assign

# COMMAND ----------

#Replace values - ApplicationId, TenantId, ClientSecret, Container Name, Data Lake Name

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           
           "fs.azure.account.oauth2.client.id": "***ApplicationId***",
           
           "fs.azure.account.oauth2.client.secret": "***ClientSecret***",
           
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/***TenantId***/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/",
  mount_point = "/mnt/mstrainingdatalake",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/mstrainingdatalake

# COMMAND ----------

display(
  dbutils.fs.ls("/mnt/mstrainingdatalake")
)

# COMMAND ----------

# Change the path based on file location in your Data Lake

dbutils.fs.head("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Working with DataFrames

# COMMAND ----------

# OPTION 1

# Run this command if you are "Directly Accessing Data Lake with Access Key"
# Make sure to set the path correctly >> Use head command to verify if file is accessible

greenTaxisFilePath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/GreenTaxis_201911.csv"

# COMMAND ----------

# OPTION 2

# Run this command if you have Mounted Data Lake
# Make sure to set the path correctly >> Use head command to verify if file is accessible

greenTaxisFilePath = "/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv"

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file by setting header as true
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file by setting header and inferring schema
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

greenTaxiDF.printSchema

# COMMAND ----------

# MAGIC %md ##Applying Schemas

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

# Read csv file by applying schema
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md ##Analyzing Data

# COMMAND ----------

display(
    greenTaxiDF.describe(
                             "passenger_count",                                     
                             "trip_distance"                                     
                        )
)

# COMMAND ----------

# MAGIC %md ##Cleaning Raw Data

# COMMAND ----------

# Count before filtering
print("Before = " + str(greenTaxiDF.count()))

# Filter inaccurate data
greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
  
                          .filter(col("trip_distance") > 0.0)
)

# Count after filtering
print("After = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Drop rows with nulls
greenTaxiDF = (
                  greenTaxiDF
                          .na.drop('all')
              )

# COMMAND ----------

# Map of default values
defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}

# Replace nulls with default values
greenTaxiDF = (
                  greenTaxiDF
                      .na.fill(defaultValueMap)
              )

# COMMAND ----------

# Drop duplicate rows
greenTaxiDF = (
                  greenTaxiDF
                          .dropDuplicates()
              )

# COMMAND ----------

# Drop duplicate rows
greenTaxiDF = (
                  greenTaxiDF
                          .where("lpep_pickup_datetime >= '2019-11-01' AND lpep_dropoff_datetime < '2019-12-01'")
              )

# COMMAND ----------

# Display the final count

print("Final count after cleanup = " + str(greenTaxiDF.count()))

# COMMAND ----------

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md ##Applying Transformations

# COMMAND ----------

greenTaxiDF = (
                  greenTaxiDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create a derived column - Trip time in minutes
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))) 
                                                    / 60
                                             )
                                   )
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create a derived column - Trip type
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripType", 
                                        when(
                                                col("RatecodeID") == 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create derived columns for year, month and day
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripYear", year(col("PickupTime")))
                        .withColumn("TripMonth", month(col("PickupTime")))
                        .withColumn("TripDay", dayofmonth(col("PickupTime")))
              )

greenTaxiDF.printSchema

# COMMAND ----------

display(greenTaxiDF)

# COMMAND ----------

greenTaxiGroupedDF = (
                          greenTaxiDF
                            .groupBy("TripDay")
                            .agg(sum("TotalAmount").alias("total"))
  
                            .orderBy(col("TripDay").desc())
                     )
    
display(greenTaxiGroupedDF)

# COMMAND ----------

# MAGIC %md ##Joining with another dataset

# COMMAND ----------

# Run this command if you are "Directly Accessing Data Lake with Access Key"
# Make sure to set the path correctly >> Use head command to verify if file is accessible

taxiZonesFilePath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/TaxiZones.csv"

# COMMAND ----------

# Run this command if you have Mounted Data Lake
# Make sure to set the path correctly >> Use head command to verify if file is accessible

taxiZonesFilePath = "/mnt/mstrainingdatalake/Raw/TaxiZones.csv"

# COMMAND ----------

dbutils.fs.head(taxiZonesFilePath)

# COMMAND ----------

# Read TaxiZones file
taxiZonesDF = (
                  spark
                      .read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(taxiZonesFilePath)
              )

display(taxiZonesDF)

# COMMAND ----------

greenTaxiWithZonesDF = (
                          greenTaxiDF.alias("g")
                                     .join(taxiZonesDF.alias("t"),                                               
                                               col("t.LocationId") == col("g.PickupLocationId"),
                                              "inner"
                                          )
                       )

display(greenTaxiWithZonesDF)

# COMMAND ----------

# MAGIC %md ###Exercise

# COMMAND ----------

# EXERCISE - JOIN greenTaxiWithZonesDF with TaxiZones on DropLocationId. And group by PickupZone and DropZone, and provide average of TotalAmount.

# COMMAND ----------

# MAGIC %md ##Working with Spark SQL

# COMMAND ----------

# Create a local temp view
greenTaxiDF.createOrReplaceTempView("GreenTaxiTripData")

# COMMAND ----------

display(
  spark.sql("SELECT PassengerCount, PickupTime FROM GreenTaxiTripData WHERE PickupLocationID = 1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT PassengerCount, PickupTime 
# MAGIC FROM GreenTaxiTripData 
# MAGIC WHERE PickupLocationID = 1

# COMMAND ----------

# MAGIC %md ##Writing Output to Data Lake

# COMMAND ----------

#Define the path - either mounted path or complete path

outputFilePath = "/mnt/mstrainingdatalake/Output/GreenTaxiOutput"

# COMMAND ----------

# Write output as CSV File
(
    greenTaxiDF   
        .write
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
        .mode("overwrite")
        .csv(outputFilePath + ".csv")
)

# COMMAND ----------

# Load the dataframe as parquet to storage
(
    greenTaxiDF  
      .write
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
      .mode("overwrite")
      .parquet(outputFilePath + ".parquet")
)

# COMMAND ----------

# MAGIC %md ###Reading JSON

# COMMAND ----------

# Run this command if you are "Directly Accessing Data Lake with Access Key"
# Make sure to set the path correctly >> Use head command to verify if file is accessible

paymentTypesFilePath = "abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net/Raw/PaymentTypes.json"

# COMMAND ----------

# Run this command if you have Mounted Data Lake
# Make sure to set the path correctly >> Use head command to verify if file is accessible

paymentTypesFilePath = "/mnt/mstrainingdatalake/Raw/PaymentTypes.json"

# COMMAND ----------

paymentTypes = (
                    spark
                        .read
                        .json(paymentTypesFilePath)
)

display(paymentTypes)

# COMMAND ----------

# MAGIC %md ###Working with Spark SQL and Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS MyDatabase

# COMMAND ----------

# Store data as a Managed Table
(
  greenTaxiDF
    .write
    .mode("overwrite")
    .saveAsTable("MyDatabase.GreenTaxis")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM MyDatabase.GreenTaxis
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED MyDatabase.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE MyDatabase.GreenTaxis

# COMMAND ----------

# Store data as an Unmanaged Table
(
  greenTaxiDF
    .write
    .mode("overwrite")
  
    .option("path", outputFilePath + "1.parquet")
    #.format("csv")   /* Default format is parquet */
  
    .saveAsTable("MyDatabase.GreenTaxis") 
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED MyDatabase.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE MyDatabase.GreenTaxis
