# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

# MAGIC %scala
# MAGIC val storage_account_name = "hotelsweather"
# MAGIC val storage_account_access_key = "5VJPsDnb8PuKw463UYcN0q3x4y4XhXOaYTGO/QeYXwq5mGViM4r5jr6Rh4emUUo/uR8jKadot+Cp+AStOe2VJg=="

# COMMAND ----------

# MAGIC %scala
# MAGIC val file_location_1 = "wasbs://hotelsweather@hotelsweather.blob.core.windows.net/m07sparksql/hotel-weather"
# MAGIC val file_location_2 = "wasbs://hotelsweather@hotelsweather.blob.core.windows.net/m07sparksql/expedia"
# MAGIC val file_type = "parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
# MAGIC   storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. 
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC
# MAGIC val sc = SparkSession.builder()
# MAGIC     .appName("test-app")
# MAGIC     .master("local")
# MAGIC     .getOrCreate()
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC   val hotelsWeather = sc.read
# MAGIC     .format(file_type)
# MAGIC     .option("header", "True")
# MAGIC         .load(file_location_1)
# MAGIC   
# MAGIC   hotelsWeather.show()
# MAGIC   hotelsWeather.printSchema()
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC   val expedia = sc.read
# MAGIC   .format("avro")
# MAGIC   .option("header", "true")
# MAGIC   .load(file_location_2)
# MAGIC   
# MAGIC expedia.show()
# MAGIC expedia.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Creata delta tables
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val hotWeaTable = "t1"
# MAGIC hotelsWeather.write.format("delta").saveAsTable("t1")
# MAGIC
# MAGIC val visitors = "t2"
# MAGIC expedia.write.format("delta").saveAsTable("t2")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC display(sc.sql("DESCRIBE DETAIL t1"))
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC display(sc.sql("DESCRIBE DETAIL t2"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step A: First query: Top 10 hotels with max absolute temperature difference by month

# COMMAND ----------

# MAGIC %scala
# MAGIC val query = """
# MAGIC WITH temp_diff_cte AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     month,
# MAGIC     name,
# MAGIC     ABS(avg_tmpr_c - avg_tmpr_f) AS temp_difference,
# MAGIC     RANK() OVER (PARTITION BY month ORDER BY ABS(avg_tmpr_c - avg_tmpr_f) DESC) AS rnk
# MAGIC   FROM t1
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM temp_diff_cte
# MAGIC WHERE rnk <= 10;
# MAGIC """
# MAGIC
# MAGIC val result = sc.sql(query)
# MAGIC result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step B: Second query: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months

# COMMAND ----------

# MAGIC %scala
# MAGIC val joined = hotelsWeather.join(expedia, hotelsWeather("id") === expedia("hotel_id"), "left")
# MAGIC
# MAGIC joined.show()
# MAGIC joined.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step C: Third query: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
