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
# MAGIC val file_type_1 = "parquet"

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
# MAGIC     .format(file_type_1)
# MAGIC     .option("header", "True")
# MAGIC         .load(file_location_1)
# MAGIC   
# MAGIC   hotelsWeather.show()
# MAGIC   hotelsWeather.printSchema()
# MAGIC

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

# COMMAND ----------

# MAGIC %scala
# MAGIC val visitors = "t3"
# MAGIC expedia.withColumnRenamed("id", "expedia_id").write.format("delta").saveAsTable("t3")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL t1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED t1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL t3

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED t3

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/t3"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/t3/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step A: First query: Top 10 hotels with max absolute temperature difference by month

# COMMAND ----------

# MAGIC %scala
# MAGIC val query_2 = """
# MAGIC with MonthlyTemperatureDifferences AS(
# MAGIC SELECT
# MAGIC id, month, ROUND(MAX(avg_tmpr_c) - MIN(avg_tmpr_c),2) AS atd
# MAGIC FROM
# MAGIC t1
# MAGIC GROUP BY id, month
# MAGIC ),
# MAGIC RankedHotels AS(
# MAGIC SELECT
# MAGIC id, month, atd,
# MAGIC RANK()OVER(PARTITION BY month ORDER BY atd DESC)AS RANK
# MAGIC FROM
# MAGIC MonthlyTemperatureDifferences
# MAGIC )
# MAGIC SELECT
# MAGIC month, id AS hotel_id, atd AS absolute_temp_difference
# MAGIC FROM 
# MAGIC RankedHotels
# MAGIC WHERE
# MAGIC rank<=10;
# MAGIC """
# MAGIC val result2 = sc.sql(query_2)
# MAGIC result2.show(50)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step B: Second query: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months

# COMMAND ----------

# MAGIC %md
# MAGIC ####### belowed - to be discused

# COMMAND ----------

# MAGIC %scala
# MAGIC val joined = hotelsWeather.join(expedia, hotelsWeather("id") === expedia("hotel_id"), "left")
# MAGIC
# MAGIC joined.show()
# MAGIC joined.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC val q3 = """
# MAGIC SELECT * FROM t1 LEFT JOIN t3 ON t1.id = t3.hotel_id
# MAGIC """
# MAGIC val result3 = sc.sql(q3)
# MAGIC result3.show(50)

# COMMAND ----------

# MAGIC %scala
# MAGIC result3.write.format("delta").saveAsTable("t4")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL t4;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED t4;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### answer - query second
# MAGIC ######Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months

# COMMAND ----------

# MAGIC %scala
# MAGIC val q4 = """
# MAGIC SELECT 
# MAGIC     hotel_id,
# MAGIC     COUNT(*) AS total_visits,
# MAGIC     MONTH(srch_ci) AS visit_month
# MAGIC FROM 
# MAGIC     t4
# MAGIC WHERE 
# MAGIC     YEAR(srch_ci) = YEAR(srch_co)
# MAGIC GROUP BY 
# MAGIC     hotel_id, visit_month
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     hotel_id,
# MAGIC     COUNT(*) AS total_visits,
# MAGIC     MONTH(srch_co) AS visit_month
# MAGIC FROM 
# MAGIC     t4
# MAGIC WHERE 
# MAGIC     YEAR(srch_ci) != YEAR(srch_co)
# MAGIC     OR MONTH(srch_ci) != MONTH(srch_co)
# MAGIC GROUP BY 
# MAGIC     hotel_id, visit_month
# MAGIC ORDER BY 
# MAGIC     total_visits DESC
# MAGIC LIMIT 10;
# MAGIC """
# MAGIC val result8 = sc.sql(q4)
# MAGIC result8.show(20)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC result8.write.saveAsTable("t5")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### checking 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL t5;

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/t5"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step C: Third query: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

# COMMAND ----------

# MAGIC %scala
# MAGIC val p = """
# MAGIC WITH tt1 AS (
# MAGIC SELECT t3.hotel_id, 
# MAGIC        t3.srch_ci, 
# MAGIC        t3.srch_co, 
# MAGIC        t1.wthr_date AS weather_day_in,
# MAGIC        t1.avg_tmpr_c AS temp_day_in,
# MAGIC        DATEDIFF(t3.srch_co, t3.srch_ci) AS stay_days
# MAGIC FROM t3
# MAGIC INNER JOIN t1 ON t3.srch_ci = t1.wthr_date
# MAGIC WHERE DATEDIFF(t3.srch_co, t3.srch_ci) > 7
# MAGIC ),
# MAGIC tt2 AS(
# MAGIC SELECT tt1.hotel_id, 
# MAGIC        tt1.srch_ci, 
# MAGIC        tt1.srch_co, 
# MAGIC        tt1.weather_day_in,
# MAGIC        tt1.temp_day_in,
# MAGIC        tt1.stay_days,
# MAGIC        t1.wthr_date AS weather_day_out,
# MAGIC        t1.avg_tmpr_c AS temp_day_out
# MAGIC FROM tt1
# MAGIC INNER JOIN t1 ON tt1.srch_co = t1.wthr_date
# MAGIC )
# MAGIC SELECT *, ROUND((tt2.temp_day_out - tt2.temp_day_in),2) AS temp_diff_out_in, (tt2.temp_day_in + tt2.temp_day_out)/2 AS temp_avg FROM tt2;
# MAGIC """
# MAGIC val result00 = sc.sql(p)
# MAGIC result00.show(10)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### creating delta table, displaying log info

# COMMAND ----------

# MAGIC %scala
# MAGIC result00.write.saveAsTable("t6")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/t6"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/t6/_delta_log"))
