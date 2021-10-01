# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Weather analysis project
# MAGIC ### Sample data: 2016.02 and 2016.03

# COMMAND ----------

# DBTITLE 1,# Load weather data from 2016.02 csv file
# File location and type
file_location = "/FileStore/tables/weather_20160201.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Save data to parquet file
df.write.format("parquet").saveAsTable("tb_weather_201602")

# COMMAND ----------

# DBTITLE 1,# Load weather data from 2016.03 csv file
# File location and type
file_location = "/FileStore/tables/weather_20160301.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Save data to parquet file
df.write.format("parquet").saveAsTable("tb_weather_201603")

# COMMAND ----------

# DBTITLE 1,# Describe columns name and data type
# MAGIC %sql
# MAGIC 
# MAGIC desc tb_weather_201602

# COMMAND ----------

# DBTITLE 1,# Describe columns name and data type
# MAGIC %sql
# MAGIC desc tb_weather_201603

# COMMAND ----------

# DBTITLE 1,# Union all weather data for analysis
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW vw_weather AS
# MAGIC select * from tb_weather_201602
# MAGIC union
# MAGIC select * from tb_weather_201603

# COMMAND ----------

# DBTITLE 1,# Validating the union operation
# MAGIC %sql
# MAGIC 
# MAGIC select distinct month(to_date(ObservationDate)) from vw_weather

# COMMAND ----------

# DBTITLE 1,# View bar chart about temperature and observation date
# MAGIC %sql
# MAGIC 
# MAGIC select to_date(ObservationDate) as day, 
# MAGIC        max(ScreenTemperature) as hottest_day 
# MAGIC   from vw_weather 
# MAGIC  group by ObservationDate 
# MAGIC  order by hottest_day desc

# COMMAND ----------

# DBTITLE 1,# Selecting the hottest day in the period
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     Region,
# MAGIC     ObservationDate,
# MAGIC     ScreenTemperature,
# MAGIC     rank() OVER (PARTITION BY year(to_date(ObservationDate)) ORDER BY ScreenTemperature DESC) as rank
# MAGIC   FROM vw_weather) tmp
# MAGIC WHERE
# MAGIC   rank = 1

# COMMAND ----------

# DBTITLE 1,# Answering the questions
# MAGIC %md
# MAGIC 
# MAGIC 1. Which date was the hottest day?
# MAGIC > 2016-03-17
# MAGIC 
# MAGIC 2. What was the temperature on that day?
# MAGIC > 15.8
# MAGIC 
# MAGIC 3. In which region was the hottest day?
# MAGIC > Highland & Eilean Siar
