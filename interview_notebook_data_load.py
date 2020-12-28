# Databricks notebook source
# File location and type
file_location = "/FileStore/shared_uploads/mshah316@hotmail.com/airlines.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_airlines = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_airlines)

# COMMAND ----------

# File location and type
file_location = "/FileStore/shared_uploads/mshah316@hotmail.com/airports.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_airports = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_airports)

# COMMAND ----------

# File location and type
file_location = "/FileStore/shared_uploads/mshah316@hotmail.com/flights/*.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_flights = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_flights)

# COMMAND ----------


# Use secrets DBUtil to get Snowflake credentials.
user = ""
password = ""

# snowflake connection options
options = {
  "sfUrl": "",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "",
  "sfSchema": "",
  "sfWarehouse": ""
}

# COMMAND ----------

# write the dataset to Snowflake.
df_airlines.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","AIRLINES") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

# write the dataset to Snowflake.
df_airports.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","AIRLPORTS") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

  # write the dataset to Snowflake.
  df_flights.write \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable","FLIGHTS") \
    .mode('overwrite')\
    .save()

# COMMAND ----------

#join airlines and flighrs on airline code

join_airlines_flights = df_airlines.join(df_flights, df_flights.AIRLINE == df_airlines.IATA_CODE).drop(df_flights.AIRLINE)

display(join_airlines_flights)

# COMMAND ----------

# join airlines and flights with airports

join_airlines_flights_airports = join_airlines_flights.join(df_airports, df_flights.ORIGIN_AIRPORT == df_airports.IATA_CODE)

display(join_airlines_flights_airports)

# COMMAND ----------

#imports
from pyspark.sql.functions import col,when,count
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# COMMAND ----------

#first report : Total number of flights by airline and airport on a monthly basis

month_arr = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']

maturity_udf = udf(lambda month: month_arr[month-1], StringType())


first_report = join_airlines_flights_airports.groupBy("MONTH","AIRPORT","AIRLINE").agg(
  count(col("MONTH")).alias('total_flights')
).withColumn("month_string", maturity_udf(col("MONTH"))).drop("MONTH").withColumnRenamed("month_string","MONTH")

display(first_report)

# COMMAND ----------

#create table in snowflake for first report

# write the dataset to Snowflake.
first_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","Total_Flights") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

# second report : On time percentage of each airline for the year 2015

second_report = join_airlines_flights_airports.where("YEAR == 2015").groupBy("AIRLINE").agg(
  count(when(col("ARRIVAL_DELAY") < 1, True)).alias('on_time_flights'),
  count(col("AIRLINE")).alias('total_flights')
).withColumn("on_time_percentage", (col("on_time_flights") / col("total_flights")) * 100)

display(second_report)

# COMMAND ----------

#create table in snowflake for second report

# write the dataset to Snowflake.
second_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","On_Time_Percentage") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

#third report : Airlines with the largest number of delays

third_report = join_airlines_flights_airports.groupBy("AIRLINE").agg(
  count(when(col("DEPARTURE_DELAY") > 0, True)).alias('departure_delay_count'),
  count(when(col("ARRIVAL_DELAY") > 0, True)).alias('arrival_delay_count')
).withColumn("total_delay_count", (col("departure_delay_count") + col("arrival_delay_count"))).orderBy('total_delay_count', ascending=False)

display(third_report)

# COMMAND ----------

#create table in snowflake for third report

# write the dataset to Snowflake.
third_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","Largest_Delays") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

#fourth report: Cancellation reasons by airport

fourth_report = join_airlines_flights_airports.where("CANCELLED == 1").groupBy("AIRPORT").agg(
  count(when(col("CANCELLATION_REASON") == "A", True)).alias('Airline_Carrier'),
  count(when(col("CANCELLATION_REASON") == "B", True)).alias('Weather'),
  count(when(col("CANCELLATION_REASON") == "C", True)).alias('National_Air_System'),
  count(when(col("CANCELLATION_REASON") == "D", True)).alias('Security'),
)

display(fourth_report)

# COMMAND ----------

#create table in snowflake for fourth report

# write the dataset to Snowflake.
fourth_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","Cancellation_Reasons") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

#fifth report : Delay reasons by airport

fifth_report = join_airlines_flights_airports.where("DEPARTURE_DELAY > 0 or ARRIVAL_DELAY > 0 ").groupBy("AIRPORT").agg(
  count(when(col("AIR_SYSTEM_DELAY") > 0, True)).alias('AIR_SYSTEM_DELAYS'),
  count(when(col("SECURITY_DELAY") > 0, True)).alias('SECURITY_DELAYS'),
  count(when(col("AIRLINE_DELAY") > 0, True)).alias('AIRLINE_DELAYS'),
  count(when(col("LATE_AIRCRAFT_DELAY") > 0, True)).alias('LATE_AIRCRAFT_DELAYS'),
  count(when(col("WEATHER_DELAY") > 0, True)).alias('WEATHER_DELAYS'),
  count(col("AIRPORT")).alias('TOTAL_DELAY_COUNT'),
).withColumn("UNACCOUNTED_DELAY_COUNT", col('TOTAL_DELAY_COUNT') - 
             (col("AIR_SYSTEM_DELAYS") + col("SECURITY_DELAYS") + col("AIRLINE_DELAYS") + col("LATE_AIRCRAFT_DELAYS") + col("WEATHER_DELAYS")) ).drop('TOTAL_DELAY_COUNT')

display(fifth_report)

# COMMAND ----------

#create table in snowflake for fifth report

# write the dataset to Snowflake.
fifth_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","Delay_Reasons") \
  .mode('overwrite')\
  .save()

# COMMAND ----------

# sixth report : Airline with the most unique routes --> the airline which has the highest number of paths that no other airline has

# distict triple of airline ORIGIN_AIRPORT DESTINATION_AIRPORT
distinct_flights_airlines = join_airlines_flights_airports.groupBy("AIRLINE","ORIGIN_AIRPORT","DESTINATION_AIRPORT").count().drop("count")

#number of airlines which have a given path
count_per_path = distinct_flights_airlines.groupby("ORIGIN_AIRPORT","DESTINATION_AIRPORT").count()

#join on airlines to determine most unique airline
joined_airlines_per_path = count_per_path.join(distinct_flights_airlines, 
                                                  (distinct_flights_airlines.ORIGIN_AIRPORT == count_per_path.ORIGIN_AIRPORT)
                                                  &
                                                  (distinct_flights_airlines.DESTINATION_AIRPORT == count_per_path.DESTINATION_AIRPORT)
                                                 ).drop(distinct_flights_airlines.DESTINATION_AIRPORT).drop(distinct_flights_airlines.ORIGIN_AIRPORT)

#number of flights that are offered by an airline that are not offered by any other airline
most_unqiue_airlines = distinct_flights_airlines.where("count == 1").groupby("AIRLINE").agg(
  count(col("AIRLINE")).alias('unique_flights_count')
)

display(most_unqiue_airlines)


# COMMAND ----------

#create table in snowflake for sixth report

# write the dataset to Snowflake.
most_unqiue_airlines.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable","Unique_Routes") \
  .mode('overwrite')\
  .save()
