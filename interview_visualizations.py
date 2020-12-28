# Databricks notebook source

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

# Read  report from snowflake
first_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "TOTAL_FLIGHTS") \
  .load()

display(first_report)

# COMMAND ----------


fights_per_airline = first_report.groupby("AIRLINE","MONTH").agg(
  {"TOTAL_FLIGHTS":"sum"}
).drop("TOTAL_FLIGHTS").withColumnRenamed("sum(TOTAL_FLIGHTS)","TOTAL_FLIGHTS")
print("************************************** NUMBER OF FLIGHTS PER MONTH BY EACH AIRLINE **************************************")
display(fights_per_airline)

# COMMAND ----------

fights_per_airport = first_report.groupby("AIRPORT").agg(
  {"TOTAL_FLIGHTS":"sum"}
).drop("TOTAL_FLIGHTS").withColumnRenamed("sum(TOTAL_FLIGHTS)","TOTAL_FLIGHTS").orderBy('TOTAL_FLIGHTS', ascending=False)
print("************************************** NUMBER OF FLIGHTS FOR TOP 12 AIRPORTS **************************************")

display(fights_per_airport.limit(12))

# COMMAND ----------

fights_per_airport = first_report.groupby("MONTH").agg(
  {"TOTAL_FLIGHTS":"sum"}
).drop("TOTAL_FLIGHTS").withColumnRenamed("sum(TOTAL_FLIGHTS)","TOTAL_FLIGHTS").orderBy('TOTAL_FLIGHTS', ascending=False)
print("************************************** NUMBER OF FLIGHTS PER MONTH **************************************")

display(fights_per_airport)

# COMMAND ----------

# Read  report from snowflake
second_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ON_TIME_PERCENTAGE") \
  .load()

print("************************************** ON TIME PERCENTAGE BY AIRLINE **************************************")

display(second_report)

# COMMAND ----------

# Read  report from snowflake
third_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "LARGEST_DELAYS") \
  .load()

print("************************************** DELAYED FIGHTS BY AIRLINE **************************************")

display(third_report)

# COMMAND ----------

# Read  report from snowflake
fourth_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "CANCELLATION_REASONS") \
  .load()

print("************************************** CANCELLATION_REASONS BY AIRPORT **************************************")

display(fourth_report)

# COMMAND ----------

weather_cancel = fourth_report.select("AIRPORT","WEATHER").orderBy('WEATHER', ascending=False).limit(8)
print("************************************** WEATHER CANCELLATION BY AIRPORT **************************************")
display(weather_cancel)

# COMMAND ----------

AIRLINE_CARRIER = fourth_report.select("AIRPORT","AIRLINE_CARRIER").orderBy('AIRLINE_CARRIER', ascending=False).limit(8)
print("************************************** AIRLINE_CARRIER CANCELLATION BY AIRPORT **************************************")
display(AIRLINE_CARRIER)

# COMMAND ----------

NATIONAL_AIR_SYSTEM = fourth_report.select("AIRPORT","NATIONAL_AIR_SYSTEM").orderBy('NATIONAL_AIR_SYSTEM', ascending=False).limit(8)
print("************************************** NATIONAL_AIR_SYSTEM CANCELLATION BY AIRPORT **************************************")
display(NATIONAL_AIR_SYSTEM)

# COMMAND ----------

SECURITY = fourth_report.select("AIRPORT","SECURITY").orderBy('SECURITY', ascending=False).limit(9)
print("************************************** SECURITY CANCELLATION BY AIRPORT **************************************")
display(SECURITY)

# COMMAND ----------

# Read  report from snowflake
fifth_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DELAY_REASONS") \
  .load()

print("************************************** DELAY_REASONS BY AIRPORT **************************************")

display(fifth_report)

# COMMAND ----------

delays = fifth_report.schema.names[1:]

for delay in delays:
  AIR_SYSTEM_DELAYS = fifth_report.select("AIRPORT",delay).orderBy(delay, ascending=False).limit(8)
  print(f"************************************** {delay} BY AIRPORT **************************************")
  display(AIR_SYSTEM_DELAYS)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Read  report from snowflake
sixth_report = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "UNIQUE_ROUTES") \
  .load()

print("************************************** UNIQUE_ROUTES BY AIRLINE **************************************")

display(sixth_report)

# COMMAND ----------


