# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1396535b-1d64-4192-9050-f56cca4d11c6",
# META       "default_lakehouse_name": "team2_LH",
# META       "default_lakehouse_workspace_id": "b58eabfa-e1c0-4347-9f2f-1f54092b9b89",
# META       "known_lakehouses": [
# META         {
# META           "id": "1396535b-1d64-4192-9050-f56cca4d11c6"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "0014c6a3-9bc2-bd6d-4db3-57d700f5a835",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7bd2006d-77ad-98eb-47a3-2082a6980823",
# META       "known_warehouses": [
# META         {
# META           "id": "7bd2006d-77ad-98eb-47a3-2082a6980823",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading in Tables

silver_measurement = spark.table("auto_silver_daily_measurement")
silver_site = spark.table("auto_silver_site")
silver_admin_area = spark.table("auto_silver_admin_area")
silver_parameter = spark.table("auto_silver_parameter")
silver_method = spark.table("auto_silver_method")
silver_cbsa = spark.table("auto_silver_cbsa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build dim_parameter

# Get one unit_of_measurement per parameter
param_units = (
    silver_measurement
    .select("parameter_code", F.col("units_of_measure").alias("unit_of_measurement"))
    .dropDuplicates(["parameter_code"])
)

# Join to get unit_of_measurement
dim_parameter = silver_parameter.join(param_units, on="parameter_code", how="left")

# Manual category mapping
dim_parameter = dim_parameter.withColumn("category",
    F.when(F.col("parameter_code").isin("88101", "81102"), "Particulate Matter")
     .otherwise("Gas")
)

# Add surrogate key
dim_parameter_window = Window.orderBy("parameter_code")
dim_parameter = dim_parameter.withColumn("parameter_key", F.row_number().over(dim_parameter_window))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build dim_method

# First, just copy silver_method
dim_method = silver_method

# Then, add surrogate key
dim_method_window = Window.orderBy("method_code")
dim_method = dim_method.withColumn("method_key", F.row_number().over(dim_method_window))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build dim_date

# Copy date_local in silver
dim_date = (
    silver_measurement.select("date_local")
    .distinct()
    .select(
        F.col("date_local").alias("date"),
        F.year("date_local").alias("year"),
        F.month("date_local").alias("month"),
        F.date_format("date_local", "MMMM").alias("month_name"),
        F.dayofmonth("date_local").alias("day"),
        F.dayofweek("date_local").alias("day_of_week"),
        F.date_format("date_local", "EEEE").alias("day_name"),
        F.quarter("date_local").alias("quarter"),
        F.when(F.dayofweek("date_local").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
)

# Create surrogate key via window
dim_date_window = Window.orderBy("date")
dim_date = dim_date.withColumn("date_key", F.row_number().over(dim_date_window))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_location

# Load in state_population
state_population = (
    spark.table("state_population")
    .withColumnRenamed("State_code", "state_code")
    .withColumnRenamed("Population", "population")
    .drop("State")
)

# Create dim_location
dim_location = (
    silver_site.select(
        "state_code",
        "county_code",
        "site_number",
        "latitude",
        "longitude",
        "city",
        "cbsa_code"
    )
)

# Joins
dim_location = dim_location.join(silver_admin_area, on = ["state_code", "county_code"], how = "left")
dim_location = dim_location.join(silver_cbsa, on = "cbsa_code", how = "left")
dim_location = dim_location.join(state_population, on = "state_code", how = "left")

# Create surrogate key via window
dim_location_window = Window.orderBy("state_code", "county_code", "site_number")
dim_location = dim_location.withColumn("location_key", F.row_number().over(dim_location_window))

# Region - keep null for now (will change later)
dim_location = dim_location.withColumn("region", F.lit(None).cast("string"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fact_daily_air_quality

# Setup and joins
fact_daily_air_quality = silver_measurement
fact_daily_air_quality = fact_daily_air_quality.join(dim_date, fact_daily_air_quality.date_local == dim_date.date, "left")
fact_daily_air_quality = fact_daily_air_quality.join(dim_location, on = ["state_code", "county_code", "site_number"], how = "left")
fact_daily_air_quality = fact_daily_air_quality.join(dim_parameter, on = "parameter_code", how = "left")
fact_daily_air_quality = fact_daily_air_quality.join(dim_method, on = "method_code", how = "left")

# Calculated fields
fact_daily_air_quality = fact_daily_air_quality.withColumn("aqi_category",
    F.when(F.col("aqi") <= 50, "Good")
     .when(F.col("aqi") <= 100, "Moderate")
     .when(F.col("aqi") <= 150, "Unhealthy for Sensitive Groups")
     .when(F.col("aqi") <= 200, "Unhealthy")
     .when(F.col("aqi") <= 300, "Very Unhealthy")
     .otherwise("Hazardous")
)

fact_daily_air_quality = fact_daily_air_quality.withColumn("exceeds_standard",
    F.col("aqi") > 100
)

# Keep only Gold columns
fact_daily_air_quality = fact_daily_air_quality.select(
    "date_key", "location_key", "parameter_key", "poc", "method_key",
    "arithmetic_mean", "first_max_value", "first_max_hour", "aqi",
    "observation_count", "observation_percent", "aqi_category", "exceeds_standard"
)

fact_daily_air_quality.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"dim_date:               {dim_date.count()}")
print(f"dim_location:           {dim_location.count()}")
print(f"dim_parameter:          {dim_parameter.count()}")
print(f"dim_method:             {dim_method.count()}")
print(f"fact_daily_air_quality: {fact_daily_air_quality.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Writing to Warehouse
fact_daily_air_quality.write.format("delta").mode("overwrite").saveAsTable("auto_fact_daily_air_quality")
dim_date.write.format("delta").mode("overwrite").saveAsTable("auto_dim_date")
dim_location.write.format("delta").mode("overwrite").saveAsTable("auto_dim_location")
dim_parameter.write.format("delta").mode("overwrite").saveAsTable("auto_dim_parameter")
dim_method.write.format("delta").mode("overwrite").saveAsTable("auto_dim_method")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summary
tables = [
    "auto_fact_daily_air_quality",
    "auto_dim_date",
    "auto_dim_location",
    "auto_dim_parameter",
    "auto_dim_method"
]

print("GOLD TRANSFORMATION COMPLETE")
for t in tables:
    print(f"  {t}: {spark.table(t).count()} rows")

# Things to do in production:
# MERGE instead of overwrite for all tables so reruns don't wipe existing data
# Read only new Silver data; filter by date
# Dimention tables also need to be MERGED
# Manage surrogate keys
# Parameterized cell so that date inputs can be taken in
# Copy Activity to move from LakeHouse to Warehouse
# Possibly additional logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze
bronze_df = spark.read.parquet("Files/BRONZE")
print("BRONZE:")
bronze_df.groupBy("year", "month").count().orderBy("year", "month").show()

# Silver
print("SILVER:")
tables = ["auto_silver_daily_measurement", "auto_silver_site", "auto_silver_admin_area",
          "auto_silver_parameter", "auto_silver_method", "auto_silver_cbsa"]
for t in tables:
    print(f"  {t}: {spark.table(t).count()} rows")

# Gold
print("GOLD:")
tables = ["auto_fact_daily_air_quality", "auto_dim_date", "auto_dim_location",
          "auto_dim_parameter", "auto_dim_method"]
for t in tables:
    print(f"  {t}: {spark.table(t).count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.parquet("Files/BRONZE")
bronze_df.select("date_local").distinct().orderBy("date_local").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
