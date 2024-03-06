# Databricks notebook source
# MAGIC %md
# MAGIC #### this code connects Databricks to an Azure Blob Storage container named ufo-data and makes it accessible as if it were a folder (/mnt/ufo-data) within Databricks. It also ensures secure access by providing the necessary account key from Databricks secrets.

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://ufo-data@moviesdatatutorial.blob.core.windows.net',
    mount_point='/mnt/ufo-data',
    extra_configs = {'fs.azure.account.key.moviesdatatutorial.blob.core.windows.net': dbutils.secrets.get('projectmoviescope', 'storageAccountKey')}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### This code snippet uses the %fs magic command in Databricks to list the files and folders within the mounted Azure Blob Storage container named /mnt/ufo-data. it's asking Databricks to show what files and folders are available within the ufo-data container that has been mounted at /mnt/ufo-data2.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/ufo-data"

# COMMAND ----------

# MAGIC %md
# MAGIC ####  this line of code is using a tool called Spark to read a CSV file named "ufo-sightings-transformed.csv". It specifies that the file has headers (the first row contains column names). Then, it loads this CSV file into a variable named "ufo" so that it can be used for further processing

# COMMAND ----------

ufo = spark.read.format("csv").option("header","true").load("/mnt/ufo-data/raw-data/ufo.csv")

# COMMAND ----------

ufo.limit(15).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####  ufo.printSchema() is a command in Spark that displays the structure of the DataFrame named "ufo". It shows the data types of each column in the DataFrame, helping you understand the layout of the data you're working with. This can be useful for checking the types of data in your DataFrame and ensuring they match your expectations.

# COMMAND ----------

ufo.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####  these imports provide essential functionalities for working with data in PySpark DataFrames, allowing you to reference columns and define their data types

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ####  this line of code is ensuring that the values in the "Encounter_Duration" column of the DataFrame ufo are interpreted as integers.

# COMMAND ----------

ufo = ufo.withColumn("Encounter_Duration", col("Encounter_Duration").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Casting the "Year" column to DateType() is not appropriate because "Year" is typically represented as an integer in datasets, not a date. Casting it to DateType() would result in an error.

# COMMAND ----------

ufo = ufo.withColumn("Year", col("Year").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### this code essentially converts the 'date_documented' column from its original format to a date format.
# MAGIC ##### The new column is derived from the existing 'date_documented' column but with a different data type or format.

# COMMAND ----------

from pyspark.sql import functions as F
ufo= ufo.withColumn('date_documented',F.to_date(ufo.date_documented))

# COMMAND ----------

ufo.write.mode("overwrite").option("header",'true').csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# MAGIC %md
# MAGIC #### By doing this, you have the ability to run SQL queries against this DataFrame using Spark SQL. Once created as a temporary table, you can reference it in SQL queries using the name "ufo_table", allowing you to perform SQL operations like SELECT, JOIN, etc., on the DataFrame

# COMMAND ----------

# Register the DataFrame as a temporary table to run SQL queries
ufo.createOrReplaceTempView("ufo_table")

# Example query: Get the count of UFO sightings per country
result = spark.sql("""
    SELECT Country, COUNT(*) as CountOfSightings
    FROM ufo_table
    GROUP BY Country
    ORDER BY CountOfSightings DESC
""")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### The three quotation marks (""") are used to show a multi-line string literal in Python. This allows you to write a string that covers multiple lines without having to use newline characters (\n).

# COMMAND ----------

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")
result.show()

# COMMAND ----------

ufo.createOrReplaceTempView("ufo_table")

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")

# Display the result as a bar chart
display(result)

# COMMAND ----------

# Count the occurrences of each UFO shape
shape_counts = ufo.groupBy("UFO_shape").count().orderBy("count", ascending=False)

# Display the distribution of UFO shapes as a pie chart
display(shape_counts)

# COMMAND ----------

# Count the number of UFO sightings per country
sightings_per_country = ufo.groupBy("Country").count().orderBy("count", ascending=False)

# Display the UFO sightings count per country as a bar chart
display(result)

# COMMAND ----------

from pyspark.sql.functions import desc

# Find the longest UFO encounter for each country
longest_encounter_per_country = ufo.groupBy("Country").agg({"length_of_encounter_seconds": "max"}) \
    .withColumnRenamed("max(length_of_encounter_seconds)", "Longest_Encounter_Seconds") \
    .orderBy(desc("Longest_Encounter_Seconds"))

# Display the longest UFO encounters per country as a bar chart
display(longest_encounter_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### displays UFO sighting data on a map, filtering out invalid or missing location data before visualization.

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)

# COMMAND ----------

#### Latitude and longitude coordinates are  represented as floating-point numbers. Casting them to DoubleType() means that they are in the correct format for performing map analysis or visualization tasks, such as plotting points on maps.

# COMMAND ----------

ufo = ufo.withColumn("latitude", col("latitude").cast(DoubleType()))
ufo = ufo.withColumn("longitude", col("longitude").cast(DoubleType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### this line of code is an essential step in the data pipeline, allowing you to save the transformed DataFrame ufo to a CSV file for further analysis, sharing, or storage.

# COMMAND ----------

ufo.write.mode("overwrite").option("header",'true').csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select necessary columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)

# COMMAND ----------

# MAGIC %md
# MAGIC #### just an edit with cluster markers added 
# MAGIC

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select necessary columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)
