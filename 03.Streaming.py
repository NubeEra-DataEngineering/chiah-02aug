# Databricks notebook source
dbutils.fs.ls("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data.cache()  # Cache data for faster reuse

# COMMAND ----------

data = data.dropna()

# COMMAND ----------

display(data)

# COMMAND ----------

data.createOrReplaceTempView("data_geo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_geo

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/structured-streaming/")

# COMMAND ----------

file_path = "/databricks-datasets/structured-streaming/events"

# Checkpointing is essential to maintain the state of a streaming query and handle possible failures. 
checkpoint_path = "/tmp/ss-tutorial/_checkpoint" 

# COMMAND ----------

raw_df = (spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("cloudFiles.schemaLocation", checkpoint_path).load(file_path))

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
transformed_df = (
    raw_df.select("*",
                  col("_metadata.file_path").alias("source_file"),
                  current_timestamp().alias("processing_time")
                  )
    )

# COMMAND ----------

target_path = "/tmp/ss-tutorial/"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

transformed_df.writeStream.trigger(availableNow=True).option("checkpointLocation",checkpoint_path).option("path",target_path).start()
