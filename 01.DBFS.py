# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.put("/tmp/training.txt", "Hello databricks file system", True)

# COMMAND ----------

dbutils.fs.ls("/tmp/")

# COMMAND ----------

# different way to call external storage
dbutils.fs.ls("s3://bkt-04aug-chiah/adb/sample_data.csv")

# COMMAND ----------

df = spark.read.csv("s3://bkt-04aug-chiah/adb/sample_data.csv")
df.show()

# COMMAND ----------

spark.read.format("parquet").load("s3://bkt-04aug-chiah/adb/userdata1.parquet")

# COMMAND ----------

spark.sql("SELECT * FROM parquet.`s3://bkt-04aug-chiah/adb/userdata1.parquet`")

# COMMAND ----------

access_key = ""
secret_key = "" 

#Mount bucket on databricks
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "bkt-04aug-chiah"
mount_name = "chiahs3"
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name)) 

# COMMAND ----------

folder_name = "adb"
file_name="sample_data.csv"
df = spark.read.format("csv").load("/mnt/%s/%s/%s" % (mount_name, folder_name, file_name))
df.show()

# COMMAND ----------

strMountPointParquetFile="/mnt/%s/%s/test.parquet" % (mount_name, folder_name)
df.write.parquet(strMountPointParquetFile)
