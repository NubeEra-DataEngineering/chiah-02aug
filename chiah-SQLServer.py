# Databricks notebook source
# MAGIC %python
# MAGIC print("welcome")

# COMMAND ----------

df_remote_table = (spark.read
                .format("sqlserver")
                .option("host", "dbserver02aug.database.windows.net")
                .option("port", "1433")
                .option("user", "serverchiah")
                .option("password", "qweasdzxc123!")
                .option("database", "db-chiah-02aug")
                .option("dbtable", "dbo.iris_data")
                .load()
)

# COMMAND ----------

print(df_remote_table)

# COMMAND ----------

df_remote_table.head()

# COMMAND ----------


