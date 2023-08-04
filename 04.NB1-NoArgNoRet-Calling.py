# Databricks notebook source
nbName = "04.NB1-NoArgNoRet-Define"
nbPath = "/Users/chia.huang@incedoinc.com/test"

dbutils.notebook.run(nbPath, 60, {})

# COMMAND ----------

# argument based notebook calling
nbPath = "/Users/chia.huang@incedoinc.com/test"
arguments = {"Tablenumber" : "1"}
sumOfTableValues = dbutils.notebook.run(nbPath, 60, arguments)
print(sumOfTableValues)

# COMMAND ----------


