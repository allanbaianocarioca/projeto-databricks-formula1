# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope-2', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.saluciana02.dfs.core.windows.net",
    formula1dl_account_key)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.saluciana02.dfs.core.windows.net", "<your-storage-account-key>")

# COMMAND ----------

display(spark.read.csv("abfss://demo@saluciana02.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


