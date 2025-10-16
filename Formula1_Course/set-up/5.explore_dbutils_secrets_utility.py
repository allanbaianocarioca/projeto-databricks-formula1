# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope-2')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope-2', key = 'formula1dl-account-key')

# COMMAND ----------


