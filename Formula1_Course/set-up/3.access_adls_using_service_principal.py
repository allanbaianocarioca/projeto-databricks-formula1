# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope-2', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope-2', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope-2', key = 'formula1-app-client-secret2')

# COMMAND ----------

#client_id = "9ac28ad1-0318-4311-b808-8f4c99f06702"
#tenant_id = "986081e6-172b-4756-94d6-04039bb175ce"
#client_secret = "GkP8Q~-Rw89kGhdM5TPf0BgQ0itnFh2K6paA2dm0"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.saluciana02.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.saluciana02.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.saluciana02.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.saluciana02.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.saluciana02.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@saluciana02.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@saluciana02.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


