# Databricks notebook source
dbutils.fs.mount.help()

# COMMAND ----------

dbutils.fs.mount(source='wasbs://input@avstorage2094.blob.core.windows.net',
                 mount_point='/mnt/blobstorage',
                 extra_configs={'fs.azure.account.key.avstorage2094.blob.core.windows.net':'AccessKey'})

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstorage')

# COMMAND ----------

display(spark.read.csv("/mnt/blobstorage/Emails.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC Using SAS Key

# COMMAND ----------

dbutils.fs.mount(source='wasbs://input@avstorage2094.blob.core.windows.net',
                 mount_point='/mnt/blobstorage1',
                 extra_configs={'fs.azure.sas.input.avstorage2094.blob.core.windows.net':'sas-tokenKey'})

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstorage1')
