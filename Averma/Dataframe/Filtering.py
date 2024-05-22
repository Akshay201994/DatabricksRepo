# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Filtering
# MAGIC
# MAGIC Filter(condition: 'ColumnOrName') -> 'DataFrame' method of pyspark.sql.dataframe.DataFrame instance
# MAGIC
# MAGIC **Filters rows using the given condition.**
# MAGIC
# MAGIC **NOTE- func:`where` is an alias for :func:`filter`.**
# MAGIC
# MAGIC
# MAGIC     
# MAGIC     Parameters
# MAGIC     ----------
# MAGIC     condition : :class:`Column` or str
# MAGIC         a :class:`Column` of :class:`types.BooleanType`
# MAGIC         or a string of SQL expressions.
# MAGIC     
# MAGIC     Returns
# MAGIC     -------
# MAGIC     :class:`DataFrame`
# MAGIC         Filtered DataFrame.
# MAGIC     
# MAGIC     Examples
# MAGIC     --------
# MAGIC     >>> df = spark.createDataFrame([
# MAGIC     ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])
# MAGIC     
# MAGIC     Filter by :class:`Column` instances.
# MAGIC     
# MAGIC     >>> df.filter(df.age > 3).show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001/

# COMMAND ----------

onlineRetailDF=spark.read.option("Header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

display(onlineRetailDF)

# COMMAND ----------

help(onlineRetailDF.filter)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #Using Filter

# COMMAND ----------

display(onlineRetailDF.filter(col("InvoiceNo")==536367))


# COMMAND ----------

display(onlineRetailDF.filter("InvoiceNo==536367"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Using Where

# COMMAND ----------

display(onlineRetailDF.where(col("InvoiceNo")==536367))

# COMMAND ----------

display(onlineRetailDF.where("InvoiceNo==536367"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Filtering Null Values

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# COMMAND ----------

users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

userDF=spark.createDataFrame(users)

# COMMAND ----------

display(userDF)

# COMMAND ----------

display(userDF.dtypes)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

usersDF=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

display(usersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #Using python

# COMMAND ----------

usersDF.select("id","current_city").filter(col("current_city").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Using SQL

# COMMAND ----------

display(usersDF.select("*").where("current_city IS NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### find users where city is not null

# COMMAND ----------

display(usersDF.select("*").where("current_city IS NOT NULL"))
