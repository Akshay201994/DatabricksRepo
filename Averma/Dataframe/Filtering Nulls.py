# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import datetime
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

df= spark.createDataFrame(users)

# COMMAND ----------

display(df)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

users_df=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

display(users_df)

# COMMAND ----------

users_df.select("id","current_city").filter(col("current_city").isNull()).show()

# COMMAND ----------

users_df.select("id","current_city").filter('current_city IS NULL').show()

# COMMAND ----------

users_df.select("*").filter(col("current_city").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get all the users whose city is not NULL

# COMMAND ----------

users_df.select("*").filter(col("current_city").isNotNull()).show()

# COMMAND ----------

users_df.select("id","current_city").filter('current_city IS NOT NULL').show()

# COMMAND ----------

display(users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get customer who paid 900.0

# COMMAND ----------

display(users_df.filter("amount_paid == 900"))

# COMMAND ----------

display(users_df.filter(col("amount_paid")== "900"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### get all the users who are not living in DALLAS

# COMMAND ----------

display(users_df.filter((col("current_city")!='Dallas') & (col("current_city")!='')))

# COMMAND ----------

display(users_df.filter("current_city != '' AND current_city != 'Dallas'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get all users whose payment is in range of 850 to 900

# COMMAND ----------

users_df.filter(col("amount_paid").between(850,900)).show()

# COMMAND ----------

users_df.filter('amount_paid BETWEEN 850 AND 900').show()

# COMMAND ----------


