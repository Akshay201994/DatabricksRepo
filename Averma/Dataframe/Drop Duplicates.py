# Databricks notebook source


# COMMAND ----------

df=spark.read.option("Header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import datetime
users=[{"id":1, 
       "first_name":'Naval',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       }, 
       {"id":1, 
       "first_name":'Naval',
       "last_name":'Yem',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":1, 
       "first_name":'Naveen',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":2, 
       "first_name":'John',
       "last_name":'Players',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":1, 
       "first_name":'Naval',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":3, 
       "first_name":'Killer',
       "last_name":'Spykar',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":4, 
       "first_name":'Levis',
       "last_name":'Jeans',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":5, 
       "first_name":'Puma',
       "last_name":'Adidas',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       }
       
]

# COMMAND ----------

import pandas as pd

# COMMAND ----------

userDF=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

display(userDF)

# COMMAND ----------

help(userDF.dropDuplicates)

# COMMAND ----------

display(userDF.dropDuplicates())

# COMMAND ----------

display(userDF.dropDuplicates(["id"]))
