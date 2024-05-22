# Databricks notebook source
from datetime import datetime,date
from pyspark.sql import Row

df=spark.createDataFrame(
    [
        Row(id=1,fullName='Akshay Verma',Salary=15000),
        Row(id=2,fullName='Rohit Raj',Salary=16000),
        Row(id=3,fullName='Rupali Singh',Salary=18000),
        Row(id=4,fullName='Kajal Sharma',Salary=12000)
    ]
)

# COMMAND ----------

df.show()

# COMMAND ----------

df1=spark.createDataFrame(
    [
        (1,'Akshay Verma',date(1994,5,20),15000),
        (2,'Rohit Raj',date(1993,8,23),16000),
        (3,'Prem Kumar',date(1998,4,3),13000),
        (4,'Rakesh Sharma',date(1996,6,20),14000)
    ]
)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df=spark.createDataFrame(
    [
        (1,'Akshay Verma',date(1994,5,20),15000),
        (2,'Rohit Raj',date(1993,8,23),16000),
        (3,'Prem Kumar',date(1998,4,3),13000),
        (4,'Rakesh Sharma',date(1996,6,20),14000)
    ],
    schema='id INT,fullName string,dob date,salary long'
)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()
