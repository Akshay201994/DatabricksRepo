# Databricks notebook source
users=[(1,"Akshay",15000,"M"),
       (2,"Rahul",None,"M"),
       (3,"Rohan",16000,"M"),
       (4,"Preeti",None,"F"),
       (5,"Ritika",17000,None),
       (None,None,None,None),
       (7,None,13000,"F")]
schema='id long,emp_Name String,salary long,gender String'

empDF=spark.createDataFrame(data=users,schema=schema)

# COMMAND ----------

display(empDF)

# COMMAND ----------



# COMMAND ----------

display(empDF.fillna(10000,subset=["salary"]))

# COMMAND ----------

display(empDF.fillna({"salary":10000,
              "gender":"NA",
              "emp_Name":"NA"}))

# COMMAND ----------

display(empDF.dropna())
