# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Working with Dataframe
# MAGIC
# MAGIC ### NOTE- df here is name of Dataframe.
# MAGIC
# MAGIC ### To create a Dataframe using Row from pyspark sql lib
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql import Row
# MAGIC
# MAGIC df=spark.createDataFrame(
# MAGIC     [
# MAGIC         Row(id=1,fullName='Akshay Verma',Salary=15000),
# MAGIC         Row(id=2,fullName='Rohit Raj',Salary=16000),
# MAGIC         Row(id=3,fullName='Rupali Singh',Salary=18000),
# MAGIC         Row(id=4,fullName='Kajal Sharma',Salary=12000)
# MAGIC     ]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### To create a Dataframe without using lib
# MAGIC
# MAGIC ```
# MAGIC df=spark.createDataFrame([(1,'Akshay','Verma'),
# MAGIC                           (2,'Rohit','Raj')],
# MAGIC                           schema='id LONG,fName String,lName String')
# MAGIC ```
# MAGIC
# MAGIC ### To import csv/json Dataset to Databricks
# MAGIC
# MAGIC ```
# MAGIC df=spark.read.csv/json('path') **Will get data in raw form**
# MAGIC df=spark.read.option("Header",True).csv/json('path').show **Will set first row as Header**
# MAGIC df=spark.read.option("Header",True).option("inferSchema",True).csv/json('path').show() **Will set first row as header and will automatically detect schema/data type and will assign**
# MAGIC ```
# MAGIC ### To print/show Dataframe
# MAGIC ```
# MAGIC
# MAGIC df.show() ==Will show result in datframe form==
# MAGIC display(df) ==Will show result in Table form==
# MAGIC
# MAGIC ```
# MAGIC ### To look into schema/Datatypes
# MAGIC ```
# MAGIC printSchema(df)
# MAGIC ```
# MAGIC
# MAGIC ### To Select or view specific columns from a Dataframe
# MAGIC 1. We can use Select cmd and also we can use below help cmd to see details.
# MAGIC
# MAGIC ```
# MAGIC help(df.select)
# MAGIC ```
# MAGIC 2. Below are 4 ways to select columns for a dataframe
# MAGIC 1. **using col**
# MAGIC 2. **String**
# MAGIC 3. **list**
# MAGIC
# MAGIC ```
# MAGIC //Using Col
# MAGIC from pyspark.sql.functions import col
# MAGIC df.select(col("columnName1"),col("columnName2")).show()
# MAGIC
# MAGIC //Using String
# MAGIC df.select("columnName1","columnName2").show()
# MAGIC
# MAGIC //Using Lists-2ways
# MAGIC df.select(df.columnName1,df.columnName2).show()
# MAGIC
# MAGIC df.select(df["columnName1"],df["columnName2"]).show()
# MAGIC ```
# MAGIC
# MAGIC ### Rename Column(Aliasing) *We cant use aliasing for Select dataframe*
# MAGIC ```
# MAGIC df.select(col("columnName").alias("newColumnName")).show()
# MAGIC ```
# MAGIC
# MAGIC ### Tail and Take Functions
# MAGIC ```
# MAGIC df.tail(num) -- Returns the last number of rows as a list of Row.
# MAGIC
# MAGIC df.take(num) -- Returns the first num of rows as a list of Row.
# MAGIC ```
# MAGIC
# MAGIC ### Dataframe.Describe function 
# MAGIC
# MAGIC **includes count,mean,stddev,min,max. If no columns are given, this function computes statistics for all numerical or string colummns.**
# MAGIC ```
# MAGIC display(df.describe()) -- For all columns
# MAGIC display(df.describe(["columnName"]) --For specific column
# MAGIC ```
# MAGIC
# MAGIC ### Dataframe Dtypes
# MAGIC **Returns all column names and their data types as a list.**
# MAGIC ```
# MAGIC df.dtypes()
# MAGIC or
# MAGIC df.printSchema()
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

help(df.select)

# COMMAND ----------

display(df.select("season","yr","mnth","holiday"))

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("season"),col("yr"),col("mnth"),col("holiday")).show()

# COMMAND ----------

df.select(df.instant,df["mnth"]).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("yr").alias("year")).show()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.take(2))

# COMMAND ----------

display(df.tail(2))

# COMMAND ----------

display(df.describe(['instant']))

# COMMAND ----------

display(df.dtypes)
