-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
(id INT,phoneName STRING,brand STRING,year INT );

INSERT INTO smartphones
VALUES
(1,'iphone14','Apple',2022),
(2,'iphone13','Apple',2021),
(3,'iphone6','Apple',2014),
(4,'ipad Air','Apple',2013),
(5,'Galaxy S22','Samsung',2022),
(6,'Galaxy Z Fold','Samsung',2022),
(7,'Galaxy S9','Samsung',2016),
(8,'12 Pro','Xiaomi',2022),
(9,'Redmi 11T Pro','Xiaomi',2022),
(10,'Redmi Note 11','Xiaomi',2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE VIEW Apple_Phones
AS SELECT *
FROM smartphones
WHERE brand='Apple'

-- COMMAND ----------

SELECT *
FROM Apple_Phones

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TEMP VIEW temp_unique_brands
AS
SELECT DISTINCT brand as brands
FROM smartphones;

SELECT *
FROM temp_unique_brands;


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_latest_phones
AS
SELECT *
FROM smartphones
WHERE year > 2020
ORDER BY year DESC;

-- COMMAND ----------

SELECT *
FROM global_temp.global_latest_phones

-- COMMAND ----------

SELECT *
FROM temp_unique_brands;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

SHOW TABLES
