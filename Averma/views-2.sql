-- Databricks notebook source
SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

SELECT *
FROM global_temp.global_latest_phones

-- COMMAND ----------

DROP TABLE smartphones

-- COMMAND ----------

DROP VIEW apple_phones;
