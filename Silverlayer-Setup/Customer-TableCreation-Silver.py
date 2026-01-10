# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silverlayer.Customer(
# MAGIC
# MAGIC customer_id INT,
# MAGIC first_name STRING,
# MAGIC last_name STRING,
# MAGIC email STRING,
# MAGIC phone STRING,
# MAGIC country STRING,
# MAGIC city STRING,
# MAGIC registration_date TIMESTAMP, 
# MAGIC date_of_birth TIMESTAMP,
# MAGIC gender STRING,
# MAGIC merge_timestamp TIMESTAMP
# MAGIC
# MAGIC ) USING delta LOCATION '/mnt/silverlayer/Customer'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer/Customer

# COMMAND ----------

