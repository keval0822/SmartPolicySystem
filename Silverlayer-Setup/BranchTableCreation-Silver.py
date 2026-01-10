# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE silverlayer.Branch(
# MAGIC   branch_id INT,
# MAGIC   branch_country STRING,
# MAGIC   branch_city STRING, 
# MAGIC   merge_timestamp timestamp
# MAGIC
# MAGIC ) USING DELTA LOCATION '/mnt/silverlayer/Branch'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer/Branch

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silverlayer.branch

# COMMAND ----------

