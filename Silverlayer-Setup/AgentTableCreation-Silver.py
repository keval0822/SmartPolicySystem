# Databricks notebook source
# MAGIC %sql
# MAGIC create database silverlayer

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE silverlayer.Agent(
# MAGIC   agent_id integer, 
# MAGIC   agent_name string, 
# MAGIC   agent_email string, 
# MAGIC   agent_phone string, 
# MAGIC   branch_id integer, 
# MAGIC   createtime timestamp, 
# MAGIC   merge_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION'/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silver-layer

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended silverlayer.Agent

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silverlayer.Agent

# COMMAND ----------

