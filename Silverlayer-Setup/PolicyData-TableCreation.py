# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.Policy limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.Policy (
# MAGIC   policy_id integer,
# MAGIC   policy_type string,
# MAGIC   customer_id integer,
# MAGIC   start_date timestamp,
# MAGIC   end_date timestamp,
# MAGIC   premium double,
# MAGIC   coverage_amount double,
# MAGIC   merge_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/silverlayer/Policy'

# COMMAND ----------

