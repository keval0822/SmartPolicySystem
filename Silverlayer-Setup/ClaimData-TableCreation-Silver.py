# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table silverlayer.Claim(
# MAGIC   claim_id int,
# MAGIC   policy_id int,
# MAGIC   date_of_claim date,
# MAGIC   claim_amount decimal(18,0),
# MAGIC   claim_status string,
# MAGIC   LastUpdatedTimeStamp timestamp,
# MAGIC   merge_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/silverlayer/Claim'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer/Claim

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.Claim

# COMMAND ----------

