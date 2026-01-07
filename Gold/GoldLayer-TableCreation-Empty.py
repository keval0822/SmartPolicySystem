# Databricks notebook source
# MAGIC %md
# MAGIC **Sales By Policy Type and Month** : This table would contain the total sales for each policy type and each month.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table goldlayer.sales_by_policy_type_and_month(
# MAGIC   policy_type string,
# MAGIC   sale_month string,
# MAGIC   total_premium double,
# MAGIC   updated_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/goldlayer/sales_by_policy_type_and_month'

# COMMAND ----------

# MAGIC %md
# MAGIC **Claims By Policy Type and Status** : This table would contain number and amount of claims policy type and claim status.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table goldlayer.Claim_by_policy_type_and_status(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   total_claim int,
# MAGIC   total_claim_amount int,
# MAGIC   updated_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/goldlayer/Claim_by_policy_type_and_status'

# COMMAND ----------

# MAGIC %md
# MAGIC **Analyze the Claim data based on the policy type like AVG, MAX, Min Count of claim**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table goldlayer.claims_analysis(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   avg_claim_amount int,
# MAGIC   max_claim_amount int,
# MAGIC   min_claim_amount int,
# MAGIC   total_claims int,
# MAGIC   updated_timestamp timestamp
# MAGIC
# MAGIC ) using delta location '/mnt/goldlayer/claims_analysis'

# COMMAND ----------

