# Databricks notebook source
# MAGIC %md
# MAGIC Remove all where Cliam_id, Policy_id, Claim_status, claim_amount and lastupdated is null

# COMMAND ----------

df= spark.sql("select * from bronzelayer.claim  where claim_id is not null and policy_id is not null and claim_status is not null and claim_amount is not null and LastUpdatedTimeStamp is not null and merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all where policy id does not exists in  Policy Table

# COMMAND ----------

df= spark.sql("select c.* from bronzelayer.claim c inner join bronzelayer.policy p on p.policy_id=c.policy_id where c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.claim_amount is not null and c.LastUpdatedTimeStamp is not null and c.merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert date_of_claim to Date column with format (MM-dd-yyy)

# COMMAND ----------

df= spark.sql("select c.claim_id,c.policy_id,date_format(c.date_of_claim, 'MM-dd-yyyy') as date_of_claim,c.claim_amount,c.claim_status,c.LastUpdatedTimeStamp from bronzelayer.claim c inner join bronzelayer.policy p on p.policy_id=c.policy_id where c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.claim_amount is not null and c.LastUpdatedTimeStamp is not null and c.merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure Claim Amount is >0

# COMMAND ----------

df= spark.sql("select c.claim_id,c.policy_id,to_date(date_format(c.date_of_claim, 'MM-dd-yyyy'),'MM-dd-yyyy') as date_of_claim,c.claim_amount,c.claim_status,c.LastUpdatedTimeStamp from bronzelayer.claim c inner join bronzelayer.policy p on p.policy_id=c.policy_id where c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.claim_amount is not null and c.LastUpdatedTimeStamp is not null and c.merge_flag = false and c.claim_amount>0")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Add the merge_timestamp

# COMMAND ----------

df.createOrReplaceTempView("Clean_Claim")
spark.sql("merge into silverlayer.Claim as t using Clean_Claim as s on t.claim_id=s.claim_id when matched then update set t.policy_id = s.policy_id, t.claim_amount=s.claim_amount, t.date_of_claim=s.date_of_claim, t.claim_status=s.claim_status , t.LastUpdatedTimeStamp=s.LastUpdatedTimeStamp , t.merge_timestamp = current_timestamp() when not matched then insert (claim_id,policy_id,claim_amount,date_of_claim,claim_status,LastUpdatedTimeStamp,merge_timestamp) values (s.claim_id,s.policy_id,s.claim_amount,s.date_of_claim,s.claim_status,s.LastUpdatedTimeStamp,current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.Claim

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.Claim set merge_flag = True where merge_flag = False

# COMMAND ----------

