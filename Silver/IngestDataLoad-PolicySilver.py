# Databricks notebook source
# MAGIC %md
# MAGIC Remove all the rows where CustomerId, PolicyId is Null

# COMMAND ----------

df= spark.sql("select p.* from bronzelayer.policy p where p.merge_flag = False and p.customer_id is not null and p.policy_id is not null ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all rows where CustomerId not exists In Customer Table

# COMMAND ----------

df= spark.sql("select p.* from bronzelayer.policy p inner join bronzelayer.customer c on p.customer_id=c.customer_id where p.merge_flag = False and p.customer_id is not null and p.policy_id is not null ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Every Policy must have premimun & Coverage amount>0

# COMMAND ----------

df= spark.sql("select p.* from bronzelayer.policy p inner join bronzelayer.customer c on p.customer_id=c.customer_id where p.merge_flag = False and p.customer_id is not null and p.policy_id is not null and p.premium >0 and p.coverage_amount>0 ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate end_date > Start_date

# COMMAND ----------

df= spark.sql("select p.* from bronzelayer.policy p inner join bronzelayer.customer c on p.customer_id=c.customer_id where p.merge_flag = False and p.customer_id is not null and p.policy_id is not null and p.premium >0 and p.coverage_amount>0  and p.end_date>p.start_date")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge table with mergeed_date_timestamp as per current_timestamp

# COMMAND ----------

df.createOrReplaceTempView("Clean_Policy")
spark.sql("merge into silverlayer.policy as t using Clean_policy as s on t.policy_id=s.policy_id when matched then update set t.customer_id=s.customer_id , t.policy_type =s.policy_type , t.premium=s.premium, t.end_date=s.end_date,t.start_date=s.start_date,t.coverage_amount=s.coverage_amount,t.merge_timestamp=current_timestamp() when not matched then insert(policy_id,customer_id,policy_type,start_date,end_date,premium,coverage_amount,merge_timestamp) values(s.policy_id,s.customer_id,s.policy_type,s.start_date,s.end_date,s.premium,s.coverage_amount,current_timestamp())")

# COMMAND ----------

# MAGIC %md
# MAGIC Update Merge flag in bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.Policy set merge_flag = True where merge_flag = False

# COMMAND ----------

