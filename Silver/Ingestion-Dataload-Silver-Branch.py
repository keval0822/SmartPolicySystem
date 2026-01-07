# Databricks notebook source
# MAGIC %md
# MAGIC Remove all where branchID is not Null

# COMMAND ----------

df=spark.sql("select * from bronzelayer.branch where branch_id is not null and merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Remove all the leading and trailling spaces in Branch Country and Convert into the Upper Case 

# COMMAND ----------

df=spark.sql("select b.branch_id,b.branch_city,upper(trim(b.branch_country)) branch_country from bronzelayer.branch b where branch_id is not null and merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Into the silverlayer Table

# COMMAND ----------

df.createOrReplaceTempView("clean_branch")
spark.sql("MERGE INTO  silverlayer.branch as t using clean_branch as s on t.branch_id = s.branch_id when matched then update set t.branch_country = s.branch_country , t.branch_city = s.branch_city , t.merge_timestamp =current_timestamp() when not matched then insert (branch_country,branch_city,branch_id,merge_timestamp) values (s.branch_country,s.branch_city,s.branch_id,current_timestamp()) ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.branch

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.branch set merge_flag = true where merge_flag=false  

# COMMAND ----------

