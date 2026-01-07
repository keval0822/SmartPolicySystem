# Databricks notebook source
df = spark.sql("select * from bronzelayer.Agent where merge_flag='False'")
display(df)



# COMMAND ----------

# MAGIC %md
# MAGIC Remove all rows where brandID not exists in Branch Table

# COMMAND ----------

df_result= spark.sql("select Agent.* from bronzelayer.Agent inner join bronzelayer.branch on Agent.branch_id = branch.branch_id  where Agent.merge_flag= 'False'")
display(df_result)

# COMMAND ----------

from pyspark.sql.functions import length, col

df_phone =df_result.filter(length(col("agent_phone"))==10)
display(df_phone)

# COMMAND ----------

df_email = df_phone.fillna({'agent_email':'kevals@gmail.com'})
display(df_email)

# COMMAND ----------

# MAGIC %md 
# MAGIC Replace Empty emailid with 'kevals@gmail.com'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronzelayer.agent where agent_email =''

# COMMAND ----------

df_email.createOrReplaceTempView("agent_temp")
df_email_1 =spark.sql("select a.agent_id , a.agent_name,a.agent_phone , a.branch_id,a.create_timestamp , regexp_replace(a.agent_email,'','kevals@gmail.com') as agent_email from agent_temp a where a.agent_email=''  union select a.agent_id , a.agent_name,a.agent_phone , a.branch_id,a.create_timestamp , a.agent_email from agent_temp a where a.agent_email !='' ")

display(df_email_1)

# COMMAND ----------

# MAGIC %md 
# MAGIC add the merge_datetimestamp (current timestamp)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_final = df_email_1.withColumn("merge_timestamp",current_timestamp()) 
display(df_final)

# COMMAND ----------

df_final.createOrReplaceTempView("clean_agent")

spark.sql(" MERGE INTO silverlayer.agent as t using clean_agent as s on t.agent_id = s.agent_id when matched then update set t.agent_phone = s.agent_phone, t.agent_email = s.agent_email, t.agent_name = s.agent_name, t.branch_id = s.branch_id,t.createtime = try_cast(s.create_timestamp as timestamp), t.merge_timestamp =  current_timestamp() when not matched then insert (agent_id, agent_name, agent_phone, branch_id, agent_email, createtime, merge_timestamp) values (s.agent_id, s.agent_name, s.agent_phone, s.branch_id, s.agent_email,try_cast(s.create_timestamp as timestamp), current_timestamp())")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silverlayer.agent

# COMMAND ----------

spark.sql("update bronzelayer.Agent set merge_flag = True where merge_flag = False")

# COMMAND ----------

