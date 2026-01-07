# Databricks notebook source
# MAGIC %md
# MAGIC Remove all where CustomerId is not null

# COMMAND ----------

df=spark.sql("select * from bronzelayer.Customer where  merge_flag= false and customer_id is not null  and gender in ('Male', 'Female')")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Check at some reg_date>dob

# COMMAND ----------

df=spark.sql("select * from bronzelayer.Customer where  merge_flag= false and customer_id is not null  and gender in ('Male', 'Female') and registration_date>date_of_birth")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Timestamp while adding the silverlayer

# COMMAND ----------

df.createOrReplaceTempView("clean_customer")
spark.sql("MERGE INTO silverlayer.Customer as t using clean_customer as s on t.customer_id = s.customer_id when matched then update set t.first_name=s.first_name,t.last_name=s.last_name,t.email=s.email,t.phone=s.phone,t.country=s.country,t.city=s.city,t.registration_date=s.registration_date,t.date_of_birth=s.date_of_birth,t.gender=s.gender,t.merge_timestamp = current_timestamp() when not matched then insert (customer_id,first_name,last_name,email,phone,country,city,registration_date,date_of_birth,gender,merge_timestamp) values (s.customer_id,s.first_name,s.last_name,s.email,s.phone,s.country,s.city,s.registration_date,s.date_of_birth,s.gender,current_timestamp())") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC Update the flag in bronzelayer

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.customer set merge_flag=true where merge_flag=false

# COMMAND ----------

