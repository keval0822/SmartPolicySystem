# Databricks notebook source

from pyspark.sql.functions import lit

schema = "agent_id integer , agent_name string , agent_email string , agent_phone string , branch_id integer, createtime timestamp"

df = spark.read.parquet("/mnt/landing/Agent_data/*.parquet")
df_with_flag = df.withColumn("merge_flag", lit("flase"))
df_with_flag.write.option("path","/mnt/bronzelayer/Agent").mode("append").saveAsTable("bronzelayer.Agent")

# COMMAND ----------

dbutils.fs.mv("/mnt/processed/Agent_Data/","/mnt/landing/Agent_data/",True)

# COMMAND ----------

from datetime import datetime 

current_date = datetime.now().strftime("%Y-%m-%d")

new_folder = "/mnt/processed/agent_data/" + current_date

dbutils.fs.mv("/mnt/landing/Agent_Data/",new_folder,True)


