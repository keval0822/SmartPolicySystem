# Databricks notebook source
from pyspark.sql.functions import lit

Policy_Schema = "policy_id int,policy_type string,customer_id int ,start_date timestamp,end_date timestamp,premium double,coverage_amount double"

df = spark.read.json("/mnt/landing/PolicyData",schema= Policy_Schema)

df_with_merge = df.withColumn("merge_flag",lit(False))

df_with_merge.write.option("path","/mnt/bronzelayer/PolicyData").mode("append").saveAsTable("bronzelayer.Policy")


# COMMAND ----------

from datetime import datetime 

def getFilePathWithDate(filepath):
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_path = filepath +'/' + current_date
    return new_file_path


# COMMAND ----------

dbutils.fs.mv("/mnt/landing/PolicyData",getFilePathWithDate("/mnt/processed/PolicyData"),True)

# COMMAND ----------

