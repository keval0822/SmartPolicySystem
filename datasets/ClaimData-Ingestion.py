# Databricks notebook source
from pyspark.sql.functions import lit

schema = "claim_id int, policy_id int, date_of_column timestamp, claim_amount double , claim_status string , LastUpdatedTimeStamp timestamp"

df= spark.read.parquet("/mnt/landing/Claim_Data/*.parquet",inferSchema=False,schema=schema)
#display(df)
df_merge_flag = df.withColumn("merge_flag",lit(False))
#display(df_merge_flag)
df_merge_flag.write.option("path","/mnt/bronzelayer/Claim").mode("append").saveAsTable("bronzelayer.Claim")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronzelayer.Claim

# COMMAND ----------

from datetime import datetime 

def getFilePathWithDate(filepath):
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_path = filepath +'/' + current_date
    return new_file_path


# COMMAND ----------

dbutils.fs.mv("/mnt/landing/Claim_Data",getFilePathWithDate('/mnt/processed/ClaimData'),True)

# COMMAND ----------

