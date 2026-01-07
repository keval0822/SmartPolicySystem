# Databricks notebook source
from pyspark.sql.functions import lit 

branch_schema = "branch_id int , branch_country string , branch_country string"
df = spark.read.parquet("/mnt/landing/BranchData/*.parquet", inferSchema=False, schema=branch_schema)
#df.show()
df_withcolumn = df.withColumn("merge_flag", lit(False))
#df_withcolumn.show()
df_withcolumn.write.option("path","/mnt/bronzelayer/branch").mode("append").saveAsTable("bronzelayer.Branch")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronzelayer.branch

# COMMAND ----------

from datetime import datetime 

def getFilePathWithDate(filepath):
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_path = filepath +'/' + current_date
    return new_file_path


# COMMAND ----------

dbutils.fs.mv("/mnt/landing/BranchData/",getFilePathWithDate('/mnt/processed/BranchData/'),True)