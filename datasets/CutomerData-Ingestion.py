# Databricks notebook source
from pyspark.sql.functions import lit 

schema = "customer_id int ,first_name string ,last_name string ,email string ,phone string,country string ,city string ,registration_date timestamp,date_of_birth timestamp,gender string"

df = spark.read.csv("/mnt/landing/Customer" , inferSchema= False , schema= schema , header= True)
#df.display()
df_with_flag= df.withColumn("merge_flag",lit(False))
df_with_flag.write.option("path","/mnt/bronzelayer/CustomerData").mode("append").saveAsTable("bronzelayer.Customer")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronzelayer.customer

# COMMAND ----------

from datetime import datetime 

def getFilePathWithDate(filepath):
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_path = filepath +'/' + current_date
    return new_file_path


# COMMAND ----------

dbutils.fs.mv("/mnt/landing/Customer",getFilePathWithDate("/mnt/processed/CustomerData"),True)

# COMMAND ----------

