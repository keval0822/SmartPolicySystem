# Databricks notebook source
dbutils.fs.mount( source = 'wasbs://bronzelayer@kevalsmartpolicyadls.blob.core.windows.net', 
                 mount_point= '/mnt/bronzelayer', extra_configs ={'fs.azure.sas.bronzelayer.kevalsmartpolicyadls.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-01-22T15:56:56Z&st=2025-12-18T07:41:56Z&spr=https&sig=%2FMZuMfIJ5Fgxxy7%2FrEJw5HBabrA8d4vHeqAGRCrHEMA%3D'})


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronzelayer

# COMMAND ----------

# MAGIC %md 

# COMMAND ----------

# MAGIC %md 
# MAGIC Mount Landing Container

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@kevalsmartpolicyadls.blob.core.windows.net', 
                 mount_point= '/mnt/landing', extra_configs ={'fs.azure.sas.landing.kevalsmartpolicyadls.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-01-22T15:56:56Z&st=2025-12-18T07:41:56Z&spr=https&sig=%2FMZuMfIJ5Fgxxy7%2FrEJw5HBabrA8d4vHeqAGRCrHEMA%3D'})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing

# COMMAND ----------

# MAGIC %md 
# MAGIC Mount Process File

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processed@kevalsmartpolicyadls.blob.core.windows.net', 
                 mount_point= '/mnt/processed', extra_configs ={'fs.azure.sas.processed.kevalsmartpolicyadls.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-01-22T15:56:56Z&st=2025-12-18T07:41:56Z&spr=https&sig=%2FMZuMfIJ5Fgxxy7%2FrEJw5HBabrA8d4vHeqAGRCrHEMA%3D'})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed

# COMMAND ----------

# MAGIC %md 
# MAGIC Create Silver Layer Mount Point
# MAGIC  

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silverlayer@kevalsmartpolicyadls.blob.core.windows.net', 
                 mount_point= '/mnt/silverlayer', extra_configs ={'fs.azure.sas.silverlayer.kevalsmartpolicyadls.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-01-22T15:56:56Z&st=2025-12-18T07:41:56Z&spr=https&sig=%2FMZuMfIJ5Fgxxy7%2FrEJw5HBabrA8d4vHeqAGRCrHEMA%3D'})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer

# COMMAND ----------

# MAGIC %md
# MAGIC Create Goldlayer Mount Point

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://goldlayer@kevalsmartpolicyadls.blob.core.windows.net', 
                 mount_point= '/mnt/goldlayer', extra_configs ={'fs.azure.sas.goldlayer.kevalsmartpolicyadls.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-01-22T15:56:56Z&st=2025-12-18T07:41:56Z&spr=https&sig=%2FMZuMfIJ5Fgxxy7%2FrEJw5HBabrA8d4vHeqAGRCrHEMA%3D'})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/goldlayer
# MAGIC

# COMMAND ----------

