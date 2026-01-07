# Databricks notebook source
# MAGIC %sql
# MAGIC --CREATE OR REPLACE VIEW vw_goldlayer_sales_by_policy_type_and_month AS
# MAGIC SELECT
# MAGIC       s.policy_type as policy_type,
# MAGIC       date_format(s.start_date, 'yyyy-MM') as sale_month,
# MAGIC       sum(s.premium) as total_premium
# MAGIC     from silverlayer.policy s 
# MAGIC   WHERE S.POLICY_TYPE IS NOT NULL
# MAGIC   GROUP by s.policy_type, date_format(s.start_date, 'yyyy-MM')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into goldlayer.sales_by_policy_type_and_month as t using vw_goldlayer_sales_by_policy_type_and_month as s on t.policy_type= s.policy_type and t.sale_month=s.sale_month
# MAGIC when matched then 
# MAGIC     update 
# MAGIC       set t.total_premium = s.total_premium , t.updated_timestamp= current_timestamp()
# MAGIC when not matched then insert (policy_type,sale_month,total_premium,updated_timestamp) values
# MAGIC     (
# MAGIC         s.policy_type,
# MAGIC         s.sale_month,
# MAGIC         s.total_premium,
# MAGIC         current_timestamp()
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_gold_claims_by_policy_type_and_status AS 
# MAGIC   SELECT  
# MAGIC         policy_type,
# MAGIC         c.claim_status as claim_status,
# MAGIC         count(*) as total_claim,
# MAGIC         sum(claim_amount) as total_claim_amount
# MAGIC
# MAGIC       FROM silverlayer.claim c 
# MAGIC       join silverlayer.policy p 
# MAGIC         on c.policy_id=p.policy_id
# MAGIC   group by 
# MAGIC       policy_type, 
# MAGIC       c.claim_status
# MAGIC   HAVING p.policy_type is NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into goldlayer.Claim_by_policy_type_and_status as t using vw_gold_claims_by_policy_type_and_status as s on t.policy_type=s.policy_type and t.claim_status=s.claim_status
# MAGIC when matched then 
# MAGIC     update 
# MAGIC       set t.total_claim = s.total_claim , t.total_claim_amount = s.total_claim_amount , t.updated_timestamp= current_timestamp()
# MAGIC when not matched then insert (policy_type,claim_status,total_claim,total_claim_amount,updated_timestamp) values
# MAGIC     (
# MAGIC         s.policy_type,
# MAGIC         s.claim_status,
# MAGIC         s.total_claim,
# MAGIC         s.total_claim_amount,
# MAGIC         current_timestamp()
# MAGIC         
# MAGIC     )

# COMMAND ----------

# MAGIC    %sql
# MAGIC   --  CREATE OR REPLACE VIEW vw_gold_claims_analysis AS
# MAGIC    select policy_type,
# MAGIC           max(claim_amount) as max_claim_amount,
# MAGIC           min(claim_amount)as min_claim_amount,   
# MAGIC           avg(claim_amount) as avg_claim_amount,
# MAGIC           count(distinct claim_id) as total_claims
# MAGIC       
# MAGIC       FROM silverlayer.claim c 
# MAGIC       join silverlayer.policy p 
# MAGIC         on c.policy_id=p.policy_id
# MAGIC   group by 
# MAGIC       policy_type, 
# MAGIC       c.claim_status
# MAGIC   HAVING p.policy_type is NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into goldlayer.claims_analysis as t using vw_gold_claims_analysis as s on t.policy_type=s.policy_type
# MAGIC when matched then 
# MAGIC     update 
# MAGIC         set 
# MAGIC             t.max_claim_amount = s.max_claim_amount,
# MAGIC             t.min_claim_amount = s.min_claim_amount,
# MAGIC             t.avg_claim_amount = s.avg_claim_amount,
# MAGIC             t.total_claims = s.total_claims,
# MAGIC             t.updated_timestamp= current_timestamp()
# MAGIC when not matched then insert (policy_type,max_claim_amount,min_claim_amount,avg_claim_amount,total_claims,updated_timestamp) values
# MAGIC     (
# MAGIC         s.policy_type,
# MAGIC         s.max_claim_amount,
# MAGIC         s.min_claim_amount,
# MAGIC         s.avg_claim_amount,
# MAGIC         s.total_claims,
# MAGIC         current_timestamp()
# MAGIC     )

# COMMAND ----------

