# Databricks notebook source
VOL = "/Volumes/workspace/default/uploads"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.northwind")
spark.sql("USE workspace.northwind")

spark.read.csv(f"{VOL}/vends.csv", header=True, inferSchema=True).write.mode("overwrite").saveAsTable("vends")
spark.read.csv(f"{VOL}/machines.csv", header=True, inferSchema=True).write.mode("overwrite").saveAsTable("machines")

# COMMAND ----------

from pyspark.sql.functions import col, sum, broadcast

spark.sql("USE workspace.northwind")

vends    = spark.table("vends")
machines = spark.table("machines")

# COMMAND ----------

vends.groupBy("machine_id").count().filter(col("machine_id") == "M1004").display()

# COMMAND ----------

vends.select("*").write.mode("overwrite").saveAsTable("check_extract")
spark.sql("DESCRIBE DETAIL check_extract").display()

# COMMAND ----------

vends.groupBy("machine_id").sum("amount").join(machines.hint("merge"), "machine_id").display()

# COMMAND ----------

vends.filter(col("vend_date") == "2026-05-15").agg(sum("amount")).display()

# COMMAND ----------

vends.repartition(64).write.mode("overwrite").saveAsTable("vends_small")
spark.sql("DESCRIBE DETAIL vends_small").display()

# COMMAND ----------

vends.write.mode("overwrite").partitionBy("machine_id").saveAsTable("vends_layout")
spark.sql("DESCRIBE DETAIL vends_layout").display()

# COMMAND ----------

vends.join(machines.hint("merge"), "machine_id") \
     .filter(col("vend_date").between("2026-05-01", "2026-05-31")) \
     .groupBy("location_type").sum("amount").display()

# COMMAND ----------

vends.select("payment_type").distinct().display()

# COMMAND ----------

chain = vends.filter(col("machine_id") == "M1004").select("vend_date", "amount")
chain.count()

# COMMAND ----------

vends.groupBy("machine_id", "vend_date").sum("amount").filter(col("vend_date") == "2026-05-15").explain()

# COMMAND ----------

vends.groupBy("machine_id").sum("amount").count()
