# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Northwind Automated Retail - The Dawn Manifest
# MAGIC **Part I of teaching: the walkthrough.** One concept per cell. Run each, read the plan it prints.
# MAGIC Verified on Databricks Free Edition (serverless).
# MAGIC
# MAGIC `location_type` lives only in `machines`, so joining on `machine_id` has no column clash.
# MAGIC `vends` is rewritten as 16 files so 16 tasks are visible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: load the CSVs from your Volume into tables
# MAGIC Upload `vends.csv` and `machines.csv` to a Volume, set `VOL` below, run this once.

# COMMAND ----------

VOL = "/Volumes/workspace/casestudy0/casestudy0vol"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.northwind")
spark.sql("USE workspace.northwind")

spark.read.csv(f"{VOL}/vends.csv", header=True, inferSchema=True).write.mode("overwrite").saveAsTable("vends")
spark.read.csv(f"{VOL}/machines.csv", header=True, inferSchema=True).write.mode("overwrite").saveAsTable("machines")

# COMMAND ----------

from pyspark.sql.functions import col, broadcast

vends    = spark.table("workspace.northwind.vends")
machines = spark.table("workspace.northwind.machines")

# COMMAND ----------

# MAGIC %md
# MAGIC # Groundwork - reading a plan
# MAGIC Before the concepts: how to read what Spark will do.

# COMMAND ----------

# MAGIC %md ## Reading a plan: explain()
# MAGIC - **Logical plan** = what you asked for. **Physical plan** = how Spark will run it. Catalyst turns the first into the second.
# MAGIC - `explain()` shows the physical plan. `explain(True)` adds the logical plans. `explain("formatted")` lays the physical plan out node by node.
# MAGIC - The **DAG** (stages and tasks) is not printed here. It is in the query profile via **See performance** after an action runs (see Part V).

# COMMAND ----------

q = vends.filter(col("machine_id") == "M1004").groupBy("machine_id").sum("amount")
q.explain()

# COMMAND ----------

q.explain(True)

# COMMAND ----------

q.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part I - Execution model
# MAGIC How Spark decides what to run, and when.

# COMMAND ----------

# MAGIC %md ## Lazy - the chain runs nothing until an action

# COMMAND ----------

chain = vends.filter(col("machine_id") == "M1004").select("vend_date", "amount")
chain.count()

# COMMAND ----------

# MAGIC %md ## Narrow vs wide - run each, does a shuffle appear in the plan?

# COMMAND ----------

vends.filter(col("machine_id") == "M1004").explain()

# COMMAND ----------

vends.select("machine_id", "amount").explain()

# COMMAND ----------

vends.withColumn("net", col("amount") * 0.91).explain()

# COMMAND ----------

vends.groupBy("machine_id").sum("amount").explain()

# COMMAND ----------

vends.orderBy("amount").explain()

# COMMAND ----------

vends.select("category").distinct().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part II - The optimiser and the engine
# MAGIC What rewrites your query, and what runs it.

# COMMAND ----------

# MAGIC %md ## Catalyst - write the filter last, find where it lands

# COMMAND ----------

vends.groupBy("machine_id", "vend_date").sum("amount") \
     .filter(col("vend_date") == "2026-05-15").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How it fits together
# MAGIC - **Catalyst** plans it (what `explain()` prints, the Catalyst cell above).
# MAGIC - **Photon** runs it (the `Photon...` names in the next cell).
# MAGIC - **Tungsten** runs only what Photon can't (the classic names on the merge hint, next cell).

# COMMAND ----------

# MAGIC %md ## Two dialects - Photon names vs the fallback (merge hint)

# COMMAND ----------

vends.join(machines, "machine_id").groupBy("location_type").sum("amount").explain()
vends.join(machines.hint("merge"), "machine_id").groupBy("location_type").sum("amount").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part III - Read less
# MAGIC Same answer, fewer bytes off disk.

# COMMAND ----------

# MAGIC %md ## Predicate pushdown - which line on the scan?

# COMMAND ----------

vends.filter(col("vend_date") == "2026-05-15").select("machine_id", "amount").explain("formatted")

# COMMAND ----------

# MAGIC %md ## Column pruning - how many columns does the scan output?

# COMMAND ----------

vends.select("machine_id", "amount").explain("formatted")

# COMMAND ----------

# MAGIC %md ## Partition pruning - how many folders get read?

# COMMAND ----------

vends.write.mode("overwrite").partitionBy("vend_date").saveAsTable("vends_by_date")
spark.table("vends_by_date").filter(col("vend_date") == "2026-05-15").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part IV - Data layout
# MAGIC How the table is organised on disk, so later reads are cheaper.

# COMMAND ----------

# MAGIC %md ## Small files and OPTIMIZE - measure the file count before and after

# COMMAND ----------

spark.sql("DESCRIBE DETAIL vends").display()
spark.sql("OPTIMIZE vends")
spark.sql("DESCRIBE DETAIL vends").display()

# COMMAND ----------

# MAGIC %md ## Liquid clustering
# MAGIC - **partitionBy** splits into folders: skip by folder, one fixed column, rewrite to change it.
# MAGIC - **CLUSTER BY** keeps one set of files: skip by file stats, 1-4 keys you can change later. Databricks now recommends it for almost everything under 100 TB; partition only when every query filters the same low-cardinality column.

# COMMAND ----------

vends.write.clusterBy("machine_id", "vend_date").mode("overwrite").saveAsTable("vends_clustered")
spark.sql("OPTIMIZE vends_clustered")
spark.sql("DESCRIBE DETAIL vends_clustered").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part V - Shuffle less, adapt at runtime
# MAGIC Joins, and what the engine changes once it sees the data.

# COMMAND ----------

# MAGIC %md ## Broadcast hash join - auto vs forced

# COMMAND ----------

vends.join(machines, "machine_id").explain()
vends.join(broadcast(machines), "machine_id").explain()

# COMMAND ----------

# MAGIC %md ## Sort-merge join - the strategy Photon cannot run

# COMMAND ----------

vends.join(machines.hint("merge"), "machine_id").explain()

# COMMAND ----------

# MAGIC %md ## AQE - the static plan is not the plan that runs
# MAGIC `explain()` shows `isFinalPlan=false`. Run the action, then open **See performance** for the DAG and the final plan.

# COMMAND ----------

q = vends.groupBy("machine_id").sum("amount")
q.explain()
q.count()

# COMMAND ----------

# MAGIC %md ## Capstone - compare the two plans
# MAGIC - **naive**: forced sort-merge, filter after the join.
# MAGIC - **tuned**: clustered Delta, filter first, broadcast.

# COMMAND ----------

vends.join(machines.hint("merge"), "machine_id") \
     .filter(col("vend_date").between("2026-05-01", "2026-05-31")) \
     .groupBy("location_type").sum("amount").explain()

spark.table("vends_clustered") \
     .filter(col("vend_date").between("2026-05-01", "2026-05-31")) \
     .join(broadcast(machines), "machine_id") \
     .groupBy("location_type").sum("amount").explain()