# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise 3: Install Libraries Notebook-Scoped
# MAGIC
# MAGIC At HealthBridge Analytics, different teams sometimes need different versions of the same library. Rather than creating a new cluster for every variation, you can install libraries **notebook-scoped** — they apply only to the current notebook session and do not affect other users or notebooks on the same compute resource.
# MAGIC
# MAGIC In this exercise, you install the `faker` library notebook-scoped and verify it works correctly.
# MAGIC
# MAGIC [faker](https://faker.readthedocs.io/) is a Python library that generates realistic fake data — such as names, addresses, phone numbers, dates, and more — for use in testing, prototyping, and seeding databases. It supports many locales and data categories, making it easy to produce large volumes of plausible synthetic data without using real personal information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1 — Install the `faker` library notebook-scoped
# MAGIC
# MAGIC Your team relies on `faker` to generate synthetic patient records for pipeline testing. Install it as a notebook-scoped library so the package is available to this notebook only and doesn't affect other users sharing the same compute.
# MAGIC
# MAGIC > 🤖 **Genie Code tip:** Open Genie Code panel (click the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/genie-code.svg) icon on the cell) and use the prompt below to get started:
# MAGIC > *"How do I install a Python package notebook-scoped in Databricks using a magic command?"*
# MAGIC
# MAGIC **Hint:** Use a `%pip` magic command to install the package. Pinning an exact version (e.g., `faker==40.8.0`) is a good practice for reproducibility.

# COMMAND ----------

# TODO: Install faker==40.8.0 notebook-scoped
!pip install faker==40.8.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2 — Verify the installation
# MAGIC
# MAGIC Before using `faker` in a real pipeline, confirm it was installed correctly by:
# MAGIC
# MAGIC 1. Importing the `Faker` class from the `faker` library.
# MAGIC 2. Creating a `Faker` instance.
# MAGIC 3. Printing a randomly generated **full name** and **date of birth**.
# MAGIC
# MAGIC > 🤖 **Genie Code tip:**
# MAGIC > *"Show me a Python example of generating a random name and date of birth using the Faker library."*

# COMMAND ----------

# Import Faker, create an instance, and print a generated name and date of birth
from faker import Faker

fake = Faker()

print("Name:", fake.name())
print("Date of Birth:", fake.date_of_birth())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Generate and Analyze Synthetic Patient Data
# MAGIC
# MAGIC Now that `faker` is installed, put it to work. Your team needs a synthetic dataset of patient admission records to test a new data pipeline. The data must look realistic enough to validate transformations and aggregations, but must contain **no real patient information** to comply with HIPAA policies.
# MAGIC
# MAGIC In this exercise, you generate a synthetic dataset and run a basic analysis on it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1 — Generate a synthetic patient admissions DataFrame
# MAGIC
# MAGIC Use `faker` and the Spark session (`spark`) to create a **Spark DataFrame** with **100 synthetic patient admission records**. Each record must include:
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `patient_id` | Integer, 1 to 100 |
# MAGIC | `full_name` | Randomly generated full name |
# MAGIC | `date_of_birth` | Random date between `1940-01-01` and `2005-12-31`, as a string |
# MAGIC | `admission_date` | Random date between `2023-01-01` and `2025-12-31`, as a string |
# MAGIC | `diagnosis_code` | One of: `I21`, `J18`, `E11`, `K80`, `N39` (randomly chosen) |
# MAGIC
# MAGIC Display the first 10 rows of the resulting DataFrame.
# MAGIC
# MAGIC > 🤖 **Genie Code tip:**
# MAGIC > *"How do I generate a list of Python dictionaries using the Faker library, then create a PySpark DataFrame from that list?"*
# MAGIC >
# MAGIC > **Hint:** Use `faker.date_of_birth()` with `minimum_age` and `maximum_age` parameters, or `faker.date_between()` with `start_date` and `end_date`. Use `random.choice()` for the diagnosis code.

# COMMAND ----------

# MAGIC %pip install faker==40.8.0
# MAGIC # Generate 100 synthetic patient records and create a Spark DataFrame
# MAGIC from faker import Faker
# MAGIC import random
# MAGIC from datetime import date
# MAGIC
# MAGIC fake = Faker()
# MAGIC Faker.seed(42)
# MAGIC random.seed(42)
# MAGIC
# MAGIC diagnosis_codes = ["I21", "J18", "E11", "K80", "N39"]
# MAGIC
# MAGIC records = [
# MAGIC     {
# MAGIC         "patient_id": i,
# MAGIC         "full_name": fake.name(),
# MAGIC         "date_of_birth": str(fake.date_between(start_date=date(1940, 1, 1), end_date=date(2005, 12, 31))),
# MAGIC         "admission_date": str(fake.date_between(start_date=date(2023, 1, 1), end_date=date(2025, 12, 31))),
# MAGIC         "diagnosis_code": random.choice(diagnosis_codes),
# MAGIC     }
# MAGIC     for i in range(1, 101)
# MAGIC ]
# MAGIC
# MAGIC patients_df = spark.createDataFrame(records)
# MAGIC
# MAGIC # Display the first 10 rows
# MAGIC display(patients_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2 — Analyze admissions by diagnosis code
# MAGIC
# MAGIC The clinical informatics team wants to know which diagnosis codes appear most frequently in the admission data. Using the Spark DataFrame you created in Task 4.1:
# MAGIC
# MAGIC 1. Count the number of admissions per `diagnosis_code`.
# MAGIC 2. Order the results from **most to least** admissions.
# MAGIC 3. Display the result.
# MAGIC
# MAGIC > 🤖 **Genie Code tip:**
# MAGIC > *"How do I group by a column, count occurrences, and sort the result in descending order using PySpark DataFrame API?"*

# COMMAND ----------

# Count admissions per diagnosis_code, ordered from most to least
from pyspark.sql import functions as F

admissions_by_diagnosis = patients_df.groupBy("diagnosis_code").count().orderBy(F.col("count").desc())
display(admissions_by_diagnosis)

# COMMAND ----------

# DBTITLE 1,Spark UI samples overview
# MAGIC %md
# MAGIC ## Spark UI Sample Code
# MAGIC
# MAGIC The cells below generate workloads you can inspect in the **Spark UI** (compute resource > **Spark UI**, or the **See performance** / **Spark Jobs** link under a cell's output).
# MAGIC
# MAGIC | Sample | Where to look in the Spark UI |
# MAGIC |---|---|
# MAGIC | 1. Job descriptions and job groups | **Jobs** tab |
# MAGIC | 2. Narrow vs. wide transformations (shuffle) | **Jobs** > job > **DAG Visualization**, **Stages** tab |
# MAGIC | 3. Partitions and tasks | **Stages** > stage > task list / **Event Timeline** |
# MAGIC | 4. Caching | **Storage** tab |
# MAGIC | 5. Broadcast vs. sort-merge join | **SQL / DataFrame** tab (query plan) |
# MAGIC | 6. Data skew | **Stages** > **Summary Metrics** (max vs. median task duration) |
# MAGIC | 7. Adaptive Query Execution (AQE) | **SQL / DataFrame** tab (`AdaptiveSparkPlan`, `AQEShuffleRead`) |
# MAGIC | 8. Shuffle spill | **Stages** > *Spill (Memory)* / *Spill (Disk)* columns |
# MAGIC | 9. Configuration and executors | **Environment** and **Executors** tabs |
# MAGIC
# MAGIC > These samples use `spark.sparkContext` for job descriptions/groups and the UI URL. That works on classic compute with **Dedicated** access mode; on Standard (shared) access mode or serverless, skip those lines.

# COMMAND ----------

# DBTITLE 1,1. Job descriptions and job groups (Jobs tab)
# Sample 1: Label jobs so they are easy to find in the Spark UI > Jobs tab
spark.addTag("healthbridge_demo")
spark.addTag("sample_1_simple_count")

df = spark.range(0, 10_000_000)
print("Row count:", df.count())  # Action -> triggers a job

spark.removeTag("sample_1_simple_count")  # Reset the tag for later jobs

# COMMAND ----------

# DBTITLE 1,2. Narrow vs wide transformations (DAG and Stages)
# Sample 2: Narrow transformations (filter, withColumn) stay in one stage;
# a wide transformation (groupBy) introduces a shuffle and a new stage.
from pyspark.sql import functions as F

spark.conf.set("spark.job.description", "Sample 2 - narrow vs wide transformations")

narrow_df = (
    spark.range(0, 20_000_000)
    .withColumn("bucket", F.col("id") % 100)   # narrow
    .filter(F.col("id") % 2 == 0)              # narrow
)

wide_df = narrow_df.groupBy("bucket").agg(F.count("*").alias("cnt"))  # wide -> shuffle

display(wide_df.orderBy("bucket"))
# In the UI: open the job > DAG Visualization and look for the Exchange (shuffle) boundary.
# In the Stages tab, compare 'Shuffle Write' and 'Shuffle Read' between the stages.

# COMMAND ----------

# DBTITLE 1,3. Partitions and tasks (Stages and Event Timeline)
# Sample 3: The number of partitions determines the number of tasks in a stage
from pyspark.sql import functions as F

spark.conf.set("spark.job.description", "Sample 3 - partitions and tasks")

base_df = spark.range(0, 10_000_000)
print("Default partitions:", base_df.select(F.spark_partition_id()).distinct().count())

few_parts = base_df.repartition(4)
many_parts = base_df.repartition(64)

few_parts.selectExpr("sum(id)").collect()   # Stage with 4 tasks
many_parts.selectExpr("sum(id)").collect()  # Stage with 64 tasks

# In the UI: Stages tab > compare the 'Tasks' column and open each stage's Event Timeline.
# To open the Spark UI:
#   1. Click the cluster/compute name in the top-right of the notebook.
#   2. Select "Spark UI" (or go to Compute > your cluster > Spark UI).
#   3. Navigate to the **Stages** tab.
#   4. Compare the two stages: one should show 4 tasks, the other 64 tasks.
#   5. Click into each stage and open the **Event Timeline** to see task parallelism.

# COMMAND ----------

# DBTITLE 1,4. Caching (Storage tab)
# Sample 4: Cached data appears in the Spark UI > Storage tab
spark.conf.set("spark.job.description", "Sample 4 - caching")

cached_df = spark.range(0, 5_000_000).withColumn("value", F.rand(seed=42))
cached_df.cache()
cached_df.count()  # First action materializes the cache
cached_df.count()  # Second action reads from cache (check the faster job duration)

# In the UI: Storage tab shows the cached RDD, its storage level, size in memory and fraction cached.
# Free the memory when done:
cached_df.unpersist()

# COMMAND ----------

# DBTITLE 1,5. Broadcast vs sort-merge join (SQL / DataFrame tab)
# Sample 5: Compare join strategies in the Spark UI > SQL / DataFrame tab
spark.conf.set("spark.job.description", "Sample 5 - join strategies")

facts = spark.range(0, 10_000_000).withColumn("dim_id", F.col("id") % 1000)
dims = spark.range(0, 1000).withColumnRenamed("id", "dim_id").withColumn("label", F.concat(F.lit("dim_"), F.col("dim_id")))

# Broadcast hash join: small table is sent to every executor, no shuffle of the large table
broadcast_join = facts.join(F.broadcast(dims), "dim_id")
broadcast_join.explain()
broadcast_join.count()

# Sort-merge join: hint forces both sides to be shuffled and sorted
smj_join = facts.join(dims.hint("merge"), "dim_id")
smj_join.explain()
smj_join.count()

# In the UI: SQL / DataFrame tab > open each query and compare BroadcastHashJoin vs SortMergeJoin nodes.

# COMMAND ----------

# DBTITLE 1,6. Data skew (Stages summary metrics)
# Sample 6: Skewed keys create a few very long tasks
spark.conf.set("spark.job.description", "Sample 6 - data skew")

# Disable AQE skew handling so the skew is clearly visible in the UI
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

skewed = spark.range(0, 20_000_000).withColumn(
    "key", F.when(F.col("id") % 100 < 90, F.lit(0)).otherwise(F.col("id") % 50)  # ~90% of rows share key 0
)
keys = spark.range(0, 50).withColumnRenamed("id", "key")

skewed.join(keys.hint("merge"), "key").groupBy("key").count().collect()

# In the UI: Stages tab > open the join stage > Summary Metrics.
# A large gap between the Median and Max task duration / shuffle read size indicates skew.

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # Restore default

# COMMAND ----------

# DBTITLE 1,7. Adaptive Query Execution (SQL / DataFrame tab)
# Sample 7: AQE coalesces shuffle partitions at runtime
spark.conf.set("spark.job.description", "Sample 7 - AQE")

agg_df = spark.range(0, 10_000_000).groupBy((F.col("id") % 10).alias("k")).count()

spark.conf.set("spark.sql.adaptive.enabled", "false")
agg_df.collect()  # Uses the full spark.sql.shuffle.partitions number of tasks

spark.conf.set("spark.sql.adaptive.enabled", "true")
agg_df.collect()  # AQE coalesces small partitions into fewer tasks

print("spark.sql.shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))
# In the UI: SQL / DataFrame tab > compare both queries. The AQE run shows AdaptiveSparkPlan
# and an AQEShuffleRead node with 'coalesced' partitions, and fewer tasks in the Stages tab.

# COMMAND ----------

# DBTITLE 1,8. Shuffle spill (Stages tab)
# Sample 8: Few, large shuffle partitions can cause spill to disk
spark.conf.set("spark.job.description", "Sample 8 - shuffle spill")

original_partitions = spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "2")  # Deliberately too few partitions

wide_rows = spark.range(0, 30_000_000).withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
wide_rows.orderBy("payload").write.format("noop").mode("overwrite").save()  # Runs the full sort without storing output

# In the UI: Stages tab > look for 'Spill (Memory)' and 'Spill (Disk)' columns in the sort stage.

# Restore settings
spark.conf.set("spark.sql.shuffle.partitions", original_partitions)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# DBTITLE 1,9. Configuration and executors (Environment and Executors tabs)
# Sample 9: Values shown in the Environment and Executors tabs
from pyspark.sql import functions as F

spark.conf.unset("spark.job.description")
spark.conf.unset("spark.job.group.id")  # Clear the job group

print("Spark version:       ", spark.version)
print("Default parallelism: ", spark.range(0, 1000).select(F.spark_partition_id()).distinct().count())  # Roughly total executor cores
print("Shuffle partitions:  ", spark.conf.get("spark.sql.shuffle.partitions"))
print("AQE enabled:         ", spark.conf.get("spark.sql.adaptive.enabled"))
print("Spark UI URL:        ", f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/#setting/clusters/{spark.conf.get('spark.databricks.clusterUsageTags.clusterId')}/sparkUi")

# In the UI: Environment tab lists all Spark properties; Executors tab shows cores,
# memory, task counts, GC time and shuffle read/write per executor.

# COMMAND ----------

