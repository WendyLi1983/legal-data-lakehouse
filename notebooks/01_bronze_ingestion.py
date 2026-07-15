# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS legal_lakehouse.bronze
# MAGIC MANAGED LOCATION 's3://wendyli-databricks-s3-bucket/unity-catalog/legal_lakehouse/bronze';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS legal_lakehouse.silver
# MAGIC MANAGED LOCATION 's3://wendyli-databricks-s3-bucket/unity-catalog/legal_lakehouse/silver';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS legal_lakehouse.gold
# MAGIC MANAGED LOCATION 's3://wendyli-databricks-s3-bucket/unity-catalog/legal_lakehouse/gold';

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("raw_path", "s3://wendyli-databricks-s3-bucket/landing/legal")
RAW_PATH = dbutils.widgets.get("raw_path")
BRONZE_DB = "legal_lakehouse.bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------

# 注：原设计使用 Auto Loader（readStream + cloudFiles）做增量流式摄取，
# 实测中在当前环境下遇到 Spark Connect 状态追踪的稳定性问题（详见项目 docs/databricks_hands_on_log.md）。
# 评估后改用批量读取，语义等价，适合本demo的静态小数据集场景。
contracts_df = (
    spark.read.format("json")
    .load(f"{RAW_PATH}/contracts_raw.json")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

contracts_df.write.format("delta").mode("append").saveAsTable(f"{BRONZE_DB}.contracts_raw")

# COMMAND ----------

obligations_df = (
    spark.read.format("json")
    .load(f"{RAW_PATH}/obligations_raw.json")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

obligations_df.write.format("delta").mode("append").saveAsTable(f"{BRONZE_DB}.obligations_raw")