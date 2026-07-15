# Databricks notebook source
from pyspark.sql import functions as F

SILVER_DB = "legal_lakehouse.silver"
GOLD_DB = "legal_lakehouse.gold"

contracts = spark.table(f"{SILVER_DB}.contracts")
obligations = spark.table(f"{SILVER_DB}.obligations")

# COMMAND ----------

obligations_enriched = obligations.join(
    contracts.select("contract_id", "counterparty", "contract_type", "matter_id"),
    on="contract_id",
    how="inner",
)

compliance_summary = (
    obligations_enriched
    .withColumn(
        "risk_bucket",
        F.when(F.col("status") == "OVERDUE", "OVERDUE")
        .when(F.col("due_date") <= F.date_add(F.current_date(), 30), "DUE_SOON")
        .otherwise("ON_TRACK"),
    )
    .groupBy("counterparty", "contract_type", "risk_bucket")
    .agg(
        F.count("*").alias("obligation_count"),
        F.min("due_date").alias("earliest_due_date"),
    )
)

# COMMAND ----------

compliance_summary.orderBy(F.desc("obligation_count")).show(15, truncate=False)

# COMMAND ----------

compliance_summary.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.compliance_obligations_summary")

# COMMAND ----------

portfolio_summary = (
    contracts
    .withColumn(
        "lifecycle_status",
        F.when(F.col("expiration_date") < F.current_date(), "EXPIRED")
        .when(F.col("expiration_date") <= F.date_add(F.current_date(), 90), "EXPIRING_SOON")
        .otherwise("ACTIVE"),
    )
    .groupBy("contract_type", "lifecycle_status")
    .agg(F.count("*").alias("contract_count"))
)

portfolio_summary.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.contract_portfolio_summary")

# COMMAND ----------

print("compliance_obligations_summary:")
spark.table(f"{GOLD_DB}.compliance_obligations_summary").orderBy(F.desc("obligation_count")).show(15, truncate=False)

print("contract_portfolio_summary:")
spark.table(f"{GOLD_DB}.contract_portfolio_summary").orderBy("contract_type", "lifecycle_status").show(30, truncate=False)