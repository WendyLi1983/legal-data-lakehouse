# Databricks notebook source
# MAGIC %run ./dq_functions

# COMMAND ----------

BRONZE_DB = "legal_lakehouse.bronze"
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_DB = "legal_lakehouse.silver"
GOLD_DB = "legal_lakehouse.gold"

raw_contracts = spark.table(f"{BRONZE_DB}.contracts_raw")

contracts_clean = (
    raw_contracts
    .withColumn(
        "effective_date_std",
        F.coalesce(
            F.try_to_date("effective_date", "yyyy-MM-dd"),
            F.try_to_date("effective_date", "MM/dd/yyyy"),
        ),
    )
    .withColumn("expiration_date_std", F.try_to_date("expiration_date", "yyyy-MM-dd"))
    .withColumn("counterparty_std", F.initcap(F.trim("counterparty")))
    .drop("effective_date", "expiration_date", "counterparty")
    .withColumnRenamed("effective_date_std", "effective_date")
    .withColumnRenamed("expiration_date_std", "expiration_date")
    .withColumnRenamed("counterparty_std", "counterparty")
)

w = Window.partitionBy("contract_id").orderBy(F.col("_ingested_at").desc())
contracts_deduped = (
    contracts_clean
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

print(f"Bronze -> Silver contracts: {raw_contracts.count()} -> {contracts_deduped.count()} "
      f"({raw_contracts.count() - contracts_deduped.count()} 条重复记录被去重)")


# COMMAND ----------

raw_obligations = spark.table(f"{BRONZE_DB}.obligations_raw")

status_map = {"open": "OPEN", "closed": "CLOSED", "overdue": "OVERDUE", "pending": "PENDING"}
map_expr = F.create_map([F.lit(x) for pair in status_map.items() for x in pair])

obligations_clean = (
    raw_obligations
    .withColumn("status_std", map_expr[F.lower(F.col("status"))])
    .withColumn("due_date_std", F.to_date("due_date", "yyyy-MM-dd"))
    .drop("status", "due_date")
    .withColumnRenamed("status_std", "status")
    .withColumnRenamed("due_date_std", "due_date")
)

# COMMAND ----------

valid_contract_ids = [row.contract_id for row in contracts_deduped.select("contract_id").distinct().collect()]
dq_result = run_all_checks(obligations_clean, valid_contract_ids)

# COMMAND ----------

print(f"Silver obligations valid: {dq_result['valid'].count()}")
print(f"Silver obligations quarantined: {dq_result['quarantine'].count()}")
dq_result["quarantine"].select("obligation_id", "contract_id", "_fail_reason").show(truncate=False)

# COMMAND ----------

contracts_deduped.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.contracts")
dq_result["valid"].write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.obligations")
dq_result["quarantine"].write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.obligations_quarantine")

print("Silver层写入完成")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM legal_lakehouse.silver.contracts;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM legal_lakehouse.silver.obligations;
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM legal_lakehouse.silver.obligations_quarantine;