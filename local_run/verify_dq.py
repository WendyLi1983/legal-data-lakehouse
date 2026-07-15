import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dq"))
from data_quality_checks import run_all_checks

spark = SparkSession.builder.appName("verify-dq").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "raw_landing")

contracts = spark.read.json(os.path.join(RAW_PATH, "contracts_raw.json"))
obligations = spark.read.json(os.path.join(RAW_PATH, "obligations_raw.json"))

valid_contract_ids = [row.contract_id for row in contracts.select("contract_id").distinct().collect()]

result = run_all_checks(obligations, valid_contract_ids)

print(f"通过校验: {result['valid'].count()} 条")
print(f"被隔离: {result['quarantine'].count()} 条")
print("\n被隔离的记录详情:")
result["quarantine"].select("obligation_id", "contract_id", "_fail_reason").show(truncate=False)

spark.stop()