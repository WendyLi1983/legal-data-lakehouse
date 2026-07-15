# Databricks notebook source
from pyspark.sql import functions as F, DataFrame


def check_not_null(df: DataFrame, columns: list) -> DataFrame:
    cond = F.lit(False)
    for c in columns:
        cond = cond | F.col(c).isNull()
    return df.withColumn("_fail_null_check", cond)


def check_referential_integrity(df: DataFrame, fk_column: str, valid_ids: list) -> DataFrame:
    return df.withColumn(
        "_fail_ref_integrity",
        ~F.col(fk_column).isin(valid_ids),
    )


def check_no_duplicates(df: DataFrame, key_column: str) -> DataFrame:
    dupe_ids = (
        df.groupBy(key_column).count().filter("count > 1").select(key_column)
    )
    return df.join(
        dupe_ids.withColumn("_fail_duplicate", F.lit(True)),
        on=key_column,
        how="left",
    ).fillna({"_fail_duplicate": False})


def run_all_checks(obligations_df: DataFrame, valid_contract_ids: list) -> dict:
    checked = check_not_null(obligations_df, ["obligation_id", "contract_id", "due_date"])
    checked = check_referential_integrity(checked, "contract_id", valid_contract_ids)
    checked = check_no_duplicates(checked, "obligation_id")

    checked = checked.withColumn(
        "_fail_reason",
        F.when(F.col("_fail_null_check"), "null_required_field")
        .when(F.col("_fail_ref_integrity"), "orphaned_contract_id")
        .when(F.col("_fail_duplicate"), "duplicate_obligation_id")
        .otherwise(F.lit(None)),
    )

    failed = checked.filter(F.col("_fail_reason").isNotNull())
    passed = checked.filter(F.col("_fail_reason").isNull()).drop(
        "_fail_null_check", "_fail_ref_integrity", "_fail_duplicate", "_fail_reason"
    )

    return {"valid": passed, "quarantine": failed}