import sys
import os
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dq"))
from data_quality_checks import check_not_null, check_referential_integrity, check_no_duplicates, run_all_checks


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .appName("dq-tests")
        .getOrCreate()
    )


def test_check_not_null_flags_missing_required_field(spark):
    df = spark.createDataFrame(
        [("OBL-1", "CTR-1", "2026-01-01"), ("OBL-2", None, "2026-01-02")],
        ["obligation_id", "contract_id", "due_date"],
    )
    result = check_not_null(df, ["obligation_id", "contract_id", "due_date"])
    flagged = result.filter("_fail_null_check = true").collect()
    assert len(flagged) == 1
    assert flagged[0]["obligation_id"] == "OBL-2"


def test_check_referential_integrity_flags_orphans(spark):
    df = spark.createDataFrame(
        [("OBL-1", "CTR-1"), ("OBL-2", "CTR-999")],
        ["obligation_id", "contract_id"],
    )
    result = check_referential_integrity(df, "contract_id", ["CTR-1"])
    flagged = result.filter("_fail_ref_integrity = true").collect()
    assert len(flagged) == 1
    assert flagged[0]["obligation_id"] == "OBL-2"


def test_check_no_duplicates_flags_repeated_keys(spark):
    df = spark.createDataFrame(
        [("OBL-1",), ("OBL-1",), ("OBL-2",)],
        ["obligation_id"],
    )
    result = check_no_duplicates(df, "obligation_id")
    flagged = result.filter("_fail_duplicate = true").count()
    assert flagged == 2


def test_run_all_checks_splits_valid_and_quarantine(spark):
    df = spark.createDataFrame(
        [
            ("OBL-1", "CTR-1", "2026-01-01"),
            ("OBL-2", "CTR-999", "2026-01-01"),
            ("OBL-3", None, "2026-01-01"),
        ],
        ["obligation_id", "contract_id", "due_date"],
    )
    result = run_all_checks(df, valid_contract_ids=["CTR-1"])
    assert result["valid"].count() == 1
    assert result["quarantine"].count() == 2