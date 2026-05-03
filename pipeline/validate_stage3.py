# pipeline/validate_stage3.py

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


CURRENT_BALANCES_COLUMNS = [
    "account_id",
    "current_balance",
    "last_transaction_timestamp",
    "updated_at",
]

RECENT_TRANSACTIONS_COLUMNS = [
    "account_id",
    "transaction_id",
    "transaction_timestamp",
    "amount",
    "transaction_type",
    "channel",
    "updated_at",
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_data_dir(explicit_data_dir: Optional[str] = None) -> Path:
    if explicit_data_dir:
        return Path(explicit_data_dir).resolve()

    local_data = project_root() / "data"

    if os.name == "nt":
        return local_data

    docker_data = Path("/data")

    if docker_data.exists():
        return docker_data

    return local_data


def add_check(checks: list[dict[str, Any]], name: str, passed: bool, details: str) -> None:
    checks.append(
        {
            "check": name,
            "passed": bool(passed),
            "details": details,
        }
    )


def load_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def count_stream_files(stream_dir: Path) -> int:
    if not stream_dir.exists():
        return 0

    return len(list(stream_dir.glob("stream_*.jsonl")))


def has_delta_log(path: Path) -> bool:
    return (path / "_delta_log").exists()


def has_parquet_files(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False

    return any(
        p.is_file() and p.suffix == ".parquet" and not p.name.startswith(".")
        for p in path.glob("*.parquet")
    )


def has_lake_files(path: Path) -> bool:
    """
    Accept either Delta output or Parquet fallback output.

    In this submission, the pipeline writes Parquet fallback locally when Delta
    runtime is not available. This validator must therefore support both.
    """
    return has_delta_log(path) or has_parquet_files(path)


def create_spark():
    """
    Create an offline-safe Spark session for validation.

    Important:
    - Do not use configure_spark_with_delta_pip().
    - Do not set spark.jars.packages.
    - Do not force Delta extensions.
    - This avoids Maven downloads and works with --network=none.
    """
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")

    spark = (
        SparkSession.builder
        .appName("NedbankStage3Validation")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.default.parallelism", "24")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")
        .config("spark.local.dir", "/tmp")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_lake_table(spark, path: Path):
    """
    Read Stage 3 output from either Delta or Parquet.

    The function first detects the folder structure:
    - If _delta_log exists, try Delta first.
    - If Delta is unavailable or the table is not Delta, fall back to Parquet.
    - If no _delta_log exists, read Parquet directly.
    """
    path_string = str(path)

    if not path.exists():
        raise FileNotFoundError(f"Output table path does not exist: {path_string}")

    if has_delta_log(path):
        try:
            return spark.read.format("delta").load(path_string)
        except Exception as delta_error:
            print(
                f"[Stage 3 Validation] Delta read failed for {path_string}. "
                f"Trying Parquet fallback. Error: {delta_error}"
            )

    return spark.read.format("parquet").load(path_string)


def validate_stage3(data_dir: Path) -> tuple[bool, dict[str, Any]]:
    from pyspark.sql import functions as F

    output_dir = data_dir / "output"
    stream_dir = data_dir / "stream"
    stream_gold_dir = output_dir / "stream_gold"

    current_balances_path = stream_gold_dir / "current_balances"
    recent_transactions_path = stream_gold_dir / "recent_transactions"
    stream_metrics_path = output_dir / "stream_metrics.json"
    validation_report_path = output_dir / "stage3_validation_report.json"

    checks: list[dict[str, Any]] = []

    print("[Stage 3 Validation] Starting validation...")
    print(f"[Stage 3 Validation] Data directory: {data_dir}")
    print(f"[Stage 3 Validation] Stream directory: {stream_dir}")
    print(f"[Stage 3 Validation] Output directory: {output_dir}")
    print(f"[Stage 3 Validation] Stream gold directory: {stream_gold_dir}")

    add_check(checks, "data_directory_exists", data_dir.exists(), str(data_dir))
    add_check(checks, "stream_directory_exists", stream_dir.exists(), str(stream_dir))
    add_check(checks, "output_directory_exists", output_dir.exists(), str(output_dir))
    add_check(checks, "stream_gold_directory_exists", stream_gold_dir.exists(), str(stream_gold_dir))

    stream_file_count = count_stream_files(stream_dir)

    add_check(
        checks,
        "stream_files_exist",
        stream_file_count > 0,
        f"Found {stream_file_count} stream_*.jsonl files",
    )

    add_check(
        checks,
        "expected_12_stream_files",
        stream_file_count == 12,
        f"Found {stream_file_count} stream_*.jsonl files",
    )

    add_check(
        checks,
        "current_balances_output_exists",
        current_balances_path.exists(),
        str(current_balances_path),
    )

    add_check(
        checks,
        "recent_transactions_output_exists",
        recent_transactions_path.exists(),
        str(recent_transactions_path),
    )

    add_check(
        checks,
        "current_balances_has_lake_files",
        has_lake_files(current_balances_path),
        f"delta_log={has_delta_log(current_balances_path)}, parquet_files={has_parquet_files(current_balances_path)}",
    )

    add_check(
        checks,
        "recent_transactions_has_lake_files",
        has_lake_files(recent_transactions_path),
        f"delta_log={has_delta_log(recent_transactions_path)}, parquet_files={has_parquet_files(recent_transactions_path)}",
    )

    metrics = load_json(stream_metrics_path)

    add_check(
        checks,
        "stream_metrics_json_exists",
        stream_metrics_path.exists(),
        str(stream_metrics_path),
    )

    add_check(
        checks,
        "stream_metrics_json_valid",
        metrics is not None,
        "stream_metrics.json parsed successfully" if metrics else "Could not parse stream_metrics.json",
    )

    spark = create_spark()

    try:
        current_balances = read_lake_table(spark, current_balances_path)
        recent_transactions = read_lake_table(spark, recent_transactions_path)

        current_balance_columns = current_balances.columns
        recent_transaction_columns = recent_transactions.columns

        add_check(
            checks,
            "current_balances_exact_schema",
            current_balance_columns == CURRENT_BALANCES_COLUMNS,
            f"actual={current_balance_columns}, expected={CURRENT_BALANCES_COLUMNS}",
        )

        add_check(
            checks,
            "recent_transactions_exact_schema",
            recent_transaction_columns == RECENT_TRANSACTIONS_COLUMNS,
            f"actual={recent_transaction_columns}, expected={RECENT_TRANSACTIONS_COLUMNS}",
        )

        current_balances_count = current_balances.count()
        recent_transactions_count = recent_transactions.count()

        add_check(
            checks,
            "current_balances_row_count_greater_than_zero",
            current_balances_count > 0,
            f"row_count={current_balances_count}",
        )

        add_check(
            checks,
            "recent_transactions_row_count_greater_than_zero",
            recent_transactions_count > 0,
            f"row_count={recent_transactions_count}",
        )

        cb_nulls = current_balances.where(
            F.col("account_id").isNull()
            | F.col("current_balance").isNull()
            | F.col("last_transaction_timestamp").isNull()
            | F.col("updated_at").isNull()
        ).count()

        rt_nulls = recent_transactions.where(
            F.col("account_id").isNull()
            | F.col("transaction_id").isNull()
            | F.col("transaction_timestamp").isNull()
            | F.col("amount").isNull()
            | F.col("transaction_type").isNull()
            | F.col("updated_at").isNull()
        ).count()

        add_check(
            checks,
            "current_balances_required_fields_not_null",
            cb_nulls == 0,
            f"null_required_rows={cb_nulls}",
        )

        add_check(
            checks,
            "recent_transactions_required_fields_not_null",
            rt_nulls == 0,
            f"null_required_rows={rt_nulls}",
        )

        cb_duplicate_accounts = (
            current_balances
            .groupBy("account_id")
            .count()
            .where(F.col("count") > 1)
            .count()
        )

        add_check(
            checks,
            "current_balances_one_row_per_account",
            cb_duplicate_accounts == 0,
            f"duplicate_account_count={cb_duplicate_accounts}",
        )

        rt_duplicate_keys = (
            recent_transactions
            .groupBy("account_id", "transaction_id")
            .count()
            .where(F.col("count") > 1)
            .count()
        )

        add_check(
            checks,
            "recent_transactions_unique_account_transaction",
            rt_duplicate_keys == 0,
            f"duplicate_key_count={rt_duplicate_keys}",
        )

        max_recent_per_account_row = (
            recent_transactions
            .groupBy("account_id")
            .count()
            .agg(F.max("count").alias("max_count"))
            .collect()[0]
        )

        max_recent_per_account = max_recent_per_account_row["max_count"] or 0

        add_check(
            checks,
            "recent_transactions_max_50_per_account",
            max_recent_per_account <= 50,
            f"max_transactions_per_account={max_recent_per_account}",
        )

        cb_sla_bad = (
            current_balances
            .withColumn(
                "latency_seconds",
                F.abs(
                    F.col("updated_at").cast("long")
                    - F.col("last_transaction_timestamp").cast("long")
                ),
            )
            .where(F.col("latency_seconds") > 300)
            .count()
        )

        rt_sla_bad = (
            recent_transactions
            .withColumn(
                "latency_seconds",
                F.abs(
                    F.col("updated_at").cast("long")
                    - F.col("transaction_timestamp").cast("long")
                ),
            )
            .where(F.col("latency_seconds") > 300)
            .count()
        )

        add_check(
            checks,
            "current_balances_sla_within_300_seconds",
            cb_sla_bad == 0,
            f"rows_over_300_seconds={cb_sla_bad}",
        )

        add_check(
            checks,
            "recent_transactions_sla_within_300_seconds",
            rt_sla_bad == 0,
            f"rows_over_300_seconds={rt_sla_bad}",
        )

        if metrics:
            add_check(
                checks,
                "metrics_status_pass",
                metrics.get("status") == "PASS",
                f"status={metrics.get('status')}",
            )

            add_check(
                checks,
                "metrics_file_count_matches_actual_stream_files",
                metrics.get("file_count") == stream_file_count,
                f"metrics={metrics.get('file_count')}, actual={stream_file_count}",
            )

            add_check(
                checks,
                "metrics_current_balances_count_matches_output",
                metrics.get("current_balances_row_count") == current_balances_count,
                f"metrics={metrics.get('current_balances_row_count')}, output={current_balances_count}",
            )

            add_check(
                checks,
                "metrics_recent_transactions_count_matches_output",
                metrics.get("recent_transactions_row_count") == recent_transactions_count,
                f"metrics={metrics.get('recent_transactions_row_count')}, output={recent_transactions_count}",
            )

    finally:
        try:
            spark.stop()
        except Exception:
            pass

    passed = all(check["passed"] for check in checks)

    report = {
        "$schema": "nedbank-de-challenge/stage3-validation-report/v2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS" if passed else "FAIL",
        "paths": {
            "data_dir": str(data_dir),
            "stream_dir": str(stream_dir),
            "output_dir": str(output_dir),
            "stream_gold_dir": str(stream_gold_dir),
            "stream_metrics": str(stream_metrics_path),
        },
        "summary": {
            "stream_file_count": stream_file_count,
            "current_balances_expected_columns": CURRENT_BALANCES_COLUMNS,
            "recent_transactions_expected_columns": RECENT_TRANSACTIONS_COLUMNS,
        },
        "checks": checks,
    }

    output_dir.mkdir(parents=True, exist_ok=True)

    with open(validation_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[Stage 3 Validation] Validation report written to: {validation_report_path}")

    if passed:
        print("[Stage 3 Validation] PASS")
    else:
        print("[Stage 3 Validation] FAIL")
        print("[Stage 3 Validation] Failed checks:")
        for check in checks:
            if not check["passed"]:
                print(f"  - {check['check']}: {check['details']}")

    return passed, report


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate official Nedbank Stage 3 stream outputs.")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Optional data directory. Example: C:\\Nedbank-Challenge\\stage3-submmision-workings\\data",
    )

    args = parser.parse_args()

    data_dir = resolve_data_dir(args.data_dir)

    passed, _ = validate_stage3(data_dir)

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()