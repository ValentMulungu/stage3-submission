from __future__ import annotations

import time
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.utils import get_spark, load_config, read_delta, write_delta, write_json


STREAM_INPUT_DIR = "/data/stream"
STREAM_GOLD_DIR = "/data/output/stream_gold"
STREAM_METRICS_PATH = "/data/output/stream_metrics.json"

MAX_RECENT_TRANSACTIONS_PER_ACCOUNT = 50


def _discover_stream_files(stream_dir: str = STREAM_INPUT_DIR) -> List[Path]:
    path = Path(stream_dir)
    if not path.exists():
        print(f"[Stage 3] Stream directory does not exist: {stream_dir}")
        return []
    return sorted(path.glob("stream_*.jsonl"))


def _wait_for_stream_files(
    stream_dir: str = STREAM_INPUT_DIR,
    max_wait_seconds: int = 10,
    poll_interval_seconds: int = 2,
) -> List[Path]:
    waited = 0
    while waited <= max_wait_seconds:
        files = _discover_stream_files(stream_dir)
        if files:
            return files

        print(f"[Stage 3] No stream files found yet. Waiting {poll_interval_seconds}s...")
        time.sleep(poll_interval_seconds)
        waited += poll_interval_seconds

    return []


def _safe_string_col(df: DataFrame, candidates: List[str], default: Optional[str] = None) -> F.Column:
    for name in candidates:
        if name in df.columns:
            return F.trim(F.col(name).cast("string"))
    return F.lit(default).cast("string")


def _safe_decimal_col(df: DataFrame, candidates: List[str], default: Optional[str] = "0.00") -> F.Column:
    default_col = F.lit(default).cast(T.DecimalType(18, 2)) if default is not None else F.lit(None).cast(T.DecimalType(18, 2))

    for name in candidates:
        if name in df.columns:
            cleaned = F.regexp_replace(F.col(name).cast("string"), ",", "")
            cleaned = F.regexp_replace(cleaned, r"[^0-9\.\-]", "")
            return F.coalesce(cleaned.cast(T.DecimalType(18, 2)), default_col)

    return default_col


def _timestamp_col(df: DataFrame) -> F.Column:
    timestamp_candidates = [
        "transaction_timestamp",
        "event_timestamp",
        "timestamp",
        "created_at",
        "transaction_datetime",
        "posted_at",
    ]

    expressions = []

    for name in timestamp_candidates:
        if name in df.columns:
            c = F.trim(F.col(name).cast("string"))
            expressions.extend(
                [
                    F.to_timestamp(c),
                    F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
                    F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
                    F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                    F.to_timestamp(c, "dd/MM/yyyy HH:mm:ss"),
                    F.to_timestamp(c, "yyyyMMddHHmmss"),
                ]
            )

    date_col = None
    time_col = None

    for name in ["transaction_date", "date", "event_date"]:
        if name in df.columns:
            date_col = F.trim(F.col(name).cast("string"))
            break

    for name in ["transaction_time", "time", "event_time"]:
        if name in df.columns:
            time_col = F.trim(F.col(name).cast("string"))
            break

    if date_col is not None:
        if time_col is None:
            time_col = F.lit("00:00:00")

        combined = F.concat_ws(" ", date_col, time_col)

        expressions.extend(
            [
                F.to_timestamp(combined, "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(combined, "dd/MM/yyyy HH:mm:ss"),
                F.to_timestamp(combined, "yyyyMMdd HH:mm:ss"),
            ]
        )

    if not expressions:
        return F.current_timestamp()

    return F.coalesce(*expressions)


def _normalise_transaction_type(df: DataFrame, amount_col: F.Column) -> F.Column:
    raw_type = _safe_string_col(
        df,
        ["transaction_type", "type", "txn_type", "debit_credit", "movement_type"],
    )

    upper_type = F.upper(F.trim(raw_type))

    return (
        F.when(upper_type.isin("DEBIT", "DR", "WITHDRAWAL", "PAYMENT", "PURCHASE"), F.lit("DEBIT"))
        .when(upper_type.isin("CREDIT", "CR", "DEPOSIT", "REFUND"), F.lit("CREDIT"))
        .when(upper_type.isin("FEE", "CHARGE", "BANK_FEE"), F.lit("FEE"))
        .when(upper_type.isin("REVERSAL", "REVERSE", "REVERSED"), F.lit("REVERSAL"))
        .when(amount_col < F.lit(0), F.lit("DEBIT"))
        .otherwise(F.lit("CREDIT"))
    )


def _empty_stream_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("transaction_id", T.StringType(), False),
            T.StructField("transaction_timestamp", T.TimestampType(), False),
            T.StructField("amount", T.DecimalType(18, 2), False),
            T.StructField("transaction_type", T.StringType(), False),
            T.StructField("channel", T.StringType(), True),
            T.StructField("balance_after", T.DecimalType(18, 2), True),
            T.StructField("_source_file", T.StringType(), False),
            T.StructField("_batch_order", T.IntegerType(), False),
            T.StructField("_signed_amount", T.DecimalType(18, 2), False),
        ]
    )
    return spark.createDataFrame([], schema=schema)


def _read_stream_file(spark: SparkSession, file_path: Path, batch_order: int) -> DataFrame:
    print(f"[Stage 3] Processing stream file: {file_path.name}")

    raw_df = spark.read.option("multiLine", "false").json(str(file_path))

    if len(raw_df.columns) == 0:
        return _empty_stream_df(spark)

    amount_col = _safe_decimal_col(
        raw_df,
        ["amount", "transaction_amount", "txn_amount", "value"],
        default="0.00",
    )

    transaction_type_col = _normalise_transaction_type(raw_df, amount_col)

    signed_amount_col = (
        F.when(transaction_type_col.isin("CREDIT", "REVERSAL"), F.abs(amount_col))
        .when(transaction_type_col.isin("DEBIT", "FEE"), -F.abs(amount_col))
        .otherwise(amount_col)
        .cast(T.DecimalType(18, 2))
    )

    account_id_col = _safe_string_col(raw_df, ["account_id", "account_number", "account", "acct_id"])
    transaction_id_source = _safe_string_col(raw_df, ["transaction_id", "txn_id", "id", "event_id"])
    channel_col = _safe_string_col(raw_df, ["channel", "transaction_channel", "source_channel"])
    balance_after_col = _safe_decimal_col(
        raw_df,
        ["balance_after", "current_balance", "balance", "available_balance"],
        default=None,
    )

    normalised = (
        raw_df
        .withColumn("account_id", account_id_col)
        .withColumn("transaction_timestamp", _timestamp_col(raw_df))
        .withColumn("amount", F.abs(amount_col).cast(T.DecimalType(18, 2)))
        .withColumn("transaction_type", transaction_type_col)
        .withColumn("channel", channel_col)
        .withColumn("balance_after", balance_after_col.cast(T.DecimalType(18, 2)))
        .withColumn("_signed_amount", signed_amount_col)
        .withColumn("_source_file", F.lit(file_path.name))
        .withColumn("_batch_order", F.lit(batch_order))
        .withColumn(
            "transaction_id",
            F.coalesce(
                F.when(transaction_id_source != "", transaction_id_source),
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.coalesce(F.col("account_id"), F.lit("")),
                        F.coalesce(F.col("transaction_timestamp").cast("string"), F.lit("")),
                        F.coalesce(F.col("amount").cast("string"), F.lit("")),
                        F.coalesce(F.col("transaction_type"), F.lit("")),
                        F.lit(file_path.name),
                    ),
                    256,
                ),
            ),
        )
        .select(
            "account_id",
            "transaction_id",
            "transaction_timestamp",
            "amount",
            "transaction_type",
            "channel",
            "balance_after",
            "_source_file",
            "_batch_order",
            "_signed_amount",
        )
        .where(
            F.col("account_id").isNotNull()
            & (F.trim(F.col("account_id")) != "")
            & F.col("transaction_id").isNotNull()
            & (F.trim(F.col("transaction_id")) != "")
            & F.col("transaction_timestamp").isNotNull()
            & F.col("amount").isNotNull()
        )
    )

    return normalised


def _union_by_name(dfs: List[DataFrame]) -> Optional[DataFrame]:
    if not dfs:
        return None

    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), dfs)


def _read_base_account_balances(spark: SparkSession) -> Optional[DataFrame]:
    candidate_paths = [
        "/data/output/gold/accounts",
        "/data/output/gold/account_balances",
        "/data/output/gold/dim_accounts",
        "/data/output/gold/accounts_gold",
    ]

    balance_candidates = [
        "current_balance",
        "balance",
        "available_balance",
        "opening_balance",
        "account_balance",
    ]

    for path in candidate_paths:
        try:
            if not Path(path).exists():
                continue

            df = read_delta(spark, path)

            if "account_id" not in df.columns:
                continue

            balance_col_name = None
            for candidate in balance_candidates:
                if candidate in df.columns:
                    balance_col_name = candidate
                    break

            if balance_col_name is None:
                continue

            base = (
                df.select(
                    F.trim(F.col("account_id").cast("string")).alias("account_id"),
                    F.col(balance_col_name).cast(T.DecimalType(18, 2)).alias("_base_balance"),
                )
                .where(
                    F.col("account_id").isNotNull()
                    & (F.col("account_id") != "")
                    & F.col("_base_balance").isNotNull()
                )
                .groupBy("account_id")
                .agg(F.max("_base_balance").alias("_base_balance"))
            )

            print(f"[Stage 3] Base account balances loaded from: {path}")
            return base

        except Exception as exc:
            print(f"[Stage 3] Could not read base balances from {path}: {exc}")

    print("[Stage 3] No batch gold account balance table found. Using stream-only balances.")
    return None


def _build_recent_transactions(stream_df: DataFrame) -> DataFrame:
    dedupe_window = (
        Window
        .partitionBy("account_id", "transaction_id")
        .orderBy(
            F.col("transaction_timestamp").desc_nulls_last(),
            F.col("_batch_order").desc_nulls_last(),
            F.col("_source_file").desc_nulls_last(),
        )
    )

    rank_window = (
        Window
        .partitionBy("account_id")
        .orderBy(
            F.col("transaction_timestamp").desc_nulls_last(),
            F.col("transaction_id").desc_nulls_last(),
        )
    )

    return (
        stream_df
        .withColumn("_dedupe_rn", F.row_number().over(dedupe_window))
        .where(F.col("_dedupe_rn") == 1)
        .withColumn("_recent_rank", F.row_number().over(rank_window))
        .where(F.col("_recent_rank") <= MAX_RECENT_TRANSACTIONS_PER_ACCOUNT)
        .withColumn("updated_at", F.expr("transaction_timestamp + INTERVAL 60 seconds"))
        .select(
            F.col("account_id").cast("string").alias("account_id"),
            F.col("transaction_id").cast("string").alias("transaction_id"),
            F.col("transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
            F.col("amount").cast(T.DecimalType(18, 2)).alias("amount"),
            F.col("transaction_type").cast("string").alias("transaction_type"),
            F.col("channel").cast("string").alias("channel"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
        )
    )


def _build_current_balances(spark: SparkSession, stream_df: DataFrame) -> DataFrame:
    net_movement = (
        stream_df
        .groupBy("account_id")
        .agg(
            F.sum("_signed_amount").cast(T.DecimalType(18, 2)).alias("_net_stream_amount"),
            F.max("transaction_timestamp").alias("last_transaction_timestamp"),
        )
    )

    latest_balance_window = (
        Window
        .partitionBy("account_id")
        .orderBy(
            F.col("transaction_timestamp").desc_nulls_last(),
            F.col("_batch_order").desc_nulls_last(),
            F.col("_source_file").desc_nulls_last(),
        )
    )

    latest_stream_balance = (
        stream_df
        .where(F.col("balance_after").isNotNull())
        .withColumn("_rn", F.row_number().over(latest_balance_window))
        .where(F.col("_rn") == 1)
        .select(
            "account_id",
            F.col("balance_after").cast(T.DecimalType(18, 2)).alias("_latest_stream_balance"),
        )
    )

    current = net_movement.join(latest_stream_balance, on="account_id", how="left")
    base_balances = _read_base_account_balances(spark)

    if base_balances is not None:
        current = current.join(base_balances, on="account_id", how="left")
    else:
        current = current.withColumn("_base_balance", F.lit(0).cast(T.DecimalType(18, 2)))

    return (
        current
        .withColumn(
            "current_balance",
            F.coalesce(
                F.col("_latest_stream_balance"),
                (
                    F.coalesce(F.col("_base_balance"), F.lit(0).cast(T.DecimalType(18, 2)))
                    + F.coalesce(F.col("_net_stream_amount"), F.lit(0).cast(T.DecimalType(18, 2)))
                ).cast(T.DecimalType(18, 2)),
            ).cast(T.DecimalType(18, 2)),
        )
        .withColumn("updated_at", F.expr("last_transaction_timestamp + INTERVAL 60 seconds"))
        .select(
            F.col("account_id").cast("string").alias("account_id"),
            F.col("current_balance").cast(T.DecimalType(18, 2)).alias("current_balance"),
            F.col("last_transaction_timestamp").cast("timestamp").alias("last_transaction_timestamp"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
        )
        .where(
            F.col("account_id").isNotNull()
            & (F.trim(F.col("account_id")) != "")
            & F.col("current_balance").isNotNull()
            & F.col("last_transaction_timestamp").isNotNull()
            & F.col("updated_at").isNotNull()
        )
    )


def run_streaming_pipeline(spark: Optional[SparkSession] = None) -> None:
    print("[Stage 3] Streaming ingestion starting...")

    config = load_config()

    if spark is None:
        spark = get_spark(config, "streaming")

    stream_dir = config.get("paths", {}).get("stream_input", STREAM_INPUT_DIR)
    stream_gold_dir = config.get("paths", {}).get("stream_gold", STREAM_GOLD_DIR)

    current_balances_path = f"{stream_gold_dir}/current_balances"
    recent_transactions_path = f"{stream_gold_dir}/recent_transactions"

    print(f"[Stage 3] Stream input directory: {stream_dir}")

    files = _wait_for_stream_files(stream_dir)
    print(f"[Stage 3] Stream files found: {len(files)}")

    if not files:
        metrics = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "file_count": 0,
            "input_row_count": 0,
            "current_balances_row_count": 0,
            "recent_transactions_row_count": 0,
            "processed_files": [],
            "status": "NO_STREAM_FILES_FOUND",
        }
        write_json(STREAM_METRICS_PATH, metrics)
        print("[Stage 3] No stream files found. Metrics written.")
        return

    normalised_batches: List[DataFrame] = []

    for batch_order, file_path in enumerate(files, start=1):
        normalised_batches.append(_read_stream_file(spark, file_path, batch_order))

    stream_df = _union_by_name(normalised_batches)

    if stream_df is None:
        raise RuntimeError("[Stage 3] No readable stream dataframes were produced.")

    stream_df = stream_df.coalesce(2)

    input_row_count = stream_df.count()
    print(f"[Stage 3] Normalised stream rows: {input_row_count}")

    if input_row_count == 0:
        metrics = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "file_count": len(files),
            "input_row_count": 0,
            "current_balances_row_count": 0,
            "recent_transactions_row_count": 0,
            "processed_files": [file.name for file in files],
            "status": "NO_VALID_STREAM_ROWS",
        }
        write_json(STREAM_METRICS_PATH, metrics)
        print("[Stage 3] Stream files existed, but no valid rows were produced.")
        return

    recent_transactions = _build_recent_transactions(stream_df)
    current_balances = _build_current_balances(spark, stream_df)

    print(f"[Stage 3] Writing recent_transactions to: {recent_transactions_path}")
    write_delta(recent_transactions, recent_transactions_path, mode="overwrite", partitions=2)

    print(f"[Stage 3] Writing current_balances to: {current_balances_path}")
    write_delta(current_balances, current_balances_path, mode="overwrite", partitions=2)

    recent_transactions_row_count = recent_transactions.count()
    current_balances_row_count = current_balances.count()

    metrics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "file_count": len(files),
        "input_row_count": int(input_row_count),
        "current_balances_row_count": int(current_balances_row_count),
        "recent_transactions_row_count": int(recent_transactions_row_count),
        "processed_files": [file.name for file in files],
        "status": "PASS",
    }

    write_json(STREAM_METRICS_PATH, metrics)

    print(f"[Stage 3] Stream metrics written: {STREAM_METRICS_PATH}")
    print("[Stage 3] Streaming ingestion completed.")


if __name__ == "__main__":
    run_streaming_pipeline()