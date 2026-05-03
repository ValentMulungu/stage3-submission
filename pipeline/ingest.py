
from __future__ import annotations

from pyspark.sql import functions as F

from pipeline.utils import get_spark, load_config, reset_path, write_delta


def output_path(config: dict, logical_name: str) -> str:
    """Return output path while supporting both config naming styles."""
    output = config.get("output", {})

    value = output.get(logical_name) or output.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing output path for '{logical_name}'. "
            f"Expected output.{logical_name} or output.{logical_name}_path. "
            f"Available output keys: {list(output.keys())}"
        )

    return value


def input_path(config: dict, logical_name: str) -> str:
    """Return input path while supporting simple input config."""
    input_cfg = config.get("input", {})

    value = input_cfg.get(logical_name) or input_cfg.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing input path for '{logical_name}'. "
            f"Expected input.{logical_name} or input.{logical_name}_path. "
            f"Available input keys: {list(input_cfg.keys())}"
        )

    return value


def add_bronze_metadata(df, source_name: str):
    """Add standard Bronze metadata fields."""
    return (
        df
        .withColumn("source_system", F.lit(source_name))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def run_ingestion() -> None:
    """Run raw-to-Bronze ingestion."""

    print("[1/3] Bronze ingestion starting...")

    config = load_config()
    spark = get_spark(config, "bronze")

    bronze_root = output_path(config, "bronze")

    accounts_path = input_path(config, "accounts")
    customers_path = input_path(config, "customers")
    transactions_path = input_path(config, "transactions")

    reset_path(bronze_root)

    accounts = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(accounts_path)
    )

    customers = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(customers_path)
    )

    transactions = (
        spark.read
        .option("multiLine", "false")
        .json(transactions_path)
    )

    accounts = add_bronze_metadata(accounts, "accounts_csv")
    customers = add_bronze_metadata(customers, "customers_csv")
    transactions = add_bronze_metadata(transactions, "transactions_jsonl")

    write_delta(accounts, f"{bronze_root}/accounts")
    write_delta(customers, f"{bronze_root}/customers")
    write_delta(transactions, f"{bronze_root}/transactions")

    spark.stop()

    print("[1/3] Bronze ingestion completed.")


if __name__ == "__main__":
    run_ingestion()