
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.utils import (
    get_spark,
    load_config,
    read_delta,
    reset_path,
    stable_bigint_key,
    write_delta,
)


def output_path(config: dict, logical_name: str) -> str:
    output = config.get("output", {})
    value = output.get(logical_name) or output.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing output path for '{logical_name}'. "
            f"Expected output.{logical_name} or output.{logical_name}_path. "
            f"Available output keys: {list(output.keys())}"
        )
    return value


def age_band_from_dob(dob_col: str) -> F.Column:
    age_years = F.floor(F.months_between(F.current_date(), F.col(dob_col)) / F.lit(12))

    return (
        F.when(age_years >= 65, F.lit("65+"))
        .when(age_years >= 56, F.lit("56-65"))
        .when(age_years >= 46, F.lit("46-55"))
        .when(age_years >= 36, F.lit("36-45"))
        .when(age_years >= 26, F.lit("26-35"))
        .when(age_years >= 18, F.lit("18-25"))
        .otherwise(F.lit(None).cast("string"))
    )


def build_dim_customers(customers):
    return (
        customers
        .withColumn("customer_sk", stable_bigint_key("customer_id"))
        .withColumn("age_band", age_band_from_dob("dob"))
        .select(
            F.col("customer_sk").cast(T.LongType()).alias("customer_sk"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("gender").cast("string").alias("gender"),
            F.col("province").cast("string").alias("province"),
            F.col("income_band").cast("string").alias("income_band"),
            F.col("segment").cast("string").alias("segment"),
            F.col("risk_score").cast("int").alias("risk_score"),
            F.col("kyc_status").cast("string").alias("kyc_status"),
            F.col("age_band").cast("string").alias("age_band"),
        )
        .dropDuplicates(["customer_id"])
    )


def build_dim_accounts(accounts, dim_customers):
    customer_lookup = dim_customers.select("customer_id").dropDuplicates(["customer_id"])

    return (
        accounts
        .join(customer_lookup, on="customer_id", how="inner")
        .withColumn("account_sk", stable_bigint_key("account_id"))
        .withColumnRenamed("account_open_date", "open_date")
        .select(
            F.col("account_sk").cast(T.LongType()).alias("account_sk"),
            F.col("account_id").cast("string").alias("account_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("account_type").cast("string").alias("account_type"),
            F.col("account_status").cast("string").alias("account_status"),
            F.col("open_date").cast(T.DateType()).alias("open_date"),
            F.col("product_tier").cast("string").alias("product_tier"),
            F.col("digital_channel").cast("string").alias("digital_channel"),
            F.col("credit_limit").cast(T.DecimalType(18, 2)).alias("credit_limit"),
            F.col("current_balance").cast(T.DecimalType(18, 2)).alias("current_balance"),
            F.col("last_activity_date").cast(T.DateType()).alias("last_activity_date"),
        )
        .dropDuplicates(["account_id"])
    )


def build_fact_transactions(transactions, dim_accounts, dim_customers):
    account_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    customer_lookup = dim_customers.select("customer_id", "customer_sk")

    return (
        transactions
        .join(account_lookup, on="account_id", how="inner")
        .join(customer_lookup, on="customer_id", how="inner")
        .withColumn("transaction_sk", stable_bigint_key("transaction_id"))
        .select(
            F.col("transaction_sk").cast(T.LongType()).alias("transaction_sk"),
            F.col("transaction_id").cast("string").alias("transaction_id"),
            F.col("account_sk").cast(T.LongType()).alias("account_sk"),
            F.col("customer_sk").cast(T.LongType()).alias("customer_sk"),
            F.col("transaction_date").cast(T.DateType()).alias("transaction_date"),
            F.col("transaction_timestamp").cast(T.TimestampType()).alias("transaction_timestamp"),
            F.col("transaction_type").cast("string").alias("transaction_type"),
            F.col("merchant_category").cast("string").alias("merchant_category"),
            F.col("merchant_subcategory").cast("string").alias("merchant_subcategory"),
            F.col("amount").cast(T.DecimalType(18, 2)).alias("amount"),
            F.col("currency").cast("string").alias("currency"),
            F.col("channel").cast("string").alias("channel"),
            F.col("location_province").cast("string").alias("province"),
            F.col("dq_flag").cast("string").alias("dq_flag"),
            F.col("ingestion_timestamp").cast(T.TimestampType()).alias("ingestion_timestamp"),
        )
        .dropDuplicates(["transaction_id"])
    )


def run_provisioning() -> None:
    print("[3/3] Gold provisioning starting...")

    config = load_config()
    spark = get_spark(config, "gold")

    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")

    reset_path(gold_root)

    customers = read_delta(spark, f"{silver_root}/customers")
    accounts = read_delta(spark, f"{silver_root}/accounts")
    transactions = read_delta(spark, f"{silver_root}/transactions")

    dim_customers = build_dim_customers(customers)
    dim_accounts = build_dim_accounts(accounts, dim_customers)
    fact_transactions = build_fact_transactions(transactions, dim_accounts, dim_customers)

    write_delta(dim_customers, f"{gold_root}/dim_customers")
    write_delta(dim_accounts, f"{gold_root}/dim_accounts")
    write_delta(fact_transactions, f"{gold_root}/fact_transactions")

    gold_metrics = {
        "gold_layer_record_counts": {
            "fact_transactions": int(fact_transactions.count()),
            "dim_accounts": int(dim_accounts.count()),
            "dim_customers": int(dim_customers.count()),
        }
    }

    Path("/data/output").mkdir(parents=True, exist_ok=True)
    with open("/data/output/gold_metrics_stage2.json", "w", encoding="utf-8") as handle:
        json.dump(gold_metrics, handle, indent=2)

    spark.stop()

    print("[3/3] Gold provisioning completed.")


if __name__ == "__main__":
    run_provisioning()