

from __future__ import annotations

import json
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.utils import (
    coerce_to_schema,
    dedupe_latest,
    get_spark,
    load_config,
    parse_flexible_date,
    read_delta,
    standardise_currency,
    write_delta,
)


def output_path(config: dict, logical_name: str) -> str:
    output = config.get("output", {})
    value = output.get(logical_name) or output.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing output path for '{logical_name}'. "
            f"Available output keys: {list(output.keys())}"
        )
    return value


def dq_rules_path() -> str:
    runtime_path = "/data/config/dq_rules.yaml"
    image_path = "/app/config/dq_rules.yaml"

    if Path(runtime_path).exists():
        return runtime_path
    if Path(image_path).exists():
        return image_path

    raise FileNotFoundError(
        "dq_rules.yaml not found. Expected /data/config/dq_rules.yaml or /app/config/dq_rules.yaml"
    )


def load_dq_rules() -> dict:
    with open(dq_rules_path(), "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def normalise_columns(df: DataFrame) -> DataFrame:
    for old_name in df.columns:
        new_name = (
            old_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .replace("/", "_")
        )
        while "__" in new_name:
            new_name = new_name.replace("__", "_")

        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def rename_if_exists(df: DataFrame, source_candidates: list[str], target_name: str) -> DataFrame:
    if target_name in df.columns:
        return df

    for source_name in source_candidates:
        if source_name in df.columns:
            return df.withColumnRenamed(source_name, target_name)

    return df


def first_existing_column(df: DataFrame, candidates: list[str], default_name: str) -> str:
    for col_name in candidates:
        if col_name in df.columns:
            return col_name

    raise ValueError(
        f"None of the expected columns exist for {default_name}. "
        f"Candidates checked: {candidates}. Available columns: {df.columns}"
    )


def clean_duplicate_columns(df: DataFrame) -> DataFrame:
    seen = set()
    selected = []

    for col_name in df.columns:
        clean_name = col_name.strip()
        if clean_name not in seen:
            seen.add(clean_name)
            selected.append(F.col(col_name).alias(clean_name))

    return df.select(selected)


def standardise_transaction_type(column_name: str) -> F.Column:
    c = F.upper(F.trim(F.col(column_name).cast("string")))

    return (
        F.when(c.isin("CR", "CREDIT", "CREDITS"), F.lit("CREDIT"))
        .when(c.isin("DR", "DEBIT", "DEBITS"), F.lit("DEBIT"))
        .when(c.isin("FEE", "FEES", "CHARGE", "CHARGES"), F.lit("FEE"))
        .when(c.isin("REV", "REVERSE", "REVERSAL", "REVERSALS"), F.lit("REVERSAL"))
        .otherwise(c)
    )


def standardise_province(column_name: str) -> F.Column:
    c = F.upper(F.trim(F.regexp_replace(F.col(column_name).cast("string"), r"[\s_]+", " ")))

    return (
        F.when(c.isin("EC", "EASTERN CAPE"), F.lit("Eastern Cape"))
        .when(c.isin("FS", "FREE STATE"), F.lit("Free State"))
        .when(c.isin("GP", "GAUTENG"), F.lit("Gauteng"))
        .when(c.isin("KZN", "KWAZULU NATAL", "KWAZULU-NATAL"), F.lit("KwaZulu-Natal"))
        .when(c.isin("LP", "LIMPOPO"), F.lit("Limpopo"))
        .when(c.isin("MP", "MPUMALANGA"), F.lit("Mpumalanga"))
        .when(c.isin("NW", "NORTH WEST"), F.lit("North West"))
        .when(c.isin("NC", "NORTHERN CAPE"), F.lit("Northern Cape"))
        .when(c.isin("WC", "WESTERN CAPE"), F.lit("Western Cape"))
        .otherwise(F.trim(F.col(column_name).cast("string")))
    )


def is_non_iso_date_string(col_name: str) -> F.Column:
    raw = F.trim(F.col(col_name).cast("string"))
    parsed = parse_flexible_date(col_name)

    return (
        raw.isNotNull()
        & parsed.isNotNull()
        & (~raw.rlike(r"^\d{4}-\d{2}-\d{2}$"))
    )


def is_currency_variant(col_name: str) -> F.Column:
    raw = F.upper(F.trim(F.col(col_name).cast("string")))
    return raw.isNotNull() & (raw != F.lit("ZAR"))


def safe_decimal(col_name: str, precision: int = 18, scale: int = 2) -> F.Column:
    cleaned = F.regexp_replace(F.trim(F.col(col_name).cast("string")), ",", "")
    return cleaned.cast(T.DecimalType(precision, scale))


def build_customers_silver(customers: DataFrame) -> tuple[DataFrame, dict]:
    customers = normalise_columns(customers)

    customers = rename_if_exists(customers, ["customer_ref", "cust_id", "client_id"], "customer_id")
    customers = rename_if_exists(customers, ["date_of_birth", "birth_date"], "dob")
    customers = rename_if_exists(customers, ["province_name", "home_province", "customer_province"], "province")
    customers = rename_if_exists(customers, ["kyc_status", "risk_kyc_status"], "kyc_status")
    customers = rename_if_exists(customers, ["risk_rating"], "risk_score")

    if "customer_id" not in customers.columns:
        raise ValueError(f"customers source has no customer_id column. Columns: {customers.columns}")

    if "dob" not in customers.columns:
        customers = customers.withColumn("dob", F.lit(None).cast("string"))

    customers = customers.withColumn("dq_date_format_dob", is_non_iso_date_string("dob"))
    customers = customers.withColumn("dob", parse_flexible_date("dob"))

    customers = (
        customers
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("first_name", F.col("first_name").cast("string") if "first_name" in customers.columns else F.lit(None).cast("string"))
        .withColumn("last_name", F.col("last_name").cast("string") if "last_name" in customers.columns else F.lit(None).cast("string"))
        .withColumn("gender", F.col("gender").cast("string") if "gender" in customers.columns else F.lit(None).cast("string"))
        .withColumn("province", standardise_province("province") if "province" in customers.columns else F.lit(None).cast("string"))
        .withColumn("income_band", F.col("income_band").cast("string") if "income_band" in customers.columns else F.lit(None).cast("string"))
        .withColumn("segment", F.col("segment").cast("string") if "segment" in customers.columns else F.lit(None).cast("string"))
        .withColumn("risk_score", F.col("risk_score").cast("int") if "risk_score" in customers.columns else F.lit(None).cast("int"))
        .withColumn("kyc_status", F.col("kyc_status").cast("string") if "kyc_status" in customers.columns else F.lit(None).cast("string"))
    )

    customers = dedupe_latest(customers, "customer_id")
    customers = clean_duplicate_columns(customers)

    metrics = {
        "customers_date_format_count": int(customers.where(F.col("dq_date_format_dob")).count())
    }

    expected_schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("dob", T.DateType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("province", T.StringType(), True),
            T.StructField("income_band", T.StringType(), True),
            T.StructField("segment", T.StringType(), True),
            T.StructField("risk_score", T.IntegerType(), True),
            T.StructField("kyc_status", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
            T.StructField("dq_date_format_dob", T.BooleanType(), True),
        ]
    )

    return coerce_to_schema(customers, expected_schema), metrics


def build_accounts_silver(accounts: DataFrame, silver_root: str) -> tuple[DataFrame, dict]:
    accounts = normalise_columns(accounts)

    accounts = rename_if_exists(accounts, ["account_ref", "acct_id"], "account_id")
    accounts = rename_if_exists(accounts, ["customer_ref", "cust_id", "client_id"], "customer_id")
    accounts = rename_if_exists(accounts, ["open_date", "opened_date", "account_open_date"], "account_open_date")

    if "account_id" not in accounts.columns:
        raise ValueError(f"accounts source has no account_id column. Columns: {accounts.columns}")

    if "customer_id" not in accounts.columns:
        raise ValueError(f"accounts source has no customer_id column. Columns: {accounts.columns}")

    if "account_open_date" not in accounts.columns:
        accounts = accounts.withColumn("account_open_date", F.lit(None).cast("string"))

    if "last_activity_date" not in accounts.columns:
        accounts = accounts.withColumn("last_activity_date", F.lit(None).cast("string"))

    accounts = (
        accounts
        .withColumn("dq_null_required", F.col("account_id").isNull() | (F.trim(F.col("account_id").cast("string")) == ""))
        .withColumn("dq_date_format_open_date", is_non_iso_date_string("account_open_date"))
        .withColumn("dq_date_format_last_activity_date", is_non_iso_date_string("last_activity_date"))
    )

    accounts_null_pk = accounts.where(F.col("dq_null_required"))
    valid_accounts = accounts.where(~F.col("dq_null_required"))

    if accounts_null_pk.limit(1).count() > 0:
        write_delta(accounts_null_pk, f"{silver_root}/quarantine_accounts_null_required")

    valid_accounts = (
        valid_accounts
        .withColumn("account_open_date", parse_flexible_date("account_open_date"))
        .withColumn("last_activity_date", parse_flexible_date("last_activity_date"))
        .withColumn("account_id", F.trim(F.col("account_id").cast("string")))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("account_type", F.col("account_type").cast("string") if "account_type" in valid_accounts.columns else F.lit(None).cast("string"))
        .withColumn("account_status", F.col("account_status").cast("string") if "account_status" in valid_accounts.columns else F.lit(None).cast("string"))
        .withColumn("product_tier", F.col("product_tier").cast("string") if "product_tier" in valid_accounts.columns else F.lit(None).cast("string"))
        .withColumn("digital_channel", F.col("digital_channel").cast("string") if "digital_channel" in valid_accounts.columns else F.lit(None).cast("string"))
        .withColumn("credit_limit", safe_decimal("credit_limit") if "credit_limit" in valid_accounts.columns else F.lit(None).cast(T.DecimalType(18, 2)))
        .withColumn("current_balance", safe_decimal("current_balance") if "current_balance" in valid_accounts.columns else F.lit(None).cast(T.DecimalType(18, 2)))
    )

    valid_accounts = dedupe_latest(valid_accounts, "account_id")
    valid_accounts = clean_duplicate_columns(valid_accounts)

    metrics = {
        "accounts_null_required_count": int(accounts_null_pk.count()),
        "accounts_date_format_count": int(
            valid_accounts.where(F.col("dq_date_format_open_date") | F.col("dq_date_format_last_activity_date")).count()
        ),
    }

    expected_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("account_type", T.StringType(), True),
            T.StructField("account_status", T.StringType(), True),
            T.StructField("account_open_date", T.DateType(), True),
            T.StructField("product_tier", T.StringType(), True),
            T.StructField("digital_channel", T.StringType(), True),
            T.StructField("credit_limit", T.DecimalType(18, 2), True),
            T.StructField("current_balance", T.DecimalType(18, 2), True),
            T.StructField("last_activity_date", T.DateType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
            T.StructField("dq_date_format_open_date", T.BooleanType(), True),
            T.StructField("dq_date_format_last_activity_date", T.BooleanType(), True),
        ]
    )

    return coerce_to_schema(valid_accounts, expected_schema), metrics


def build_transactions_silver(transactions: DataFrame, valid_accounts: DataFrame, silver_root: str) -> tuple[DataFrame, dict]:
    transactions = normalise_columns(transactions)

    transactions = rename_if_exists(transactions, ["txn_id", "transaction_ref", "transaction_reference"], "transaction_id")
    transactions = rename_if_exists(transactions, ["acct_id", "account_ref"], "account_id")
    transactions = rename_if_exists(transactions, ["txn_type", "type"], "transaction_type")
    transactions = rename_if_exists(transactions, ["transaction_amount", "txn_amount", "value"], "amount")
    transactions = rename_if_exists(transactions, ["merchant_cat", "merchant_category_code"], "merchant_category")
    transactions = rename_if_exists(transactions, ["merchant_sub_category", "merchant_subcat"], "merchant_subcategory")

    if "transaction_id" not in transactions.columns:
        raise ValueError(f"transactions source has no transaction_id column. Columns: {transactions.columns}")

    if "account_id" not in transactions.columns:
        raise ValueError(f"transactions source has no account_id column. Columns: {transactions.columns}")

    if "location" in transactions.columns:
        transactions = transactions.withColumn(
            "location_province",
            F.col("location.province").cast("string"),
        )
    elif "location_province" not in transactions.columns:
        transactions = transactions.withColumn("location_province", F.lit(None).cast("string"))

    date_candidates = ["transaction_date", "txn_date", "date", "transaction_dt"]
    timestamp_candidates = ["transaction_timestamp", "txn_timestamp", "timestamp", "datetime", "transaction_datetime"]
    time_candidates = ["transaction_time", "txn_time", "time"]

    has_timestamp = any(c in transactions.columns for c in timestamp_candidates)
    has_date = any(c in transactions.columns for c in date_candidates)
    has_time = any(c in transactions.columns for c in time_candidates)

    raw_date_col = first_existing_column(transactions, date_candidates, "transaction_date") if has_date else None

    if has_timestamp:
        timestamp_col = first_existing_column(transactions, timestamp_candidates, "transaction_timestamp")
        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(F.col(timestamp_col).cast("string")),
        )
        transactions = transactions.withColumn("dq_date_format", F.lit(False))
    elif has_date and has_time:
        time_col = first_existing_column(transactions, time_candidates, "transaction_time")
        transactions = transactions.withColumn("dq_date_format", is_non_iso_date_string(raw_date_col))

        clean_date = F.date_format(parse_flexible_date(raw_date_col), "yyyy-MM-dd")
        clean_time = F.coalesce(F.trim(F.col(time_col).cast("string")), F.lit("00:00:00"))

        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(F.concat_ws(" ", clean_date, clean_time), "yyyy-MM-dd HH:mm:ss"),
        )
    elif has_date:
        transactions = transactions.withColumn("dq_date_format", is_non_iso_date_string(raw_date_col))
        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(parse_flexible_date(raw_date_col)),
        )
    else:
        transactions = transactions.withColumn("dq_date_format", F.lit(False))
        transactions = transactions.withColumn("_transaction_timestamp_clean", F.lit(None).cast(T.TimestampType()))

    for duplicate_target in ["transaction_timestamp", "transaction_date"]:
        if duplicate_target in transactions.columns:
            transactions = transactions.drop(duplicate_target)

    transactions = (
        transactions
        .withColumn("transaction_timestamp", F.col("_transaction_timestamp_clean").cast(T.TimestampType()))
        .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp")))
        .drop("_transaction_timestamp_clean")
        .withColumn("transaction_id", F.trim(F.col("transaction_id").cast("string")))
        .withColumn("account_id", F.trim(F.col("account_id").cast("string")))
        .withColumn("transaction_type", standardise_transaction_type("transaction_type") if "transaction_type" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("amount_source_text", F.trim(F.col("amount").cast("string")))
        .withColumn("amount", safe_decimal("amount"))
        .withColumn(
            "dq_type_mismatch",
            F.when(F.col("amount_source_text").isNull(), F.lit(False))
             .when(F.col("amount").isNull(), F.lit(True))
             .otherwise(F.lit(False))
        )
        .withColumn("dq_currency_variant", is_currency_variant("currency") if "currency" in transactions.columns else F.lit(False))
        .withColumn("currency", standardise_currency("currency") if "currency" in transactions.columns else F.lit("ZAR").cast("string"))
        .withColumn("merchant_category", F.col("merchant_category").cast("string") if "merchant_category" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast("string") if "merchant_subcategory" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("channel", F.col("channel").cast("string") if "channel" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("location_province", standardise_province("location_province"))
    )

    duplicate_window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").desc_nulls_last(),
        F.col("ingestion_timestamp").desc_nulls_last(),
    )

    transactions = transactions.withColumn("dup_rank", F.row_number().over(duplicate_window))
    duplicate_excluded = transactions.where(F.col("dup_rank") > 1)

    if duplicate_excluded.limit(1).count() > 0:
        duplicate_excluded = duplicate_excluded.withColumn("dq_flag", F.lit("DUPLICATE_DEDUPED"))
        write_delta(duplicate_excluded, f"{silver_root}/quarantine_transactions_duplicates")

    transactions = transactions.where(F.col("dup_rank") == 1).drop("dup_rank")

    account_lookup = valid_accounts.select("account_id").dropDuplicates(["account_id"])

    orphaned_transactions = (
        transactions.alias("t")
        .join(account_lookup.alias("a"), on="account_id", how="left_anti")
        .withColumn("dq_flag", F.lit("ORPHANED_ACCOUNT"))
    )

    if orphaned_transactions.limit(1).count() > 0:
        write_delta(orphaned_transactions, f"{silver_root}/quarantine_transactions_orphaned_account")

    valid_transactions = (
        transactions.alias("t")
        .join(account_lookup.alias("a"), on="account_id", how="inner")
    )

    valid_transactions = (
        valid_transactions
        .withColumn(
            "dq_flag",
            F.when(F.col("dq_date_format"), F.lit("DATE_FORMAT"))
             .when(F.col("dq_currency_variant"), F.lit("CURRENCY_VARIANT"))
             .when(F.col("dq_type_mismatch"), F.lit("TYPE_MISMATCH"))
             .otherwise(F.lit(None).cast("string"))
        )
        .drop("amount_source_text")
    )

    valid_transactions = clean_duplicate_columns(valid_transactions)

    valid_transactions = valid_transactions.select(
        "transaction_id",
        "account_id",
        "transaction_timestamp",
        "transaction_date",
        "transaction_type",
        "amount",
        "currency",
        "merchant_category",
        "merchant_subcategory",
        "channel",
        "location_province",
        "dq_flag",
        "ingestion_timestamp",
        "dq_date_format",
        "dq_currency_variant",
        "dq_type_mismatch",
    )

    metrics = {
        "transactions_duplicate_deduped_count": int(duplicate_excluded.count()),
        "transactions_orphaned_account_count": int(orphaned_transactions.count()),
        "transactions_date_format_count": int(valid_transactions.where(F.col("dq_date_format")).count()),
        "transactions_currency_variant_count": int(valid_transactions.where(F.col("dq_currency_variant")).count()),
        "transactions_type_mismatch_count": int(valid_transactions.where(F.col("dq_type_mismatch")).count()),
    }

    expected_schema = T.StructType(
        [
            T.StructField("transaction_id", T.StringType(), False),
            T.StructField("account_id", T.StringType(), True),
            T.StructField("transaction_timestamp", T.TimestampType(), True),
            T.StructField("transaction_date", T.DateType(), True),
            T.StructField("transaction_type", T.StringType(), True),
            T.StructField("amount", T.DecimalType(18, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("merchant_category", T.StringType(), True),
            T.StructField("merchant_subcategory", T.StringType(), True),
            T.StructField("channel", T.StringType(), True),
            T.StructField("location_province", T.StringType(), True),
            T.StructField("dq_flag", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
            T.StructField("dq_date_format", T.BooleanType(), True),
            T.StructField("dq_currency_variant", T.BooleanType(), True),
            T.StructField("dq_type_mismatch", T.BooleanType(), True),
        ]
    )

    return coerce_to_schema(valid_transactions, expected_schema), metrics


def run_transformation() -> None:
    print("[2/3] Silver transformation starting...")

    config = load_config()
    _ = load_dq_rules()
    spark = get_spark(config, "silver")

    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")

    customers_bronze = read_delta(spark, f"{bronze_root}/customers")
    accounts_bronze = read_delta(spark, f"{bronze_root}/accounts")
    transactions_bronze = read_delta(spark, f"{bronze_root}/transactions")

    customers_silver, customer_metrics = build_customers_silver(customers_bronze)
    accounts_silver, account_metrics = build_accounts_silver(accounts_bronze, silver_root)
    transactions_silver, transaction_metrics = build_transactions_silver(
        transactions_bronze,
        accounts_silver,
        silver_root,
    )

    write_delta(customers_silver, f"{silver_root}/customers")
    write_delta(accounts_silver, f"{silver_root}/accounts")
    write_delta(transactions_silver, f"{silver_root}/transactions")

    transform_metrics = {
        "stage": "2",
        "source_record_counts": {
            "customers_raw": int(customers_bronze.count()),
            "accounts_raw": int(accounts_bronze.count()),
            "transactions_raw": int(transactions_bronze.count()),
        },
        "silver_record_counts": {
            "customers": int(customers_silver.count()),
            "accounts": int(accounts_silver.count()),
            "transactions": int(transactions_silver.count()),
        },
        "dq_metrics": {
            **customer_metrics,
            **account_metrics,
            **transaction_metrics,
        },
    }

    Path("/data/output").mkdir(parents=True, exist_ok=True)
    with open("/data/output/transform_metrics_stage2.json", "w", encoding="utf-8") as handle:
        json.dump(transform_metrics, handle, indent=2)

    spark.stop()

    print("[2/3] Silver transformation completed.")


if __name__ == "__main__":
    run_transformation()