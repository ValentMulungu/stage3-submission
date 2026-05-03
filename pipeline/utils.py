from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"
FALLBACK_CONFIG_PATH = "/app/config/pipeline_config.yaml"

DEFAULT_PARTITIONS = 4
SPARK_LOCAL_DIR = "/data/output/_spark_tmp"

DQ_CODES = [
    "ORPHANED_ACCOUNT",
    "DUPLICATE_DEDUPED",
    "TYPE_MISMATCH",
    "DATE_FORMAT",
    "CURRENCY_VARIANT",
    "NULL_REQUIRED",
]


def load_config() -> Dict:
    config_path = os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)

    if not os.path.exists(config_path):
        config_path = FALLBACK_CONFIG_PATH

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            "Pipeline configuration file not found. Checked "
            f"{os.environ.get('PIPELINE_CONFIG', DEFAULT_CONFIG_PATH)} "
            f"and {FALLBACK_CONFIG_PATH}."
        )

    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _delta_runtime_available() -> bool:
    if os.environ.get("DISABLE_DELTA", "").strip().lower() in {"1", "true", "yes"}:
        return False

    if os.environ.get("ENABLE_DELTA", "").strip().lower() in {"1", "true", "yes"}:
        return True

    for directory in ["/opt/spark/jars", "/usr/local/spark/jars", "/spark/jars", "/app/jars"]:
        root = Path(directory)
        if root.exists() and list(root.glob("*delta*.jar")):
            return True

    return False


def get_spark(config: Dict, stage_name: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")

    Path(SPARK_LOCAL_DIR).mkdir(parents=True, exist_ok=True)

    spark_config = config.get("spark", {}) or {}

    master = spark_config.get("master", "local[2]")
    app_name = f"{spark_config.get('app_name', 'nedbank-de-pipeline')}-{stage_name}"

    use_delta = _delta_runtime_available()

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.executor.memory", spark_config.get("executor_memory", "1g"))
        .config("spark.driver.memory", spark_config.get("driver_memory", "512m"))
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "2")
        .config("spark.local.dir", SPARK_LOCAL_DIR)
        .config("spark.sql.warehouse.dir", f"{SPARK_LOCAL_DIR}/spark-warehouse")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={SPARK_LOCAL_DIR} "
            f"-Dderby.system.home={SPARK_LOCAL_DIR}/derby "
            f"-Dorg.xerial.snappy.tempdir={SPARK_LOCAL_DIR}",
        )
        .config(
            "spark.executor.extraJavaOptions",
            f"-Djava.io.tmpdir={SPARK_LOCAL_DIR} "
            f"-Dorg.xerial.snappy.tempdir={SPARK_LOCAL_DIR}",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.shuffle.compress", "false")
        .config("spark.shuffle.spill.compress", "false")
        .config("spark.io.compression.codec", "lz4")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.hadoop.parquet.compression", "UNCOMPRESSED")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.filterPushdown", "false")
    )

    if use_delta:
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("pipeline.storage_format", "delta" if use_delta else "parquet")

    if use_delta:
        print(f"[Spark] Delta runtime detected for stage: {stage_name}")
    else:
        print(f"[Spark] Delta runtime not detected. Using Parquet fallback for stage: {stage_name}")

    return spark


def reset_path(path: str) -> None:
    if not path:
        return

    normalised_path = str(Path(path))

    if normalised_path.startswith(("/data/output", "/tmp")):
        shutil.rmtree(normalised_path, ignore_errors=True)


def _storage_format_from_df(df: DataFrame) -> str:
    try:
        return df.sparkSession.conf.get("pipeline.storage_format", "parquet")
    except Exception:
        return "parquet"


def _is_stage3_quarantine_path(path: str) -> bool:
    stage = os.environ.get("STAGE", "").strip()
    return stage == "3" and "/quarantine_" in path.replace("\\", "/")


def _write_stage3_quarantine_marker(path: str) -> None:
    marker_dir = Path(path)
    marker_dir.mkdir(parents=True, exist_ok=True)

    marker_payload = {
        "path": path,
        "status": "SKIPPED_HEAVY_QUARANTINE_WRITE",
        "reason": "Stage 3 reuses Stage 2 batch DQ logic, but skips heavy Silver quarantine materialisation to stay within 2GB RAM and 512MB tmpfs.",
        "note": "DQ counts are still calculated and reported in dq_report.json.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(marker_dir / "_STAGE3_SKIPPED_HEAVY_QUARANTINE_WRITE.json", "w", encoding="utf-8") as handle:
        json.dump(marker_payload, handle, indent=2)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitions: int = DEFAULT_PARTITIONS,
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    if _is_stage3_quarantine_path(path):
        _write_stage3_quarantine_marker(path)
        print(f"[Stage 3] Skipped heavy quarantine write: {path}")
        return

    storage_format = _storage_format_from_df(df)

    try:
        writer = (
            df.coalesce(2)
            .write
            .format(storage_format)
            .mode(mode)
            .option("overwriteSchema", "true")
            .option("maxRecordsPerFile", 500000)
        )

        if storage_format == "parquet":
            writer = writer.option("compression", "uncompressed")

        writer.save(path)

    except Exception as exc:
        message = str(exc)

        if storage_format == "delta" and (
            "delta" in message.lower()
            or "DeltaSparkSessionExtension" in message
            or "DeltaCatalog" in message
            or "DATA_SOURCE_NOT_FOUND" in message
        ):
            print(f"[write_delta] Delta write failed at {path}. Falling back to uncompressed Parquet.")

            (
                df.coalesce(2)
                .write
                .format("parquet")
                .mode(mode)
                .option("compression", "uncompressed")
                .option("maxRecordsPerFile", 500000)
                .save(path)
            )
        else:
            raise


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    storage_format = spark.conf.get("pipeline.storage_format", "parquet")

    try:
        return spark.read.format(storage_format).load(path)
    except Exception as exc:
        message = str(exc)

        if storage_format == "delta" and (
            "delta" in message.lower()
            or "DeltaSparkSessionExtension" in message
            or "DeltaCatalog" in message
            or "DATA_SOURCE_NOT_FOUND" in message
        ):
            print(f"[read_delta] Delta read failed at {path}. Falling back to Parquet.")
            return spark.read.format("parquet").load(path)

        raise


def stable_bigint_key(*cols: str) -> F.Column:
    prepared = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]

    return F.pmod(
        F.xxhash64(*prepared),
        F.lit(9223372036854775807),
    ).cast(T.LongType())


def parse_flexible_date(col_name: str) -> F.Column:
    c = F.trim(F.col(col_name).cast("string"))

    epoch_seconds = (
        F.when(c.rlike(r"^[0-9]{13}$"), (c.cast("double") / F.lit(1000)).cast("long"))
        .when(c.rlike(r"^[0-9]{10}$"), c.cast("long"))
    )

    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "dd/MM/yyyy"),
        F.to_date(c, "yyyy/MM/dd"),
        F.to_date(c, "yyyyMMdd"),
        F.to_date(F.from_unixtime(epoch_seconds)),
    )


def parse_timestamp(date_col: str, time_col: str) -> F.Column:
    parsed_date = F.date_format(parse_flexible_date(date_col), "yyyy-MM-dd")
    raw_time = F.trim(F.col(time_col).cast("string"))

    parsed_time = (
        F.when(raw_time.isNull() | (raw_time == ""), F.lit("00:00:00"))
        .when(raw_time.rlike(r"^[0-9]{1,2}:[0-9]{2}$"), F.concat(raw_time, F.lit(":00")))
        .otherwise(raw_time)
    )

    return F.coalesce(
        F.to_timestamp(F.concat_ws(" ", parsed_date, parsed_time), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.concat_ws(" ", parsed_date, parsed_time), "yyyy-MM-dd HH:mm"),
    )


def standardise_currency(col_name: str) -> F.Column:
    c = F.upper(F.trim(F.col(col_name).cast("string")))

    return (
        F.when(c.isin("ZAR", "R", "RAND", "RANDS", "710"), F.lit("ZAR"))
        .when(c.isin("USD", "US DOLLAR", "US DOLLARS", "$", "840"), F.lit("USD"))
        .when(c.isin("EUR", "EURO", "EUROS", "978"), F.lit("EUR"))
        .when(c.isNull() | (c == ""), F.lit(None).cast("string"))
        .otherwise(c)
    )


def dedupe_latest(
    df: DataFrame,
    key_col: str,
    order_cols: Optional[Iterable[F.Column]] = None,
) -> DataFrame:
    if order_cols is None:
        order_cols = [F.col("ingestion_timestamp").desc_nulls_last()]

    w = Window.partitionBy(key_col).orderBy(*order_cols)

    return (
        df
        .where(
            F.col(key_col).isNotNull()
            & (F.trim(F.col(key_col).cast("string")) != "")
        )
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_missing_columns(df: DataFrame, required: Dict[str, T.DataType]) -> DataFrame:
    for name, dtype in required.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))

    return df


def coerce_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

    return df.select(
        [F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields]
    )


def write_json(path: str, payload: Dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def write_dq_report(path: str, counts: Optional[Dict[str, int]] = None) -> None:
    counts = counts or {}

    issue_counts = {code: int(counts.get(code, 0)) for code in DQ_CODES}
    total_flagged = sum(issue_counts.values())

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_flagged_records": total_flagged,
            "issue_count": issue_counts,
        },
        "issues": [
            {
                "issue_code": code,
                "record_count": issue_counts[code],
                "handling_action": _handling_action_for(code),
            }
            for code in DQ_CODES
        ],
    }

    write_json(path, report)


def _handling_action_for(code: str) -> str:
    return {
        "ORPHANED_ACCOUNT": "quarantine_from_gold_fact_and_report",
        "DUPLICATE_DEDUPED": "deduplicate_keep_latest_and_report",
        "TYPE_MISMATCH": "cast_to_expected_type_and_report_if_uncastable",
        "DATE_FORMAT": "parse_supported_formats_and_report_if_unparseable",
        "CURRENCY_VARIANT": "standardise_to_ZAR",
        "NULL_REQUIRED": "reject_from_silver_and_report",
    }.get(code, "report")