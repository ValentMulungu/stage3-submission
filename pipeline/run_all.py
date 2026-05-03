from __future__ import annotations

import gc
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

os.environ["STAGE"] = "3"

from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_streaming_pipeline
from pipeline.transform import run_transformation


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def pick_existing_path(paths: list[str]) -> str:
    for path in paths:
        if Path(path).exists():
            return path
    raise FileNotFoundError(f"None of these paths exist: {paths}")


def get_rule_value(dq_rules: dict[str, Any], rule_code: str, field_name: str, fallback: str) -> str:
    rule = dq_rules.get(rule_code, {}) or {}
    value = rule.get(field_name)
    return fallback if value is None or str(value).strip() == "" else str(value)


def stop_spark_session(stage_name: str) -> None:
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession

        active_session = SparkSession.getActiveSession()
        instantiated_session = getattr(SparkSession, "_instantiatedSession", None)

        for session in [active_session, instantiated_session]:
            if session is not None:
                try:
                    session.catalog.clearCache()
                except Exception:
                    pass
                try:
                    session.stop()
                except Exception:
                    pass

        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None

        active_context = getattr(SparkContext, "_active_spark_context", None)
        if active_context is not None:
            try:
                active_context.stop()
            except Exception:
                pass
            SparkContext._active_spark_context = None

        gc.collect()
        print(f"[Stage 3] Spark session stopped after {stage_name}.")

    except Exception as exc:
        print(f"[Stage 3] Spark cleanup warning after {stage_name}: {exc}")


def _dq_issue(
    issue_code: str,
    count: int,
    dq_rules: dict[str, Any],
    default_action: str,
    default_detection: str,
) -> dict[str, Any]:
    return {
        "issue_code": issue_code,
        "records_affected": int(count),
        "handling_action": get_rule_value(dq_rules, issue_code, "handling_action", default_action),
        "detection": (
            get_rule_value(dq_rules, issue_code, "detection", "")
            or get_rule_value(dq_rules, issue_code, "detection_approach", "")
            or get_rule_value(dq_rules, issue_code, "description", default_detection)
        ),
    }


def write_dq_report(start_time: float, run_timestamp: str) -> None:
    transform_metrics_path = "/data/output/transform_metrics_stage2.json"
    gold_metrics_path = "/data/output/gold_metrics_stage2.json"

    dq_rules_path = pick_existing_path(
        ["/data/config/dq_rules.yaml", "/app/config/dq_rules.yaml"]
    )

    transform_metrics = load_yaml(transform_metrics_path)
    gold_metrics = load_yaml(gold_metrics_path)
    dq_rules = load_yaml(dq_rules_path).get("dq_rules", {}) or {}

    source_counts = transform_metrics.get("source_record_counts", {}) or {}
    dq_metrics = transform_metrics.get("dq_metrics", {}) or {}
    gold_counts = gold_metrics.get("gold_layer_record_counts", {}) or {}

    accounts_raw = int(source_counts.get("accounts_raw", 0))
    transactions_raw = int(source_counts.get("transactions_raw", 0))
    customers_raw = int(source_counts.get("customers_raw", 0))

    dq_issues: list[dict[str, Any]] = []

    duplicate_count = int(dq_metrics.get("transactions_duplicate_deduped_count", 0))
    if duplicate_count > 0:
        dq_issues.append(
            _dq_issue(
                "DUPLICATE_DEDUPED",
                duplicate_count,
                dq_rules,
                "DEDUPLICATED_KEEP_FIRST",
                "duplicate transaction_id with one retained",
            )
        )

    orphan_count = int(dq_metrics.get("transactions_orphaned_account_count", 0))
    if orphan_count > 0:
        dq_issues.append(
            _dq_issue(
                "ORPHANED_ACCOUNT",
                orphan_count,
                dq_rules,
                "QUARANTINED",
                "transactions.account_id has no match in accounts.account_id",
            )
        )

    mismatch_count = int(dq_metrics.get("transactions_type_mismatch_count", 0))
    if mismatch_count > 0:
        dq_issues.append(
            _dq_issue(
                "TYPE_MISMATCH",
                mismatch_count,
                dq_rules,
                "CAST_TO_DECIMAL",
                "amount exists but cast to DECIMAL(18,2) fails",
            )
        )

    currency_count = int(dq_metrics.get("transactions_currency_variant_count", 0))
    if currency_count > 0:
        dq_issues.append(
            _dq_issue(
                "CURRENCY_VARIANT",
                currency_count,
                dq_rules,
                "NORMALISED_CURRENCY",
                "currency not already standardised to ZAR",
            )
        )

    null_required_count = int(dq_metrics.get("accounts_null_required_count", 0))
    if null_required_count > 0:
        dq_issues.append(
            _dq_issue(
                "NULL_REQUIRED",
                null_required_count,
                dq_rules,
                "EXCLUDED_NULL_PK",
                "account_id is null or blank",
            )
        )

    date_count = (
        int(dq_metrics.get("customers_date_format_count", 0))
        + int(dq_metrics.get("accounts_date_format_count", 0))
        + int(dq_metrics.get("transactions_date_format_count", 0))
    )
    if date_count > 0:
        dq_issues.append(
            _dq_issue(
                "DATE_FORMAT",
                date_count,
                dq_rules,
                "NORMALISED_DATE",
                "date values require parsing from non-ISO formats",
            )
        )

    dq_report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_timestamp,
        "stage": "3",
        "source_record_counts": {
            "accounts_raw": accounts_raw,
            "transactions_raw": transactions_raw,
            "customers_raw": customers_raw,
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": int(gold_counts.get("fact_transactions", 0)),
            "dim_accounts": int(gold_counts.get("dim_accounts", 0)),
            "dim_customers": int(gold_counts.get("dim_customers", 0)),
        },
        "execution_duration_seconds": int(round(time.time() - start_time)),
    }

    Path("/data/output").mkdir(parents=True, exist_ok=True)

    with open("/data/output/dq_report.json", "w", encoding="utf-8") as handle:
        json.dump(dq_report, handle, indent=2)


def main() -> None:
    start_time = time.time()
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("[Stage 3] Batch pipeline starting...")

    try:
        run_ingestion()
        stop_spark_session("bronze ingestion")

        run_transformation()
        stop_spark_session("silver transformation")

        run_provisioning()
        stop_spark_session("gold provisioning")

        write_dq_report(start_time, run_timestamp)
        print("[Stage 3] Batch pipeline completed.")

        print("[Stage 3] Streaming pipeline starting...")
        run_streaming_pipeline()
        stop_spark_session("streaming pipeline")
        print("[Stage 3] Streaming pipeline completed.")

    finally:
        stop_spark_session("final cleanup")


if __name__ == "__main__":
    main()