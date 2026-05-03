# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Valent Mulungu
**Date:** 2026-05-03
**Status:** Final

---

## Context

Stage 3 introduced a streaming requirement on top of the existing batch pipeline. The requirement was to process transaction events delivered as JSONL files in `/data/stream/`, and update two new Gold tables:

* `current_balances` (one row per account, continuously updated)
* `recent_transactions` (last 50 transactions per account)

The pipeline had to process all stream files within a 5-minute SLA window from event timestamp to update time. At the same time, the Stage 2 batch pipeline still had to run fully, including Bronze → Silver → Gold processing and the generation of `dq_report.json`.

The streaming interface was not a real-time system but a directory-based micro-batch input. Files were pre-staged and had to be discovered, sorted, and processed sequentially. This required implementing a polling and file tracking mechanism.

Coming into Stage 3, my pipeline already had a structured design across `ingest.py`, `transform.py`, `provision.py`, and `run_all.py`, with shared utilities in `utils.py`. Stage 2 changes had already introduced data quality handling and scaling fixes.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

The existing architecture helped significantly in terms of structure and reuse. The separation between ingestion, transformation, and provisioning made it possible to introduce a new module (`stream_ingest.py`) without disrupting the batch flow. The entry point in `run_all.py` already orchestrated the pipeline stages, so extending it to include streaming after batch execution was straightforward.

Using Spark DataFrames across the pipeline was another advantage. I was able to reuse transformation patterns such as schema standardisation, casting, and window functions. For example, the deduplication logic used in Stage 2 (`ROW_NUMBER` over `(account_id, transaction_id)`) translated directly into the streaming pipeline.

However, there were also challenges. The Stage 1 design assumed full batch recomputation, particularly in `provision.py`, where Gold tables were overwritten. Stage 3 required incremental logic, which meant I had to introduce separate handling for streaming outputs instead of reusing existing provisioning logic.

Another issue was that transformation logic was not centralised. Some parsing and casting logic had to be reimplemented in `stream_ingest.py`, leading to duplication.

Overall, I would estimate that about 70–80% of the existing code was reused, but streaming required additional modules and adjustments to the processing approach.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

One of the main changes I would make is introducing a shared transformation layer. Currently, both `transform.py` and `stream_ingest.py` perform similar operations such as parsing dates, handling type mismatches, and normalising fields. If this logic was centralised (for example in a `pipeline/common_transformations.py` module), it would reduce duplication and make the pipeline easier to maintain.

I would also redesign how schemas are handled. In my current implementation, schema definitions and transformations are spread across different files. If I had defined all schemas in a central configuration or module, adding new outputs like `current_balances` would have required fewer changes.

Another key change would be designing the Gold layer for incremental updates from the start. In `provision.py`, the assumption was always full overwrite, which worked for Stage 1 and Stage 2 but did not align with streaming requirements. For Stage 3, I had to implement separate logic for maintaining running balances and limiting transactions to the most recent 50.

Finally, I would address performance earlier. During Stage 2, I encountered OutOfMemory issues due to joins and large shuffles. This required introducing partitioning strategies and disabling broadcast joins in `utils.py`. If these optimisations were part of the original design, the transition to Stage 3 would have been smoother.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If I had known about the streaming requirement from the beginning, I would design the pipeline as an incremental-first system rather than a batch-first system.

For ingestion, I would create a unified interface that can accept both batch inputs (`/data/input/`) and streaming inputs (`/data/stream/`). This would allow both pipelines to share the same transformation logic rather than duplicating it.

For state management, I would design around Delta Lake merge operations from the start. Instead of treating Gold as a final batch output, I would treat it as a continuously updated layer. This would make implementing `current_balances` and `recent_transactions` more natural.

I would also restructure the pipeline entry point. Instead of a single sequential `run_all.py`, I would design it to support different modes (e.g. batch, stream, or both), or separate execution paths that share common libraries.

In terms of performance, I would incorporate partitioning and memory management strategies early. The need to disable broadcast joins and control partition sizes became clear only after scaling issues in Stage 2, but these are fundamental design considerations.

Overall, I would aim for a design where batch and streaming pipelines are different execution paths built on top of the same reusable components.

---

## Appendix (optional)

The final pipeline structure includes:

```text
pipeline/
  ingest.py
  transform.py
  provision.py
  stream_ingest.py
  run_all.py
  utils.py
```

Batch and streaming pipelines share Spark configuration and utility functions but operate on separate input and output paths.
