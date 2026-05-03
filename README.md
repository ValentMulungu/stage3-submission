# Nedbank Data Engineering Challenge – Stage 3

## Overview

This project implements a complete **end-to-end data engineering pipeline** following the **Medallion Architecture (Bronze → Silver → Gold)** with an extension into **micro-batch streaming**.

The solution is built using **PySpark**, designed to be:
- Deterministic
- Resource-efficient
- Fully containerized
- Compliant with strict runtime constraints

### Runtime Constraints
- 2 GB RAM
- 2 vCPU
- No internet access
- Read-only container filesystem

---

## Architecture

### Batch Pipeline (Stage 1 & 2)
Raw Data → Bronze → Silver → Gold

### Streaming Pipeline (Stage 3)
Stream Files → Standardization → Deduplication → Validation → Stream Gold

### Execution Flow
1. Bronze ingestion
2. Silver transformation (Data Quality applied)
3. Gold provisioning
4. Streaming micro-batch processing

---

## Input Data

### Batch Inputs
/data/input/accounts.csv  
/data/input/transactions.jsonl  
/data/input/customers.csv  

### Streaming Inputs
/data/stream/stream_*.jsonl  

- Total files: 12  
- Total events: 2,415  

---

## Output Data

### Batch Outputs
/data/output/bronze  
/data/output/silver  
/data/output/gold  
/data/output/dq_report.json  

### Streaming Outputs

#### current_balances
/data/output/stream_gold/current_balances  

#### recent_transactions
/data/output/stream_gold/recent_transactions  

Retention Rule:
- Last 50 transactions per account

---

## Data Quality Framework

Config-driven rules:
/data/config/dq_rules.yaml  

Issues handled:
- DUPLICATE_DEDUPED  
- ORPHANED_ACCOUNT  
- TYPE_MISMATCH  
- DATE_FORMAT  
- CURRENCY_VARIANT  
- NULL_REQUIRED  

Output:
/data/output/dq_report.json  

---

## Streaming Metrics

/data/output/stream_metrics.json  

---

## Execution

### Build
docker build -t nedbank-stage3 .

### Run
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v ${PWD}/data:/data \
  nedbank-stage3

---

## Key Design Decisions

- Medallion Architecture
- Micro-batch streaming
- Config-driven DQ rules
- Resource-constrained optimization
- Fault-tolerant design

---

## Performance Strategy

- Reduced shuffle partitions
- Avoided expensive Spark actions
- Controlled file sizes
- Optimized memory usage

---

## Author

Valent Mulungu
