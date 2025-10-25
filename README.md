# Order Processing Pipeline

A small, threaded pipeline that reads orders, validates them, applies a discount, and writes valid/invalid CSVs.

## Features
- Input formats: CSV (`.csv`) and JSON Lines (`.jsonl`)
- Validation: required fields; amount ≥ 0; currency in {USD, EUR, BRL}; `customer_id` like `CUST-123`; `loyalty_points` integer ≥ 0
- Optional loyalty discount
- Duplicate protection (idempotency)
- Light concurrency: multiple **Discount** workers (configurable)

## Quick Start
```bash
pip install pyyaml
python main.py
```

Configuration (config.yaml):
```bash
input_file: "data/input.csv"
valid_output: "data/valid_orders.csv"
invalid_output: "data/invalid_orders.csv"

threads:
  reader: 1
  validator: 1
  discount: 4
  writer: 1

retry:
  max_attempts: 3
  base_delay: 0.1

discount:
  strategy: "loyalty"
  loyalty_rate: 0.05
```

What it does

Reader (Factory Method): picks CSV or JSONL reader and streams rows.

Validator (Template Method): checks fields, formats, idempotency; routes invalid rows.

Discount (Template Method + Strategy): applies discount; adds final_amount and discount_applied.

Writer: saves valid and invalid rows to separate CSV files.

Expected Input Columns:
order_id, customer_id, amount, currency, category, loyalty_points

Notes:

Set threads.discount to use more discount workers.

Retries are used when opening files (retry.max_attempts, retry.base_delay).

Idempotency data is stored at data/idempotent.json (delete it to reprocess the same orders).