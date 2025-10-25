# validator.py
import re

# flexible imports (package or flat)
try:
    from pipeline.base_stage import PipelineStage, SENTINEL
    from utils.idempotency_store import IdempotencyStore
except ImportError:
    from base_stage import PipelineStage, SENTINEL
    from idempotency_store import IdempotencyStore

REQUIRED = ["order_id", "customer_id", "amount", "currency", "category", "loyalty_points"]
ALLOWED_CCY = {"USD", "EUR", "BRL"}
CUSTOMER_RE = re.compile(r"^CUST-\d{3}$")  # simple, can relax if needed

class Validator(PipelineStage):
    def __init__(self, in_q, valid_q, invalid_q, store: IdempotencyStore):
        super().__init__(in_q, valid_q)
        self.invalid_queue = invalid_q
        self.store = store

    def process(self, row):
         # required fields
        for k in REQUIRED:
            if k not in row or row[k] in (None, ""):
                self.invalid_queue.put({**row, "error": f"missing:{k}"})
                return None

        # amount: number and >= 0
        try:
            amt = float(row["amount"])
        except Exception:
            self.invalid_queue.put({**row, "error": "invalid_amount"})
            return None
        if amt < 0:
            self.invalid_queue.put({**row, "error": "negative_amount"})
            return None

        # currency
        if row["currency"] not in ALLOWED_CCY:
            self.invalid_queue.put({**row, "error": "unsupported_currency"})
            return None

        # customer_id format (simple)
        if not CUSTOMER_RE.match(str(row["customer_id"])):
            self.invalid_queue.put({**row, "error": "invalid_customer_id"})
            return None

        # loyalty_points: integer and >= 0 (simple)
        try:
            lp = int(row["loyalty_points"])
            if lp < 0:
                raise ValueError
        except Exception:
            self.invalid_queue.put({**row, "error": "invalid_loyalty_points"})
            return None

        # duplicates (idempotency)
        oid = str(row["order_id"])
        if self.store.already_done(oid):
            self.invalid_queue.put({**row, "error": "duplicate"})
            return None

        # mark + emit normalized fields
        self.store.mark_done(oid)
        return {**row, "amount": amt, "loyalty_points": lp}

    def on_finish(self):
        # writer waits for a sentinel on INVALID queue too
        self.invalid_queue.put(SENTINEL)
