# idempotency_store.py
import json
import os
try:
    import yaml
except Exception:
    yaml = None
try:
    from utils.retry_decorator import retry
except ImportError:
    from retry_decorator import retry

class IdempotencyStore:
    def __init__(self, path="data/idempotent.json"):
        self.path = path
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except:
                self.data = {}
        else:
            self.data = {}

        # load retry defaults (best effort)
        self._retry_max = 3
        self._retry_base = 0.1
        try:
            if yaml:
                with open("config.yaml", "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
                r = (cfg.get("retry") or {})
                self._retry_max = int(r.get("max_attempts", 3))
                self._retry_base = float(r.get("base_delay", 0.1))
        except Exception:
            pass

    def already_done(self, order_id):
        return str(order_id) in self.data

    def mark_done(self, order_id):
        self.data[str(order_id)] = True

        @retry(max_attempts=self._retry_max, base_delay=self._retry_base)
        def _write():
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2)

        _write()
