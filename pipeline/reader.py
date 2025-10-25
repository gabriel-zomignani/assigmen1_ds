# pipeline/reader.py
import csv
import json
try:
    from utils.retry_decorator import retry
except ImportError:
    from retry_decorator import retry

SENTINEL = None

class _BaseReader:
    """Shared minimal retry/open logic."""
    def __init__(self, config, out_q):
        self.config = config
        self.out_q = out_q
        self.file = config["input_file"]
        r = (config.get("retry") or {})
        self.max_attempts = int(r.get("max_attempts", 3))
        self.base_delay = float(r.get("base_delay", 0.1))

    def _open_with_retry(self, path, mode, **kw):
        @retry(max_attempts=self.max_attempts, base_delay=self.base_delay)
        def _op():
            return open(path, mode, **kw)
        return _op()

    def run(self):
        raise NotImplementedError


class CsvReader(_BaseReader):
    def run(self):
        print("reader (csv) started.")
        try:
            f = self._open_with_retry(self.file, "r", newline="", encoding="utf-8")
            if not f:
                print("[error] reader failed to open CSV after retries.")
                return
            with f:
                r = csv.DictReader(f)
                for row in r:
                    self.out_q.put(row)
        finally:
            self.out_q.put(SENTINEL)
            print("reader (csv) finished and sent termination signal.")


class JsonlReader(_BaseReader):
    def run(self):
        print("reader (jsonl) started.")
        try:
            f = self._open_with_retry(self.file, "r", encoding="utf-8")
            if not f:
                print("[error] reader failed to open JSONL after retries.")
                return
            with f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        self.out_q.put(json.loads(line))
                    except json.JSONDecodeError:
                        print("invalid JSON line skipped.")
        finally:
            self.out_q.put(SENTINEL)
            print("reader (jsonl) finished and sent termination signal.")


class ReaderFactory:
    """Factory Method: choose reader by file extension."""
    @staticmethod
    def create(config, out_q):
        path = str(config["input_file"]).lower()
        if path.endswith(".csv"):
            return CsvReader(config, out_q)
        if path.endswith(".jsonl"):
            return JsonlReader(config, out_q)
        print("unsupported file format:", path)
        return CsvReader(config, out_q)  # default fallback
