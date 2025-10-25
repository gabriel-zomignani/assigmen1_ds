# pipeline/writer.py  (drop-in)
import csv
try:
    from utils.retry_decorator import retry
except ImportError:
    from retry_decorator import retry

SENTINEL = None

class Writer:
    """
    Writes two CSVs (valid / invalid).
    - Retries opening files using config.retry.{max_attempts, base_delay}
    - Prints final counters at the end.
    """
    def __init__(self, config, valid_queue, invalid_queue):
        self.valid_queue = valid_queue
        self.invalid_queue = invalid_queue
        self.valid_output = config["valid_output"]
        self.invalid_output = config["invalid_output"]

        r = (config.get("retry") or {})
        self.max_attempts = int(r.get("max_attempts", 3))
        self.base_delay = float(r.get("base_delay", 0.1))

    def _open_with_retry(self, path, mode, **kw):
        @retry(max_attempts=self.max_attempts, base_delay=self.base_delay)
        def _op():
            return open(path, mode, **kw)
        return _op()

    def run(self):
        print("Writer started.")

        # Try to open both files with retry; exit gracefully if it fails.
        try:
            valid_file = self._open_with_retry(self.valid_output, "w", newline="", encoding="utf-8")
            invalid_file = self._open_with_retry(self.invalid_output, "w", newline="", encoding="utf-8")
        except Exception as e:
            print(f"[error] Writer could not open output files after retries: {e}")
            return

        valid_writer = None
        invalid_writer = None
        valid_done = False
        invalid_done = False
        valid_count = 0
        invalid_count = 0

        with valid_file, invalid_file:
            while True:
                # consume valid queue
                if not valid_done:
                    item = self.valid_queue.get()
                    if item is SENTINEL:
                        valid_done = True
                    else:
                        if valid_writer is None:
                            valid_writer = csv.DictWriter(valid_file, fieldnames=item.keys())
                            valid_writer.writeheader()
                        valid_writer.writerow(item)
                        valid_count += 1

                # consume invalid queue
                if not invalid_done:
                    item = self.invalid_queue.get()
                    if item is SENTINEL:
                        invalid_done = True
                    else:
                        if invalid_writer is None:
                            invalid_writer = csv.DictWriter(invalid_file, fieldnames=item.keys())
                            invalid_writer.writeheader()
                        invalid_writer.writerow(item)
                        invalid_count += 1

                if valid_done and invalid_done:
                    break

        print(f"Writer finished. valid={valid_count} invalid={invalid_count}")
