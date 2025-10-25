import csv
import json

class Reader:
    def __init__(self, config, out_q):
        self.file = config["input_file"]
        self.out_q = out_q

    def run(self):
        print("reader started.")
        try:
            if self.file.endswith(".csv"):
                self._read_csv()
            elif self.file.endswith(".jsonl"):
                self._read_jsonl()
            else:
                print("unsupported file format:", self.file)
        finally:
            self.out_q.put(None)
            print("reader finished and sent termination signal.")

    def _read_csv(self):
        with open(self.file, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                self.out_q.put(row)

    def _read_jsonl(self):
        with open(self.file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    self.out_q.put(json.loads(line.strip()))
                except json.JSONDecodeError:
                    print("invalid JSON skipped.")
