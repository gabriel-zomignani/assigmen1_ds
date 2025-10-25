import csv
import json

class Reader:
    def __init__(self, config, output_queue):
        self.input_file = config["input_file"]
        self.output_queue = output_queue

    def run(self):
        print("reader started.")

        try:
            if self.input_file.endswith(".csv"):
                self._read_csv()
            elif self.input_file.endswith(".jsonl"):
                self._read_jsonl()
            else:
                print(f"unsupported file format: {self.input_file}")
        finally:
            self.output_queue.put(None)
            print("reader finished and sent termination signal.")

    def _read_csv(self):
        with open(self.input_file, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.output_queue.put(row)

    def _read_jsonl(self):
        with open(self.input_file, "r", encoding="utf-8") as file:
            for line in file:
                try:
                    order = json.loads(line.strip())
                    self.output_queue.put(order)
                except json.JSONDecodeError:
                    print("invalid JSON skipped.")
