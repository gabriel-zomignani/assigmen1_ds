import json
import os

class IdempotencyStore:
    def __init__(self, path="data/idempotent.json"):
        self.path = path
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    self.data = json.load(f)
            except:
                self.data = {}
        else:
            self.data = {}

    def already_done(self, order_id):
        return str(order_id) in self.data

    def mark_done(self, order_id):
        self.data[str(order_id)] = True
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

