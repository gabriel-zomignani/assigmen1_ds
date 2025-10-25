class Validator:
    def __init__(self, in_q, ok_q, bad_q, store=None):
        self.in_q = in_q
        self.ok_q = ok_q
        self.bad_q = bad_q
        self.store = store

    def run(self):
        print("validator started.")
        while True:
            order = self.in_q.get()

            if order is None:
                self.ok_q.put(None)
                self.bad_q.put(None)
                print("validator finished.")
                break

            if self.store and self.store.already_done(order["order_id"]):
                print("skipping duplicate order", order["order_id"])
                continue

            error = self.validate_order(order)

            if error:
                order["error"] = error
                self.bad_q.put(order)
            else:
                if self.store:
                    self.store.mark_done(order["order_id"])
                self.ok_q.put(order)

    def validate_order(self, order):
        """Checks if the order has required fields and valid values."""

        required = ["order_id", "customer_id", "amount", "currency", "category", "loyalty_points"]
        for field in required:
            if field not in order or order[field] == "":
                return f"Missing field: {field}"

        try:
            float(order["amount"])
        except:
            return "Amount must be a number"

        if order["currency"] not in ["USD", "EUR", "BRL"]:
            return f"Unsupported currency: {order['currency']}"

        return None
