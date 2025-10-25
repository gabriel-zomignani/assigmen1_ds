class Validator:
    def __init__(self, input_queue, valid_queue, invalid_queue):
        self.input_queue = input_queue
        self.valid_queue = valid_queue
        self.invalid_queue = invalid_queue

    def run(self):
        print("Validator started.")

        while True:
            order = self.input_queue.get()

            if order is None:
                self.valid_queue.put(None)
                self.invalid_queue.put(None)
                print("Validator finished and sent termination signal.")
                break

            error = self.validate_order(order)

            if error:
                order["error"] = error
                self.invalid_queue.put(order)
            else:
                self.valid_queue.put(order)

    def validate_order(self, order):
        required_fields = ["order_id", "customer_id", "amount", "currency", "category", "loyalty_points"]

        for field in required_fields:
            if field not in order or order[field] == "":
                return f"Missing field: {field}"

        try:
            float(order["amount"])
        except ValueError:
            return "Amount must be a number"

        if order["currency"] not in ["USD", "EUR", "BRL"]:
            return "Unsupported currency"

        return None
