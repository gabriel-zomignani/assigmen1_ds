class DiscountApplier:
    def __init__(self, config, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.config = config
        self.strategy = config["discount"]["strategy"]
        self.loyalty_rate = config["discount"].get("loyalty_rate", 0)

    def run(self):
        print("Discount stage started.")

        while True:
            order = self.input_queue.get()

            if order is None:  # Finalização
                self.output_queue.put(None)
                print("Discount stage finished.")
                break

            # Aplica desconto se configurado
            if self.strategy == "loyalty":
                try:
                    amount = float(order["amount"])
                    discount = amount * self.loyalty_rate
                    order["final_amount"] = amount - discount
                    order["discount_applied"] = discount
                except:
                    order["final_amount"] = order["amount"]
                    order["discount_applied"] = 0
            else:
                # Sem desconto
                order["final_amount"] = order["amount"]
                order["discount_applied"] = 0

            self.output_queue.put(order)
