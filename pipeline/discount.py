from utils.discount_strategy import LoyaltyDiscount, DiscountStrategy

class DiscountApplier:
    def __init__(self, config, in_q, out_q):
        self.in_q = in_q
        self.out_q = out_q
        strat = config["discount"]["strategy"]

        if strat == "loyalty":
            rate = config["discount"].get("loyalty_rate", 0.05)
            self.strategy = LoyaltyDiscount(rate)
        else:
            self.strategy = DiscountStrategy()

    def run(self):
        print("Discount stage started.")
        while True:
            order = self.in_q.get()
            if order is None:
                self.out_q.put(None)
                print("Discount stage finished.")
                break

            order = self.strategy.apply(order)
            self.out_q.put(order)
