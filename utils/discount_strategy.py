class DiscountStrategy:
    def apply(self, order):
        return order

class LoyaltyDiscount(DiscountStrategy):
    def __init__(self, rate):
        self.rate = rate

    def apply(self, order):
        try:
            amt = float(order["amount"])
            disc = amt * self.rate
            order["final_amount"] = amt - disc
            order["discount_applied"] = disc
        except:
            # if something is weird, just leave it
            order["final_amount"] = order.get("amount")
            order["discount_applied"] = 0
        return order
