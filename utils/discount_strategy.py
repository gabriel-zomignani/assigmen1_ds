# utils/discount_strategy.py
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
        except Exception:
            order["final_amount"] = order.get("amount")
            order["discount_applied"] = 0.0
        return order

# factory function that returns a callable
def get_discount_strategy(name: str):

    n = (name or "loyalty").lower()

    def _loyalty(row, rate):
        try:
            amt = float(row["amount"])
        except Exception:
            try:
                amt = float(row.get("final_amount", 0))
            except Exception:
                amt = 0.0
        disc = amt * float(rate)
        return amt - disc, disc

    def _none(row, rate):
        try:
            amt = float(row["amount"])
        except Exception:
            try:
                amt = float(row.get("final_amount", 0))
            except Exception:
                amt = 0.0
        return amt, 0.0

    if n in ("loyalty", "default"):
        return _loyalty
    return _none
