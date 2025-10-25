# pipeline/discount.py
try:
    from pipeline.base_stage import PipelineStage
    from utils.discount_strategy import get_discount_strategy
except ImportError:
    # fallback if running flat (not packages)
    from base_stage import PipelineStage
    from discount_strategy import get_discount_strategy

class DiscountApplier(PipelineStage):
    """
    Applies configured discount and emits to writer queue.
    """
    def __init__(self, config, in_q, out_q):
        super().__init__(in_q, out_q)
        dcfg = (config.get("discount") or {})
        self.strategy = get_discount_strategy(dcfg.get("strategy", "loyalty"))
        self.rate = float(dcfg.get("loyalty_rate", 0.05))

    def process(self, row):
        final_amount, discount_applied = self.strategy(row, self.rate)
        row["final_amount"] = final_amount
        row["discount_applied"] = discount_applied
        return row
