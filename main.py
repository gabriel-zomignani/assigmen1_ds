# main.py
import threading
import queue
import yaml

# flexible imports (package or flat)
try:
    from pipeline.reader import ReaderFactory
    from pipeline.validator import Validator
    from pipeline.discount import DiscountApplier
    from pipeline.writer import Writer
    from utils.idempotency_store import IdempotencyStore
except ImportError:
    from reader import ReaderFactory
    from validator import Validator
    from discount import DiscountApplier
    from writer import Writer
    from idempotency_store import IdempotencyStore

SENTINEL = None

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()

    # queues
    q_read_to_validate = queue.Queue()
    q_valid_to_discount = queue.Queue()
    q_invalid_to_writer = queue.Queue()
    q_discount_to_writer = queue.Queue()

    n_discount = int((cfg.get("threads") or {}).get("discount", 1) or 1)

    reader = ReaderFactory.create(cfg, q_read_to_validate)
    store = IdempotencyStore()
    validator = Validator(q_read_to_validate, q_valid_to_discount, q_invalid_to_writer, store=store)
    discounts = [DiscountApplier(cfg, q_valid_to_discount, q_discount_to_writer) for _ in range(n_discount)]
    writer = Writer(cfg, q_discount_to_writer, q_invalid_to_writer)

    t_reader = threading.Thread(target=reader.run, name="reader")
    t_validator = threading.Thread(target=validator.run, name="validator")
    t_discounts = [threading.Thread(target=d.run, name=f"discount-{i+1}") for i, d in enumerate(discounts)]
    t_writer = threading.Thread(target=writer.run, name="writer")

    # start
    t_writer.start()
    for t in t_discounts: t.start()
    t_validator.start()
    t_reader.start()

    # wait
    t_reader.join()
    t_validator.join()

    # validator sends 1 sentinel to discount; if we have more workers, complete here
    extra = max(0, n_discount - 1)
    for _ in range(extra):
        q_valid_to_discount.put(SENTINEL)

    for t in t_discounts: t.join()
    t_writer.join()

    print("System finished. Check output files in data/ folder.")

if __name__ == "__main__":
    main()
