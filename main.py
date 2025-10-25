import threading
import queue
import yaml

from pipeline.reader import Reader
from pipeline.validator import Validator
from pipeline.discount import DiscountApplier
from pipeline.writer import Writer
from utils.idempotency_store import IdempotencyStore


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def main():
    print("System starting...")

    config = load_config()

    # stops duplicate orders
    idem = IdempotencyStore("data/idempotent.json")

    # queues for each step
    q1 = queue.Queue()
    q2 = queue.Queue()
    q3 = queue.Queue()
    q_err = queue.Queue()

    # set up pipeline parts
    reader = Reader(config, q1)
    validator = Validator(q1, q2, q_err, idem)
    discount = DiscountApplier(config, q2, q3)
    writer = Writer(config, q3, q_err)

    # run everything on threads
    stuff = [
        threading.Thread(target=reader.run),
        threading.Thread(target=validator.run),
        threading.Thread(target=discount.run),
        threading.Thread(target=writer.run)
    ]

    for t in stuff:
        t.start()

    for t in stuff:
        t.join()

    print("System finished. Check output files in data/ folder.")


if __name__ == "__main__":
    main()
