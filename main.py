import threading
import queue
import yaml

from pipeline.reader import Reader
from pipeline.validator import Validator
from pipeline.discount import DiscountApplier
from pipeline.writer import Writer

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def main():
    config = load_config()

    q_validate = queue.Queue()
    q_discount = queue.Queue()
    q_output = queue.Queue()
    q_invalid = queue.Queue()

    reader = Reader(config, q_validate)
    validator = Validator(q_validate, q_discount, q_invalid)
    discount = DiscountApplier(config, q_discount, q_output)
    writer = Writer(config, q_output, q_invalid)

    threads = [
        threading.Thread(target=reader.run),
        threading.Thread(target=validator.run),
        threading.Thread(target=discount.run),
        threading.Thread(target=writer.run)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
