import csv

class Writer:
    def __init__(self, config, valid_queue, invalid_queue):
        self.valid_queue = valid_queue
        self.invalid_queue = invalid_queue
        self.valid_output = config["valid_output"]
        self.invalid_output = config["invalid_output"]

    def run(self):
        print("Writer started.")

        valid_done = False
        invalid_done = False

        valid_file = open(self.valid_output, mode="w", newline="", encoding="utf-8")
        invalid_file = open(self.invalid_output, mode="w", newline="", encoding="utf-8")

        valid_writer = None
        invalid_writer = None

        while True:
            if not valid_done:
                order = self.valid_queue.get()
                if order is None:
                    valid_done = True
                else:
                    if valid_writer is None:
                        valid_writer = csv.DictWriter(valid_file, fieldnames=order.keys())
                        valid_writer.writeheader()
                    valid_writer.writerow(order)

            if not invalid_done:
                order = self.invalid_queue.get()
                if order is None:
                    invalid_done = True
                else:
                    if invalid_writer is None:
                        invalid_writer = csv.DictWriter(invalid_file, fieldnames=order.keys())
                        invalid_writer.writeheader()
                    invalid_writer.writerow(order)

            if valid_done and invalid_done:
                print("Writer finished.")
                break

        valid_file.close()
        invalid_file.close()
