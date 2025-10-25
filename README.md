ORDER PROCESSING SYSTEM

This project is a simple multithreaded order processing system. It reads orders from a file, validates the data, applies discounts, and writes valid and invalid orders into separate output files.

1. PROJECT FILES

• main.py – Main program
• config.yaml – System configuration file
• requirements.txt – List of dependencies
• Folder: pipeline

reader.py – Reads orders from the input file

validator.py – Checks if orders are valid

discount.py – Applies discount to valid orders

writer.py – Writes output files

• Folder: utils

discount_strategy.py – Handles discount logic (strategy pattern)

idempotency_store.py – Prevents duplicate order processing

retry_decorator.py – Retry mechanism with simple backoff

• Folder: data

input.csv – Example input file

valid_orders.csv – Output: valid processed orders

invalid_orders.csv – Output: invalid orders with errors

idempotent.json – Stores IDs of already processed orders

2. CONFIGURATION (config.yaml)

Example:

input_file: data/input.csv
valid_output: data/valid_orders.csv
invalid_output: data/invalid_orders.csv
discount:
strategy: loyalty
loyalty_rate: 0.05

3. INSTALLATION

Install required libraries:
pip install -r requirements.txt

requirements.txt content:
pyyaml

4. HOW TO RUN

Run the system using:
python main.py

After running, output files will be saved inside the data/ folder:
• valid_orders.csv
• invalid_orders.csv

5. HOW THE SYSTEM WORKS (PIPELINE)

Reader → reads orders from the input file.

Validator → checks required fields and invalid values.

Discount → applies discount (example: loyalty points).

Writer → saves valid and invalid orders in separate CSV files.

Idempotency → avoids processing the same order twice.

Multithreading → each step runs in its own thread using queues to communicate.