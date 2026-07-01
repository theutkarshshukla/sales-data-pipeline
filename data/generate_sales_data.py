"""
generate_sales_data.py

Generates a synthetic raw sales CSV that mimics real-world messy data:
- duplicate rows
- null values in some columns
- inconsistent date formats
- mixed-case / inconsistent text fields

This mess is intentional. Your Bronze layer will ingest it as-is, and
your Silver layer will be responsible for cleaning it. That's the whole
point of the Medallion Architecture: Bronze = raw truth, Silver = clean,
Gold = business-ready aggregates.

Usage:
    python generate_sales_data.py --rows 5000 --out raw_sales.csv
"""

import argparse
import csv
import random
from datetime import datetime, timedelta

REGIONS = ["North", "South", "East", "West", "north", "SOUTH"]  # inconsistent casing on purpose
PRODUCTS = [
    ("P001", "Wireless Mouse", "Electronics", 799),
    ("P002", "Office Chair", "Furniture", 5499),
    ("P003", "Notebook Set", "Stationery", 199),
    ("P004", "Mechanical Keyboard", "Electronics", 3499),
    ("P005", "Desk Lamp", "Furniture", 899),
    ("P006", "Water Bottle", "Lifestyle", 349),
    ("P007", "Backpack", "Lifestyle", 1899),
    ("P008", "Monitor Stand", "Electronics", 1299),
]
PAYMENT_METHODS = ["Credit Card", "UPI", "Net Banking", "Cash on Delivery", None]
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]  # inconsistent formats on purpose


def random_date(start_days_ago=365, end_days_ago=0):
    days = random.randint(end_days_ago, start_days_ago)
    dt = datetime.now() - timedelta(days=days)
    fmt = random.choice(DATE_FORMATS)
    return dt.strftime(fmt)


def generate_rows(n):
    rows = []
    customer_pool = [f"C{1000+i}" for i in range(300)]  # 300 unique customers reused across orders
    customer_names = {cid: f"Customer_{cid[-3:]}" for cid in customer_pool}

    for i in range(n):
        order_id = f"O{10000+i}"
        customer_id = random.choice(customer_pool)
        product = random.choice(PRODUCTS)
        product_id, product_name, category, unit_price = product

        quantity = random.randint(1, 5)
        # occasionally inject a null quantity to simulate bad source data
        if random.random() < 0.02:
            quantity = None

        unit_price_val = unit_price
        if random.random() < 0.01:
            unit_price_val = None  # missing price

        region = random.choice(REGIONS)
        payment_method = random.choice(PAYMENT_METHODS)
        order_date = random_date()

        row = {
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_name": customer_names[customer_id],
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price_val,
            "region": region,
            "payment_method": payment_method,
            "order_date": order_date,
        }
        rows.append(row)

    # inject exact duplicate rows (simulates re-sent/duplicate records from source system)
    duplicate_count = max(1, n // 100)
    rows.extend(random.sample(rows, duplicate_count))
    random.shuffle(rows)
    return rows


def write_csv(rows, out_path):
    fieldnames = [
        "order_id", "customer_id", "customer_name", "product_id",
        "product_name", "category", "quantity", "unit_price",
        "region", "payment_method", "order_date",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--out", type=str, default="raw_sales.csv")
    args = parser.parse_args()

    data = generate_rows(args.rows)
    write_csv(data, args.out)
    print(f"Generated {len(data)} rows (including duplicates) -> {args.out}")
