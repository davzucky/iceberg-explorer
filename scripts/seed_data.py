"""Seed script to populate Iceberg catalog with sample data."""

import json
import time
from datetime import datetime

import duckdb


def seed_database() -> None:
    """Seed the Iceberg catalog with sample namespaces, tables, and data."""
    conn = duckdb.connect(":memory:")

    try:
        conn.execute("INSTALL iceberg; LOAD iceberg;")

        settings_path = "/home/vscode/.config/iceberg-explorer/lakekeeper-config.json"
        with open(settings_path, encoding="utf-8") as config_file:
            catalog_config = json.load(config_file)["catalog"]

        catalog_uri = catalog_config["uri"]
        warehouse = catalog_config["warehouse"]
        attach_options = [
            "TYPE ICEBERG",
            f"ENDPOINT '{catalog_uri}'",
            "AUTHORIZATION_TYPE 'none'",
        ]

        conn.execute(f"ATTACH '{warehouse}' AS iceberg (" + ", ".join(attach_options) + ")")

        print(f"Attached to catalog: {catalog_uri}")
        time.sleep(2)

        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'nyc' AND table_name = 'taxi_trips';"
        ).fetchone()
        if result and result[0] > 0:
            print("Database already seeded. Skipping...")
            return

        print("Creating namespace: nyc")
        conn.execute("CREATE SCHEMA IF NOT EXISTS iceberg.nyc;")
        time.sleep(1)

        print("Creating namespace: sales")
        conn.execute("CREATE SCHEMA IF NOT EXISTS iceberg.sales;")
        time.sleep(1)

        print("Creating table: iceberg.nyc.taxi_trips")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.nyc.taxi_trips (
                id BIGINT,
                vendor_id INTEGER,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                rate_code INTEGER,
                store_and_fwd_flag VARCHAR(1),
                payment_type INTEGER,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE
            );
        """)
        time.sleep(1)

        print("Seeding data into iceberg.nyc.taxi_trips")
        taxi_trips_data = []
        for i in range(150):
            taxi_trips_data.append(
                (
                    i + 1,
                    (i % 2) + 1,
                    datetime(2026, 1, 1, 8 + (i % 12), (i * 5) % 60, 0),
                    datetime(2026, 1, 1, 9 + (i % 12), (i * 7) % 60, 0),
                    (i % 5) + 1,
                    2.5 + (i * 0.1),
                    (i % 5) + 1,
                    "N" if i % 3 == 0 else "Y",
                    (i % 4) + 1,
                    10.0 + (i * 0.5),
                    0.5,
                    0.5,
                    1.0 + (i * 0.1),
                    0.0,
                    0.3,
                    12.5 + (i * 0.8),
                )
            )

        conn.executemany(
            "INSERT INTO iceberg.nyc.taxi_trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            taxi_trips_data,
        )
        time.sleep(1)

        print("Creating table: iceberg.nyc.zones")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.nyc.zones (
                zone_id INTEGER,
                borough VARCHAR(50),
                zone VARCHAR(100),
                service_zone VARCHAR(50)
            );
        """)
        time.sleep(1)

        print("Seeding data into iceberg.nyc.zones")
        zones_data = []
        boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
        for i in range(120):
            borough = boroughs[i % len(boroughs)]
            zones_data.append(
                (
                    i + 1,
                    borough,
                    f"Zone {i + 1} - {borough}",
                    "Yellow Zone" if i % 3 == 0 else "Boro Zone",
                )
            )

        conn.executemany("INSERT INTO iceberg.nyc.zones VALUES (?, ?, ?, ?);", zones_data)
        time.sleep(1)

        print("Creating table: iceberg.sales.orders")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.sales.orders (
                order_id BIGINT,
                customer_id INTEGER,
                order_date DATE,
                ship_date DATE,
                ship_mode VARCHAR(20),
                quantity INTEGER,
                discount DOUBLE,
                profit DOUBLE,
                sales DOUBLE,
                region VARCHAR(20),
                product_category VARCHAR(50),
                product_name VARCHAR(100)
            );
        """)
        time.sleep(1)

        print("Seeding data into iceberg.sales.orders")
        orders_data = []
        regions = ["East", "West", "Central", "South"]
        categories = ["Office Supplies", "Furniture", "Technology"]
        for i in range(180):
            region = regions[i % len(regions)]
            category = categories[i % len(categories)]
            quantity = (i % 10) + 1
            sales = quantity * (10 + (i % 50))
            profit = sales * 0.15 - (i % 20)
            orders_data.append(
                (
                    i + 1,
                    (i % 50) + 1,
                    datetime(2026, 1, 1 + (i % 30), 1 + (i % 28), 1).date(),
                    datetime(2026, 1, 2 + (i % 30), 1 + (i % 28), 1).date(),
                    "Standard Class"
                    if i % 3 == 0
                    else "Second Class"
                    if i % 3 == 1
                    else "First Class",
                    quantity,
                    (i % 20) / 100.0,
                    profit,
                    sales,
                    region,
                    category,
                    f"{category} Product {i + 1}",
                )
            )

        conn.executemany(
            "INSERT INTO iceberg.sales.orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            orders_data,
        )
        time.sleep(1)

        print("Creating table: iceberg.sales.products")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.sales.products (
                product_id INTEGER,
                product_name VARCHAR(100),
                category VARCHAR(50),
                sub_category VARCHAR(50),
                unit_price DOUBLE,
                units_in_stock INTEGER
            );
        """)
        time.sleep(1)

        print("Seeding data into iceberg.sales.products")
        products_data = []
        categories = [
            ("Office Supplies", ["Binders", "Paper", "Pens", "Labels", "Storage"]),
            ("Furniture", ["Chairs", "Bookcases", "Tables", "Furnishings"]),
            ("Technology", ["Phones", "Accessories", "Machines", "Copiers"]),
        ]
        for i in range(150):
            category = categories[i % len(categories)]
            subcategory = category[1][i % len(category[1])]
            products_data.append(
                (
                    i + 1,
                    f"{subcategory} - Model {(i % 10) + 1}",
                    category[0],
                    subcategory,
                    10.0 + (i * 1.5),
                    (i % 100) + 10,
                )
            )

        conn.executemany(
            "INSERT INTO iceberg.sales.products VALUES (?, ?, ?, ?, ?, ?);",
            products_data,
        )

        print("\nSeeding completed successfully!")
        print("\nSummary:")
        print("- Namespaces: 2 (nyc, sales)")
        print("- Tables: 4 (nyc.taxi_trips, nyc.zones, sales.orders, sales.products)")
        print("- Total rows: 600 (150 + 120 + 180 + 150)")
        print("\nYou can now explore this data in Iceberg Explorer!")

    except Exception as e:
        print(f"Error during seeding: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    seed_database()
