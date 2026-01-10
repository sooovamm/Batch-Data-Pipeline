import pandas as pd
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "nat141408",
    "database": "analytics_db"
}

csvpath = r"C:\\Amazon Sale Report.xlsx"


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_raw_data():
    df = pd.read_excel(csvpath)

    # Strong normalization
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    return df


def ingest_products(df, conn):
    products = df[["sku", "style", "category", "size"]].drop_duplicates().copy()

    query = """
        INSERT IGNORE INTO stg_products (sku, style, category, size)
        VALUES (%s, %s, %s, %s)
    """

    data = list(products.itertuples(index=False, name=None))

    cursor = conn.cursor()
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()


def ingest_orders(df, conn):
    orders = df[[
        "order_id",
        "date",
        "status",
        "sales_channel",
        "sku",
        "qty",
        "amount"
    ]].copy()

    orders.rename(columns={
        "date": "order_date",
        "status": "order_status"
    }, inplace=True)

    orders["order_date"] = pd.to_datetime(
        orders["order_date"],
        errors="coerce"
    ).dt.date

    query = """
        INSERT IGNORE INTO stg_orders
        (order_id, order_date, order_status, sales_channel, sku, qty, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    data = list(orders.itertuples(index=False, name=None))

    cursor = conn.cursor()
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()


def ingest_shipping(df, conn):
    shipping = df[[
        "order_id",
        "fulfilled_by",
        "ship_service_level",
        "b2b"
    ]].drop_duplicates().copy()

    query = """
        INSERT IGNORE INTO stg_shipping
        (order_id, fulfilled_by, ship_service_level, b2b_flag)
        VALUES (%s, %s, %s, %s)
    """

    data = list(shipping.itertuples(index=False, name=None))

    cursor = conn.cursor()
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()


def ingest_customers(df, conn):
    customers = df[[
        "b2b",
        "sales_channel"
    ]].drop_duplicates().copy()

    query = """
        INSERT IGNORE INTO stg_customers (b2b_flag, sales_channel)
        VALUES (%s, %s)
    """

    data = list(customers.itertuples(index=False, name=None))

    cursor = conn.cursor()
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()


def main():
    df = load_raw_data()
    conn = get_connection()

    ingest_products(df, conn)
    ingest_orders(df, conn)
    ingest_shipping(df, conn)
    ingest_customers(df, conn)

    conn.close()
    print("Ingestion completed successfully.")


if __name__ == "__main__":
    main()
