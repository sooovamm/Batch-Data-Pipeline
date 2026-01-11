from fastapi import FastAPI
from mjpr1 import get_connection

app = FastAPI(title="Sales Analytics API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/revenue/summary")
def revenue_summary():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            SUM(amount) AS total_revenue,
            COUNT(DISTINCT order_id) AS total_orders
        FROM fact_orders
    """

    cursor.execute(query)
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    return result

@app.get("/revenue/trend")
def revenue_trend():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            order_date,
            SUM(amount) AS revenue
        FROM fact_orders
        GROUP BY order_date
        ORDER BY order_date
    """

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result


@app.get("/products/top")
def top_products(limit: int = 10):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            p.sku,
            p.category,
            SUM(f.amount) AS revenue
        FROM fact_orders f
        JOIN dim_products p ON f.sku = p.sku
        GROUP BY p.sku, p.category
        ORDER BY revenue DESC
        LIMIT %s
    """

    cursor.execute(query, (limit,))
    result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result


