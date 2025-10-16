import mysql.connector
import pandas as pd

# --- Kết nối MySQL ---
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="sqoopuser",
        password="Abc123456!@#",
        database="doan_bigdata"
    )

# --- Lấy dữ liệu ---
def fetch_data(limit=None):
    conn = get_connection()
    query = "SELECT * FROM gold_data"
    # query = "SELECT * FROM gold_data ORDER BY datetime DESC"
    if limit:
        query += f" LIMIT {limit}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- Thêm bản ghi ---
def insert_record(datetime, close_price, high_price, low_price, open_price, volume):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO gold_data(datetime, close_price, high_price, low_price, open_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (datetime, close_price, high_price, low_price, open_price, volume))
    conn.commit()
    conn.close()

# --- Xóa bản ghi ---
def delete_record(record_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gold_data WHERE id = %s", (record_id,))
    conn.commit()
    conn.close()
