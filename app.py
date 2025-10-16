import streamlit as st
import pandas as pd
import os
from db_utils import fetch_data, insert_record, delete_record
import gold

MYSQL_DB = "doan_bigdata"
MYSQL_USER = "sqoopuser"
MYSQL_PASSWORD = "Abc123456!@#"
TABLE = "gold_data"

st.set_page_config(
    page_title="Gold Data Management Dashboard",
    layout="wide"
)

st.markdown("""
<style>
.stApp {
    background: linear-gradient(135deg, #f4f6f7 0%, #fefefe 100%);
    font-family: "Segoe UI", sans-serif;
    color: #2c3e50;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #083358 0%, #1a5276 100%);
    padding-top: 20px;
}

.sidebar-title {
    font-size: 20px;
    font-weight: 700;
    color: #f1c40f; /* vàng gold */
    text-align: center;
    margin-bottom: 25px;
}

[data-testid="stSidebar"] p {
    color: #fdfdfd !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    font-size: 13px !important;
    letter-spacing: 0.5px;
}

[data-testid="stSidebar"] label {
    color: #fdfdfd !important;
    font-weight: 500 !important;
    font-size: 15px !important;
}

[data-testid="stSidebar"] [role="radio"][aria-checked="true"] label {
    color: #f1c40f !important;
    font-weight: 700 !important;
}

.stButton>button,
div.stFormSubmitButton>button {
    border-radius: 8px;
    padding: 8px 18px;
    background-color: #f1c40f; /* vàng gold */
    color: #2c3e50;
    font-weight: 600;
    border: none;
    transition: all 0.3s ease;
    box-shadow: 0 2px 4px rgba(0,0,0,0.15);
}
.stButton>button:hover,
div.stFormSubmitButton>button:hover {
    background-color: #f39c12; /* vàng đậm hơn khi hover */
    color: white;
    transform: scale(1.03);
}
.stButton>button:hover {
    background-color: #f39c12; /* vàng đậm hơn khi hover */
    color: white;
    transform: scale(1.03);
}

[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    background-color: white;
    box-shadow: 0 0 8px rgba(0,0,0,0.1);
}
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown(
    """
    <div class="sidebar-title">--Gold Data--</div>
    """,
    unsafe_allow_html=True
)

st.sidebar.markdown("**CHỌN CHỨC NĂNG**") 

menu = ["QUẢN LÝ DỮ LIỆU", "BACKUP / RESTORE", "TRUY VẤN SQL", "BIỂU ĐỒ GIÁ VÀNG"] 
choice = st.sidebar.radio("", menu) 

st.markdown('<div class="subtitle">Quản lý và phân tích dữ liệu giá vàng (MySQL + Hadoop + Streamlit)</div>', unsafe_allow_html=True)

if choice == "QUẢN LÝ DỮ LIỆU":
    st.header("Quản lý dữ liệu bảng `gold_data`")

    col_main = st.container()
    col_add, col_del = st.columns([1.5, 1])

    with col_add:
        st.subheader("Thêm bản ghi mới")
        with st.form("add_form"):
            datetime = st.text_input("Datetime (YYYY-MM-DD HH:MM:SS)")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                close_price = st.number_input("Close price", value=0.0)
            with col2:
                high_price = st.number_input("High price", value=0.0)
            with col3:
                low_price = st.number_input("Low price", value=0.0)
            with col4:
                open_price = st.number_input("Open price", value=0.0)
            with col5:
                volume = st.number_input("Volume", value=0, step=1)
            submitted = st.form_submit_button("Thêm dữ liệu")
            if submitted:
                insert_record(datetime, close_price, high_price, low_price, open_price, volume)
                st.success("Đã thêm bản ghi mới thành công!")

    with col_del:
        st.subheader("Xóa bản ghi")
        df_temp = fetch_data()
        record_id = st.number_input("Nhập ID cần xóa", min_value=1, step=1)
        if st.button("Xóa bản ghi"):
            delete_record(record_id)
            st.warning(f"Đã xóa bản ghi ID = {record_id}")
            
    st.markdown("---")
    st.subheader("Dữ liệu hiện tại trong bảng `gold_data`")
    df = fetch_data()
    st.dataframe(df, use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Tải CSV",
        data=csv,
        file_name='gold_data_export.csv',
        mime='text/csv'
    )
    if st.button("Làm mới dữ liệu"):
        st.rerun()

elif choice == "BACKUP / RESTORE":
    st.header("Sao lưu & Phục hồi dữ liệu MySQL")
    st.info("Chức năng này giúp bạn sao lưu và phục hồi toàn bộ dữ liệu bảng `gold_data` trong MySQL.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Backup dữ liệu"):
            os.system(f"mysqldump -u {MYSQL_USER} -p'{MYSQL_PASSWORD}' {MYSQL_DB} {TABLE} > backup.sql")
            st.success("Backup thành công! File: backup.sql")
    with col2:
        if st.button("Restore dữ liệu"):
            os.system(f"mysql -u {MYSQL_USER} -p'{MYSQL_PASSWORD}' {MYSQL_DB} < backup.sql")
            st.success("Phục hồi dữ liệu thành công!")

elif choice == "TRUY VẤN SQL":
    st.header("Thực thi truy vấn SQL (chỉ SELECT)")
    st.caption("Nhập câu lệnh SQL tùy chỉnh, ví dụ: `SELECT * FROM gold_data WHERE close_price > 2700 LIMIT 10;`")

    query = st.text_area("Nhập truy vấn:", "SELECT * FROM gold_data LIMIT 10;")
    if st.button("Chạy truy vấn"):
        import mysql.connector
        conn = mysql.connector.connect(
            host="localhost",
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        df = pd.read_sql(query, conn)
        conn.close()
        st.dataframe(df, use_container_width=True, hide_index=True)


elif choice == "BIỂU ĐỒ GIÁ VÀNG":
    st.header("Phân tích và trực quan hóa giá vàng")
    
    # --- Load dữ liệu 1 lần ---
    gold_data = gold.load_gold_data()

    # --- KPI ---
    kpi = gold.get_gold_kpi(gold_data)
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Giá hiện tại (USD/oz)", f"{kpi['Giá đóng cửa gần nhất']:.2f}")
    col2.metric("Trung bình 30 ngày", f"{kpi['Trung bình 30 ngày']:.2f}")
    col3.metric("Biến động 30 ngày (%)", f"{kpi['Độ biến động 30 ngày (%)']:.2f}")
    col4.metric("Lợi suất TB ngày (%)", f"{kpi['Lợi suất trung bình (%)']:.3f}")
    col5.metric("Khối lượng TB", f"{kpi['Khối lượng giao dịch trung bình']:.0f}")

    st.markdown("---")
    
    # --- Chọn loại biểu đồ ---
    chart = st.selectbox("Chọn loại biểu đồ", [
        "Xu hướng giá",
        "Lợi suất",
        "Độ biến động",
        "Volume vs Spread"
    ])

    # --- Hiển thị biểu đồ ---
    if chart == "Xu hướng giá":
        st.pyplot(gold.plot_gold_trend(gold_data))
    elif chart == "Lợi suất":
        st.pyplot(gold.plot_gold_return_hist(gold_data))
    elif chart == "Độ biến động":
        st.pyplot(gold.plot_gold_volatility(gold_data))
    elif chart == "Volume vs Spread":
        st.pyplot(gold.plot_volume_vs_spread(gold_data))
