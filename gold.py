import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

# =====================================
# --- Hàm đọc và xử lý dữ liệu ---
# =====================================
@st.cache_data
def load_gold_data(csv_file="gold_cleaned.csv"):
    gold = pd.read_csv(csv_file, parse_dates=["Datetime"])
    gold = gold.drop(columns=["Unnamed: 0"], errors="ignore")
    gold["Datetime"] = pd.to_datetime(gold["Datetime"], utc=True).dt.tz_convert(None)
    gold = gold.set_index("Datetime").sort_index()
    # Tạo các cột phân tích
    gold["Return"] = gold["Close"].pct_change()
    gold["MA7"] = gold["Close"].rolling(7).mean()
    gold["MA30"] = gold["Close"].rolling(30).mean()
    gold["Volatility30"] = gold["Return"].rolling(30).std()
    gold["Spread"] = gold["High"] - gold["Low"]
    return gold

# =====================================
# --- Hàm tính KPI ---
# =====================================
def get_gold_kpi(gold):
    latest_close = gold["Close"].iloc[-1]
    avg_30d = gold["MA30"].iloc[-1]
    volatility = gold["Volatility30"].iloc[-30:].mean() * 100
    avg_return = gold["Return"].mean() * 100
    avg_volume = gold["Volume"].mean()
    return {
        "Giá đóng cửa gần nhất": latest_close,
        "Trung bình 30 ngày": avg_30d,
        "Độ biến động 30 ngày (%)": volatility,
        "Lợi suất trung bình (%)": avg_return,
        "Khối lượng giao dịch trung bình": avg_volume
    }

# =====================================
# --- Hàm tạo biểu đồ ---
# =====================================
def plot_gold_trend(gold):
    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(gold.index, gold["Close"], color="gold", label="Close")
    ax.plot(gold.index, gold["MA7"], color="red", linestyle="--", label="MA 7 ngày")
    ax.plot(gold.index, gold["MA30"], color="blue", linestyle=":", label="MA 30 ngày")
    ax.set_title("Xu hướng giá vàng theo thời gian")
    ax.set_xlabel("Datetime")
    ax.set_ylabel("Giá vàng (USD/oz)")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig

def plot_gold_return_hist(gold):
    fig, ax = plt.subplots(figsize=(8,4))
    ax.hist(gold["Return"].dropna()*100, bins=40, color="orange", edgecolor="black")
    ax.set_title("Phân phối lợi suất hàng ngày của giá vàng")
    ax.set_xlabel("Lợi suất ngày (%)")
    ax.set_ylabel("Số ngày")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig

def plot_gold_volatility(gold):
    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(gold.index, gold["Volatility30"], color="purple")
    ax.set_title("Độ biến động (Volatility) 30 ngày của giá vàng")
    ax.set_xlabel("Datetime")
    ax.set_ylabel("Độ lệch chuẩn lợi suất (σ)")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig

def plot_volume_vs_spread(gold):
    fig, ax = plt.subplots(figsize=(8,5))
    ax.scatter(gold["Volume"], gold["Spread"], alpha=0.3, color="green")
    ax.set_title("Mối quan hệ giữa khối lượng giao dịch và độ biến động giá")
    ax.set_xlabel("Volume")
    ax.set_ylabel("Spread (High - Low)")
    plt.tight_layout()
    return fig
