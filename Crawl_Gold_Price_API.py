import yfinance as yf

# Mã vàng quốc tế: 
symbol = "GC=F" #Gold Future


# Crawl dữ liệu 1 năm gần nhất, theo từng giờ
gold = yf.download(
    tickers=symbol,
    period="1y",      # khoảng thời gian: 1 năm gần nhất
    interval="1h"      # dữ liệu theo từng giờ
)

# Xem 5 dòng đầu
print(gold.head())

# Lưu vào CSV
filename = "gold_hourly_1_year.csv"
gold.to_csv(filename)
print(f"Đã lưu dữ liệu vàng theo giờ vào {filename}")
