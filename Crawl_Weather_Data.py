import requests
import pandas as pd

# Toạ độ TP.HCM
lat, lon = 10.8427, 106.6757

# Khoảng thời gian ~3 tháng (90 ngày ≈ 2160 records theo giờ)
start_date = "2025-06-01"
end_date = "2025-08-31"

url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={lat}&longitude={lon}"
    f"&start_date=2025-06-01&end_date=2025-08-31"
    f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
    f"windspeed_10m,winddirection_10m,pressure_msl,cloudcover"
)

# Gửi request
response = requests.get(url)
data = response.json()

# Trích xuất dữ liệu theo giờ
hours = {var: data["hourly"][var] for var in data["hourly"]}

# Chuyển sang DataFrame
df = pd.DataFrame(hours)


# Lưu ra CSV
filename = "weather_hourly.csv"
df.to_csv(filename, index=False, encoding="utf-8")

print(" Đã crawl xong, lưu vào:", filename)
print(" Số records:", len(df))
print(df.head())