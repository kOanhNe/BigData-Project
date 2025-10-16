import pandas as pd

#Đọc file gốc
df = pd.read_csv("weather_hourly.csv")

#Đổi tên cột cho dễ hiểu
df.rename(columns={
    "time": "Datetime",
    "temperature_2m": "Temperature",
    "relative_humidity_2m": "Humidity",
    "precipitation": "Precipitation",
    "windspeed_10m": "WindSpeed",
    "winddirection_10m": "WindDirection",
    "pressure_msl": "Pressure",
    "cloudcover": "CloudCover"
}, inplace=True)

#Chuyển cột Datetime sang kiểu datetime thật
df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")

#Loại bỏ dòng bị lỗi hoặc thiếu thời gian
df = df.dropna(subset=["Datetime"])

#Sắp xếp đúng thứ tự thời gian
df = df.sort_values(by="Datetime")

#Kiểm tra nhanh dữ liệu
print(df.head())
print(df.tail())
print(df.info())

#Lưu lại file sạch
df.to_csv("weather_cleaned.csv", index=False)
print("Đã chuẩn hóa và lưu file thành weather_cleaned.csv thành công!")
