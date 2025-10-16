import pandas as pd

#Đọc file và bỏ qua dòng đầu tiên vì đó là tên ticker (GC=F)
df = pd.read_csv("gold_hourly_1_year.csv", skiprows=2)

#Đổi tên cột cho gọn
df.columns = ["Datetime", "Close", "High", "Low", "Open", "Volume"]

# Chuyển Datetime sang datetime thực
df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')


#Xóa các dòng bị trống hoặc lỗi datetime
df  = df.dropna(subset=["Datetime"])

#Reset index bắt đầu từ 1
df.index = range(1, len(df) + 1)

#Kiểm tra giá trị null
print(df.isnull().sum())

print(df.head())
print(df.tail())

#Lưu lại file sạch
df.to_csv("gold_cleaned.csv")
print("Đã làm sạch và lưu file thành gold_cleaned.csv")

