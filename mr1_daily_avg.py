#!/usr/bin/env python3
"""
CHƯƠNG TRÌNH 1: GIÁ TRUNG BÌNH THEO NGÀY

CẤU TRÚC FILE gold_cleaned.csv (thực tế):
------------------------------------------
0: STT
1: DateTime (YYYY-MM-DD HH:MM:SS+00:00)
2: Open   (Giá mở cửa)
3: High   (Giá cao nhất)
4: Low    (Giá thấp nhất)
5: Close  (Giá đóng cửa)
6: Volume (Khối lượng giao dịch)
"""

from pyspark import SparkContext

sc = SparkContext("local[*]", "MR1_DailyAvgPrice")

print("CHƯƠNG TRÌNH 1: GIÁ TRUNG BÌNH VÀNG THEO NGÀY")

# Đọc dữ liệu từ HDFS
lines = sc.textFile("hdfs://localhost:9000/gold_weather/input/gold_cleaned.csv")
header = lines.first()
data = lines.filter(lambda x: x != header)

# MAP
def mapper(line):
    try:
        parts = line.strip().split(',')
        if len(parts) < 7:
            raise ValueError("Không đủ cột dữ liệu")

        # Lấy ngày (bỏ giờ)
        date = parts[1].split(' ')[0]

        # Ép kiểu các giá trị số
        open_p = float(parts[2])
        high = float(parts[3])
        low = float(parts[4])
        close = float(parts[5])
        volume = float(parts[6])

        # Trả về (ngày, (close, high, low, count))
        return (date, (close, high, low, 1))
    except Exception as e:
        print(f"[SKIP] Lỗi dòng: {line} → {e}")
        return None

mapped = data.map(mapper).filter(lambda x: x is not None)

# REDUCE
reduced = mapped.reduceByKey(lambda a, b: (
    a[0] + b[0],
    a[1] + b[1],
    a[2] + b[2],
    a[3] + b[3]
))

# TÍNH TRUNG BÌNH
result = reduced.map(lambda x: (
    x[0],
    f"{x[0]},{x[1][0]/x[1][3]:.2f},{x[1][1]/x[1][3]:.2f},{x[1][2]/x[1][3]:.2f}"
))

# HIỂN THỊ MẪU
print("\nKẾT QUẢ (10 dòng đầu):")
print("Date,Avg_Close,Avg_High,Avg_Low")
for line in result.takeOrdered(10, key=lambda x: x[0]):
    print(line[1])

# LƯU RA HDFS
result.sortByKey().map(lambda x: x[1]) \
    .saveAsTextFile("hdfs://localhost:9000/gold_weather/output/mr1_daily_avg")

print("\nĐã lưu: /gold_weather/output/mr1_daily_avg")
sc.stop()
