#!/usr/bin/env python3
"""
CHƯƠNG TRÌNH 4: BIÊN ĐỘ GIÁ THEO TUẦN

CẤU TRÚC FILE gold_cleaned.csv:
--------------------------------
0: ID
1: DateTime (YYYY-MM-DD HH:MM:SS+00:00)
2: Open
3: High
4: Low
5: Close
6: Volume
"""
from pyspark import SparkContext
from datetime import datetime

sc = SparkContext("local[*]", "MR4_WeeklyRange")

print("CHƯƠNG TRÌNH 4: BIÊN ĐỘ GIÁ THEO TUẦN")

lines = sc.textFile("hdfs://localhost:9000/gold_weather/input/gold_cleaned.csv")
header = lines.first()
data = lines.filter(lambda x: x != header)

def mapper(line):
    try:
        parts = line.strip().split(',')
        if len(parts) < 7:
            raise ValueError("Không đủ cột dữ liệu")

        # Xác định tuần trong năm (YYYY-WW)
        dt = datetime.strptime(parts[1][:19], "%Y-%m-%d %H:%M:%S")
        week = f"{dt.year}-W{dt.isocalendar().week:02d}"

        high = float(parts[3])
        low = float(parts[4])
        daily_range = high - low

        return (week, (daily_range, 1))
    except Exception as e:
        print(f"[SKIP] {e} → {line}")
        return None

mapped = data.map(mapper).filter(lambda x: x is not None)

reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính trung bình biên độ theo tuần
result = reduced.map(lambda x: f"{x[0]},{x[1][0]/x[1][1]:.2f}")

print("\nKẾT QUẢ (10 dòng đầu):")
print("Week,Avg_Range")
for line in result.take(10):
    print(line)

result.saveAsTextFile("hdfs://localhost:9000/gold_weather/output/mr4_volatility")
print("\nĐã lưu: /gold_weather/output/mr4_volatility")

sc.stop()
