#!/usr/bin/env python3
"""
CHƯƠNG TRÌNH 3: XU HƯỚNG GIÁ THEO GIỜ

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

sc = SparkContext("local[*]", "MR3_HourlyTrend")

print("CHƯƠNG TRÌNH 3: XU HƯỚNG GIÁ THEO GIỜ")

lines = sc.textFile("hdfs://localhost:9000/gold_weather/input/gold_cleaned.csv")
header = lines.first()
data = lines.filter(lambda x: x != header)

def mapper(line):
    try:
        parts = line.strip().split(',')
        if len(parts) < 7:
            raise ValueError("Không đủ cột dữ liệu")
        # Lấy giờ: YYYY-MM-DD HH
        hour = parts[1][:13]
        open_p = float(parts[2])
        close_p = float(parts[5])

        if close_p > open_p:
            trend = "Tăng"
        elif close_p < open_p:
            trend = "Giảm"
        else:
            trend = "Không_đổi"
        return ((hour, trend), 1)
    except Exception as e:
        print(f"[SKIP] {e} → {line}")
        return None

mapped = data.map(mapper).filter(lambda x: x is not None)
reduced = mapped.reduceByKey(lambda a, b: a + b)

# (hour, trend, count)
result = reduced.map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")

print("\nKẾT QUẢ (10 dòng đầu):")
print("Hour,Trend,Count")
for line in result.take(10):
    print(line)

result.saveAsTextFile("hdfs://localhost:9000/gold_weather/output/mr3_trend")
print("\nĐã lưu: /gold_weather/output/mr3_trend")

sc.stop()
