#!/usr/bin/env python3
"""
CHƯƠNG TRÌNH 2: KHỐI LƯỢNG CAO NHẤT THEO THÁNG

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

sc = SparkContext("local[*]", "MR2_MonthlyMaxVolume")

print("CHƯƠNG TRÌNH 2: KHỐI LƯỢNG CAO NHẤT THEO THÁNG")


lines = sc.textFile("hdfs://localhost:9000/gold_weather/input/gold_cleaned.csv")
header = lines.first()
data = lines.filter(lambda x: x != header)

def mapper(line):
    try:
        parts = line.strip().split(',')
        if len(parts) < 7:
            raise ValueError("Không đủ cột dữ liệu")
        # Lấy tháng YYYY-MM
        month = parts[1][:7]
        volume = float(parts[6])
        return (month, volume)
    except Exception as e:
        print(f"[SKIP] {e} → {line}")
        return None

mapped = data.map(mapper).filter(lambda x: x is not None)
reduced = mapped.reduceByKey(lambda a, b: max(a, b))

result = reduced.sortByKey().map(lambda x: f"{x[0]},{x[1]:.0f}")

print("\nKẾT QUẢ:")
print("Month,Max_Volume")
for line in result.collect():
    print(line)

result.saveAsTextFile("hdfs://localhost:9000/gold_weather/output/mr2_max_volume")
print("\nĐã lưu: /gold_weather/output/mr2_max_volume")

sc.stop()
