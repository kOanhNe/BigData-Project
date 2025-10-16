#!/usr/bin/env python3
"""
MAPPER: Đọc dữ liệu thời tiết, xuất (ngày, nhiệt độ)
"""

import sys

# Đọc từ stdin (dữ liệu từ Hadoop)
for line in sys.stdin:
    line = line.strip()
    
    # Bỏ header
    if line.startswith('Datetime'):
        continue
    
    try:
        parts = line.split(',')
        datetime = parts[0].split(' ')[0]
        date = datetime.split(' ')[0]  # Lấy phần ngày
        temperature = float(parts[1])

        # Output: ngày \t nhiệt độ
        print(f"{date}\t{temperature}")
    
    except:
        continue