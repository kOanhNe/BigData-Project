#!/usr/bin/env python3
"""
REDUCER: Tính nhiệt độ trung bình theo ngày
"""

import sys

current_date = None
total_temp = 0
count = 0

# Đọc từ stdin (output của mapper)
for line in sys.stdin:
    line = line.strip()
    date, temp = line.split('\t')
    temp = float(temp)
    
    # Nếu gặp ngày mới
    if current_date != date:
        # In kết quả ngày cũ
        if current_date is not None:
            avg_temp = round(total_temp / count, 1)
            print(f"{current_date}\t{avg_temp}")
        
        # Reset cho ngày mới
        current_date = date
        total_temp = temp
        count = 1
    else:
        # Cùng ngày, cộng dồn
        total_temp += temp
        count += 1

# In ngày cuối cùng
if current_date is not None:
    avg_temp = round(total_temp / count, 1)
    print(f"{current_date}\t{avg_temp}")