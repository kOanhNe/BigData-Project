#!/usr/bin/env python3
"""
REDUCER: Đếm tổng số giờ áp suất thấp theo ngày
"""

import sys

current_date = None
count = 0

for line in sys.stdin:
    line = line.strip()
    
    try:
        date, num = line.split('\t')
        num = int(num)
        
        if current_date != date:
            if current_date is not None:
                # Chỉ in những ngày có áp suất thấp
                if count > 0:
                    print(f"{current_date}\t{count}\tCẢNH_BÁO")
            
            current_date = date
            count = num
        else:
            count += num
    except:
        continue

if current_date is not None and count > 0:
    print(f"{current_date}\t{count}\tCẢNH_BÁO")