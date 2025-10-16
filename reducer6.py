#!/usr/bin/env python3
"""
REDUCER: Tính tổng lượng mưa theo ngày
"""

import sys

current_date = None
total_rain = 0

for line in sys.stdin:
    line = line.strip()
    date, rain = line.split('\t')
    rain = float(rain)
    
    if current_date != date:
        if current_date is not None:
            # Phân loại
            if total_rain == 0:
                category = "Không_mưa"
            elif total_rain < 5:
                category = "Mưa_nhỏ"
            elif total_rain < 20:
                category = "Mưa_vừa"
            else:
                category = "Mưa_lớn"
            
            print(f"{current_date}\t{round(total_rain, 2)}\t{category}")
        
        current_date = date
        total_rain = rain
    else:
        total_rain += rain

# In ngày cuối
if current_date is not None:
    if total_rain == 0:
        category = "Không_mưa"
    elif total_rain < 5:
        category = "Mưa_nhỏ"
    elif total_rain < 20:
        category = "Mưa_vừa"
    else:
        category = "Mưa_lớn"
    
    print(f"{current_date}\t{round(total_rain, 2)}\t{category}")