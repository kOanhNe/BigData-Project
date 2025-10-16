#!/usr/bin/env python3
"""
MAPPER: Đếm số giờ gió mạnh (>10 km/h) theo hướng
"""

import sys

for line in sys.stdin:
    
    if line.startswith('Datetime'):
        continue
    
    try:
        parts = line.strip().split(',')
        wind_speed = float(parts[4])
        wind_direction = float(parts[5])
        
        # Chỉ đếm gió mạnh
        if wind_speed <= 2.78:  # 10 km/h ≈ 2.78 m/s
            continue
        
        # Phân loại hướng
        if wind_direction < 45 or wind_direction >= 315:
            direction = "Bắc"
        elif wind_direction < 90:
            direction = "Đông_Bắc"
        elif wind_direction < 135:
            direction = "Đông"
        elif wind_direction < 180:
            direction = "Đông_Nam"
        elif wind_direction < 225:
            direction = "Nam"
        elif wind_direction < 270:
            direction = "Tây_Nam"
        elif wind_direction < 315:
            direction = "Tây"
        else:
            direction = "Tây_Bắc"
        
        print(f"{direction}\t1")
    
    except:
        continue