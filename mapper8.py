#!/usr/bin/env python3
"""
MAPPER: Đếm số giờ áp suất thấp theo ngày
"""

import sys

for line in sys.stdin:
    line = line.strip()
    
    if line.startswith('Datetime'):
        continue
    
    try:
        parts = line.split(',')
        date = parts[0].split(' ')[0]
        pressure = float(parts[6])
        
        # Chỉ đếm áp suất thấp
        if pressure < 1008:
            print(f"{date}\t1")
    
    except:
        continue