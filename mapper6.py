#!/usr/bin/env python3
"""
MAPPER: Lấy ngày và lượng mưa
"""

import sys

for line in sys.stdin:
    
    if line.startswith('Datetime'):
        continue
    
    try:
        parts = line.strip().split(',')
        date = parts[0].split(' ')[0]
        rain = float(parts[3])  # Cột Precipitation
        
        print(f"{date}\t{rain}")
    
    except:
        continue