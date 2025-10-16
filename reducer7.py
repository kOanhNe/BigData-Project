#!/usr/bin/env python3
"""
REDUCER: Đếm tổng số giờ gió mạnh theo hướng
"""

import sys

current_direction = None
count = 0

for line in sys.stdin:
    direction, num = line.strip().split('\t')
    num = int(num)
    
    if current_direction != direction:
        if current_direction is not None:
            print(f"{current_direction}\t{count}")
        
        current_direction = direction
        count = num
    else:
        count += num

if current_direction is not None:
    print(f"{current_direction}\t{count}")