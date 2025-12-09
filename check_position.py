#!/usr/bin/env python3
from confluent_kafka import Consumer, TopicPartition
import time

c = Consumer({
    'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
    'group.id': 'test-position-check',
    'auto.offset.reset': 'latest'
})

c.subscribe(['user_action_log'])
time.sleep(3)

partitions = c.assignment()
print(f'Assigned partitions: {[p.partition for p in partitions] if partitions else "None"}')

if partitions:
    tp = partitions[0]
    # 현재 position 확인
    try:
        position = c.position([tp])
        print(f'Current position: {position[0].offset if position else "None"}')
    except Exception as e:
        print(f'Position error: {e}')
    
    # Watermark 확인
    try:
        low, high = c.get_watermark_offsets(tp, timeout=1)
        print(f'Low watermark (earliest): {low}')
        print(f'High watermark (latest): {high}')
        if position:
            print(f'Position vs High: {position[0].offset} vs {high}')
            print(f'Difference: {high - position[0].offset}')
    except Exception as e:
        print(f'Watermark error: {e}')

c.close()



