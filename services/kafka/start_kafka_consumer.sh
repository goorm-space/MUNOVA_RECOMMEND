#!/bin/bash

echo "🚀 Kafka Consumer 실행 중..."
cd /app
export PYTHONPATH=/app:$PYTHONPATH

# Kafka Consumer 실행
python3 -m services.kafka.kafka_consumer

