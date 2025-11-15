#!/bin/bash

echo "🚀 모든 Redis Stream Consumer 실행 중..."
cd /app
export PYTHONPATH=/app:$PYTHONPATH
for i in {0..9}
do
  echo "실행!"
  python3 /app/services/redis/consumer_${i}.py &
done
wait