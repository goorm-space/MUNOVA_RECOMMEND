#!/bin/bash

echo "🚀 배치 추천 시스템 시작 중..."

cd /app
export PYTHONPATH=/app:$PYTHONPATH

# config.yml 파일 확인
if [ ! -f "/app/config.yml" ] && [ ! -f "config.yml" ]; then
    echo "❌ 오류: config.yml 파일을 찾을 수 없습니다!"
    echo "   config.prod.yml.example을 복사하여 config.yml을 생성하세요:"
    echo "   cp config.prod.yml.example config.yml"
    echo "   그리고 MongoDB, Kafka 정보를 수정하세요."
    exit 1
fi

# MongoDB 연결 대기 (최대 60초)
echo "⏳ MongoDB 연결 대기 중..."
MAX_WAIT=60
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    python3 -c "
import sys
from services.mongodb import get_mongodb_db
db = get_mongodb_db()
if db is not None:
    sys.exit(0)
else:
    sys.exit(1)
" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "✅ MongoDB 연결 확인됨"
        break
    fi
    WAIT_COUNT=$((WAIT_COUNT + 5))
    echo "   대기 중... (${WAIT_COUNT}/${MAX_WAIT}초)"
    sleep 5
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    echo "❌ MongoDB 연결 실패. 배치 추천 시스템 종료."
    exit 1
fi

# 배치 추천 실행
echo "📊 배치 추천 프로세스 시작..."
python3 -m services.batch_recommender_runner








