#!/bin/bash

echo "🚀 추천 서버 시작 중..."

cd /app
export PYTHONPATH=/app:$PYTHONPATH

# 1. FastAPI 서버 시작 (백그라운드)
echo "📡 FastAPI 서버 시작..."
python3 -m uvicorn services.api_server:app --host 0.0.0.0 --port 8001 &
API_PID=$!
echo "✅ FastAPI 서버 시작됨 (PID: $API_PID)"

# FastAPI 서버가 시작될 때까지 잠시 대기
sleep 3

# 2. Redis Stream Consumer 시작 (Redis 연결 실패 시 종료됨)
echo "🔄 Redis Stream Consumer 시작..."
bash services/redis/start_all_consumers.sh &
CONSUMER_PID=$!
echo "✅ Consumer 시작됨 (PID: $CONSUMER_PID)"

# 종료 시그널 처리
trap "echo '🛑 서버 종료 중...'; kill $API_PID $CONSUMER_PID 2>/dev/null; wait; exit" SIGTERM SIGINT

# 대기
echo "⏳ 서버가 실행 중입니다."
echo "📡 FastAPI: http://0.0.0.0:8001"
echo "📚 API 문서: http://0.0.0.0:8001/docs"
echo "종료하려면 Ctrl+C를 누르세요."

# 프로세스들이 종료될 때까지 대기
wait

