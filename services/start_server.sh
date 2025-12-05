#!/bin/bash

echo "🚀 추천 서버 시작 중..."

cd /app
export PYTHONPATH=/app:$PYTHONPATH

# 1. FastAPI 서버 시작 (백그라운드)
echo "📡 FastAPI 서버 시작..."
python3 -m uvicorn services.api_server:app --host 0.0.0.0 --port 8001 &
API_PID=$!
echo "✅ FastAPI 서버 시작됨 (PID: $API_PID)"

# 2. Kafka Consumer 시작 (백그라운드)
# Consumer 개수는 config.yml의 consumer_count에서 가져오거나 기본값 5개 사용
echo "📨 Kafka Consumer 시작..."
CONSUMER_PIDS=()
CONSUMER_COUNT=${KAFKA_CONSUMER_COUNT:-5}  # 환경변수 또는 기본값 5개
for i in $(seq 0 $((CONSUMER_COUNT - 1))); do
    METRICS_PORT=$((9000 + i))
    export KAFKA_METRICS_PORT=$METRICS_PORT
    python3 -m services.kafka.kafka_consumer &
    CONSUMER_PIDS+=($!)
    echo "✅ Consumer-$i 시작됨 (PID: ${CONSUMER_PIDS[$i]}, 메트릭 포트: $METRICS_PORT)"
    sleep 0.5  # 각 Consumer 시작 간격
done
echo "✅ 총 ${#CONSUMER_PIDS[@]}개 Consumer 시작 완료"

# 종료 시그널 처리
trap "echo '🛑 서버 종료 중...'; kill $API_PID 2>/dev/null; for pid in ${CONSUMER_PIDS[@]}; do kill \$pid 2>/dev/null; done; wait; exit" SIGTERM SIGINT

# 대기
echo "⏳ 서버가 실행 중입니다."
echo "📡 FastAPI: http://0.0.0.0:8001"
echo "📚 API 문서: http://0.0.0.0:8001/docs"
echo "종료하려면 Ctrl+C를 누르세요."

# 프로세스들이 종료될 때까지 대기
wait

