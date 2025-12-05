#!/bin/bash

# Consumer Group 상태를 2초 주기로 모니터링

echo "📊 Consumer Group 모니터링 시작 (2초 주기)"
echo "종료하려면 Ctrl+C를 누르세요"
echo "=========================================="
echo ""

while true; do
    clear
    echo "📊 Consumer Group 상태 - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="
    docker exec kafka1 /bin/bash -c 'cd /opt/kafka && bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group munova-recommendation-consumer --describe' 2>&1 | grep -v "WARN\|ERROR" | head -15
    echo ""
    echo "다음 업데이트까지 2초 대기 중... (Ctrl+C로 종료)"
    sleep 2
done
