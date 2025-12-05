#!/bin/bash

# recommend 서버 시스템 메트릭 확인 스크립트

echo "📊 Recommend 서버 시스템 메트릭"
echo "=========================================="
echo ""

# CPU 사용률
echo "🖥️  CPU 사용률:"
docker stats recommend --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}"
echo ""

# 메모리 상세 정보
echo "💾 메모리 상세 정보:"
docker exec recommend sh -c 'cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || echo "메모리 정보를 가져올 수 없습니다"' | awk '{printf "  사용 중: %.2f MB\n", $1/1024/1024}'
docker exec recommend sh -c 'cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo "메모리 정보를 가져올 수 없습니다"' | awk '{printf "  제한: %.2f MB\n", $1/1024/1024}'
echo ""

# 프로세스 정보
echo "🔧 프로세스 정보:"
docker exec recommend sh -c 'ls /proc/*/comm 2>/dev/null | wc -l' | awk '{print "  프로세스 수: " $1}'
echo ""

# 네트워크 통계
echo "🌐 네트워크 통계:"
docker exec recommend sh -c 'cat /proc/net/sockstat 2>/dev/null | head -3 || echo "네트워크 정보를 가져올 수 없습니다"'
echo ""

# 디스크 사용량
echo "💿 디스크 사용량:"
docker exec recommend sh -c 'df -h / 2>/dev/null | tail -1 || echo "디스크 정보를 가져올 수 없습니다"'
echo ""

echo "=========================================="
echo "💡 2초마다 자동 업데이트하려면:"
echo "   watch -n 2 ./check_server_metrics.sh"
echo ""


