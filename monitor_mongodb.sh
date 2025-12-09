#!/bin/bash

# MongoDB 저장량 실시간 모니터링 스크립트

echo "📊 MongoDB 저장량 모니터링 시작"
echo "종료하려면 Ctrl+C를 누르세요"
echo "=========================================="
echo ""

INTERVAL=${1:-2}  # 기본 2초 간격

while true; do
    clear
    echo "📊 MongoDB 저장량 모니터링 - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="
    
    docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin recommend --eval "
    var collections = ['user_action_logs', 'recommendation_logs', 'user_action_summaries', 'recommendation_history'];
    var totalDocs = 0;
    var totalSize = 0;
    
    print('컬렉션별 문서 수 및 용량:');
    print('----------------------------------------');
    
    collections.forEach(function(name) {
        try {
            var count = db[name].countDocuments({});
            if (count > 0 || name === 'user_action_logs' || name === 'recommendation_logs') {
                var stats = db[name].stats();
                var sizeMB = Math.round((stats.storageSize + stats.totalIndexSize) / 1024 / 1024);
                totalDocs += count;
                totalSize += stats.storageSize + stats.totalIndexSize;
                print(name + ':');
                print('  문서 수: ' + count.toLocaleString() + '개');
                print('  용량: ' + sizeMB + 'MB');
                print('');
            }
        } catch(e) {
            // 컬렉션이 없으면 무시
        }
    });
    
    print('----------------------------------------');
    print('총 문서 수: ' + totalDocs.toLocaleString() + '개');
    print('총 용량: ' + Math.round(totalSize / 1024 / 1024) + 'MB (' + Math.round(totalSize / 1024 / 1024 / 1024 * 100) / 100 + 'GB)');
    " --quiet 2>&1 | grep -v "switched"
    
    echo ""
    echo "다음 업데이트까지 ${INTERVAL}초 대기 중... (Ctrl+C로 종료)"
    sleep $INTERVAL
done





