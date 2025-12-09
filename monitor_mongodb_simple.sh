#!/bin/bash

# MongoDB 저장량 간단 모니터링 (watch 명령어용)

docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin recommend --eval "
var collections = ['user_action_logs', 'recommendation_logs'];
var totalDocs = 0;
var totalSize = 0;

print('=== MongoDB 저장량 ===');
print('시간: ' + new Date().toLocaleString());
print('');

collections.forEach(function(name) {
    try {
        var count = db[name].countDocuments({});
        var stats = db[name].stats();
        var sizeMB = Math.round((stats.storageSize + stats.totalIndexSize) / 1024 / 1024);
        totalDocs += count;
        totalSize += stats.storageSize + stats.totalIndexSize;
        print(name + ': ' + count.toLocaleString() + '개 문서, ' + sizeMB + 'MB');
    } catch(e) {
        print(name + ': 컬렉션 없음');
    }
});

print('');
print('총: ' + totalDocs.toLocaleString() + '개 문서, ' + Math.round(totalSize / 1024 / 1024) + 'MB');
" --quiet 2>&1 | grep -v "switched"





