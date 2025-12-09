#!/bin/bash

# MongoDB 데이터 삭제 스크립트
# 주의: 이 스크립트는 MongoDB 데이터를 삭제합니다!

set -e  # 에러 발생 시 스크립트 중단

echo "⚠️  MongoDB 데이터 삭제 스크립트"
echo "=========================================="
echo ""

# MongoDB 컨테이너 확인
if ! docker ps | grep -q recommend-mongodb; then
    echo "❌ MongoDB 컨테이너가 실행 중이 아닙니다!"
    exit 1
fi

# 현재 용량 확인
echo "📊 현재 데이터베이스 용량:"
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
db = db.getSiblingDB('recommend');
var totalStorage = 0;
var totalIndex = 0;
db.getCollectionNames().forEach(function(name) {
  try {
    var stats = db[name].stats();
    if (stats.count > 0) {
      var storageMB = Math.round(stats.storageSize / 1024 / 1024);
      var indexMB = Math.round(stats.totalIndexSize / 1024 / 1024);
      totalStorage += stats.storageSize;
      totalIndex += stats.totalIndexSize;
      print(name + ': ' + stats.count.toLocaleString() + '개 문서, ' + (storageMB + indexMB) + 'MB');
    }
  } catch(e) {}
});
var totalMB = Math.round((totalStorage + totalIndex) / 1024 / 1024);
print('총 용량: ' + totalMB + 'MB (' + Math.round(totalMB / 1024 * 100) / 100 + 'GB)');
" --quiet 2>&1 | grep -v "switched" || true

echo ""
echo "=========================================="
echo "삭제 옵션을 선택하세요:"
echo "1. user_action_logs 전체 삭제"
echo "2. recommendation_logs 전체 삭제"
echo "3. 모든 컬렉션 전체 삭제 (주의!)"
echo "4. 특정 날짜 이전 데이터만 삭제"
echo "5. 취소"
echo ""
read -p "선택 (1-5): " choice

# 입력 검증
if [[ ! "$choice" =~ ^[1-5]$ ]]; then
    echo "❌ 잘못된 선택입니다. 1-5 사이의 숫자를 입력하세요."
    exit 1
fi

case $choice in
  1)
    echo ""
    echo "⚠️  user_action_logs 컬렉션을 삭제합니다..."
    read -p "정말 삭제하시겠습니까? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
      echo "삭제 중..."
      docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      db = db.getSiblingDB('recommend');
      var count = db.user_action_logs.countDocuments({});
      print('삭제 전 문서 수: ' + count.toLocaleString() + '개');
      var result = db.user_action_logs.deleteMany({});
      print('✅ user_action_logs 삭제 완료 (삭제된 문서: ' + result.deletedCount.toLocaleString() + '개)');
      " --quiet 2>&1 | grep -v "switched" || true
      echo ""
      echo "✅ 삭제 완료!"
    else
      echo "❌ 취소되었습니다."
      exit 0
    fi
    ;;
  2)
    echo ""
    echo "⚠️  recommendation_logs 컬렉션을 삭제합니다..."
    read -p "정말 삭제하시겠습니까? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
      echo "삭제 중..."
      docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      db = db.getSiblingDB('recommend');
      var count = db.recommendation_logs.countDocuments({});
      print('삭제 전 문서 수: ' + count.toLocaleString() + '개');
      var result = db.recommendation_logs.deleteMany({});
      print('✅ recommendation_logs 삭제 완료 (삭제된 문서: ' + result.deletedCount.toLocaleString() + '개)');
      " --quiet 2>&1 | grep -v "switched" || true
      echo ""
      echo "✅ 삭제 완료!"
    else
      echo "❌ 취소되었습니다."
      exit 0
    fi
    ;;
  3)
    echo ""
    echo "⚠️⚠️⚠️  경고: 모든 컬렉션을 삭제합니다! ⚠️⚠️⚠️"
    read -p "정말 모든 데이터를 삭제하시겠습니까? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
      echo "삭제 중..."
      docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      db = db.getSiblingDB('recommend');
      db.getCollectionNames().forEach(function(name) {
        if (name !== '_index_creation_lock') {
          var count = db[name].countDocuments({});
          if (count > 0) {
            print('삭제 중: ' + name + ' (' + count.toLocaleString() + '개)');
            var result = db[name].deleteMany({});
            print('  삭제 완료: ' + result.deletedCount.toLocaleString() + '개');
          }
        }
      });
      print('✅ 모든 컬렉션 삭제 완료');
      " --quiet 2>&1 | grep -v "switched" || true
      echo ""
      echo "✅ 삭제 완료!"
    else
      echo "❌ 취소되었습니다."
      exit 0
    fi
    ;;
  4)
    echo ""
    echo "특정 날짜 이전 데이터를 삭제합니다."
    echo "예시: 2025-12-01 (YYYY-MM-DD 형식)"
    read -p "삭제할 날짜를 입력하세요 (이 날짜 이전 데이터 삭제): " delete_date
    if [ -n "$delete_date" ]; then
      echo ""
      echo "⚠️  $delete_date 이전 데이터를 삭제합니다..."
      read -p "정말 삭제하시겠습니까? (yes/no): " confirm
      if [ "$confirm" = "yes" ]; then
        echo "삭제 중..."
        docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
        db = db.getSiblingDB('recommend');
        var date = new Date('$delete_date');
        print('삭제 기준 날짜: ' + date.toISOString());
        
        // user_action_logs 삭제
        var count1 = db.user_action_logs.countDocuments({created_at: {\$lt: date}});
        print('user_action_logs 삭제 대상: ' + count1.toLocaleString() + '개');
        if (count1 > 0) {
          var result1 = db.user_action_logs.deleteMany({created_at: {\$lt: date}});
          print('✅ user_action_logs 삭제 완료 (삭제된 문서: ' + result1.deletedCount.toLocaleString() + '개)');
        } else {
          print('user_action_logs: 삭제할 데이터가 없습니다.');
        }
        
        // recommendation_logs 삭제 (created_at 필드가 있다면)
        try {
          var count2 = db.recommendation_logs.countDocuments({created_at: {\$lt: date}});
          print('recommendation_logs 삭제 대상: ' + count2.toLocaleString() + '개');
          if (count2 > 0) {
            var result2 = db.recommendation_logs.deleteMany({created_at: {\$lt: date}});
            print('✅ recommendation_logs 삭제 완료 (삭제된 문서: ' + result2.deletedCount.toLocaleString() + '개)');
          } else {
            print('recommendation_logs: 삭제할 데이터가 없습니다.');
          }
        } catch(e) {
          print('recommendation_logs는 created_at 필드가 없거나 삭제할 수 없습니다.');
        }
        " --quiet 2>&1 | grep -v "switched" || true
        echo ""
        echo "✅ 삭제 완료!"
      else
        echo "❌ 취소되었습니다."
        exit 0
      fi
    else
      echo "❌ 날짜를 입력하지 않았습니다."
      exit 1
    fi
    ;;
  5)
    echo "❌ 취소되었습니다."
    exit 0
    ;;
esac

echo ""
echo "📊 삭제 후 용량:"
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
db = db.getSiblingDB('recommend');
var totalStorage = 0;
var totalIndex = 0;
db.getCollectionNames().forEach(function(name) {
  try {
    var stats = db[name].stats();
    if (stats.count > 0) {
      totalStorage += stats.storageSize;
      totalIndex += stats.totalIndexSize;
    }
  } catch(e) {}
});
var totalMB = Math.round((totalStorage + totalIndex) / 1024 / 1024);
print('총 용량: ' + totalMB + 'MB (' + Math.round(totalMB / 1024 * 100) / 100 + 'GB)');
" --quiet 2>&1 | grep -v "switched" || true

echo ""
echo "💡 참고: MongoDB는 삭제 후에도 디스크 공간을 즉시 반환하지 않을 수 있습니다."
echo "   디스크 공간을 완전히 확보하려면 컨테이너를 재시작하거나 compact를 실행하세요."
echo ""
echo "   컨테이너 재시작: docker restart recommend-mongodb"





