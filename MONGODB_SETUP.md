# MongoDB 로그 적재 설정 완료

## ✅ 구현 완료

### 1. MongoDB 연결 설정
- **파일**: `services/mongodb.py`
- **상태**: ✅ 연결 성공
- **설정**: `config.yml`에 MongoDB 설정 있음
  - Host: mongodb (Docker 서비스명)
  - Port: 27017
  - Database: recommend
  - Username: admin
  - Password: admin123
  - Enabled: true

### 2. 로그 저장 기능 추가
- **파일**: `services/recommender.py`
- **기능**: 
  - 사용자 행동 로그 저장 (`user_action_logs` 컬렉션)
  - 추천 로그 저장 (`recommendation_logs` 컬렉션)

### 3. 저장되는 데이터

#### 사용자 행동 로그 (`user_action_logs`)
```json
{
  "member_id": 12345,
  "product_id": 67890,
  "event_type": "product_detail_view",
  "stream_key": "user_action_log",
  "message_id": "12345",
  "raw_fields": {
    "eventType": "product_detail_view",
    "service": "product",
    "memberId": 12345,
    "data": {"product_id": 67890},
    "_kafka_metadata": {
      "topic": "user_action_log",
      "partition": 0,
      "offset": 12345
    }
  },
  "created_at": "2024-12-02T..."
}
```

#### 추천 로그 (`recommendation_logs`)
```json
{
  "member_id": 12345,
  "product_id": 67890,
  "score": 0.85,
  "event_type": "product_detail_view",
  "metadata": {
    "kafka_topic": "user_action_log",
    "kafka_partition": 0,
    "kafka_offset": 12345,
    "category_id": 1,
    "brand_id": 2,
    "price": 10000
  },
  "created_at": "2024-12-02T..."
}
```

### 4. 인덱스 자동 생성
- **member_id + product_id**: 복합 인덱스
- **created_at**: 시간 기반 인덱스
- **event_type**: 이벤트 타입 인덱스

---

## 🚀 사용 방법

### Consumer 재시작
```bash
docker restart recommend
```

### MongoDB 데이터 확인
```bash
# 사용자 행동 로그 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "use recommend; db.user_action_logs.find().limit(5).pretty()"

# 추천 로그 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "use recommend; db.recommendation_logs.find().limit(5).pretty()"

# 문서 수 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "use recommend; db.user_action_logs.countDocuments()"
```

---

## ⚠️ 성능 고려사항

### 현재 구현
- 각 메시지마다 개별 `insert_one()` 호출
- 동기 처리 (MongoDB 응답 대기)

### 초당 100만건 처리 시
- **현재 방식**: 각 메시지마다 MongoDB 쓰기
- **예상 병목**: MongoDB 쓰기 성능
- **권장**: 배치 처리로 개선 필요

### 향후 개선 방안

#### 1. 배치 Insert (권장)
```python
# 배치 단위로 모아서 insert_many() 사용
documents = []
for message in batch:
    documents.append(document)
    
if documents:
    collection.insert_many(documents, ordered=False)
```

#### 2. 비동기 처리
- MongoDB 쓰기를 별도 스레드/프로세스로 처리
- 메인 처리 흐름과 분리

#### 3. Write Concern 조정
- `w=0` 또는 `w=1`로 설정하여 성능 향상
- 데이터 손실 가능성 있음 (주의 필요)

---

## 📊 모니터링

### MongoDB 성능 확인
```bash
# 현재 연결 수 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.serverStatus().connections"

# 쓰기 성능 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.serverStatus().opcounters"
```

### 컬렉션 통계
```bash
# 컬렉션 크기 확인
docker exec recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "use recommend; db.user_action_logs.stats()"
```

---

## 🔧 설정 확인

### config.yml
```yaml
mongodb:
  host: mongodb
  port: 27017
  database: recommend
  username: admin
  password: admin123
  enabled: true
  auth_source: admin
  max_pool_size: 100
  min_pool_size: 10
```

### MongoDB 연결 풀
- **최대 연결**: 100개
- **최소 연결**: 10개
- **타임아웃**: 5초

---

## ✅ 다음 단계

1. **Consumer 재시작**: MongoDB 로그 저장 기능 활성화
2. **테스트**: 메시지 전송 후 MongoDB 확인
3. **모니터링**: 성능 및 저장 확인
4. **최적화**: 필요 시 배치 처리로 개선

