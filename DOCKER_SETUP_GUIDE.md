# Docker 인프라 설정 가이드

## 📋 수정 완료 사항

### ✅ 1. 배치 컨테이너 추가

**변경 파일:** `docker-compose.yml`

- `batch-recommender` 서비스 추가
- `recommend` 컨테이너와 동일한 이미지 사용
- `services/start_batch.sh` 스크립트로 실행
- MongoDB 연결 대기 로직 포함

**실행 방식:**
```yaml
batch-recommender:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: recommend-batch
  restart: always
  networks:
    - munova-net
  command: ["bash", "services/start_batch.sh"]
  depends_on:
    - mongodb
    - recommend
```

### ✅ 2. 배치 실행 스크립트 생성

**새 파일:** `services/start_batch.sh`

- MongoDB 연결 대기 (최대 60초)
- PYTHONPATH 설정
- `batch_recommender_runner.py` 실행

### ✅ 3. MongoDB 인덱스 생성 Race Condition 해결

**변경 파일:** `services/mongodb.py`

- Lock 메커니즘 추가 (`_index_creation_lock` 컬렉션 사용)
- 여러 프로세스가 동시에 인덱스 생성 시도 시 대기
- Lock TTL: 5분 (만료 시 자동 해제)
- Lock 획득 대기 시간: 최대 30초

**동작 방식:**
1. 인덱스 생성 전 Lock 획득 시도
2. 다른 프로세스가 Lock을 보유 중이면 대기
3. Lock 획득 성공 시 인덱스 생성
4. 완료 후 Lock 해제

### ✅ 4. Import 경로 수정

**변경 파일:**
- `services/batch_recommender_runner.py`: `from services.recommender_batch import BatchRecommender`
- `services/recommender_batch.py`: `from services.recommender import Recommender`

## 🔧 실행 방법

### 1. Docker Compose로 전체 시스템 시작

```bash
# 네트워크 생성 (이미 있으면 스킵)
docker network create munova-net

# 전체 시스템 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f batch-recommender
docker-compose logs -f recommend
```

### 2. 개별 컨테이너 상태 확인

```bash
# 모든 컨테이너 상태 확인
docker-compose ps

# 배치 컨테이너 로그 확인
docker logs -f recommend-batch

# Kafka Consumer 컨테이너 로그 확인
docker logs -f recommend
```

### 3. 배치 추천 시스템 동작 확인

```bash
# 배치 컨테이너에서 직접 실행 확인
docker exec -it recommend-batch bash
cd /app
python3 -m services.batch_recommender_runner
```

## 📊 시스템 구성도

```
┌─────────────────────────────────────────────────────────┐
│                    munova-net 네트워크                    │
│                                                           │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │   recommend  │    │ batch-       │    │  mongodb  │ │
│  │  (Kafka      │    │ recommender  │    │           │ │
│  │   Consumer)  │    │ (5분 배치)   │    │           │ │
│  └──────┬───────┘    └──────┬───────┘    └─────┬─────┘ │
│         │                   │                  │        │
│         └───────────────────┴──────────────────┘        │
│                          MongoDB                          │
└───────────────────────────────────────────────────────────┘
```

## ⚠️ 주의사항

### 1. Connection Pool 관리

**현재 설정:**
- `max_pool_size: 50`
- `min_pool_size: 10`

**실제 사용:**
- Kafka Consumer 10개: 각각 최대 5개 연결 = 최대 50개
- 배치 프로세스 1개: 최대 5개 연결 = 최대 5개
- **총 최대 연결 수: 55개** (하지만 max_pool_size 50으로 제한됨)

**권장 사항:**
- 배치 프로세스는 읽기 위주이므로 connection pool을 줄일 수 있음
- 필요시 `max_pool_size`를 60으로 증가 고려

### 2. MongoDB 인덱스 생성

- 첫 실행 시 `recommend` 컨테이너가 인덱스를 생성
- `batch-recommender` 컨테이너는 Lock을 통해 대기 후 인덱스 생성 시도
- 이미 인덱스가 있으면 자동으로 스킵됨 (MongoDB 기본 동작)

### 3. 배치 실행 주기

- 기본 주기: **5분 (300초)**
- `services/batch_recommender_runner.py`의 `INTERVAL_SECONDS`로 조정 가능

### 4. 장애 복구

**배치 컨테이너 재시작:**
```bash
docker-compose restart batch-recommender
```

**Kafka Consumer 컨테이너 재시작:**
```bash
docker-compose restart recommend
```

**전체 시스템 재시작:**
```bash
docker-compose down
docker-compose up -d
```

## 🧪 검증 체크리스트

### ✅ 필수 검증 항목

- [ ] `docker-compose up -d` 실행 후 모든 컨테이너가 정상 시작되는가?
- [ ] `recommend-batch` 컨테이너가 정상적으로 실행되는가?
- [ ] 배치 추천이 5분마다 실행되는가?
- [ ] MongoDB 인덱스가 정상적으로 생성되는가?
- [ ] Kafka Consumer와 배치가 동시에 MongoDB에 접근 가능한가?
- [ ] 배치 실행 시 에러가 발생하지 않는가?

### 검증 명령어

```bash
# 1. 컨테이너 상태 확인
docker-compose ps

# 2. 배치 로그에서 "배치 추천 시작" 메시지 확인
docker logs recommend-batch | grep "배치 추천"

# 3. MongoDB 인덱스 확인
docker exec -it recommend-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin
use recommend
db.user_action_summaries.getIndexes()
db.user_recommendations.getIndexes()

# 4. 배치 실행 주기 확인 (5분마다 실행되는지)
docker logs -f recommend-batch
```

## 📝 추가 최적화 권장사항

### 1. 배치 전용 Connection Pool 설정

배치 프로세스는 읽기 위주이므로 별도의 connection pool 설정을 고려:

```python
# services/mongodb.py에 배치 전용 설정 추가
BATCH_MAX_POOL_SIZE = 5
BATCH_MIN_POOL_SIZE = 2
```

### 2. 배치 실행 시간 모니터링

배치 실행 시간이 5분을 초과하면 다음 배치와 겹칠 수 있으므로 모니터링 필요:

```python
# services/batch_recommender_runner.py에 실행 시간 로깅 추가
import time
start_time = time.time()
batch.rebuild_all_user_recommendations()
elapsed = time.time() - start_time
logger.info(f"배치 실행 시간: {elapsed:.2f}초")
```

### 3. Health Check 추가

docker-compose.yml에 health check 추가:

```yaml
batch-recommender:
  healthcheck:
    test: ["CMD", "python3", "-c", "from services.mongodb import get_mongodb_db; exit(0 if get_mongodb_db() else 1)"]
    interval: 30s
    timeout: 10s
    retries: 3
```

## 🎯 최종 평가

### ✅ 현재 구성으로 배치 추천 동작 가능 여부: **가능**

**이유:**
1. ✅ 배치 컨테이너가 추가됨
2. ✅ 배치 실행 스크립트가 생성됨
3. ✅ MongoDB 인덱스 생성 race condition 해결됨
4. ✅ Import 경로 수정 완료
5. ✅ 네트워크 및 의존성 설정 완료

**남은 작업:**
- 실제 환경에서 테스트 및 검증
- 필요시 connection pool 크기 조정
- 모니터링 및 알림 설정







