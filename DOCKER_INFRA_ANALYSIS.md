# Docker 인프라 구성 검증 결과

## 📋 현재 구성 분석

### ✅ 정상 동작하는 부분

1. **네트워크 구성**
   - `recommend` 컨테이너와 `mongodb` 컨테이너가 `munova-net` 네트워크에 포함됨
   - MongoDB 호스트명 `mongodb`로 접근 가능

2. **Kafka Consumer 실행**
   - `start_server.sh`에서 Kafka Consumer가 정상적으로 시작됨
   - 환경변수 `KAFKA_CONSUMER_COUNT`로 Consumer 개수 제어 가능

3. **Config 파일 마운트**
   - `docker-compose.yml`에서 `.:/app` 볼륨 마운트로 `config.yml` 접근 가능

4. **PYTHONPATH 설정**
   - `start_server.sh`에서 `PYTHONPATH=/app` 설정됨

### ❌ 문제점 및 개선 필요 사항

#### 1. **배치 스크립트가 실행되지 않음** (치명적)

**문제:**
- `batch_recommender_runner.py`가 `docker-compose.yml`에 정의되어 있지 않음
- `start_server.sh`에서도 배치 스크립트를 실행하지 않음
- **결과: 5분 배치 추천이 전혀 실행되지 않음**

**해결 방안:**
- `docker-compose.yml`에 별도의 배치 컨테이너 추가
- 또는 `start_server.sh`에 배치 프로세스 추가

#### 2. **MongoDB 인덱스 생성 Race Condition**

**문제:**
- `mongodb.py`의 `create_indexes()`가 모듈 로드 시 자동 실행됨
- 여러 프로세스(Kafka Consumer 10개 + 배치 1개)가 동시에 인덱스 생성 시도 가능
- 중복 인덱스 생성 시도로 인한 경고/에러 발생 가능

**해결 방안:**
- 인덱스 생성에 lock 메커니즘 추가
- 또는 초기화 컨테이너로 한 번만 실행

#### 3. **Connection Pool 관리**

**문제:**
- 현재 설정: `max_pool_size: 50`, `min_pool_size: 10`
- Kafka Consumer 10개 + 배치 1개 = 최대 11개 프로세스
- 각 프로세스가 별도의 MongoClient를 생성하면 총 연결 수가 과도할 수 있음

**해결 방안:**
- 배치 프로세스는 별도의 connection pool 사용
- 또는 connection pool 크기 조정

#### 4. **배치 스크립트 실행 환경**

**문제:**
- `batch_recommender_runner.py`가 컨테이너 내부에서 실행 가능한지 확인 필요
- PYTHONPATH, 작업 디렉토리 설정 필요

**해결 방안:**
- 배치 전용 entrypoint 스크립트 생성
- 또는 docker-compose에서 직접 실행

## 🔧 수정 제안

### Option 1: 별도 배치 컨테이너 (권장)

장점:
- 독립적인 스케일링 가능
- 장애 격리
- 리소스 제한 설정 가능

### Option 2: 같은 컨테이너 내 배치 프로세스

장점:
- 리소스 효율적
- 네트워크 오버헤드 없음

단점:
- 하나의 컨테이너가 여러 역할 수행 (단일 책임 원칙 위배)
- 장애 격리 어려움

## 📊 최종 평가

### 현재 구성으로 배치 추천 동작 가능 여부: ❌ **불가능**

**이유:**
1. 배치 스크립트가 실행되지 않음
2. 배치 컨테이너/프로세스가 정의되어 있지 않음

### 필요한 수정 사항

1. ✅ 배치 컨테이너 추가 (필수)
2. ✅ MongoDB 인덱스 생성 race condition 해결 (권장)
3. ✅ Connection pool 최적화 (권장)
4. ✅ 배치 실행 스크립트 생성 (필수)







