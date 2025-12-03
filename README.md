# MUNOVA_RECOMMEND
munova의 추천서버

## 개요

Munova 추천 서버는 사용자 행동 로그를 소비하여 실시간 추천을 제공하는 서비스입니다.
현재는 **Kafka**를 통해 사용자 행동 로그를 소비합니다.

## Kafka Consumer

### 요구사항

- Python 3.8+
- Kafka 브로커 (KRaft 모드 또는 Zookeeper 모드)
- confluent-kafka 라이브러리

### 설치

#### Docker 환경 (권장)

Docker 컨테이너 내부에서는 이미 설치되어 있습니다:

```bash
docker exec recommend pip list | grep confluent-kafka
```

#### 로컬 개발 환경 (macOS)

로컬에서 개발할 경우 `librdkafka` 라이브러리가 필요합니다:

```bash
# 1. librdkafka 설치
brew install librdkafka

# 2. 환경 변수 설정 (가상환경 활성화 전)
export C_INCLUDE_PATH=$(brew --prefix librdkafka)/include:$C_INCLUDE_PATH
export LIBRARY_PATH=$(brew --prefix librdkafka)/lib:$LIBRARY_PATH

# 3. 가상환경 활성화 및 설치
source .venv/bin/activate
pip install confluent-kafka==2.3.0
```

또는 `setup_local_env.sh` 스크립트 사용:

```bash
source setup_local_env.sh
source .venv/bin/activate
pip install confluent-kafka==2.3.0
```

### 설정

`config.yml` 파일에서 Kafka 설정을 확인하세요:

```yaml
kafka:
  bootstrap_servers: localhost:9092  # 로컬: localhost:9092, Docker: kafka:9092
  consumer_group_id: munova-recommendation-consumer
  topic: user_action_log
  batch_size: 10  # 배치 처리 크기
  metrics_port: 9000  # Prometheus 메트릭 서버 포트
```

### 실행

#### 직접 실행

```bash
python -m services.kafka.kafka_consumer
```

#### 스크립트 실행

```bash
./services/kafka/start_kafka_consumer.sh
```

### 메시지 형식

Kafka 토픽 `user_action_log`에서 소비하는 메시지 형식:

```json
{
  "eventType": "product_detail_view",
  "service": "product",
  "memberId": 12345,
  "data": {
    "product_id": 67890
  },
  "eventTime": "2024-01-01T00:00:00Z",
  "eventTimestamp": 1704067200000,
  "producerTime": 1704067200000,
  "version": 1
}
```

### 지원하는 이벤트 타입

- `product_detail_view`: 상품 상세 페이지 조회
- `product_like`: 상품 좋아요
- `cancel_product_like`: 상품 좋아요 취소
- `product_add_cart`: 장바구니 추가
- `product_search`: 상품 검색

### 특징

- **배치 처리**: 10개 메시지를 한 번에 처리
- **수동 커밋**: 처리 성공 시에만 오프셋 커밋
- **에러 핸들링**: 처리 실패 시 오프셋 커밋하지 않음 (재시도 가능)
- **Graceful Shutdown**: SIGINT/SIGTERM 시 안전하게 종료
- **메트릭**: Prometheus 메트릭 수집
- **Latency 추적**: Producer Time을 이용한 지연 시간 측정

### 모니터링

#### Prometheus 메트릭

- `kafka_consumer_messages_total`: 처리된 메시지 수
- `kafka_consumer_latency_ms`: 메시지 처리 지연 시간
- `kafka_consumer_errors_total`: 에러 수
- `kafka_consumer_messages_processed`: 처리된 메시지 수 (Gauge)

메트릭은 기본적으로 `http://localhost:9000/metrics`에서 확인할 수 있습니다.

#### 로그

로그는 INFO 레벨로 출력되며, 다음 정보를 포함합니다:
- 메시지 처리 상태
- 에러 정보
- 주기적인 처리 통계 (100개마다)

### 로컬 테스트

1. Kafka 브로커 실행 (예: Docker Compose)

```bash
docker-compose up -d kafka
```

2. 테스트 메시지 전송

```bash
# Kafka console producer 사용
kafka-console-producer --bootstrap-server localhost:9092 --topic user_action_log
```

3. Consumer 실행

```bash
python -m services.kafka.kafka_consumer
```

### Consumer Lag 확인

Kafka Consumer Group의 lag을 확인하려면:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group munova-recommendation-consumer \
  --describe
```

### 문제 해결

#### Consumer가 메시지를 받지 못하는 경우

1. Kafka 브로커 연결 확인
2. 토픽 존재 여부 확인: `kafka-topics --list --bootstrap-server localhost:9092`
3. Consumer Group 상태 확인: `kafka-consumer-groups --bootstrap-server localhost:9092 --group munova-recommendation-consumer --describe`

#### 처리 속도가 느린 경우

- `batch_size` 설정을 조정하세요 (기본값: 10)
- Consumer 인스턴스를 여러 개 실행하여 파티션 분산 처리

#### 메시지 처리 실패 시

- 로그에서 에러 원인 확인
- 실패한 메시지는 오프셋이 커밋되지 않으므로 재시도 가능
- 필요시 DLQ(Dead Letter Queue) 구현 고려
