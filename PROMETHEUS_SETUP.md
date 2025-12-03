# Prometheus Consumer 메트릭 설정 가이드

## 문제: Grafana에서 "nodata" 표시

Consumer 메트릭은 정상적으로 수집되고 있지만, Prometheus가 스크랩하지 못하면 Grafana에서 "nodata"가 표시됩니다.

## 확인 사항

### 1. Consumer 메트릭 엔드포인트 확인

Consumer 메트릭은 다음 엔드포인트에서 제공됩니다:

- **포트**: `9000` (또는 `9001` - 포트 충돌 시)
- **엔드포인트**: `http://recommend:9000/metrics` (Docker 네트워크 내)
- **엔드포인트**: `http://localhost:9000/metrics` (호스트에서)

메트릭 확인:
```bash
docker exec recommend python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9000/metrics').read().decode('utf-8')[:500])"
```

### 2. Prometheus 설정 확인

Prometheus가 Consumer 메트릭을 스크랩하도록 설정되어 있어야 합니다.

#### Prometheus 설정 예시 (`prometheus.yml`)

```yaml
scrape_configs:
  - job_name: 'kafka-consumer'
    scrape_interval: 5s
    static_configs:
      - targets: ['recommend:9000']  # Docker 네트워크 내
        labels:
          service: 'kafka-consumer'
```

또는 호스트에서 접근하는 경우:

```yaml
scrape_configs:
  - job_name: 'kafka-consumer'
    scrape_interval: 5s
    static_configs:
      - targets: ['host.docker.internal:9000']  # Docker Desktop
        labels:
          service: 'kafka-consumer'
```

### 3. Prometheus 타겟 확인

Prometheus UI에서 타겟 상태 확인:

1. Prometheus UI 접속: `http://localhost:9090`
2. **Status** → **Targets** 메뉴 이동
3. `kafka-consumer` 타겟이 **UP** 상태인지 확인
4. **UP**이 아니면 에러 메시지 확인

### 4. Grafana 데이터소스 확인

Grafana에서 Prometheus 데이터소스가 올바르게 설정되어 있는지 확인:

1. Grafana UI 접속: `http://localhost:3000`
2. **Configuration** → **Data Sources** 메뉴 이동
3. Prometheus 데이터소스 선택
4. **URL**이 올바른지 확인 (예: `http://prometheus:9090`)

### 5. 대시보드 쿼리 확인

Grafana 대시보드의 쿼리가 올바른지 확인:

#### 사용 가능한 메트릭

- `kafka_consumer_messages_total{status="success"}` - 성공 처리 수
- `kafka_consumer_messages_total{status="failure"}` - 실패 처리 수
- `kafka_consumer_latency_ms_bucket` - 지연 시간 히스토그램
- `kafka_consumer_errors_total` - 에러 수
- `kafka_consumer_messages_processed` - 처리된 메시지 수

#### 쿼리 예시

```promql
# 성공 처리 수 (누적)
kafka_consumer_messages_total{status="success"}

# 성공 처리률 (초당)
rate(kafka_consumer_messages_total{status="success"}[1m])

# 실패 처리률 (초당)
rate(kafka_consumer_messages_total{status="failure"}[1m])

# P95 지연 시간
histogram_quantile(0.95, rate(kafka_consumer_latency_ms_bucket[1m]))
```

## 해결 방법

### 방법 1: Prometheus 설정 추가

Prometheus 설정 파일에 Consumer 타겟 추가:

```yaml
scrape_configs:
  - job_name: 'kafka-consumer'
    scrape_interval: 5s
    static_configs:
      - targets: ['recommend:9000']
        labels:
          service: 'kafka-consumer'
          instance: 'consumer-1'
```

### 방법 2: Docker 네트워크 확인

Consumer와 Prometheus가 같은 Docker 네트워크에 있는지 확인:

```bash
docker network inspect munova-net | grep -A 5 "Containers"
```

### 방법 3: 포트 매핑 확인

`docker-compose.yml`에서 포트 매핑 확인:

```yaml
ports:
  - "9000:9000"  # Consumer Prometheus 메트릭 포트
```

호스트에서 접근하는 경우:

```yaml
scrape_configs:
  - job_name: 'kafka-consumer'
    static_configs:
      - targets: ['host.docker.internal:9000']  # 또는 'localhost:9000'
```

### 방법 4: 메트릭 엔드포인트 직접 확인

브라우저나 curl로 메트릭 확인:

```bash
# Docker 컨테이너 내부에서
docker exec recommend python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9000/metrics').read().decode('utf-8')[:1000])"

# 호스트에서 (포트 매핑이 있는 경우)
curl http://localhost:9000/metrics | grep kafka_consumer
```

## 디버깅 체크리스트

- [ ] Consumer 메트릭 엔드포인트가 응답하는가? (`http://localhost:9000/metrics`)
- [ ] Prometheus가 Consumer 타겟을 스크랩하도록 설정되어 있는가?
- [ ] Prometheus 타겟 상태가 **UP**인가?
- [ ] Grafana 데이터소스가 올바른 Prometheus URL을 가리키는가?
- [ ] 대시보드 쿼리의 메트릭 이름이 올바른가?
- [ ] Consumer와 Prometheus가 같은 Docker 네트워크에 있는가?

## 빠른 테스트

Prometheus에서 직접 쿼리 테스트:

1. Prometheus UI 접속: `http://localhost:9090`
2. **Graph** 탭에서 쿼리 입력:
   ```
   kafka_consumer_messages_total
   ```
3. **Execute** 클릭
4. 결과가 나오면 정상, "No data"면 Prometheus 설정 문제

## 참고

- Consumer 메트릭 포트: `9000` (또는 `9001`)
- Prometheus 스크랩 간격: `5s` (권장)
- 메트릭 형식: Prometheus 표준 형식

