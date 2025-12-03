# Poll CPU 병목 최적화 분석

## 🔍 현재 Poll 동작 분석

### 현재 구현
```python
def consume_batch(self, timeout: float = 0.5):
    while len(messages) < self.batch_size and (time.time() - start_time) < timeout:
        msg = self.consumer.poll(timeout=0.1)  # 0.1초마다 poll 호출
        if msg is None:
            continue
        messages.append(msg)
```

### 문제점
1. **Poll 호출 빈도가 높음**
   - `poll(timeout=0.1)`을 최대 0.3초 동안 반복 호출
   - 배치 크기 100을 채우기 위해 최대 100번 poll 호출 가능
   - 메시지가 없을 때도 0.1초마다 poll 호출 → CPU 낭비

2. **현재 설정**
   - `batch_size`: 100
   - `consume_batch timeout`: 0.3초
   - `poll timeout`: 0.1초
   - `fetch.min.bytes`: 5KB
   - `max.partition.fetch.bytes`: 5MB

---

## 💡 배치 크기 증가 효과 분석

### 배치 크기를 200 → 500으로 증가 시

#### ✅ 장점
1. **Poll 호출 횟수 감소**
   - 현재: 100개 메시지 = 최대 100번 poll
   - 개선: 500개 메시지 = 최대 500번 poll (하지만 한 번에 더 많이 가져옴)
   - **실제로는**: `fetch.min.bytes`와 `max.partition.fetch.bytes` 설정에 따라 한 번의 poll로 여러 메시지를 가져올 수 있음

2. **처리 효율 증가**
   - 배치 단위로 처리하면 오버헤드 감소
   - DB 커밋 횟수 감소 (현재는 각 메시지마다 커밋하지만)

#### ⚠️ 단점
1. **메모리 사용량 증가**
   - 500개 메시지를 메모리에 보관
   - 각 메시지가 크면 메모리 부족 가능

2. **처리 지연 증가**
   - 배치를 채우기까지 시간 소요
   - 실시간성 저하

3. **실패 시 재처리 범위 증가**
   - 배치 중 하나라도 실패하면 전체 재처리 필요

### 결론: 배치 크기 증가는 효과적 ✅
- **권장**: 200-300 정도로 증가
- **이유**: Poll 호출 횟수 감소 + 처리 효율 증가
- **주의**: 메모리 사용량 모니터링 필요

---

## 🔄 10개 Consumer 효과 분석

### 현재 구조
- **파티션**: 10개
- **Consumer**: 10개 (각 파티션당 1개)
- **처리 방식**: 병렬 처리

### 10개 Consumer의 효과

#### ✅ 장점
1. **병렬 처리**
   - 각 Consumer가 독립적으로 poll
   - 전체 처리량 = Consumer 수 × 단일 Consumer 처리량
   - **이론상**: 10배 처리량 증가 가능

2. **부하 분산**
   - 각 Consumer가 하나의 파티션만 처리
   - 파티션별 부하 분산

3. **확장성**
   - 파티션 수만큼 Consumer 증가 가능
   - 수평 확장 용이

#### ⚠️ 단점
1. **Poll 호출 횟수 증가**
   - Consumer 1개: poll 호출 N번
   - Consumer 10개: poll 호출 10N번 (각각 독립적으로)
   - **하지만**: 각 Consumer의 poll은 독립적이므로 CPU 코어가 충분하면 병렬 처리 가능

2. **리소스 사용량 증가**
   - 메모리: Consumer당 메모리 사용
   - 네트워크: 각 Consumer가 독립적으로 Kafka와 통신

### 결론: 10개 Consumer는 효과적 ✅
- **이유**: 병렬 처리로 전체 처리량 증가
- **조건**: CPU 코어가 충분해야 함 (현재 12.5 코어)
- **효과**: 단일 Consumer 대비 5-10배 처리량 증가 가능

---

## 🎯 최적화 권장 사항

### 1. 배치 크기 증가 ✅
```yaml
kafka:
  batch_size: 300  # 100 → 300 (Poll 호출 횟수 감소)
```

**효과**:
- Poll 호출 횟수 감소
- 처리 효율 증가
- CPU 사용량 감소 예상

### 2. Fetch 설정 최적화 ✅
```python
'fetch.min.bytes': 10240,  # 5KB → 10KB (한 번에 더 많이 가져오기)
'max.partition.fetch.bytes': 10485760,  # 5MB → 10MB
```

**효과**:
- 한 번의 poll로 더 많은 메시지 가져오기
- Poll 호출 횟수 추가 감소

### 3. Poll Timeout 조정 ✅
```python
msg = self.consumer.poll(timeout=0.2)  # 0.1 → 0.2초 (메시지 없을 때 대기 시간 증가)
```

**효과**:
- 메시지가 없을 때 불필요한 poll 호출 감소
- CPU 사용량 감소

### 4. Consume Batch Timeout 조정 ✅
```python
messages = self.consume_batch(timeout=0.5)  # 0.3 → 0.5초 (더 큰 배치 수집 가능)
```

**효과**:
- 더 큰 배치 수집 가능
- Poll 호출 횟수 감소

---

## 📊 예상 효과

### 현재 (batch_size=100)
- Poll 호출: 배치당 최대 100번
- CPU 사용: 높음 (빈번한 poll)

### 최적화 후 (batch_size=300, fetch 최적화)
- Poll 호출: 배치당 최대 50-100번 (fetch로 한 번에 여러 개 가져옴)
- CPU 사용: 30-40% 감소 예상

### 10개 Consumer 효과
- 전체 처리량: 5-10배 증가
- CPU 사용: 각 Consumer가 독립적으로 사용하므로 병렬 처리 가능
- **조건**: CPU 코어가 충분해야 함 (현재 12.5 코어로 충분)

---

## 🚀 적용 방법

### 1. Config 수정
```yaml
kafka:
  batch_size: 300
```

### 2. Consumer 코드 수정
```python
# fetch 설정 증가
'fetch.min.bytes': 10240,  # 10KB
'max.partition.fetch.bytes': 10485760,  # 10MB

# poll timeout 증가
msg = self.consumer.poll(timeout=0.2)

# consume_batch timeout 증가
messages = self.consume_batch(timeout=0.5)
```

### 3. 재시작
```bash
docker restart recommend
```

---

## 📝 결론

### 배치 크기 증가: 효과적 ✅
- **권장**: 200-300
- **효과**: Poll 호출 횟수 감소, CPU 사용량 감소

### 10개 Consumer: 효과적 ✅
- **이유**: 병렬 처리로 전체 처리량 증가
- **조건**: CPU 코어 충분 (현재 12.5 코어로 충분)
- **효과**: 5-10배 처리량 증가 가능

### 최적화 조합
1. 배치 크기 300
2. Fetch 설정 증가
3. Poll timeout 조정
4. 10개 Consumer 유지

**예상 CPU 감소**: 80% → 30-40%

