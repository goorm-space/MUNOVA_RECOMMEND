from redis.cluster import RedisCluster, ClusterNode
from redis.exceptions import ConnectionError, TimeoutError, ResponseError
import time, yaml
from services.influx_logger import send_metric
from services.recommender import Recommender

# Prometheus metrics (전역으로 한 번만 생성)
try:
    from prometheus_client import Histogram, Counter, Gauge
    from prometheus_client import CollectorRegistry, REGISTRY
    
    # Latency 히스토그램 (밀리초 단위)
    latency_histogram = Histogram(
        'redis_stream_consumer_latency_ms',
        'Redis Stream Consumer Latency (milliseconds)',
        ['stream_key', 'consumer', 'event_type'],
        buckets=[10, 25, 50, 100, 200, 500, 1000, 2000, 5000, 10000]  # 히스토그램 버킷
    )
    
    # 메시지 처리 카운터
    messages_counter = Counter(
        'redis_stream_consumer_messages_total',
        'Total number of messages consumed from Redis Stream',
        ['stream_key', 'consumer', 'has_producer_time']
    )
    
    # 처리 중인 메시지 수 (Gauge)
    messages_processed = Gauge(
        'redis_stream_consumer_messages_processed',
        'Number of messages processed',
        ['stream_key', 'consumer']
    )
    
    PROMETHEUS_ENABLED = True
except ImportError:
    # prometheus-client가 없으면 비활성화
    PROMETHEUS_ENABLED = False
    latency_histogram = None
    messages_counter = None
    messages_processed = None

with open("/app/config.yml", "r") as f:
    config = yaml.safe_load(f)

def create_redis_client():
    """Redis Cluster 클라이언트 생성"""
    nodes = [ClusterNode(n["host"], n["port"]) for n in config["redis"]["cluster"]["nodes"]]
    password = config["redis"]["cluster"].get("password", "")
    return RedisCluster(
        startup_nodes=nodes,
        password=password if password else None,
        decode_responses=True,
        skip_full_coverage_check=True,  # 일부 노드 연결 실패해도 계속 진행
        socket_connect_timeout=10,
        socket_timeout=10,
        retry_on_timeout=True,
        health_check_interval=30,  # 30초마다 연결 상태 확인
        read_from_replicas=False,  # 마스터에서만 읽기
        # 노드 정보를 호스트 이름으로 유지하도록 설정
        reinitialize_steps=10  # 10번 실패 시 재초기화
    )

# Redis 클라이언트 초기화
try:
    r = create_redis_client()
    # 연결 테스트
    r.ping()
    print("✅ Redis Cluster 연결 완료")
except Exception as e:
    print(f"❌ Redis Cluster 연결 실패: {e}")
    print("❌ Consumer를 종료합니다.")
    exit(1)

# DB 세션 생성 (추천 로직용)
from services.database import SessionLocal
db_session = SessionLocal() if SessionLocal else None

# 추천 로직 초기화
recommender = Recommender(redis_client=r, db_session=db_session)

# 🔢 STREAM_INDEX는 파일별로 다르게 치환됨 (예: 0~9)
STREAM_INDEX = 3
STREAM_KEY = f"{config['app']['stream_prefix']}{STREAM_INDEX}"
GROUP = config["app"]["consumer_group"]
CONSUMER = f"consumer-{STREAM_INDEX}"

# Consumer Group 생성 (Stream이 없으면 생성, 있으면 Group만 생성)
try:
    # Stream이 있는지 확인
    if r.exists(STREAM_KEY):
        # Stream이 있으면 Consumer Group만 생성 (id='0'으로 시작)
        try:
            r.xgroup_create(STREAM_KEY, GROUP, id='0', mkstream=False)
            print(f"✅ {CONSUMER} Consumer Group 생성 완료 (기존 Stream)")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                print(f"✅ {CONSUMER} Consumer Group 이미 존재")
            else:
                print(f"⚠️ {CONSUMER} Consumer Group 생성 실패 (기존 Stream): {e}")
    else:
        # Stream이 없으면 Stream과 Group 함께 생성
        r.xgroup_create(STREAM_KEY, GROUP, mkstream=True)
        print(f"✅ {CONSUMER} Stream 및 Consumer Group 생성 완료")
except Exception as e:
    if "BUSYGROUP" in str(e):
        print(f"✅ {CONSUMER} Consumer Group 이미 존재")
    else:
        print(f"⚠️ {CONSUMER} Consumer Group 생성 실패: {e}")

print(f"🔄 {CONSUMER} 시작 (stream={STREAM_KEY})")

# Prometheus HTTP 서버 시작 (각 Consumer가 별도 포트로 메트릭 노출)
if PROMETHEUS_ENABLED:
    try:
        from prometheus_client import start_http_server
        # Consumer별로 다른 포트 사용 (8002~8011)
        metrics_port = 8002 + STREAM_INDEX
        start_http_server(metrics_port)
        print(f"📊 {CONSUMER} Prometheus metrics 서버 시작: http://0.0.0.0:{metrics_port}/metrics")
    except Exception as e:
        print(f"⚠️ {CONSUMER} Prometheus 서버 시작 실패: {e}")

count = 0
DLQ_STREAM = f"{STREAM_KEY}:dlq"
consecutive_errors = 0
max_consecutive_errors = 10

while True:
    try:
        # 연결 상태 확인 (주기적으로)
        if consecutive_errors > 0 and consecutive_errors % 5 == 0:
            try:
                r.ping()
                print(f"✅ {CONSUMER} 연결 상태 정상")
                consecutive_errors = 0
            except Exception as ping_e:
                print(f"⚠️ {CONSUMER} 연결 상태 확인 실패: {ping_e}")
                # 재연결 시도
                try:
                    r.close()
                    r = create_redis_client()
                    r.ping()
                    print(f"✅ {CONSUMER} 재연결 성공")
                    consecutive_errors = 0
                except Exception as reconnect_e:
                    print(f"❌ {CONSUMER} 재연결 실패: {reconnect_e}")
        
        # xreadgroup 호출 전 연결 상태 확인
        try:
            r.ping()
        except Exception as ping_e:
            # 연결이 끊어진 경우 재연결
            print(f"⚠️ {CONSUMER} 연결 끊김 감지. 재연결 시도...")
            try:
                r.close()
                r = create_redis_client()
                r.ping()
                recommender.redis_client = r  # Recommender에도 새 클라이언트 전달
                print(f"✅ {CONSUMER} 재연결 성공")
            except Exception as reconnect_e:
                print(f"❌ {CONSUMER} 재연결 실패: {reconnect_e}")
                consecutive_errors += 1
                time.sleep(5)
                continue
        
        # 메시지 읽기 시도
        try:
            messages = r.xreadgroup(GROUP, CONSUMER, {STREAM_KEY: '>'}, count=50, block=5000)
        except ResponseError as read_e:
            # NOGROUP 오류: Stream이나 Consumer Group이 없을 때
            error_msg = str(read_e)
            if "NOGROUP" in error_msg or "No such key" in error_msg:
                # Consumer Group 재생성 시도
                try:
                    r.xgroup_create(STREAM_KEY, GROUP, mkstream=True)
                    print(f"✅ {CONSUMER} Consumer Group 재생성 완료")
                    consecutive_errors = 0
                    time.sleep(1)
                    continue
                except Exception as create_e:
                    # 이미 존재하거나 다른 오류
                    if "BUSYGROUP" not in str(create_e):
                        print(f"⚠️ {CONSUMER} Consumer Group 생성 실패: {create_e}")
                    consecutive_errors += 1
                    time.sleep(2)
                    continue
            else:
                print(f"❌ {CONSUMER} Redis 응답 오류: {read_e}")
                consecutive_errors += 1
                time.sleep(2)
                continue
        except (ConnectionError, TimeoutError) as read_e:
            # 읽기 중 연결 오류 발생
            error_msg = str(read_e)
            if "172.20.0" in error_msg or "Connection refused" in error_msg:
                print(f"⚠️ {CONSUMER} 읽기 중 연결 오류: {read_e}. 노드 정보 갱신 시도...")
                try:
                    # 클러스터 노드 정보 강제 갱신
                    r.cluster_nodes()
                    # 재연결
                    r.close()
                    r = create_redis_client()
                    recommender.redis_client = r
                    print(f"✅ {CONSUMER} 노드 정보 갱신 및 재연결 완료")
                except Exception as refresh_e:
                    print(f"❌ {CONSUMER} 노드 정보 갱신 실패: {refresh_e}")
            consecutive_errors += 1
            time.sleep(2)
            continue
        except Exception as read_e:
            print(f"❌ {CONSUMER} 읽기 오류: {read_e}")
            consecutive_errors += 1
            time.sleep(2)
            continue
        
        if not messages:
            consecutive_errors = 0  # 메시지가 없어도 정상
            continue
        
        consecutive_errors = 0  # 성공 시 에러 카운터 리셋
        
        for stream, entries in messages:
            for msg_id, fields in entries:
                try:
                    # Latency 계산 (producer_time이 있는 경우)
                    latency_ms = None
                    event_type = fields.get('event_type', 'unknown')
                    has_producer_time = 'producer_time' in fields
                    
                    if has_producer_time:
                        try:
                            # 현재 시간 (밀리초)
                            now_ms = int(time.time() * 1000)
                            # Producer가 메시지를 보낸 시간 (밀리초)
                            producer_ms = int(fields.get('producer_time'))
                            # Latency 계산
                            latency_ms = now_ms - producer_ms
                            
                            # 음수 latency 처리 (시간 동기화 문제)
                            if latency_ms < 0:
                                print(f"⚠️ 음수 latency 감지: {latency_ms}ms (시간 동기화 문제 가능성)")
                                latency_ms = 0  # 0으로 처리하거나 로그만 남김
                            
                            # Latency 로그 출력 (샘플링: 10개 중 1개만 출력)
                            if count % 10 == 0:
                                member_id = fields.get('member_id', 'unknown')
                                print(f"⏱️ [LATENCY] {latency_ms}ms | stream={stream} | event={event_type} | member={member_id} | producer_time={producer_ms} | consumer_time={now_ms}")
                            
                            # InfluxDB로 latency 메트릭 전송
                            send_metric("redis_stream_latency",
                                        {"stream": stream, "consumer": CONSUMER, "event_type": event_type},
                                        {"latency_ms": latency_ms})
                            
                            # Prometheus Histogram에 기록
                            if PROMETHEUS_ENABLED and latency_histogram:
                                latency_histogram.labels(
                                    stream_key=stream,
                                    consumer=CONSUMER,
                                    event_type=event_type
                                ).observe(latency_ms)
                            
                        except (ValueError, TypeError) as latency_e:
                            print(f"⚠️ Latency 계산 실패: {latency_e} | producer_time={fields.get('producer_time')}")
                    
                    # Prometheus Counter 증가
                    if PROMETHEUS_ENABLED and messages_counter:
                        messages_counter.labels(
                            stream_key=stream,
                            consumer=CONSUMER,
                            has_producer_time='true' if has_producer_time else 'false'
                        ).inc()
                    # else:
                    #     # producer_time이 없는 경우는 로그 출력하지 않음 (너무 많을 수 있음)
                    #     pass
                    
                    # 추천 로직 처리
                    recommender.process_stream_message(fields)
                    count += 1
                    r.xack(stream, GROUP, msg_id)
                    
                    # Prometheus Gauge 업데이트
                    if PROMETHEUS_ENABLED and messages_processed:
                        messages_processed.labels(
                            stream_key=stream,
                            consumer=CONSUMER
                        ).set(count)
                    
                    if count % 100 == 0:
                        print(f"📦 {count}개 처리 완료 ({stream})")
                        send_metric("recommend_consumer",
                                    {"stream": stream, "consumer": CONSUMER},
                                    {"processed": count})
                except Exception as inner_e:
                    print(f"❌ 메시지 처리 중 오류 ({stream}:{msg_id}): {inner_e}")
                    # DLQ로 원본 메시지와 에러 전송
                    try:
                        dlq_fields = {"error": str(inner_e)}
                        # 원본 필드도 함께 전달(평탄화 그대로 유지)
                        dlq_fields.update({str(k): str(v) for k, v in fields.items()})
                        r.xadd(DLQ_STREAM, dlq_fields, maxlen=100000, approximate=True)
                        send_metric("recommend_consumer_dlq",
                                    {"stream": stream, "consumer": CONSUMER},
                                    {"dlq_count": 1})
                    except Exception as dlq_e:
                        print(f"⚠️ DLQ 적재 실패: {dlq_e}")
                    finally:
                        # 실패 메시지는 Ack하지 않음 → 재시도 전략 or 주기적 클레임 정책 별도 고려
                        pass
                    continue
    except (ConnectionError, TimeoutError) as e:
        consecutive_errors += 1
        error_msg = str(e)
        print(f"❌ {CONSUMER} 연결 오류 ({consecutive_errors}/{max_consecutive_errors}): {error_msg}")
        
        # IP 주소로 연결 시도 실패 시, 클러스터 노드 정보 갱신
        if "172.20.0" in error_msg or "Connection refused" in error_msg:
            try:
                # 클러스터 노드 정보 강제 갱신
                r.cluster_nodes()
                print(f"🔄 {CONSUMER} 클러스터 노드 정보 갱신 완료")
            except Exception as refresh_e:
                print(f"⚠️ {CONSUMER} 노드 정보 갱신 실패: {refresh_e}")
        
        if consecutive_errors >= max_consecutive_errors:
            print(f"❌ {CONSUMER} 연속 오류 {max_consecutive_errors}회 발생. 재연결 시도...")
            try:
                r.close()
                time.sleep(5)
                r = create_redis_client()
                r.ping()
                # Recommender에도 새 클라이언트 전달
                recommender.redis_client = r
                print(f"✅ {CONSUMER} 재연결 성공")
                consecutive_errors = 0
            except Exception as reconnect_e:
                print(f"❌ {CONSUMER} 재연결 실패: {reconnect_e}")
                time.sleep(10)
        else:
            time.sleep(2)
    except ResponseError as e:
        # Redis 명령 오류 (예: 스트림이 없음 등)
        print(f"⚠️ {CONSUMER} Redis 응답 오류: {e}")
        time.sleep(2)
    except Exception as e:
        consecutive_errors += 1
        print(f"❌ {CONSUMER} 예상치 못한 오류 ({consecutive_errors}/{max_consecutive_errors}): {e}")
        if consecutive_errors >= max_consecutive_errors:
            print(f"❌ {CONSUMER} 연속 오류 {max_consecutive_errors}회 발생. 잠시 대기...")
            time.sleep(10)
            consecutive_errors = 0
        else:
            time.sleep(2)