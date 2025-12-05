import json
import logging
import signal
import sys
import time
import yaml
from typing import Dict, Any, List, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException
import os

from services.recommender import Recommender
from services.mongodb import save_user_action_logs_batch, save_recommendation_logs_batch
from datetime import datetime


# 로깅 설정 (Protobuf import 전에 설정)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Protobuf import
try:
    import user_action_log_pb2
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    logger.warning("⚠️ Protobuf 모듈을 찾을 수 없습니다. protoc로 컴파일이 필요합니다.")

# Prometheus metrics (전역으로 한 번만 생성)
try:
    from prometheus_client import Histogram, Counter, Gauge
    from prometheus_client import start_http_server
    
    # Latency 히스토그램 (밀리초 단위)
    latency_histogram = Histogram(
        'kafka_consumer_latency_ms',
        'Kafka Consumer Latency (milliseconds)',
        ['topic', 'partition', 'event_type'],
        buckets=[10, 25, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    )
    
    # 메시지 처리 카운터
    messages_counter = Counter(
        'kafka_consumer_messages_total',
        'Total number of messages consumed from Kafka',
        ['topic', 'partition', 'event_type', 'status']
    )
    
    # 처리 중인 메시지 수 (Gauge)
    messages_processed = Gauge(
        'kafka_consumer_messages_processed',
        'Number of messages processed',
        ['topic', 'partition']
    )
    
    # 에러 카운터
    error_counter = Counter(
        'kafka_consumer_errors_total',
        'Total number of errors',
        ['error_type']
    )
    
    PROMETHEUS_ENABLED = True
except ImportError:
    PROMETHEUS_ENABLED = False
    latency_histogram = None
    messages_counter = None
    messages_processed = None
    error_counter = None

# 설정 로드
config_path = "/app/config.yml" if os.path.exists("/app/config.yml") else "config.yml"
with open(config_path, "r") as f: # r: 읽기 f: 파일 객체
    config = yaml.safe_load(f)

# 환경변수에서 메트릭 포트 가져오기 (우선순위)
if 'KAFKA_METRICS_PORT' in os.environ:
    if 'kafka' not in config:
        config['kafka'] = {}
    config['kafka']['metrics_port'] = int(os.environ['KAFKA_METRICS_PORT'])

# Consumer
class MunovaKafkaConsumer:
    # 초기화
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        consumer_group_id: Optional[str] = None,
        topic: Optional[str] = None,
        batch_size: int = 10,
        db_session=None  # 하위 호환성을 위해 유지 (사용하지 않음)
    ):
        # config 설정에서 값 가져오기
        kafka_config = config.get("kafka", {})
        self.bootstrap_servers = bootstrap_servers or kafka_config.get("bootstrap_servers", "localhost:9092")
        self.consumer_group_id = consumer_group_id or kafka_config.get("consumer_group_id", "munova-recommendation-consumer")
        self.topic = topic or kafka_config.get("topic", "user_action_log") # 구독할 토픽 이름
        self.batch_size = batch_size or kafka_config.get("batch_size", 10) # batch size 크기 (기본 10)
        # MongoDB 호환성을위해 db_session은 안함

        # Consumer 설정 (Rebalancing 방지 최적화)
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers, # Kafka Cluster Broker 주소
            'group.id': self.consumer_group_id, # Consumer Group Id 지정
            'auto.offset.reset': 'latest',  # 최신 메시지만 -> 새로운 메시지만
            'enable.auto.commit': False,  # 수동 커밋 (Kafka가 메시지 읽은 오프셋을 자동으로 저장할지 여부)
            'session.timeout.ms': 60000,  # heartbeat를 안보내면 session이 끊겼다고 판단하는 시간: 60초
            'heartbeat.interval.ms': 3000,  # heartbeat 보내는 주기: 3초
            'max.poll.interval.ms': 300000,  # 메시지 처리 최대 시간: 5분 (배치 처리 고려)
            'partition.assignment.strategy': 'range',  # 파티션 할당 전략: range -> 파티션을 연속적으로 consumer에 나눔, roundrobin -> 파티션을 순서대로 분배
            'fetch.min.bytes': 10240,  # Consumer가 브로커에서 fetch 할 최소 데이터 크기
            'max.partition.fetch.bytes': 10485760,  # 파티션당 한번에 가져올 최대 fetch 크기
            # 참고: fetch.max.wait.ms는 confluent-kafka에서 지원하지 않음
        }
        # 배치 처리는 consume_batch() 메서드에서 timeout으로 제어
        
        # Kafka Consumer 객체를 저장하는 변수
        self.consumer: Optional[Consumer] = None
        
        # Recommender Class 인스턴스를 생성하여 추천 로직 처리 담당 (Kafka에서 읽은 메시지로 추천 점수 계산, MongoDB에 저장)
        self.recommender = Recommender()
        
        # 실행 상태 플래그
        self.running = False # Consumer가 현재 실행중인지 상태표시 (run() 안에서 True/False 변경)
        self.shutdown_requested = False # 종료 요청 플래 -> 시그널 수신 시 True로 변경되어 Graceful shutdown 트리거
        
        # 파티션 할당 추적
        self.assigned_partitions = set()  # (topic, partition) 튜플 세트로 저장
        
        # 모니터링
        self.processed_count = 0
        self.error_count = 0
        
        # Graceful shutdown을 위한 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler) # Ctrl + C
        signal.signal(signal.SIGTERM, self._signal_handler) # 프로세스 종료

    def _signal_handler(self, signum, frame):
        logger.info(f"시그널 {signum} 수신. 종료 중...")
        self.shutdown_requested = True

    # Context manager 진입
    def __enter__(self):
        self.start()
        return self
    # Context manager 종료
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Kafka Consumer를 시작하고, 토픽 구독, 파티션 콜백 설정, Prometheus 메트릭 서버까지 초기화하는 초기화 및 실행 메서드
    def start(self):
        try:
            # Consumer 생성
            self.consumer = Consumer(self.consumer_config)

            # Consumer를 지정된 Topic에 구독: on_assign 콜백 -> 파티션이 Consumer에게 할당될 때 호출, on_revoke 콜백 -> 파티션이 Consumer에서 제거될 때 호출
            self.consumer.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
            logger.info(f"✅ Kafka Consumer 시작: topic={self.topic}, group={self.consumer_group_id}")
            
            # Prometheus 메트릭 서버 시작
            if PROMETHEUS_ENABLED:
                try:
                    kafka_config = config.get("kafka", {})
                    # 환경변수에서 메트릭 포트 가져오기 (우선순위)
                    import os
                    metrics_port = int(os.environ.get('KAFKA_METRICS_PORT', kafka_config.get("metrics_port", 9000)))
                    
                    # 포트가 이미 사용 중인지 확인
                    import socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex(('0.0.0.0', metrics_port))
                    sock.close()
                    
                    if result == 0:
                        # 환경변수로 지정된 포트가 사용 중이면 경고만 하고 계속 진행 (메트릭 서버는 시작하지 않음)
                        if 'KAFKA_METRICS_PORT' in os.environ:
                            logger.warning(f"⚠️ 지정된 메트릭 포트 {metrics_port}가 이미 사용 중입니다. 메트릭 서버를 시작하지 않습니다.")
                            metrics_port = None
                        else:
                            # 환경변수가 없으면 다른 포트 시도 (단일 Consumer 실행 시)
                            logger.warning(f"⚠️ 포트 {metrics_port} 사용 중. 다른 포트 시도...")
                            # 9000~9009 범위에서 사용 가능한 포트 찾기
                            found_port = None
                            for port in range(9000, 9010):
                                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                test_sock.settimeout(1)
                                test_result = test_sock.connect_ex(('0.0.0.0', port))
                                test_sock.close()
                                if test_result != 0:
                                    found_port = port
                                    break
                            
                            if found_port:
                                metrics_port = found_port
                                logger.warning(f"⚠️ 사용 가능한 포트 {metrics_port} 사용")
                            else:
                                logger.warning("⚠️ 사용 가능한 메트릭 포트(9000~9009)를 찾을 수 없습니다. 메트릭 서버를 시작하지 않습니다.")
                                metrics_port = None
                    
                    if metrics_port:
                        start_http_server(metrics_port)
                        logger.info(f"📊 Prometheus metrics 서버 시작: http://0.0.0.0:{metrics_port}/metrics")
                    else:
                        logger.warning("⚠️ 메트릭 서버를 시작하지 않습니다.")
                except Exception as e:
                    logger.warning(f"⚠️ Prometheus 서버 시작 실패: {e}")

            # Consumer 시작 성공 시 True
            self.running = True
        except Exception as e:
            logger.error(f"❌ Consumer 시작 실패: {e}", exc_info=True)
            raise

    # Consumer group 내에서 파티션을 균등하게 나누는데, Rebalancing이 발생하면 호출
    def _on_assign(self, consumer, partitions):
        # 파티션 할당 정보를 (topic, partition) 튜플로 저장
        self.assigned_partitions = {(p.topic, p.partition) for p in partitions} # 현재 Consumer가 담당하는 파티션 추적
        if len(partitions) == 0:
            logger.warning(f"⚠️ 파티션이 할당되지 않았습니다. 가능한 원인:")
            logger.warning(f"   1. 다른 Consumer가 이미 모든 파티션을 사용 중")
            logger.warning(f"   2. Consumer Group 세션 타임아웃 대기 중 (약 30초)")
            logger.warning(f"   3. Topic이 존재하지 않거나 파티션이 없음")
            logger.warning(f"   해결 방법: ./reset_consumer_group.sh 실행 또는 30초 대기")
        else:
            logger.info(f"📋 파티션 할당됨: {len(partitions)}개 파티션")
            for p in partitions:
                logger.info(f"   - Partition {p.partition} (Topic: {p.topic})")

    # Rebalancing이 시작될 때, Consumer가 기존에 가지고 있던 파티션을 반환하기 직전에 호출
    def _on_revoke(self, consumer, partitions):
        logger.warning(f"🔄 Rebalancing 시작: {len(partitions)}개 파티션 해제 예정")
        # 파티션 해제 전에 assigned_partitions 업데이트 (커밋 방지)
        for p in partitions:
            self.assigned_partitions.discard((p.topic, p.partition)) # 해제 될 파티션 제거
        logger.info(f"⏳ 파티션 재할당 대기 중... (현재 할당된 파티션: {len(self.assigned_partitions)}개)")

    # 배치 단위로 메시지를 수 timeout만큼 모음
    def consume_batch(self, timeout: float = 0.5) -> List[Any]:
        if not self.consumer:
            raise RuntimeError("Consumer가 시작되지 않았습니다. start()를 먼저 호출하세요.")

        # 초기화
        messages = []
        start_time = time.time()

        # 조건
        # 1. 수집한 메시지 수가 batch_size보다 작을때
        # 2. 배치 수집 시간이 timeout을 초과하지 않을 때
        while len(messages) < self.batch_size and (time.time() - start_time) < timeout:
            try:
                # kafka에서 한 번에 하나의 메시지를 가져옴 -> timeout 만큼 메시지 없으면 None 반환
                msg = self.consumer.poll(timeout=0.2) # timeout 증가시 poll 호출 줄어듬
                
                if msg is None:
                    continue
                
                # 성능 최적화: 로그 제거 (메시지가 많을 때 로그 오버헤드 방지)
                # logger.debug(f"📥 메시지 수신: partition={msg.partition()}, offset={msg.offset()}")

                # kafka 메시지가 에러인 경우
                # 1. _PARTITION_EOF: 파티션 끝에 도달 → 정상, 무시
                # 2. UNKNOWN_TOPIC_OR_PART: 잘못된 토픽/파티션 → 경고 로그, Prometheus 에러 카운트 증가
                # 3. 그 외 Kafka 오류 → 로그, Prometheus 카운트 증가
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 파티션 끝에 도달 (정상)
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.error(f"❌ 알 수 없는 토픽 또는 파티션: {msg.error()}")
                        error_counter.labels(error_type="unknown_topic").inc() if error_counter else None
                        continue
                    else:
                        logger.error(f"❌ Kafka 오류: {msg.error()}")
                        error_counter.labels(error_type="kafka_error").inc() if error_counter else None
                        continue
                
                messages.append(msg) # 리스트에 메시지 추가

            # 예외처리
            except KafkaException as e:
                logger.error(f"❌ Kafka 예외 발생: {e}", exc_info=True)
                error_counter.labels(error_type="kafka_exception").inc() if error_counter else None
                time.sleep(0.1)
                continue
            except Exception as e:
                logger.error(f"❌ 예상치 못한 오류: {e}", exc_info=True)
                error_counter.labels(error_type="unexpected_error").inc() if error_counter else None
                time.sleep(0.1)
                continue
        
        return messages

    # 메시지 처리 -> 역직렬화
    def _parse_message(self, msg: Any) -> Optional[Dict[str, Any]]:
        try:
            # msg.value()는 byte[] (protobuf 직렬화 결과)
            value = msg.value()
            if value is None:
                return None
            
            if not PROTOBUF_AVAILABLE:
                logger.error("❌ Protobuf 모듈을 사용할 수 없습니다. protoc로 컴파일이 필요합니다.")
                error_counter.labels(error_type="protobuf_unavailable").inc() if error_counter else None
                return None
            
            # Protobuf 역직렬화: value(byte[]) -> Protobuf 객체
            proto_msg = user_action_log_pb2.UserActionLog()
            proto_msg.ParseFromString(value)
            
            # 기존 JSON 형식과 호환되도록 딕셔너리로 변환
            data = {
                'eventType': proto_msg.event_type,
                'service': proto_msg.service,
                'memberId': proto_msg.member_id if proto_msg.member_id != -1 else None,  # -1은 None으로 변환
                'data': json.loads(proto_msg.data_json) if proto_msg.data_json else {},  # JSON 문자열 파싱
                'eventTimestamp': proto_msg.event_timestamp,
                'eventTime': None,  # 필요시 timestamp에서 변환
                'producerTime': proto_msg.producer_time,
                'version': proto_msg.version
            }
            
            # eventTime을 ISO 형식으로 변환 (선택사항)
            if proto_msg.event_timestamp > 0:
                from datetime import datetime, timezone
                event_time = datetime.fromtimestamp(proto_msg.event_timestamp / 1000.0, tz=timezone.utc)
                data['eventTime'] = event_time.isoformat()
            
            return data
            
        except Exception as e:
            logger.error(f"❌ Protobuf 역직렬화 실패: {e}, topic={msg.topic()}", exc_info=True)
            error_counter.labels(error_type="protobuf_decode_error").inc() if error_counter else None
            return None

    # Consumer 배치 1회 처리 파이프라인
    def process_messages(self, messages: List[Any]) -> bool:
        # 비어있는 배치면 문제없음으로 간주
        if not messages:
            return True
        
        processed_messages = [] # 정상 처리된 kafka 메시지들 -> 오프셋 커밋할 때 사용
        failed_messages = [] # 처리 도중 예외난 메시지들 -> 커밋 안 함으로 재시도 가능

        # MongoDB 배치 저장 실패 여부 플래그
        mongodb_failed = False
        # MongoDB 배치 저장을 위한 문서 리스트
        user_action_logs = []
        recommendation_logs = []
        
        for msg in messages:
            try:
                # 메시지 파싱 (Kafka 원본 형식 그대로)
                kafka_data = self._parse_message(msg) # Protobuf -> dict 변환

                # 파싱 실패 처리
                if kafka_data is None:
                    # 파싱 실패 메트릭 기록
                    if PROMETHEUS_ENABLED and messages_counter:
                        messages_counter.labels(
                            topic=msg.topic(),
                            partition=str(msg.partition()),
                            event_type='unknown',
                            status='failure'
                        ).inc()
                    failed_messages.append(msg)
                    continue
                
                # Latency 계산 (producerTime이 있는 경우)
                latency_ms = None
                event_type = kafka_data.get('eventType', 'unknown')
                has_producer_time = 'producerTime' in kafka_data

                # end-to-end latency 모니터링
                if has_producer_time:
                    try:
                        now_ms = int(time.time() * 1000)
                        producer_ms = int(kafka_data.get('producerTime', 0))
                        latency_ms = now_ms - producer_ms
                        
                        if latency_ms < 0:
                            logger.warning(f"⚠️ 음수 latency 감지: {latency_ms}ms")
                            latency_ms = 0
                        
                        # Prometheus Histogram에 기록
                        if PROMETHEUS_ENABLED and latency_histogram:
                            latency_histogram.labels(
                                topic=msg.topic(),
                                partition=str(msg.partition()),
                                event_type=event_type
                            ).observe(latency_ms)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Latency 계산 실패: {e}")
                
                # Kafka 메타데이터 추가 (MongoDB 저장용)
                kafka_data['_kafka_metadata'] = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp() else None
                }
                
                # 추천 로직 처리 (Kafka 메시지 직접 전달)
                # 성능 최적화: 로그 제거 (메시지가 많을 때 로그 오버헤드 방지)
                # logger.debug(f"🔄 메시지 처리 시작: eventType={event_type}, memberId={kafka_data.get('memberId')}, productId={kafka_data.get('data', {}).get('product_id')}, partition={msg.partition()}, offset={msg.offset()}")
                
                # 추천 로직: 실제 점수 계산 + 사용자/상품 요약 업데이트
                result = self.recommender.process_kafka_message(kafka_data)
                
                # MongoDB 저장용 문서 준비 (배치 저장)
                if result:
                    member_id = result.get('member_id')
                    product_id = result.get('product_id')
                    event_type = result.get('event_type')
                    score = result.get('score', 0)
                    category_id = result.get('category_id')
                    brand_id = result.get('brand_id')
                    price = result.get('price')
                    kafka_metadata = result.get('kafka_metadata', {})
                    
                    # 사용자 행동 로그 문서
                    user_action_doc = {
                        "member_id": member_id,
                        "product_id": product_id,
                        "event_type": event_type,
                        "stream_key": kafka_metadata.get('topic'),
                        "message_id": str(kafka_metadata.get('offset')),
                        "raw_fields": kafka_data,  # 원본 데이터 보존
                        "created_at": datetime.utcnow()
                    }
                    user_action_logs.append(user_action_doc)
                    
                    # 추천 점수가 있으면 추천 로그도 준비
                    if score > 0:
                        recommendation_doc = {
                            "member_id": member_id,
                            "product_id": product_id,
                            "score": score,
                            "event_type": event_type,
                            "metadata": {
                                'kafka_topic': kafka_metadata.get('topic'),
                                'kafka_partition': kafka_metadata.get('partition'),
                                'kafka_offset': kafka_metadata.get('offset'),
                                'category_id': category_id,
                                'brand_id': brand_id,
                                'price': price
                            },
                            "created_at": datetime.utcnow()
                        }
                        recommendation_logs.append(recommendation_doc)
                
                logger.debug(f"✅ 메시지 처리 완료: eventType={event_type}")
                
                # Prometheus Counter 증가
                if PROMETHEUS_ENABLED and messages_counter:
                    messages_counter.labels(
                        topic=msg.topic(),
                        partition=str(msg.partition()),
                        event_type=event_type,
                        status='success'
                    ).inc()
                
                processed_messages.append(msg)
                self.processed_count += 1
                
                # 주기적으로 로그 출력 (성능 최적화: 빈도 감소)
                if self.processed_count % 1000 == 0:
                    logger.info(f"📦 {self.processed_count}개 메시지 처리 완료")

            # 예외 처리
            except Exception as e:
                logger.error(f"❌ 메시지 처리 중 오류: {e}", exc_info=True)
                error_counter.labels(error_type="processing_error").inc() if error_counter else None
                
                # 실패 메트릭 기록
                try:
                    event_type = 'unknown'
                    if PROMETHEUS_ENABLED and messages_counter:
                        # 메시지에서 event_type 추출 시도
                        try:
                            kafka_data = self._parse_message(msg)
                            if kafka_data:
                                event_type = kafka_data.get('eventType', 'unknown')
                        except:
                            pass
                        
                        messages_counter.labels(
                            topic=msg.topic(),
                            partition=str(msg.partition()),
                            event_type=event_type,
                            status='failure'
                        ).inc()
                except:
                    pass
                
                failed_messages.append(msg)
                self.error_count += 1
                
                # DLQ로 전송 (선택사항)
                # TODO: DLQ 구현
        
        # 배치 단위로 MongoDB에 저장 (성능 최적화: insert_many 사용)
        # 비동기 처리로 저장 실패 여부와 관계 없이 메시지를 '처리된 것'으로 봄
        if user_action_logs:
            try:
                save_success = save_user_action_logs_batch(user_action_logs)
                # 성능 최적화: 성공 로그 제거 (오류만 로깅)
                if not save_success:
                    logger.warning(f"⚠️ MongoDB 사용자 행동 로그 배치 저장 실패: {len(user_action_logs)}개")
                    mongodb_failed = True
            except Exception as e:
                logger.error(f"❌ MongoDB 사용자 행동 로그 배치 저장 중 오류: {e}", exc_info=True)
                mongodb_failed = True
        if recommendation_logs:
            try:
                save_success = save_recommendation_logs_batch(recommendation_logs)
                # 성능 최적화: 성공 로그 제거 (오류만 로깅)
                if not save_success:
                    logger.warning(f"⚠️ MongoDB 추천 로그 배치 저장 실패: {len(recommendation_logs)}개")
                    mongodb_failed = True
            except Exception as e:
                logger.error(f"❌ MongoDB 추천 로그 배치 저장 중 오류: {e}", exc_info=True)
                mongodb_failed = True
        
        # 성공적으로 처리된 메시지만 커밋 (MongoDB 저장 성공 여부와 무관하게 처리 완료된 메시지만 커밋)
        if mongodb_failed:
            logger.error("❌ MongoDB 배치 저장 실패로 인해 오프셋 커밋을 수행하지 않습니다. 배치를 실패로 처리합니다.")
        else:
            if processed_messages:
                try:
                    # 파티션이 할당되어 있는지 확인 (rebalancing 중이면 커밋하지 않음)
                    if len(self.assigned_partitions) == 0:
                        logger.warning("⚠️ 파티션이 할당되지 않음. 오프셋 커밋 건너뜀")
                        return True  # 메시지는 처리했으므로 True 반환

                    # 각 파티션별로 마지막 오프셋 추적
                    from confluent_kafka import TopicPartition
                    offsets_to_commit = {}

                    for msg in processed_messages:
                        topic = msg.topic()
                        partition = msg.partition()
                        offset = msg.offset()

                        # 할당된 파티션인지 확인
                        if (topic, partition) not in self.assigned_partitions:
                            logger.warning(f"⚠️ 파티션 {topic}:{partition}가 할당되지 않음. 커밋 건너뜀")
                            continue

                        key = (topic, partition)
                        # 각 파티션의 최대 오프셋만 유지 (다음 오프셋 커밋)
                        if key not in offsets_to_commit or offsets_to_commit[key] < offset + 1:
                            offsets_to_commit[key] = offset + 1

                    # 실제 커밋 호출
                    tps_to_commit = [
                        TopicPartition(topic, partition, offset)
                        for (topic, partition), offset in offsets_to_commit.items()
                    ]

                    if tps_to_commit:
                        self.consumer.commit(offsets=tps_to_commit, asynchronous=False) # False: 동기 커밋 -> 안정성 우선
                        logger.debug(f"✅ {len(processed_messages)}개 메시지 커밋 완료 ({len(tps_to_commit)}개 파티션)")
                    else:
                        logger.debug("⚠️ 커밋할 오프셋이 없음 (모든 파티션이 할당되지 않음)")

                # 커밋 실패 처리
                except Exception as e:
                    # UNKNOWN_TOPIC_OR_PART 에러는 rebalancing 중일 때 발생할 수 있으므로 경고만
                    error_msg = str(e)
                    if "UNKNOWN_TOPIC_OR_PART" in error_msg or "Unknown topic or partition" in error_msg:
                        logger.warning(f"⚠️ 오프셋 커밋 실패 (rebalancing 중일 수 있음): {e}")
                    else:
                        logger.error(f"❌ 오프셋 커밋 실패: {e}", exc_info=True)
                        error_counter.labels(error_type="commit_error").inc() if error_counter else None
                    # 커밋 실패해도 메시지는 처리했으므로 True 반환 (다음 배치에서 재시도)
                    return False
        
        # 실패한 메시지는 커밋하지 않음 (재시도 가능)
        if failed_messages:
            logger.warning(f"⚠️ {len(failed_messages)}개 메시지 처리 실패 (커밋하지 않음)")
        
        # Prometheus Gauge 업데이트
        if PROMETHEUS_ENABLED and messages_processed:
            for msg in processed_messages:
                messages_processed.labels(
                    topic=msg.topic(),
                    partition=str(msg.partition())
                ).set(self.processed_count)
        
        return (not mongodb_failed) and (len(failed_messages) == 0) # 배치 내 하나라도 실패시 false 반환

    # 종료 시 close()에서 마지막 한 번 호출됨
    def commit(self):
        if self.consumer:
            try:
                self.consumer.commit()
                logger.debug("✅ 오프셋 커밋 완료")
            except Exception as e:
                logger.error(f"❌ 오프셋 커밋 실패: {e}", exc_info=True)

    # Kafka Consumer 메인 루프
    def run(self):
        if not self.running:
            self.start()
        
        logger.info("🔄 Kafka Consumer 메인 루프 시작")
        
        consecutive_errors = 0
        max_consecutive_errors = 10 # 10번 연속 에러나면 잠깐 쉼
        
        # 파티션 할당 대기 (최대 60초)
        partition_wait_time = 0
        max_partition_wait = 60
        partition_check_interval = 5
        
        while not self.shutdown_requested:
            try:
                # 파티션 할당 대기
                if len(self.assigned_partitions) == 0 and partition_wait_time < max_partition_wait:
                    if partition_wait_time % partition_check_interval == 0:
                        logger.warning(f"⏳ 파티션 할당 대기 중... ({partition_wait_time}/{max_partition_wait}초)")
                        logger.warning(f"   Consumer Group: {self.consumer_group_id}")
                        logger.warning(f"   Topic: {self.topic}")
                        logger.warning(f"   해결 방법: ./reset_consumer_group.sh 실행 또는 세션 타임아웃(30초) 대기")
                    partition_wait_time += 1
                    time.sleep(1)
                    continue
                elif len(self.assigned_partitions) > 0:
                    if partition_wait_time > 0:
                        logger.info(f"✅ 파티션 할당 완료: {len(self.assigned_partitions)}개 파티션")
                    partition_wait_time = 0
                
                # 배치로 메시지 수집 (타임아웃 증가로 배치 효율 향상)
                messages = self.consume_batch(timeout=2.0)  # 최대 {timeout}초 동안 모아보고 process_messages에 넘김
                
                if not messages:
                    consecutive_errors = 0  # 메시지가 없어도 정상
                    continue
                
                # 메시지 처리
                success = self.process_messages(messages)
                
                if success:
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"❌ 연속 오류 {max_consecutive_errors}회 발생. 잠시 대기...")
                        time.sleep(5)
                        consecutive_errors = 0

            # 예외 처리: 예상 못 한 버그/환경 오류 에서도 프로세스가 죽지않고 다시 시도해봄 회복 로직
            except KeyboardInterrupt:
                logger.info("⚠️ 키보드 인터럽트 수신")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"❌ 예상치 못한 오류 ({consecutive_errors}/{max_consecutive_errors}): {e}", exc_info=True)
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"❌ 연속 오류 {max_consecutive_errors}회 발생. 잠시 대기...")
                    time.sleep(10)
                    consecutive_errors = 0
                else:
                    time.sleep(2)
        
        logger.info("🛑 Kafka Consumer 종료")

    # Consumer 종료 Graceful shutdown
    def close(self):
        logger.info("🛑 Consumer 종료 중...")
        self.running = False
        
        if self.consumer:
            try:
                # 마지막 커밋
                self.commit()
                # Consumer 종료
                self.consumer.close()
                logger.info("✅ Consumer 종료 완료")
            except Exception as e:
                logger.error(f"❌ Consumer 종료 중 오류: {e}", exc_info=True)


def main():
    consumer = MunovaKafkaConsumer()
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("⚠️ 키보드 인터럽트 수신")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

