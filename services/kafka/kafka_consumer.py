"""
Munova Recommendation Server - Kafka Consumer Implementation

Kafka에서 사용자 행동 로그를 소비하고 추천 서비스에 전달하는 Consumer
"""
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
from services.database import SessionLocal
from services.mongodb import save_user_action_logs_batch, save_recommendation_logs_batch
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# 환경변수에서 메트릭 포트 가져오기 (우선순위)
if 'KAFKA_METRICS_PORT' in os.environ:
    if 'kafka' not in config:
        config['kafka'] = {}
    config['kafka']['metrics_port'] = int(os.environ['KAFKA_METRICS_PORT'])


class MunovaKafkaConsumer:
    """
    Munova 추천 서버용 Kafka Consumer
    
    Kafka에서 사용자 행동 로그를 소비하고 추천 서비스에 전달합니다.
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        consumer_group_id: Optional[str] = None,
        topic: Optional[str] = None,
        batch_size: int = 10,
        db_session=None
    ):
        """
        Kafka Consumer 초기화
        
        Args:
            bootstrap_servers: Kafka 브로커 주소 (예: localhost:9092)
            consumer_group_id: Consumer Group ID
            topic: 구독할 토픽 이름
            batch_size: 배치 처리 크기 (기본값: 10)
            db_session: 데이터베이스 세션
        """
        # 설정에서 값 가져오기
        kafka_config = config.get("kafka", {})
        self.bootstrap_servers = bootstrap_servers or kafka_config.get("bootstrap_servers", "localhost:9092")
        self.consumer_group_id = consumer_group_id or kafka_config.get("consumer_group_id", "munova-recommendation-consumer")
        self.topic = topic or kafka_config.get("topic", "user_action_log")
        self.batch_size = batch_size or kafka_config.get("batch_size", 10)
        
        # Consumer 설정
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group_id,
            'auto.offset.reset': 'latest',  # 새로운 메시지만 읽기
            'enable.auto.commit': False,  # 수동 커밋
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'partition.assignment.strategy': 'range',  # 파티션 할당 전략: range 또는 roundrobin
            'fetch.min.bytes': 10240,  # 최소 fetch 크기 (10KB) - Poll 최적화: 5KB -> 10KB
            'max.partition.fetch.bytes': 10485760,  # 파티션당 최대 fetch 크기 (10MB) - Poll 최적화: 5MB -> 10MB
            # 참고: fetch.max.wait.ms는 confluent-kafka에서 지원하지 않음
        }
        # 참고: confluent_kafka는 max.poll.records를 지원하지 않음
        # 배치 처리는 consume_batch() 메서드에서 timeout으로 제어
        
        # Consumer 인스턴스
        self.consumer: Optional[Consumer] = None
        
        # 추천 서비스 초기화
        self.recommender = Recommender(db_session=db_session)
        self.db_session = db_session
        
        # 실행 상태 플래그
        self.running = False
        self.shutdown_requested = False
        
        # 파티션 할당 추적
        self.assigned_partitions = []
        
        # 메트릭
        self.processed_count = 0
        self.error_count = 0
        
        # Graceful shutdown을 위한 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러 (Graceful shutdown)"""
        logger.info(f"시그널 {signum} 수신. 종료 중...")
        self.shutdown_requested = True
    
    def __enter__(self):
        """Context manager 진입"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 종료"""
        self.close()
    
    def start(self):
        """Consumer 시작"""
        try:
            # Consumer 생성
            self.consumer = Consumer(self.consumer_config)
            
            # 토픽 구독 (파티션 할당 콜백 포함)
            self.consumer.subscribe([self.topic], on_assign=self._on_assign)
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
            
            self.running = True
        except Exception as e:
            logger.error(f"❌ Consumer 시작 실패: {e}", exc_info=True)
            raise
    
    def _on_assign(self, consumer, partitions):
        """파티션 할당 콜백"""
        self.assigned_partitions = partitions
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
    
    def consume_batch(self, timeout: float = 0.5) -> List[Any]:
        """
        배치로 메시지 수집
        
        Args:
            timeout: 폴링 타임아웃 (초)
        
        Returns:
            메시지 리스트
        """
        if not self.consumer:
            raise RuntimeError("Consumer가 시작되지 않았습니다. start()를 먼저 호출하세요.")
        
        messages = []
        start_time = time.time()
        
        while len(messages) < self.batch_size and (time.time() - start_time) < timeout:
            try:
                msg = self.consumer.poll(timeout=0.2)  # Poll 최적화: 0.1 -> 0.2초 (불필요한 poll 호출 감소)
                
                if msg is None:
                    continue
                
                logger.debug(f"📥 메시지 수신: partition={msg.partition()}, offset={msg.offset()}")
                
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
                
                messages.append(msg)
                
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
    
    def _parse_message(self, msg: Any) -> Optional[Dict[str, Any]]:
        """
        Kafka 메시지를 JSON으로 파싱
        
        Args:
            msg: Kafka 메시지 객체
        
        Returns:
            파싱된 JSON 딕셔너리 (Kafka 원본 형식)
        """
        try:
            # JSON 디코딩
            value = msg.value()
            if value is None:
                return None
            
            data = json.loads(value.decode('utf-8'))
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON 디코딩 실패: {e}, 메시지: {msg.value()}")
            error_counter.labels(error_type="json_decode_error").inc() if error_counter else None
            return None
        except Exception as e:
            logger.error(f"❌ 메시지 파싱 실패: {e}", exc_info=True)
            error_counter.labels(error_type="parse_error").inc() if error_counter else None
            return None
    
    def process_messages(self, messages: List[Any]) -> bool:
        """
        배치 메시지 처리
        
        Args:
            messages: 처리할 메시지 리스트
        
        Returns:
            처리 성공 여부 (모든 메시지가 성공적으로 처리되면 True)
        """
        if not messages:
            return True
        
        processed_messages = []
        failed_messages = []
        
        # MongoDB 배치 저장을 위한 문서 리스트
        user_action_logs = []
        recommendation_logs = []
        
        for msg in messages:
            try:
                # 메시지 파싱 (Kafka 원본 형식 그대로)
                kafka_data = self._parse_message(msg)
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
                # 성능 최적화: 로그 레벨 낮춤 (INFO -> DEBUG)
                logger.debug(f"🔄 메시지 처리 시작: eventType={event_type}, memberId={kafka_data.get('memberId')}, productId={kafka_data.get('data', {}).get('product_id')}, partition={msg.partition()}, offset={msg.offset()}")
                
                # 추천 로직 처리 (MongoDB 저장은 배치로 처리)
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
        if user_action_logs:
            try:
                save_success = save_user_action_logs_batch(user_action_logs)
                if save_success:
                    logger.debug(f"✅ MongoDB 사용자 행동 로그 배치 저장 완료: {len(user_action_logs)}개")
                else:
                    logger.warning(f"⚠️ MongoDB 사용자 행동 로그 배치 저장 실패: {len(user_action_logs)}개")
            except Exception as e:
                logger.error(f"❌ MongoDB 사용자 행동 로그 배치 저장 중 오류: {e}", exc_info=True)
        
        if recommendation_logs:
            try:
                save_success = save_recommendation_logs_batch(recommendation_logs)
                if save_success:
                    logger.debug(f"✅ MongoDB 추천 로그 배치 저장 완료: {len(recommendation_logs)}개")
                else:
                    logger.warning(f"⚠️ MongoDB 추천 로그 배치 저장 실패: {len(recommendation_logs)}개")
            except Exception as e:
                logger.error(f"❌ MongoDB 추천 로그 배치 저장 중 오류: {e}", exc_info=True)
        
        # 성공적으로 처리된 메시지만 커밋 (MongoDB 저장 성공 여부와 무관하게 처리 완료된 메시지만 커밋)
        if processed_messages:
            try:
                # 각 파티션별로 마지막 오프셋 추적
                from confluent_kafka import TopicPartition
                offsets_to_commit = {}
                
                for msg in processed_messages:
                    topic = msg.topic()
                    partition = msg.partition()
                    offset = msg.offset()
                    
                    key = (topic, partition)
                    # 각 파티션의 최대 오프셋만 유지 (다음 오프셋 커밋)
                    if key not in offsets_to_commit or offsets_to_commit[key] < offset + 1:
                        offsets_to_commit[key] = offset + 1
                
                # TopicPartition 리스트 생성
                tps_to_commit = [
                    TopicPartition(topic, partition, offset)
                    for (topic, partition), offset in offsets_to_commit.items()
                ]
                
                if tps_to_commit:
                    self.consumer.commit(offsets=tps_to_commit, asynchronous=False)
                    logger.debug(f"✅ {len(processed_messages)}개 메시지 커밋 완료 ({len(tps_to_commit)}개 파티션)")
                
            except Exception as e:
                logger.error(f"❌ 오프셋 커밋 실패: {e}", exc_info=True)
                error_counter.labels(error_type="commit_error").inc() if error_counter else None
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
        
        return len(failed_messages) == 0
    
    def commit(self):
        """수동 오프셋 커밋"""
        if self.consumer:
            try:
                self.consumer.commit()
                logger.debug("✅ 오프셋 커밋 완료")
            except Exception as e:
                logger.error(f"❌ 오프셋 커밋 실패: {e}", exc_info=True)
    
    def run(self):
        """메인 루프 실행"""
        if not self.running:
            self.start()
        
        logger.info("🔄 Kafka Consumer 메인 루프 시작")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        # 파티션 할당 대기 (최대 60초)
        partition_wait_time = 0
        max_partition_wait = 60
        partition_check_interval = 5
        
        while not self.shutdown_requested:
            try:
                # 파티션 할당 확인
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
                messages = self.consume_batch(timeout=0.5)  # Poll 최적화: 0.3 -> 0.5초 (더 큰 배치 수집)
                
                if not messages:
                    consecutive_errors = 0  # 메시지가 없어도 정상
                    continue
                
                if len(messages) > 0:
                    logger.debug(f"📦 {len(messages)}개 메시지 수신")  # 성능 최적화: 로그 레벨 낮춤
                
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
    
    def close(self):
        """Consumer 종료 (Graceful shutdown)"""
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
    """메인 실행 함수"""
    # DB 세션 생성
    db_session = SessionLocal() if SessionLocal else None
    
    # Consumer 생성 및 실행
    consumer = MunovaKafkaConsumer(
        db_session=db_session
    )
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("⚠️ 키보드 인터럽트 수신")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

