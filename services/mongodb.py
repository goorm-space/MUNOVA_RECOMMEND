import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import yaml
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)

# 설정 로드
config_path = "/app/config.yml" if os.path.exists("/app/config.yml") else "config.yml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# MongoDB 클라이언트 (전역)
mongodb_client: Optional[MongoClient] = None
mongodb_db = None
mongodb_warning_logged = False  # Db 경고 로그 한번만 찍기


# MongoClient 싱글톤 생성 -> 이미 있으면 ping으로 살아있는지 확인 후 재사용
def get_mongodb_client() -> Optional[MongoClient]:
    global mongodb_client, mongodb_db

    if not config.get("mongodb", {}).get("enabled", False):
        logger.info("[get_mongodb_client] MongoDB 비활성화 상태(enabled=False)")
        return None

    # MongoDB 재연결
    if mongodb_client is not None:
        try:
            # 연결 상태 확인
            mongodb_client.admin.command('ping')
            return mongodb_client
        except (ConnectionFailure, ServerSelectionTimeoutError):
            # 연결이 끊어진 경우 재연결
            logger.warning("[get_mongodb_client] 기존 MongoDB 커넥션 끊김. 재연결 시도")
            mongodb_client = None
            mongodb_db = None

    try:
        mongodb_config = config["mongodb"]
        host = mongodb_config.get("host", "localhost")
        port = mongodb_config.get("port", 27017)
        username = mongodb_config.get("username", "")
        password = mongodb_config.get("password", "")
        auth_source = mongodb_config.get("auth_source", "admin")
        max_pool_size = mongodb_config.get("max_pool_size", 100)
        min_pool_size = mongodb_config.get("min_pool_size", 10)

        # 연결 문자열 생성
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}"
        else:
            connection_string = f"mongodb://{host}:{port}/"

        mongodb_client = MongoClient(
            connection_string,
            maxPoolSize=max_pool_size,  # 내부적으로 유지할 커넥션 풀 크기
            minPoolSize=min_pool_size,  # 내부적으로 유지할 커넥션 풀 크기
            serverSelectionTimeoutMS=5000,  # 어느 서버에 붙을지 결정하는데 최대 5초 기다림
            connectTimeoutMS=5000,  # TCP connect 타임아웃 5초
            socketTimeoutMS=5000  # 이미 연결된 후에도 응답 기다리는데 최대 시간 5초
        )

        # 연결 테스트
        mongodb_client.admin.command('ping')
        logger.info("✅ [get_mongodb_client] MongoDB 연결 완료")
        return mongodb_client

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error("⚠️ [get_mongodb_client] MongoDB 연결 실패", exc_info=True)
        mongodb_client = None
        mongodb_db = None
        return None
    except Exception as e:
        logger.error("⚠️ [get_mongodb_client] MongoDB 초기화 오류", exc_info=True)
        mongodb_client = None
        mongodb_db = None
        return None


def get_mongodb_db():
    global mongodb_db, mongodb_warning_logged

    client = get_mongodb_client()
    if client is None:
        if not mongodb_warning_logged:
            logger.warning("[get_mongodb_db] MongoDB 클라이언트를 가져오지 못했습니다. (enabled=False이거나 연결 실패)")
            mongodb_warning_logged = True
        return None

    if mongodb_warning_logged:
        logger.info("[get_mongodb_db] MongoDB 연결 복구됨")
        mongodb_warning_logged = False

    if mongodb_db is None:
        # 실제 DB 객체는 한 번만 생성
        database_name = config.get("mongodb", {}).get("database", "recommend")
        mongodb_db = client[database_name]
        logger.info(f"MongoDB DB 핸들 생성: database={database_name}")

    return mongodb_db


# 컬렉션 이름 상수
COLLECTION_RECOMMENDATION_LOGS = "recommendation_logs"  # 추천 결과 로그
COLLECTION_USER_ACTION_LOGS = "user_action_logs"  # 사용자 행동 로그
COLLECTION_RECOMMENDATION_HISTORY = "recommendation_history"  # 추천 히스토리 -> 스냅샷으로 저장으로 빠르게
COLLECTION_USER_ACTION_SUMMARIES = "user_action_summaries"  # 사용자 행동 요약 (MySQL 대체)
COLLECTION_USER_RECOMMENDATIONS = "user_recommendations"  # 사용자 추천 결과 (MySQL 대체)
COLLECTION_PRODUCT_RECOMMENDATIONS = "product_recommendations"  # 상품 추천 결과 (MySQL 대체)


# 추천 결과(log)를 저장
def save_recommendation_log(
        member_id: int,
        product_id: int,
        score: float,
        event_type: str,
        metadata: Optional[Dict[str, Any]] = None
) -> bool:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_recommendation_log] MongoDB 사용 불가 — 로그 저장 스킵")
        return False

    try:
        collection = db[COLLECTION_RECOMMENDATION_LOGS]

        document = {
            "member_id": member_id,
            "product_id": product_id,
            "score": score,
            "event_type": event_type,
            "created_at": datetime.utcnow(),
            "metadata": metadata or {}
        }

        collection.insert_one(document)
        return True
    except Exception as e:
        logger.error("[save_recommendation_log] 추천 로그 저장 실패", exc_info=True)
        return False


# 원본 사용자 행동 이벤트 저장 -> 가공x
def save_user_action_log(
        member_id: int,
        product_id: int,
        event_type: str,
        fields: Dict[str, Any],
        stream_key: Optional[str] = None,
        message_id: Optional[str] = None
) -> bool:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_user_action_log] MongoDB 사용 불가 — 저장 스킵")
        return False

    try:
        collection = db[COLLECTION_USER_ACTION_LOGS]

        document = {
            "member_id": member_id,
            "product_id": product_id,
            "event_type": event_type,
            "kafka_topic": stream_key,
            "message_id": message_id,
            "raw_fields": fields,  # 원본 데이터 보존
            "created_at": datetime.utcnow()
        }

        collection.insert_one(document)
        return True
    except Exception as e:
        logger.error("⚠️ [save_user_action_log] 사용자 행동 로그 저장 실패", exc_info=True)
        return False


# kafka에서 모든 사용자 행동 로그 한 배치를 MongoDB에 저장
def save_user_action_logs_batch(documents: List[Dict[str, Any]]) -> bool:
    if not documents:
        return True

    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_user_action_logs_batch] MongoDB 사용 불가 — 배치 스킵")
        return False

    try:
        collection = db[COLLECTION_USER_ACTION_LOGS]
        # 고성능: ordered=False, write concern 최적화 (w=0: ack 없음, j=False: journaling 없음)
        from pymongo import WriteConcern
        # WriteConcern(w=0, j=False): w=0 -> write 성공 여부에 대한 ACK 안받음, j=false -> journal에 기록될 때까지 기다리지않음
        # 이미 consumer에서 DLQ로직으로 처리하므로 무리 없이 그냥 넣음
        # ordered=False -> 중간에 하나 실패해도, 무시하고 그냥 넣음
        collection.with_options(write_concern=WriteConcern(w=0, j=False)).insert_many(documents, ordered=False)
        return True
    except Exception as e:
        logger.error("⚠️ [save_user_action_logs_batch] 사용자 행동 로그 배치 저장 실패", exc_info=True)
        return False


# 추천 로그 저장
def save_recommendation_logs_batch(documents: List[Dict[str, Any]]) -> bool:
    if not documents:
        return True

    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_recommendation_logs_batch] MongoDB 사용 불가 — 배치 스킵")
        return False

    try:
        collection = db[COLLECTION_RECOMMENDATION_LOGS]
        # 고성능: ordered=False, write concern 최적화 (w=0: ack 없음, j=False: journaling 없음)
        from pymongo import WriteConcern
        collection.with_options(write_concern=WriteConcern(w=0, j=False)).insert_many(documents, ordered=False)
        return True
    except Exception as e:
        logger.error("⚠️ [save_recommendation_logs_batch] 추천 로그 배치 저장 실패", exc_info=True)
        return False


# 추천 결과 스냅샷 저장
def save_recommendation_history(
        member_id: int,
        recommendations: List[Dict[str, Any]],
        recommendation_type: str = "user_based"
) -> bool:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_recommendation_history] MongoDB 사용 불가 — 스냅샷 스킵")
        return False

    try:
        collection = db[COLLECTION_RECOMMENDATION_HISTORY]

        document = {
            "member_id": member_id,
            "recommendation_type": recommendation_type,
            "recommendations": recommendations,
            "count": len(recommendations),
            "created_at": datetime.utcnow()
        }

        collection.insert_one(document)
        return True
    except Exception as e:
        logger.error("⚠️ [save_recommendation_history] 추천 히스토리 저장 실패", exc_info=True)
        return False


# Index 생성 (Race Condition 방지를 위한 Lock 메커니즘 포함)
def create_indexes():
    db = get_mongodb_db()
    if db is None:
        logger.warning("[create_indexes] MongoDB 사용 불가 — 인덱스 생성 스킵")
        return

    # Lock 컬렉션을 사용한 동시 실행 방지
    lock_collection = db["_index_creation_lock"]
    lock_key = "index_creation_in_progress"
    
    # Lock 획득 시도 (최대 30초 대기)
    import time
    max_wait = 30
    wait_interval = 1
    waited = 0
    
    while waited < max_wait:
        try:
            # Lock 문서가 없거나 만료된 경우에만 생성 시도
            lock_doc = lock_collection.find_one({"_id": lock_key})
            if lock_doc is None:
                # Lock 생성 시도 (5분 TTL)
                lock_collection.insert_one({
                    "_id": lock_key,
                    "created_at": datetime.utcnow(),
                    "expires_at": datetime.utcnow().replace(second=0, microsecond=0) + timedelta(minutes=5)
                })
                logger.info("[create_indexes] Lock 획득 성공 - 인덱스 생성 시작")
                break
            else:
                # Lock이 만료되었는지 확인
                expires_at = lock_doc.get("expires_at")
                if expires_at and datetime.utcnow() > expires_at:
                    # 만료된 Lock 삭제 후 재시도
                    lock_collection.delete_one({"_id": lock_key})
                    continue
                else:
                    # 다른 프로세스가 인덱스 생성 중
                    logger.info(f"[create_indexes] 다른 프로세스가 인덱스 생성 중. 대기 중... ({waited}/{max_wait}초)")
                    time.sleep(wait_interval)
                    waited += wait_interval
        except Exception as e:
            # 중복 키 에러는 다른 프로세스가 이미 Lock을 획득한 것
            if "duplicate key" in str(e).lower() or "E11000" in str(e):
                logger.info(f"[create_indexes] Lock 획득 실패 (다른 프로세스 실행 중). 대기 중... ({waited}/{max_wait}초)")
                time.sleep(wait_interval)
                waited += wait_interval
            else:
                logger.warning(f"[create_indexes] Lock 획득 중 오류: {e}")
                time.sleep(wait_interval)
                waited += wait_interval
    
    if waited >= max_wait:
        logger.warning("[create_indexes] Lock 획득 시간 초과. 다른 프로세스가 인덱스 생성 중일 수 있습니다. 인덱스 생성 스킵.")
        return
    
    try:
        # 추천 로그 인덱스
        recommendation_logs = db[COLLECTION_RECOMMENDATION_LOGS]
        recommendation_logs.create_index([("member_id", 1), ("product_id", 1)])
        recommendation_logs.create_index([("created_at", -1)])
        recommendation_logs.create_index([("score", -1)])

        # 사용자 행동 로그 인덱스
        user_action_logs = db[COLLECTION_USER_ACTION_LOGS]
        user_action_logs.create_index([("member_id", 1), ("product_id", 1)])
        user_action_logs.create_index([("created_at", -1)])
        user_action_logs.create_index(
            [("created_at", 1)],
            expireAfterSeconds=60 * 60 * 24 * 90  # 90일 뒤 자동 삭제
        )
        # 추천 히스토리 인덱스
        recommendation_history = db[COLLECTION_RECOMMENDATION_HISTORY]
        recommendation_history.create_index([("member_id", 1), ("created_at", -1)])
        recommendation_history.create_index([("recommendation_type", 1)])

        # 사용자 행동 요약 인덱스 (MySQL 대체)
        user_action_summaries = db[COLLECTION_USER_ACTION_SUMMARIES]
        user_action_summaries.create_index([("member_id", 1), ("product_id", 1)], unique=True)
        user_action_summaries.create_index([("member_id", 1)])
        user_action_summaries.create_index([("product_id", 1)])
        user_action_summaries.create_index([("category_id", 1)])
        user_action_summaries.create_index([("brand_id", 1)])

        # 사용자 추천 결과 인덱스 (MySQL 대체)
        user_recommendations = db[COLLECTION_USER_RECOMMENDATIONS]
        user_recommendations.create_index([("member_id", 1), ("product_id", 1)], unique=True)
        user_recommendations.create_index([("member_id", 1), ("score", -1)])
        user_recommendations.create_index([("product_id", 1)])

        # 상품 추천 결과 인덱스 (MySQL 대체)
        product_recommendations = db[COLLECTION_PRODUCT_RECOMMENDATIONS]
        product_recommendations.create_index([("source_product_id", 1), ("target_product_id", 1)], unique=True)
        product_recommendations.create_index([("source_product_id", 1)])

        logger.info("✅ [create_indexes] MongoDB 인덱스 생성 완료")
    except Exception as e:
        logger.error("⚠️ [create_indexes] MongoDB 인덱스 생성 실패", exc_info=True)
        return
    finally:
        # Lock 해제
        try:
            lock_collection.delete_one({"_id": lock_key})
            logger.debug("[create_indexes] Lock 해제 완료")
        except Exception as e:
            logger.warning(f"[create_indexes] Lock 해제 중 오류 (무시 가능): {e}")


# 집계 결과 스냅샷 -> UserActionSummary
def save_user_action_summary(
        member_id: int,
        product_id: int,
        clicked: int = 0,
        liked: Optional[bool] = None,
        in_cart: Optional[bool] = None,
        purchased: Optional[bool] = None,
        clicked_at: Optional[datetime] = None,
        liked_at: Optional[datetime] = None,
        in_cart_at: Optional[datetime] = None,
        purchased_at: Optional[datetime] = None,
        category_id: Optional[int] = None,
        brand_id: Optional[int] = None,
        price: Optional[int] = None,
) -> bool:
    """
    UserActionSummary 스냅샷 upsert.

    - clicked: 이번 이벤트에서 증가한 클릭 수(difference / delta)만 전달받는다고 가정.
      여기서 MongoDB의 기존 clicked 값에 누적해서 저장한다.
    - liked / in_cart / purchased: 현재 상태로 덮어쓰는 플래그.
    """
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_user_action_summary] MongoDB 사용 불가 — 저장 스킵")
        return False

    try:
        collection = db[COLLECTION_USER_ACTION_SUMMARIES]

        # 기존 문서 조회
        existing = collection.find_one({
            "member_id": member_id,
            "product_id": product_id
        })

        update_doc = {
            "member_id": member_id,
            "product_id": product_id,
            "last_updated": datetime.utcnow()
        }

        # 기존 문서 존재 시 누적/갱신 로직
        if existing:
            # clicked는 누적 (증가)
            if clicked > 0:
                update_doc["clicked"] = existing.get("clicked", 0) + clicked
                update_doc["clicked_at"] = clicked_at or datetime.utcnow()
            else:
                update_doc["clicked"] = existing.get("clicked", 0)
                update_doc["clicked_at"] = existing.get("clicked_at")

            # liked, in_cart, purchased는 덮어쓰기
            update_doc["liked"] = liked if liked is not None else existing.get("liked")
            update_doc["in_cart"] = in_cart if in_cart is not None else existing.get("in_cart")
            update_doc["purchased"] = purchased if purchased is not None else existing.get("purchased")
            update_doc["liked_at"] = liked_at if liked_at else existing.get("liked_at")
            update_doc["in_cart_at"] = in_cart_at if in_cart_at else existing.get("in_cart_at")
            update_doc["purchased_at"] = purchased_at if purchased_at else existing.get("purchased_at")
        # 기존 문서 없을때 새로 생성
        else:
            update_doc["clicked"] = clicked
            update_doc["liked"] = liked
            update_doc["in_cart"] = in_cart
            update_doc["purchased"] = purchased
            update_doc["clicked_at"] = clicked_at
            update_doc["liked_at"] = liked_at
            update_doc["in_cart_at"] = in_cart_at
            update_doc["purchased_at"] = purchased_at
            update_doc["created_at"] = datetime.utcnow()

        # 메타데이터 업데이트
        if category_id is not None:
            update_doc["category_id"] = category_id
        elif existing and "category_id" in existing:
            update_doc["category_id"] = existing["category_id"]

        if brand_id is not None:
            update_doc["brand_id"] = brand_id
        elif existing and "brand_id" in existing:
            update_doc["brand_id"] = existing["brand_id"]

        if price is not None:
            update_doc["price"] = price
        elif existing and "price" in existing:
            update_doc["price"] = existing["price"]

        # Upsert (없으면 insert, 있으면 update)
        collection.update_one(
            {"member_id": member_id, "product_id": product_id},
            {"$set": update_doc},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error("⚠️ [save_user_action_summary] 사용자 행동 요약 저장 실패", exc_info=True)
        return False


# 유저가 상품에 대한 쌓아둔 요약 스냅샷을 가져옴
def get_user_action_summary(member_id: int, product_id: int) -> Optional[Dict[str, Any]]:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[get_user_action_summary] MongoDB 사용 불가 — 조회 스킵")
        return None

    try:
        collection = db[COLLECTION_USER_ACTION_SUMMARIES]
        doc = collection.find_one({
            "member_id": member_id,
            "product_id": product_id
        })
        return doc
    except Exception as e:
        logger.error(f"⚠️ [get_user_action_summary] 조회 실패 (member_id={member_id}, product_id={product_id})",
                     exc_info=True)
        return None


# 유저별 추천 Top 8만 유지하는 저장 로직
def save_user_recommendation(member_id: int, product_id: int, score: float) -> bool:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_user_recommendation] MongoDB 사용 불가 — 저장 스킵")
        return False

    try:
        collection = db[COLLECTION_USER_RECOMMENDATIONS]

        # 기존 추천이 있으면 업데이트, 없으면 생성
        collection.update_one(
            {"member_id": member_id, "product_id": product_id},
            {
                "$set": {
                    "member_id": member_id,
                    "product_id": product_id,
                    "score": score,
                    "updated_at": datetime.utcnow()
                },
                "$setOnInsert": {
                    "created_at": datetime.utcnow()
                }
            },
            upsert=True
        )

        # 상위 8개만 유지 (나머지 삭제)
        all_recommendations = list(collection.find(
            {"member_id": member_id}
        ).sort("score", -1))

        if len(all_recommendations) > 8:
            # 8개 이후의 문서 삭제
            ids_to_delete = [doc["_id"] for doc in all_recommendations[8:]]
            collection.delete_many({"_id": {"$in": ids_to_delete}})

        return True
    except Exception as e:
        logger.error("⚠️ [save_user_recommendation] 사용자 추천 결과 저장 실패", exc_info=True)
        return False


# 사용자 추천 결과 조회
def get_user_recommendations(member_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[get_user_recommendations] MongoDB 사용 불가 — 조회 스킵")
        return []

    try:
        collection = db[COLLECTION_USER_RECOMMENDATIONS]
        # find + sort + limit
        recommendations = list(collection.find(
            {"member_id": member_id}
        ).sort("score", -1).limit(limit))

        # ObjectId를 문자열로 변환
        for rec in recommendations:
            rec["id"] = str(rec.pop("_id"))

        return recommendations
    except Exception as e:
        logger.error("⚠️ [get_user_recommendations] 사용자 추천 결과 조회 실패", exc_info=True)
        return []


# 상품 추천 결과 저장
def save_product_recommendation(source_product_id: int, target_product_id: int) -> bool:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[save_product_recommendation] MongoDB 사용 불가 — 저장 스킵")
        return False

    try:
        collection = db[COLLECTION_PRODUCT_RECOMMENDATIONS]
        collection.update_one(
            {"source_product_id": source_product_id, "target_product_id": target_product_id},
            {
                "$set": {
                    "source_product_id": source_product_id,
                    "target_product_id": target_product_id,
                    "updated_at": datetime.utcnow()
                },
                "$setOnInsert": {
                    "created_at": datetime.utcnow()
                }
            },
            upsert=True
        )
        return True
    except Exception as e:
        logger.error("⚠️ [save_product_recommendation] 상품 추천 결과 저장 실패", exc_info=True)
        return False


# 추천 상품 결과 조회
def get_product_recommendations(product_id: int, limit: int = 4) -> List[Dict[str, Any]]:
    db = get_mongodb_db()
    if db is None:
        logger.warning("[get_product_recommendations] MongoDB 사용 불가 — 조회 스킵")
        return []

    try:
        collection = db[COLLECTION_PRODUCT_RECOMMENDATIONS]
        recommendations = list(collection.find(
            {"source_product_id": product_id}
        ).sort("updated_at", -1).limit(limit))

        # ObjectId를 문자열로 변환
        for rec in recommendations:
            rec["id"] = str(rec.pop("_id"))

        return recommendations
    except Exception as e:
        logger.error("⚠️ [get_product_recommendations] 상품 추천 결과 조회 실패", exc_info=True)
        return []


# 초기화 시 인덱스 생성
if __name__ != "__main__":
    # 모듈 로드 시 자동으로 인덱스 생성 (한 번만 실행)
    try:
        create_indexes()
    except Exception as e:
        logger.error("⚠️ [module-init] 초기 인덱스 생성 실패", exc_info=True)  # 🔧 수정
