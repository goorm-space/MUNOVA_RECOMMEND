"""
MongoDB 연결 및 컬렉션 관리
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from typing import Optional, Dict, Any, List
import yaml
import os
from datetime import datetime

# 설정 로드
config_path = "/app/config.yml" if os.path.exists("/app/config.yml") else "config.yml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# MongoDB 클라이언트 (전역)
mongodb_client: Optional[MongoClient] = None
mongodb_db = None


def get_mongodb_client() -> Optional[MongoClient]:
    """MongoDB 클라이언트 생성 및 반환"""
    global mongodb_client
    
    if not config.get("mongodb", {}).get("enabled", False):
        return None
    
    if mongodb_client is not None:
        try:
            # 연결 상태 확인
            mongodb_client.admin.command('ping')
            return mongodb_client
        except (ConnectionFailure, ServerSelectionTimeoutError):
            # 연결이 끊어진 경우 재연결
            mongodb_client = None
    
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
            maxPoolSize=max_pool_size,
            minPoolSize=min_pool_size,
            serverSelectionTimeoutMS=5000,  # 5초 타임아웃
            connectTimeoutMS=5000,
            socketTimeoutMS=5000
        )
        
        # 연결 테스트
        mongodb_client.admin.command('ping')
        print("✅ MongoDB 연결 완료")
        return mongodb_client
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"⚠️ MongoDB 연결 실패: {e}")
        mongodb_client = None
        return None
    except Exception as e:
        print(f"⚠️ MongoDB 초기화 오류: {e}")
        mongodb_client = None
        return None


def get_mongodb_db():
    """MongoDB 데이터베이스 반환"""
    global mongodb_db
    
    client = get_mongodb_client()
    if client is None:
        return None
    
    if mongodb_db is None:
        database_name = config.get("mongodb", {}).get("database", "recommend")
        mongodb_db = client[database_name]
    
    return mongodb_db


# 컬렉션 이름 상수
COLLECTION_RECOMMENDATION_LOGS = "recommendation_logs"  # 추천 결과 로그
COLLECTION_USER_ACTION_LOGS = "user_action_logs"  # 사용자 행동 로그
COLLECTION_RECOMMENDATION_HISTORY = "recommendation_history"  # 추천 히스토리
COLLECTION_USER_ACTION_SUMMARIES = "user_action_summaries"  # 사용자 행동 요약 (MySQL 대체)
COLLECTION_USER_RECOMMENDATIONS = "user_recommendations"  # 사용자 추천 결과 (MySQL 대체)
COLLECTION_PRODUCT_RECOMMENDATIONS = "product_recommendations"  # 상품 추천 결과 (MySQL 대체)


def save_recommendation_log(
    member_id: int,
    product_id: int,
    score: float,
    event_type: str,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    추천 결과 로그를 MongoDB에 저장
    
    Args:
        member_id: 사용자 ID
        product_id: 상품 ID
        score: 추천 점수
        event_type: 이벤트 타입
        metadata: 추가 메타데이터
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
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
        print(f"⚠️ 추천 로그 저장 실패: {e}")
        return False


def save_user_action_log(
    member_id: int,
    product_id: int,
    event_type: str,
    fields: Dict[str, Any],
    stream_key: Optional[str] = None,
    message_id: Optional[str] = None
) -> bool:
    """
    사용자 행동 로그를 MongoDB에 저장 (원본 이벤트)
    
    Args:
        member_id: 사용자 ID
        product_id: 상품 ID
        event_type: 이벤트 타입
        fields: 원본 이벤트 필드
    stream_key: Kafka Topic (이전 Redis Stream 키, 호환성 유지)
    message_id: Kafka Offset (이전 Redis Stream 메시지 ID, 호환성 유지)
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
        return False
    
    try:
        collection = db[COLLECTION_USER_ACTION_LOGS]
        
        document = {
            "member_id": member_id,
            "product_id": product_id,
            "event_type": event_type,
            "stream_key": stream_key,
            "message_id": message_id,
            "raw_fields": fields,  # 원본 데이터 보존
            "created_at": datetime.utcnow()
        }
        
        collection.insert_one(document)
        return True
    except Exception as e:
        print(f"⚠️ 사용자 행동 로그 저장 실패: {e}")
        return False


def save_user_action_logs_batch(documents: List[Dict[str, Any]]) -> bool:
    """
    사용자 행동 로그를 배치로 MongoDB에 저장 (성능 최적화)
    
    Args:
        documents: 저장할 문서 리스트 (이미 포맷된 문서)
    
    Returns:
        저장 성공 여부
    """
    if not documents:
        return True
    
    db = get_mongodb_db()
    if db is None:
        return False
    
    try:
        collection = db[COLLECTION_USER_ACTION_LOGS]
        # 고성능: ordered=False, write concern 최적화 (w=0: ack 없음, j=False: journaling 없음)
        from pymongo import WriteConcern
        collection.with_options(write_concern=WriteConcern(w=0, j=False)).insert_many(documents, ordered=False)
        return True
    except Exception as e:
        print(f"⚠️ 사용자 행동 로그 배치 저장 실패: {e}")
        return False


def save_recommendation_logs_batch(documents: List[Dict[str, Any]]) -> bool:
    """
    추천 로그를 배치로 MongoDB에 저장 (성능 최적화)
    
    Args:
        documents: 저장할 문서 리스트 (이미 포맷된 문서)
    
    Returns:
        저장 성공 여부
    """
    if not documents:
        return True
    
    db = get_mongodb_db()
    if db is None:
        return False
    
    try:
        collection = db[COLLECTION_RECOMMENDATION_LOGS]
        # 고성능: ordered=False, write concern 최적화 (w=0: ack 없음, j=False: journaling 없음)
        from pymongo import WriteConcern
        collection.with_options(write_concern=WriteConcern(w=0, j=False)).insert_many(documents, ordered=False)
        return True
    except Exception as e:
        print(f"⚠️ 추천 로그 배치 저장 실패: {e}")
        return False


def save_recommendation_history(
    member_id: int,
    recommendations: List[Dict[str, Any]],
    recommendation_type: str = "user_based"
) -> bool:
    """
    추천 히스토리를 MongoDB에 저장 (사용자별 추천 목록 스냅샷)
    
    Args:
        member_id: 사용자 ID
        recommendations: 추천 상품 목록 [{"product_id": int, "score": float}, ...]
        recommendation_type: 추천 타입 ("user_based", "product_based" 등)
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
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
        print(f"⚠️ 추천 히스토리 저장 실패: {e}")
        return False


def create_indexes():
    """MongoDB 컬렉션에 인덱스 생성 (성능 최적화)"""
    db = get_mongodb_db()
    if db is None:
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
        user_action_logs.create_index([("event_type", 1)])
        
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
        
        print("✅ MongoDB 인덱스 생성 완료")
    except Exception as e:
        print(f"⚠️ MongoDB 인덱스 생성 실패: {e}")


# ============================================
# MySQL 대체 함수들 (MongoDB로 전환)
# ============================================

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
    session_id: Optional[str] = None
) -> bool:
    """
    사용자 행동 요약을 MongoDB에 저장/업데이트 (MySQL UserActionSummary 대체)
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
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
        
        # 기존 값 유지하면서 업데이트
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
            
        if session_id is not None:
            update_doc["last_session_id"] = session_id
        elif existing and "last_session_id" in existing:
            update_doc["last_session_id"] = existing["last_session_id"]
        
        # Upsert (없으면 생성, 있으면 업데이트)
        collection.update_one(
            {"member_id": member_id, "product_id": product_id},
            {"$set": update_doc},
            upsert=True
        )
        return True
    except Exception as e:
        print(f"⚠️ 사용자 행동 요약 저장 실패: {e}")
        return False


def get_user_action_summary(member_id: int, product_id: int) -> Optional[Dict[str, Any]]:
    """
    사용자 행동 요약 조회 (MySQL UserActionSummary 대체)
    
    Returns:
        사용자 행동 요약 딕셔너리 또는 None
    """
    db = get_mongodb_db()
    if db is None:
        return None
    
    try:
        collection = db[COLLECTION_USER_ACTION_SUMMARIES]
        doc = collection.find_one({
            "member_id": member_id,
            "product_id": product_id
        })
        return doc
    except Exception as e:
        print(f"⚠️ 사용자 행동 요약 조회 실패: {e}")
        return None


def save_user_recommendation(member_id: int, product_id: int, score: float) -> bool:
    """
    사용자 추천 결과 저장 (상위 8개만 유지) - MySQL UserRecommendation 대체
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
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
        print(f"⚠️ 사용자 추천 결과 저장 실패: {e}")
        return False


def get_user_recommendations(member_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    """
    사용자 추천 결과 조회 (MySQL UserRecommendation 대체)
    
    Returns:
        추천 결과 리스트
    """
    db = get_mongodb_db()
    if db is None:
        return []
    
    try:
        collection = db[COLLECTION_USER_RECOMMENDATIONS]
        recommendations = list(collection.find(
            {"member_id": member_id}
        ).sort("score", -1).limit(limit))
        
        # ObjectId를 문자열로 변환
        for rec in recommendations:
            rec["id"] = str(rec.pop("_id"))
        
        return recommendations
    except Exception as e:
        print(f"⚠️ 사용자 추천 결과 조회 실패: {e}")
        return []


def save_product_recommendation(source_product_id: int, target_product_id: int) -> bool:
    """
    상품 추천 결과 저장 (MySQL ProductRecommendation 대체)
    
    Returns:
        저장 성공 여부
    """
    db = get_mongodb_db()
    if db is None:
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
        print(f"⚠️ 상품 추천 결과 저장 실패: {e}")
        return False


def get_product_recommendations(product_id: int, limit: int = 4) -> List[Dict[str, Any]]:
    """
    상품 추천 결과 조회 (MySQL ProductRecommendation 대체)
    
    Returns:
        추천 결과 리스트
    """
    db = get_mongodb_db()
    if db is None:
        return []
    
    try:
        collection = db[COLLECTION_PRODUCT_RECOMMENDATIONS]
        recommendations = list(collection.find(
            {"source_product_id": product_id}
        ).limit(limit))
        
        # ObjectId를 문자열로 변환
        for rec in recommendations:
            rec["id"] = str(rec.pop("_id"))
        
        return recommendations
    except Exception as e:
        print(f"⚠️ 상품 추천 결과 조회 실패: {e}")
        return []


# 초기화 시 인덱스 생성
if __name__ != "__main__":
    # 모듈 로드 시 자동으로 인덱스 생성 (한 번만 실행)
    try:
        create_indexes()
    except Exception as e:
        print(f"⚠️ 초기 인덱스 생성 실패: {e}")

