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
        stream_key: Redis Stream 키
        message_id: Redis Stream 메시지 ID
    
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
        
        print("✅ MongoDB 인덱스 생성 완료")
    except Exception as e:
        print(f"⚠️ MongoDB 인덱스 생성 실패: {e}")


# 초기화 시 인덱스 생성
if __name__ != "__main__":
    # 모듈 로드 시 자동으로 인덱스 생성 (한 번만 실행)
    try:
        create_indexes()
    except Exception as e:
        print(f"⚠️ 초기 인덱스 생성 실패: {e}")

