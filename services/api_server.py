"""
FastAPI 서버 - 추천 상품 제공 API
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import Response
from typing import List, Optional
from sqlalchemy.orm import Session
import yaml
from redis.cluster import RedisCluster, ClusterNode
from redis.exceptions import ConnectionError, TimeoutError
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# 설정 로드 (로컬/도커 환경 모두 지원)
import os
config_path = "/app/config.yml" if os.path.exists("/app/config.yml") else "config.yml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# DB 및 추천 로직 import
from services.database import get_db, UserRecommendation, ProductRecommendation, UserActionSummary
from services.recommender import Recommender

app = FastAPI(
    title="MUNOVA Recommend Server",
    description="추천 서버 API",
    version="1.0.0"
)

# 전역 Redis 클라이언트 및 Recommender
redis_client: Optional[RedisCluster] = None
recommender: Optional[Recommender] = None


def create_redis_client():
    """Redis Cluster 클라이언트 생성 (재연결용)"""
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
        reinitialize_steps=10  # 10번 실패 시 재초기화
    )


@app.on_event("startup")
async def startup_event():
    """서버 시작 시 Redis 클라이언트 및 Recommender 초기화"""
    global redis_client, recommender
    
    try:
        # Redis Cluster 연결
        redis_client = create_redis_client()
        # 연결 테스트
        redis_client.ping()
        print("✅ Redis Cluster 연결 완료 (FastAPI)")
        
        # Recommender 초기화 (DB 세션은 매 요청마다 생성)
        recommender = Recommender(redis_client=redis_client, db_session=None)
        print("✅ Recommender 초기화 완료")
    except Exception as e:
        print(f"⚠️ Redis Cluster 연결 실패: {e}")
        print("⚠️ Redis 없이도 API 서버는 동작하지만, 추천 점수 계산 기능은 사용할 수 없습니다.")
        redis_client = None
        recommender = None


@app.on_event("shutdown")
async def shutdown_event():
    """서버 종료 시 Redis 클라이언트 종료"""
    global redis_client
    if redis_client:
        try:
            redis_client.close()
            print("✅ Redis Cluster 연결 종료")
        except Exception as e:
            print(f"⚠️ Redis 연결 종료 중 오류: {e}")


@app.get("/")
def root():
    """헬스 체크"""
    return {"status": "ok", "service": "recommend-server"}


@app.get("/health")
def health():
    """헬스 체크"""
    return {"status": "healthy"}


@app.get("/metrics")
def metrics():
    """Prometheus metrics 엔드포인트"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/recommend/user/{member_id}")
def get_user_recommendations(
    member_id: int,
    limit: Optional[int] = 8,
    db: Session = Depends(get_db)
):
    """
    사용자 기반 추천 상품 조회
    
    Args:
        member_id: 사용자 ID
        limit: 추천 상품 개수 (기본값: 8)
        db: 데이터베이스 세션
    
    Returns:
        추천 상품 목록
    """
    try:
        if db is None:
            return {
                "member_id": member_id,
                "count": 0,
                "recommendations": [],
                "message": "데이터베이스가 연결되지 않았습니다."
            }
        
        recommendations = db.query(UserRecommendation).filter(
            UserRecommendation.member_id == member_id
        ).order_by(UserRecommendation.score.desc()).limit(limit).all()
        
        result = [
            {
                "product_id": rec.product_id,
                "score": rec.score,
                "created_at": rec.created_at.isoformat() if rec.created_at else None,
                "updated_at": rec.updated_at.isoformat() if rec.updated_at else None
            }
            for rec in recommendations
        ]
        
        return {
            "member_id": member_id,
            "count": len(result),
            "recommendations": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"추천 상품 조회 실패: {str(e)}")


@app.get("/api/recommend/product/{product_id}")
def get_product_recommendations(
    product_id: int,
    limit: Optional[int] = 4,
    db: Session = Depends(get_db)
):
    """
    상품 기반 유사 상품 추천 조회
    
    Args:
        product_id: 상품 ID
        limit: 추천 상품 개수 (기본값: 4)
        db: 데이터베이스 세션
    
    Returns:
        유사 상품 목록
    """
    try:
        if db is None:
            return {
                "product_id": product_id,
                "count": 0,
                "recommendations": [],
                "message": "데이터베이스가 연결되지 않았습니다."
            }
        
        recommendations = db.query(ProductRecommendation).filter(
            ProductRecommendation.source_product_id == product_id
        ).limit(limit).all()
        
        result = [
            {
                "target_product_id": rec.target_product_id,
                "created_at": rec.created_at.isoformat() if rec.created_at else None
            }
            for rec in recommendations
        ]
        
        return {
            "product_id": product_id,
            "count": len(result),
            "recommendations": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유사 상품 조회 실패: {str(e)}")


@app.get("/api/recommend/user/{member_id}/product/{product_id}/score")
def get_recommendation_score(
    member_id: int,
    product_id: int,
    db: Session = Depends(get_db)
):
    """
    추천 점수 조회
    
    Args:
        member_id: 사용자 ID
        product_id: 상품 ID
        db: 데이터베이스 세션
    
    Returns:
        추천 점수 (0~100)
    """
    global redis_client, recommender
    
    if recommender is None:
        raise HTTPException(status_code=500, detail="Recommender가 초기화되지 않았습니다. Redis 연결을 확인하세요.")
    
    # Redis 연결 상태 확인 및 재연결
    if redis_client:
        try:
            redis_client.ping()
        except (ConnectionError, TimeoutError) as e:
            print(f"⚠️ Redis 연결 끊김 감지: {e}. 재연결 시도...")
            try:
                redis_client.close()
                redis_client = create_redis_client()
                redis_client.ping()
                # Recommender에도 새 클라이언트 전달
                recommender.redis_client = redis_client
                print("✅ Redis 재연결 성공")
            except Exception as reconnect_e:
                print(f"❌ Redis 재연결 실패: {reconnect_e}")
                raise HTTPException(status_code=500, detail="Redis 연결이 끊어졌고 재연결에 실패했습니다.")
    
    try:
        # DB 세션을 Recommender에 설정
        recommender.db_session = db
        
        # 점수 계산
        score = recommender.get_recommendation_score(member_id, product_id)
        
        return {
            "member_id": member_id,
            "product_id": product_id,
            "score": round(score, 2)
        }
    except (ConnectionError, TimeoutError) as e:
        raise HTTPException(status_code=500, detail=f"Redis 연결 오류: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"추천 점수 계산 실패: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

