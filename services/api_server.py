"""
FastAPI 서버 - 추천 상품 제공 API
"""
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from typing import List, Optional
import yaml
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# 설정 로드 (로컬/도커 환경 모두 지원)
import os
config_path = "/app/config.yml" if os.path.exists("/app/config.yml") else "config.yml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# 추천 로직 import
from services.recommender import Recommender
from services.mongodb import get_user_recommendations, get_product_recommendations

app = FastAPI(
    title="MUNOVA Recommend Server",
    description="추천 서버 API",
    version="1.0.0"
)

# 전역 Recommender
recommender: Optional[Recommender] = None


@app.on_event("startup")
async def startup_event():
    """서버 시작 시 Recommender 초기화"""
    global recommender
    
    try:
        # Recommender 초기화 (MongoDB 사용)
        recommender = Recommender()
        print("✅ Recommender 초기화 완료")
    except Exception as e:
        print(f"⚠️ Recommender 초기화 실패: {e}")
        recommender = None


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
def get_user_recommendations_api(
    member_id: int,
    limit: Optional[int] = 8
):
    """
    사용자 기반 추천 상품 조회 (MongoDB에서 조회)
    
    Args:
        member_id: 사용자 ID
        limit: 추천 상품 개수 (기본값: 8)
    
    Returns:
        추천 상품 목록
    """
    try:
        recommendations = get_user_recommendations(member_id, limit)
        
        result = [
            {
                "product_id": rec.get("product_id"),
                "score": rec.get("score"),
                "created_at": rec.get("created_at").isoformat() if rec.get("created_at") else None,
                "updated_at": rec.get("updated_at").isoformat() if rec.get("updated_at") else None
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
def get_product_recommendations_api(
    product_id: int,
    limit: Optional[int] = 4
):
    """
    상품 기반 유사 상품 추천 조회 (MongoDB에서 조회)
    
    Args:
        product_id: 상품 ID
        limit: 추천 상품 개수 (기본값: 4)
    
    Returns:
        유사 상품 목록
    """
    try:
        recommendations = get_product_recommendations(product_id, limit)
        
        result = [
            {
                "target_product_id": rec.get("target_product_id"),
                "created_at": rec.get("created_at").isoformat() if rec.get("created_at") else None
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
    product_id: int
):
    """
    추천 점수 조회 (MongoDB에서 사용자 행동 조회 후 계산)
    
    Args:
        member_id: 사용자 ID
        product_id: 상품 ID
    
    Returns:
        추천 점수 (0~100)
    """
    global recommender
    
    if recommender is None:
        raise HTTPException(status_code=500, detail="Recommender가 초기화되지 않았습니다.")
    
    try:
        # 점수 계산 (MongoDB에서 자동으로 조회)
        score = recommender.get_recommendation_score(member_id, product_id)
        
        return {
            "member_id": member_id,
            "product_id": product_id,
            "score": round(score, 2)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"추천 점수 계산 실패: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

