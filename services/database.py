"""
데이터베이스 연결 및 모델 정의
"""
from sqlalchemy import create_engine, Column, Integer, Boolean, DateTime, Float, ForeignKey, UniqueConstraint, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import yaml

# 설정 로드
with open("/app/config.yml", "r") as f:
    config = yaml.safe_load(f)

Base = declarative_base()


class UserActionSummary(Base):
    """사용자 행동 요약 테이블"""
    __tablename__ = "user_action_summary"
    __table_args__ = (
        UniqueConstraint('member_id', 'product_id', name='uq_member_product'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    member_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    
    # 행동 정보
    clicked = Column(Integer, default=0)
    liked = Column(Boolean, default=False)
    in_cart = Column(Boolean, default=False)
    purchased = Column(Boolean, default=False)
    
    # 행동 시간
    clicked_at = Column(DateTime, nullable=True)
    liked_at = Column(DateTime, nullable=True)
    in_cart_at = Column(DateTime, nullable=True)
    purchased_at = Column(DateTime, nullable=True)
    
    # 상품 메타데이터 (추천 정밀도 향상을 위해 저장)
    category_id = Column(Integer, nullable=True, index=True)
    brand_id = Column(Integer, nullable=True, index=True)
    price = Column(Integer, nullable=True)  # 가격 정보
    
    # 세션 정보 (나중에 세션 기반 추천에 활용)
    last_session_id = Column(String(255), nullable=True)
    
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UserRecommendation(Base):
    """사용자 기반 추천 결과 테이블"""
    __tablename__ = "user_recommendation"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    member_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    score = Column(Float, nullable=False)  # 추천 점수 (0~100)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProductRecommendation(Base):
    """상품 기반 유사 상품 추천 결과 테이블"""
    __tablename__ = "product_recommendation"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_product_id = Column(Integer, nullable=False, index=True)  # 기준 상품
    target_product_id = Column(Integer, nullable=False, index=True)  # 추천 상품
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# DB 연결 설정 (나중에 config.yml에서 읽어오도록 수정)
def get_database_url():
    """데이터베이스 URL 생성"""
    db_config = config.get("database", {})
    if not db_config:
        return None
    
    db_type = db_config.get("type", "mysql")
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 3306)
    database = db_config.get("database", "recommend")
    username = db_config.get("username", "root")
    password = db_config.get("password", "")
    
    if db_type == "mysql":
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    elif db_type == "postgresql":
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    else:
        return None


# DB 엔진 및 세션 생성
database_url = get_database_url()
engine = None
SessionLocal = None

if database_url:
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        echo=False  # SQL 로그 출력 여부
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # 테이블 생성 (개발용 - 프로덕션에서는 마이그레이션 도구 사용 권장)
    # Base.metadata.create_all(bind=engine)
else:
    print("⚠️ 데이터베이스 설정이 없습니다. config.yml에 database 설정을 추가하세요.")


def get_db():
    """DB 세션 생성 (의존성 주입용)"""
    if SessionLocal is None:
        # DB가 설정되지 않은 경우 None 반환 (테스트용)
        yield None
        return
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

