from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import math
from services.mongodb import (
    save_user_action_log, save_recommendation_log,
    save_user_action_summary, get_user_action_summary,
    save_user_recommendation
)

# 가중치 상수
CLICK_WEIGHT = 0.1
LIKE_WEIGHT = 0.15
CART_WEIGHT = 0.35
PURCHASE_WEIGHT = 0.5
DECAY_RATE = 0.05  # 하루당 5% 감쇠
MIN_DECAY = 0.5     # 최소 유지 비율
CACHE_TTL_MINUTES = 10


class UserActionSummary:
    """사용자 행동 요약 데이터"""
    def __init__(
        self,
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
        last_updated: Optional[datetime] = None,
        # 추가 메타데이터 (정밀한 추천을 위해)
        category_id: Optional[int] = None,
        brand_id: Optional[int] = None,
        price: Optional[int] = None,
        session_id: Optional[str] = None
    ):
        self.member_id = member_id
        self.product_id = product_id
        self.clicked = clicked
        self.liked = liked
        self.in_cart = in_cart
        self.purchased = purchased
        self.clicked_at = clicked_at
        self.liked_at = liked_at
        self.in_cart_at = in_cart_at
        self.purchased_at = purchased_at
        self.last_updated = last_updated or datetime.now()
        # 추가 메타데이터
        self.category_id = category_id
        self.brand_id = brand_id
        self.price = price
        self.session_id = session_id


class Recommender:
    def __init__(self, db_session=None):
        """
        Args:
            db_session: (사용하지 않음, MongoDB로 전환됨) 하위 호환성을 위해 유지
        """
        # MySQL 제거됨, MongoDB만 사용
        pass
        # 처리 대상 이벤트(추천 반영)
        self.supported_events = {
            "product_detail",
            "product_like",
            "cancel_product_like",
            "create_cart",
            "delete_cart",
            "end_payment",
            "refund",
        }
        # 조회성/운영성 이벤트(기본 무시)
        self.readonly_event_prefixes = ("find_",)
        self.readonly_event_suffixes = ("_detail",)
    
    def get_recommendation_score(
        self,
        member_id: int,
        product_id: int,
        summary: Optional[UserActionSummary] = None
    ) -> float:
        """
        추천 점수 계산 (0~100)
        
        Args:
            member_id: 사용자 ID
            product_id: 상품 ID
            summary: 사용자 행동 요약 (없으면 기본값 사용)
        
        Returns:
            추천 점수 (0~100)
        """
        if summary is None:
            summary = self._get_user_action_summary(member_id, product_id)
        
        now = datetime.now()
        
        total_score = (
            self._score_with_decay(summary.clicked_at, CLICK_WEIGHT, now)
            + self._score_with_decay(summary.liked_at, LIKE_WEIGHT, now, summary.liked)
            + self._score_with_decay(summary.in_cart_at, CART_WEIGHT, now, summary.in_cart)
            + self._score_with_decay(summary.purchased_at, PURCHASE_WEIGHT, now, summary.purchased)
        )
        
        max_score = CLICK_WEIGHT + LIKE_WEIGHT + CART_WEIGHT + PURCHASE_WEIGHT
        return (total_score / max_score) * 100
    
    def _score_with_decay(
        self,
        action_time: Optional[datetime],
        weight: float,
        now: datetime,
        condition: Optional[bool] = None
    ) -> float:
        """
        시간 감쇠를 적용한 점수 계산
        
        Args:
            action_time: 행동 발생 시간
            weight: 가중치
            now: 현재 시간
            condition: 조건 (liked, in_cart, purchased 등)
        
        Returns:
            감쇠가 적용된 점수
        """
        if condition is False:
            return 0
        if action_time is None:
            return 0
        
        days = (now - action_time).days
        decay_factor = max(MIN_DECAY, 1 - DECAY_RATE * days)
        return weight * decay_factor
    
    def _get_user_action_summary(
        self,
        member_id: int,
        product_id: int
    ) -> UserActionSummary:
        """
        사용자 행동 요약 조회 (MongoDB에서 조회)
        
        Args:
            member_id: 사용자 ID
            product_id: 상품 ID
        
        Returns:
            UserActionSummary 객체
        """
        # MongoDB에서 조회
        try:
            doc = get_user_action_summary(member_id, product_id)
            
            if doc:
                # MongoDB 문서를 UserActionSummary로 변환
                return UserActionSummary(
                    member_id=doc.get("member_id", member_id),
                    product_id=doc.get("product_id", product_id),
                    clicked=doc.get("clicked", 0) or 0,
                    liked=doc.get("liked"),
                    in_cart=doc.get("in_cart"),
                    purchased=doc.get("purchased"),
                    clicked_at=doc.get("clicked_at"),
                    liked_at=doc.get("liked_at"),
                    in_cart_at=doc.get("in_cart_at"),
                    purchased_at=doc.get("purchased_at"),
                    last_updated=doc.get("last_updated"),
                    category_id=doc.get("category_id"),
                    brand_id=doc.get("brand_id"),
                    price=doc.get("price"),
                    session_id=doc.get("last_session_id")
                )
        except Exception as e:
            print(f"⚠️ MongoDB 조회 실패: {e}")
        
        # 기본값 반환
        return UserActionSummary(member_id, product_id, 0, False, False, False)
    
    def update_user_action(
        self,
        member_id: int,
        product_id: int,
        clicked: int = 0,
        liked: Optional[bool] = None,
        in_cart: Optional[bool] = None,
        purchased: Optional[bool] = None,
        # 추가 메타데이터
        category_id: Optional[int] = None,
        brand_id: Optional[int] = None,
        price: Optional[int] = None,
        session_id: Optional[str] = None
    ) -> UserActionSummary:
        """
        사용자 행동 업데이트
        
        Args:
            member_id: 사용자 ID
            product_id: 상품 ID
            clicked: 클릭 횟수 증가량
            liked: 좋아요 여부
            in_cart: 장바구니 추가 여부
            purchased: 구매 여부
        
        Returns:
            업데이트된 UserActionSummary
        """
        # 기존 요약 조회
        summary = self._get_user_action_summary(member_id, product_id)
        now = datetime.now()
        
        # 행동 값 업데이트
        if clicked > 0:
            summary.clicked = (summary.clicked or 0) + clicked
            summary.clicked_at = now
        
        if liked is not None:
            summary.liked = liked
            summary.liked_at = now if liked else None
        
        if in_cart is not None:
            summary.in_cart = in_cart
            summary.in_cart_at = now if in_cart else None
        
        if purchased is not None:
            summary.purchased = purchased
            summary.purchased_at = now if purchased else None
        
        # 메타데이터 업데이트 (최신 정보로 갱신)
        if category_id is not None:
            summary.category_id = category_id
        if brand_id is not None:
            summary.brand_id = brand_id
        if price is not None:
            summary.price = price
        if session_id is not None:
            summary.session_id = session_id
        
        summary.last_updated = now
        
        # MongoDB에 저장
        try:
            save_user_action_summary(
                member_id=summary.member_id,
                product_id=summary.product_id,
                clicked=summary.clicked,
                liked=summary.liked,
                in_cart=summary.in_cart,
                purchased=summary.purchased,
                clicked_at=summary.clicked_at,
                liked_at=summary.liked_at,
                in_cart_at=summary.in_cart_at,
                purchased_at=summary.purchased_at,
                category_id=summary.category_id,
                brand_id=summary.brand_id,
                price=summary.price,
                session_id=summary.session_id
            )
        except Exception as e:
            print(f"⚠️ MongoDB 저장 실패: {e}")
        
        return summary
    
    def process_stream_message(self, fields: Dict[str, Any]) -> None:
        """
        Redis Stream에서 받은 메시지 처리 (하위 호환성 유지 - Kafka로 전환됨)
        
        실제 로그 형식:
        {
            "event_time": "2025-01-15T10:30:45.123Z",
            "session_id": "uuid",
            "version": "1",
            "event_type": "product_like",  // product_detail_view, product_like 등
            "service": "product",
            "member_id": "1001",  // 문자열로 저장됨
            "data.product_id": "123"  // 문자열로 저장됨
        }
        
        Args:
            fields: Stream 메시지의 필드 딕셔너리
        """
        try:
            # 필수 필드 파싱 (문자열로 저장되므로 변환 필요)
            member_id_str = fields.get("member_id", "0")
            event_type = fields.get("event_type", "")
            service = fields.get("service", "")
            
            # member_id를 정수로 변환
            try:
                member_id = int(member_id_str) if member_id_str else 0
            except (ValueError, TypeError):
                print(f"⚠️ 잘못된 member_id 형식: {member_id_str}")
                return
            
            # data. 접두사 제거하여 data 딕셔너리 생성
            data = {k[5:]: v for k, v in fields.items() if k.startswith("data.")}
            
            # product_id 파싱 (문자열로 저장되므로 변환 필요)
            product_id_str = data.get("product_id", "0")
            try:
                product_id = int(product_id_str) if product_id_str else 0
            except (ValueError, TypeError):
                print(f"⚠️ 잘못된 product_id 형식: {product_id_str}")
                return
            
            # 추가 메타데이터 파싱 (정밀한 추천을 위해)
            category_id = None
            brand_id = None
            price = None
            session_id = fields.get("session_id")
            
            # data에서 추가 정보 추출
            category_id_str = data.get("category_id") or data.get("categoryId")
            if category_id_str:
                try:
                    category_id = int(category_id_str)
                except (ValueError, TypeError):
                    pass
            
            brand_id_str = data.get("brand_id") or data.get("brandId")
            if brand_id_str:
                try:
                    brand_id = int(brand_id_str)
                except (ValueError, TypeError):
                    pass
            
            price_str = data.get("price")
            if price_str:
                try:
                    price = int(float(price_str))  # 소수점도 처리
                except (ValueError, TypeError):
                    pass
            
            # 유효성 검사
            if member_id == 0 or product_id == 0:
                return
            
            # service가 "product"가 아니면 무시 (다른 서비스 로그는 제외)
            if service != "product":
                return
            
            # 조회성 이벤트(검색/목록/상세 조회 등) 기본 무시
            if event_type.startswith(self.readonly_event_prefixes) or event_type.endswith(self.readonly_event_suffixes):
                # product_detail은 추천 신호로 사용하므로 예외 처리
                if event_type != "product_detail":
                    return
            
            # 이벤트 타입에 따라 행동 업데이트
            clicked = 0
            liked = None
            in_cart = None
            purchased = None
            
            # 권장 이벤트명
            if event_type == "product_detail":
                clicked = 1
            elif event_type == "product_like":
                liked = True
            elif event_type == "cancel_product_like":
                liked = False
            elif event_type == "create_cart" or event_type == "product_add_to_cart" or event_type == "add_to_cart":
                in_cart = True
            elif event_type == "delete_cart" or event_type == "product_remove_from_cart" or event_type == "remove_from_cart":
                in_cart = False
            elif event_type == "end_payment" or event_type == "product_purchase" or event_type == "purchase":
                purchased = True
            elif event_type == "refund":
                purchased = False
            # 기존 형식도 지원 (하위 호환성)
            elif event_type == "view" or event_type == "click":
                clicked = 1
            elif event_type == "like":
                liked = True
            elif event_type == "unlike":
                liked = False
            
            # 행동이 없으면 무시
            if clicked == 0 and liked is None and in_cart is None and purchased is None:
                return
            
            # 사용자 행동 업데이트 (메타데이터 포함)
            summary = self.update_user_action(
                member_id=member_id,
                product_id=product_id,
                clicked=clicked,
                liked=liked,
                in_cart=in_cart,
                purchased=purchased,
                category_id=category_id,
                brand_id=brand_id,
                price=price,
                session_id=session_id
            )
            
            # 추천 점수 계산
            score = self.get_recommendation_score(member_id, product_id, summary)
            
            # 추천 점수가 높으면 MongoDB에 저장 (상위 8개만 유지)
            if score > 0:
                save_user_recommendation(member_id, product_id, score)
            
            # 로그 출력 (디버깅용, 나중에 제거 가능)
            if score > 0:
                print(f"📊 [추천] member={member_id}, product={product_id}, event={event_type}, score={score:.2f}")
            
        except Exception as e:
            print(f"❌ 메시지 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
    
    def _update_user_recommendation(self, member_id: int, product_id: int, score: float) -> None:
        """
        사용자 추천 결과 업데이트 (상위 8개만 유지) - MongoDB 사용
        
        Args:
            member_id: 사용자 ID
            product_id: 상품 ID
            score: 추천 점수
        """
        # MongoDB에 저장 (상위 8개만 유지하는 로직은 save_user_recommendation 내부에서 처리)
        save_user_recommendation(member_id, product_id, score)
    
    def process_kafka_message(self, kafka_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Kafka에서 받은 메시지 직접 처리
        
        Kafka 메시지 형식:
        {
            "eventType": "product_detail_view",
            "service": "product",
            "memberId": 12345,
            "data": {
                "product_id": 67890
            },
            "eventTime": "2024-01-01T00:00:00Z",
            "eventTimestamp": 1704067200000,
            "producerTime": 1704067200000,
            "version": 1
        }
        
        Args:
            kafka_data: Kafka에서 받은 JSON 메시지 딕셔너리
        
        Returns:
            MongoDB 저장용 메타데이터 딕셔너리 (score, category_id 등 포함) 또는 None
        """
        try:
            # 필수 필드 추출
            event_type = kafka_data.get('eventType', '')
            service = kafka_data.get('service', '')
            member_id = kafka_data.get('memberId', 0)
            data = kafka_data.get('data', {})
            
            # 유효성 검사
            if not member_id or not isinstance(member_id, int):
                return
            
            # product_id 추출
            product_id = data.get('product_id') or data.get('productId', 0)
            if not product_id or not isinstance(product_id, int):
                return
            
            # service가 "product"가 아니면 무시
            if service != "product":
                return
            
            # 이벤트 타입 매핑 (Kafka 형식 -> 내부 형식)
            event_type_mapping = {
                'product_detail_view': 'product_detail',
                'product_add_cart': 'create_cart',
            }
            event_type = event_type_mapping.get(event_type, event_type)
            
            # 조회성 이벤트 무시 (product_detail은 예외)
            if event_type.startswith(self.readonly_event_prefixes) or event_type.endswith(self.readonly_event_suffixes):
                if event_type != "product_detail":
                    return
            
            # 추가 메타데이터 추출
            category_id = data.get('category_id') or data.get('categoryId')
            brand_id = data.get('brand_id') or data.get('brandId')
            price = data.get('price')
            session_id = kafka_data.get('sessionId') or kafka_data.get('session_id')
            
            # 타입 변환
            if category_id:
                try:
                    category_id = int(category_id)
                except (ValueError, TypeError):
                    category_id = None
            
            if brand_id:
                try:
                    brand_id = int(brand_id)
                except (ValueError, TypeError):
                    brand_id = None
            
            if price:
                try:
                    price = int(float(price))
                except (ValueError, TypeError):
                    price = None
            
            # 이벤트 타입에 따라 행동 업데이트
            clicked = 0
            liked = None
            in_cart = None
            purchased = None
            
            if event_type == "product_detail" or event_type == "product_detail_view":
                clicked = 1
            elif event_type == "product_like":
                liked = True
            elif event_type == "cancel_product_like":
                liked = False
            elif event_type == "create_cart" or event_type == "product_add_cart":
                in_cart = True
            elif event_type == "delete_cart":
                in_cart = False
            elif event_type == "end_payment" or event_type == "product_purchase":
                purchased = True
            elif event_type == "refund":
                purchased = False
            
            # 행동이 없으면 무시
            if clicked == 0 and liked is None and in_cart is None and purchased is None:
                return
            
            # 사용자 행동 업데이트
            summary = self.update_user_action(
                member_id=member_id,
                product_id=product_id,
                clicked=clicked,
                liked=liked,
                in_cart=in_cart,
                purchased=purchased,
                category_id=category_id,
                brand_id=brand_id,
                price=price,
                session_id=session_id
            )
            
            # 추천 점수 계산
            score = self.get_recommendation_score(member_id, product_id, summary)
            
            # 추천 점수가 높으면 MongoDB에 저장 (상위 8개만 유지)
            if score > 0:
                save_user_recommendation(member_id, product_id, score)
            
            # MongoDB 배치 저장을 위한 메타데이터 반환
            kafka_metadata = kafka_data.get('_kafka_metadata', {})
            return {
                'member_id': member_id,
                'product_id': product_id,
                'event_type': event_type,
                'score': score,
                'category_id': category_id,
                'brand_id': brand_id,
                'price': price,
                'kafka_metadata': kafka_metadata
            }
            
        except Exception as e:
            print(f"❌ Kafka 메시지 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
            return None

