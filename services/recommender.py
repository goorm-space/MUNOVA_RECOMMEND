# logger
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from services.mongodb import (
    save_user_action_summary, get_user_action_summary,
    save_user_recommendation
)

logger = logging.getLogger(__name__)
# 가중치 상수
CLICK_WEIGHT = 0.1
LIKE_WEIGHT = 0.15
CART_WEIGHT = 0.35
PURCHASE_WEIGHT = 0.5
DECAY_RATE = 0.05  # 하루당 5% 감쇠
MIN_DECAY = 0.5  # 최소 유지 비율
CACHE_TTL_MINUTES = 10


# 사용자 행동 데이터 -> 한 유저가 한 상품에 대해 지금까지 쌓은 행동을 요약한 "해동 요약 스냅샷"
class UserActionSummary:
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


class Recommender:
    def __init__(self, db_session=None):
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

    # 점수 계산
    def get_recommendation_score(
            self,
            member_id: int,
            product_id: int,
            summary: Optional[UserActionSummary] = None
    ) -> float:
        # 추천 점수 계산
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

    # 시간 감소를 적용하여 점수 계산
    def _score_with_decay(
            self,
            action_time: Optional[datetime],
            weight: float,
            now: datetime,
            condition: Optional[bool] = None
    ) -> float:
        if condition is False:
            return 0
        if action_time is None:
            return 0

        days = (now - action_time).days
        decay_factor = max(MIN_DECAY, 1 - DECAY_RATE * days)
        return weight * decay_factor

    # MongoDB에서 요약 읽기
    def _get_user_action_summary(
            self,
            member_id: int,
            product_id: int
    ) -> UserActionSummary:
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
                )
        except Exception as e:
            logger.error(f"MongoDB 조회 실패: {e}", exc_info=True)

        # 없으면 행동 안한 상태로 기본 객체 return
        return UserActionSummary(member_id, product_id, 0, False, False, False)

    # 행동 요약 업데이트 + MongoDB 반영
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
    ) -> UserActionSummary:
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

        summary.last_updated = now

        # MongoDB에 저장
        try:
            save_user_action_summary(
                member_id=summary.member_id,
                product_id=summary.product_id,
                clicked=clicked,
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
            )
        except Exception as e:
            logger.error(f"MongoDB 저장 실패: {e}", exc_info=True)
        return summary

    # wrapper 함수 -> 사용자 추천 결과 업데이트 (상위 8개만 유지)
    def _update_user_recommendation(self, member_id: int, product_id: int, score: float) -> None:
        save_user_recommendation(member_id, product_id, score)

    # consumer가 호출하는 메서드 엄청 중요!!!
    def process_kafka_message(self, kafka_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            # 필수 필드 추출
            event_type = kafka_data.get('eventType', '')
            service = kafka_data.get('service', '')
            member_id = kafka_data.get('memberId', 0)
            data = kafka_data.get('data', {})

            # 유효성 검사
            if not member_id or not isinstance(member_id, int):
                return None

            # product_id 추출
            product_id = data.get('product_id') or data.get('productId', 0)
            if not product_id or not isinstance(product_id, int):
                return None

            # TODO: 나중에 수정 지금은 product만 있음
            # 이벤트 타입 매핑 (Kafka 형식 -> 내부 형식)
            event_type_mapping = {
                'product_detail_view': 'product_detail',
                'product_add_cart': 'create_cart',
            }
            event_type = event_type_mapping.get(event_type, event_type)

            # 추가 메타데이터 추출
            category_id = data.get('category_id')
            brand_id = data.get('brand_id')
            price = data.get('price')

            # 타입 변환
            if category_id is not None:
                try:
                    category_id = int(category_id)
                except (ValueError, TypeError):
                    category_id = None

            if brand_id is not None:
                try:
                    brand_id = int(brand_id)
                except (ValueError, TypeError):
                    brand_id = None

            if price is not None:
                try:
                    price = int(price)
                except:
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
                return None

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
            )

            # 추천 점수 계산
            score = self.get_recommendation_score(member_id, product_id, summary)

            # 추천 점수가 높으면 MongoDB에 저장 (상위 8개만 유지)
            if score > 0:
                try:
                    save_user_recommendation(member_id, product_id, score)
                    logger.debug(
                        f"[추천] member={member_id}, product={product_id}, event={event_type}, score={score:.2f}")
                except Exception as e:
                    logger.error(f"추천 저장 실패 (member={member_id}, product={product_id}): {e}", exc_info=True)

            # MongoDB 배치 저장을 위한 메타데이터 반환 -> 실제 MongoDB에 저장되는 데이터
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
            logger.error(f"Kafka 메시지 처리 오류: {e}", exc_info=True)
            raise
