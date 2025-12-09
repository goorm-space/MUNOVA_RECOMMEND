# recommender_batch.py
import logging
from typing import Dict, Any, List

from services.recommender import Recommender  # 기존 점수 계산 로직 재활용
from services.mongodb import (
    get_mongodb_db,
    save_user_recommendation,
)

logger = logging.getLogger(__name__)


class BatchRecommender:

    def __init__(self):
        self.db = get_mongodb_db()
        self.recommender = Recommender()

    def rebuild_all_user_recommendations(self):
        """
        전체 사용자 행동 요약(user_action_summaries)을 기반으로
        매 5분마다 추천 점수를 다시 계산하는 배치 메인 함수
        """
        if self.db is None:
            logger.error("❌ MongoDB 연결 실패. 배치 추천 종료.")
            return

        summaries = list(self.db["user_action_summaries"].find({}))
        if not summaries:
            logger.info("⚠️ user_action_summaries 비어있음. 배치 종료.")
            return

        logger.info(f"🔥 배치 추천 시작: 총 {len(summaries)}개 사용자-상품 요약 처리")

        # member_id 기준으로 그룹핑
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for s in summaries:
            member = s["member_id"]
            grouped.setdefault(member, []).append(s)

        processed = 0

        # 각 사용자에 대해 top-N 계산
        for member_id, docs in grouped.items():

            # 각 product에 대해 점수 계산
            scored_items = []
            for doc in docs:
                summary = self._convert_doc_to_summary(doc)
                score = self.recommender.get_recommendation_score(
                    member_id, summary.product_id, summary
                )

                if score > 0:
                    scored_items.append((summary.product_id, score))

            # 점수 순으로 정렬 후 상위 8개 저장
            top_items = sorted(scored_items, key=lambda x: x[1], reverse=True)[:8]

            for product_id, score in top_items:
                save_user_recommendation(member_id, product_id, score)

            processed += 1
            if processed % 100 == 0:
                logger.info(f"📦 {processed}명 사용자 처리 완료")

        logger.info("✅ 배치 추천 완료")

    # doc -> UserActionSummary 변환
    def _convert_doc_to_summary(self, doc: Dict[str, Any]):
        return self.recommender._get_user_action_summary(
            member_id=doc.get("member_id"),
            product_id=doc.get("product_id"),
        )
