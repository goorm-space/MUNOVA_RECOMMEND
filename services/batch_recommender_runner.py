# batch_recommender_runner.py
import logging
import time
import sys
import os

# PYTHONPATH 설정 (모듈 import를 위해)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.recommender_batch import BatchRecommender

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

INTERVAL_SECONDS = 300  # 5분


def main():
    batch = BatchRecommender()

    while True:
        try:
            batch.rebuild_all_user_recommendations()
        except Exception as e:
            logging.error(f"❌ 배치 추천 실행 중 오류 발생: {e}", exc_info=True)

        logging.info(f"⏳ 다음 배치까지 대기: {INTERVAL_SECONDS}초")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
