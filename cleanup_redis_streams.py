#!/usr/bin/env python3
"""
Redis Stream 정리 스크립트
불필요한 Stream 데이터를 삭제합니다.
"""
from redis.cluster import RedisCluster, ClusterNode
import yaml

# 설정 로드
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Redis Cluster 연결
nodes = [ClusterNode(n["host"], n["port"]) for n in config["redis"]["cluster"]["nodes"]]
password = config["redis"]["cluster"].get("password", "")

r = RedisCluster(
    startup_nodes=nodes,
    password=password if password else None,
    decode_responses=True,
    skip_full_coverage_check=True
)

print("=" * 60)
print("🗑️ Redis Stream 정리 시작")
print("=" * 60)

# 삭제할 Stream 목록
streams_to_delete = [
    "product_action_stream",
    "member_action_stream",
    "chat_action_stream",
    "coupon_action_stream",
    "order_action_stream",
    "payment_action_stream",
    "recommend_action_stream"
]

# 현재 존재하는 모든 Stream 확인
all_keys = r.keys("*stream*")
print(f"\n📦 발견된 Stream 키: {len(all_keys)}개")
for key in sorted(all_keys):
    try:
        info = r.xinfo_stream(key)
        length = info.get("length", 0)
        print(f"  - {key}: {length}개 메시지")
    except:
        print(f"  - {key}: (Stream이 아님)")

print("\n" + "=" * 60)
print("삭제할 Stream:")
for stream in streams_to_delete:
    print(f"  - {stream}")

# 사용자 확인
response = input("\n⚠️ 위 Stream들을 삭제하시겠습니까? (yes/no): ")
if response.lower() != "yes":
    print("❌ 취소되었습니다.")
    exit(0)

# 삭제 실행
print("\n🗑️ Stream 삭제 중...")
deleted_count = 0
for stream in streams_to_delete:
    try:
        if r.exists(stream):
            # Stream 길이 확인
            try:
                info = r.xinfo_stream(stream)
                length = info.get("length", 0)
                if length > 0:
                    print(f"  ⚠️ {stream}: {length}개 메시지가 있습니다. 삭제하시겠습니까?")
            except:
                pass
            
            r.delete(stream)
            print(f"  ✅ {stream} 삭제 완료")
            deleted_count += 1
        else:
            print(f"  ℹ️ {stream}: 없음 (이미 삭제됨)")
    except Exception as e:
        print(f"  ❌ {stream} 삭제 실패: {e}")

print("\n" + "=" * 60)
print(f"✅ 정리 완료! {deleted_count}개 Stream 삭제됨")
print("=" * 60)

# user_action_stream_0~9 확인
print("\n📊 user_action_stream_0~9 상태:")
for i in range(10):
    stream_key = f"user_action_stream_{i}"
    try:
        info = r.xinfo_stream(stream_key)
        length = info.get("length", 0)
        if length > 0:
            print(f"  Stream {i}: {length}개 메시지")
        else:
            print(f"  Stream {i}: 0개 메시지 (정상)")
    except:
        print(f"  Stream {i}: 없음 (정상)")

