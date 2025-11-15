from redis.cluster import RedisCluster, ClusterNode
import time, yaml
from services.influx_logger import send_metric

with open("/app/config.yml", "r") as f:
    config = yaml.safe_load(f)

nodes = [ClusterNode(n["host"], n["port"]) for n in config["redis"]["cluster"]["nodes"]]
password = config["redis"]["cluster"].get("password", "")
r = RedisCluster(startup_nodes=nodes, password=password if password else None, decode_responses=True)
print("✅ Redis Cluster 연결 완료")

# 🔢 STREAM_INDEX는 파일별로 다르게 치환됨 (예: 0~9)
STREAM_INDEX = __STREAM_INDEX__
STREAM_KEY = f"{config['app']['stream_prefix']}{STREAM_INDEX}"
GROUP = config["app"]["consumer_group"]
CONSUMER = f"consumer-{STREAM_INDEX}"

try:
    r.xgroup_create(STREAM_KEY, GROUP, mkstream=True)
except Exception:
    pass

print(f"🔄 {CONSUMER} 시작 (stream={STREAM_KEY})")

count = 0
while True:
    try:
        messages = r.xreadgroup(GROUP, CONSUMER, {STREAM_KEY: '>'}, count=50, block=5000)
        if not messages:
            continue
        for stream, entries in messages:
            for msg_id, fields in entries:
                count += 1
                r.xack(stream, GROUP, msg_id)
                if count % 100 == 0:
                    print(f"📦 {count}개 처리 완료 ({stream})")
                    send_metric("recommend_consumer",
                                {"stream": stream, "consumer": CONSUMER},
                                {"processed": count})
    except Exception as e:
        print(f"❌ {CONSUMER} 오류: {e}")
        time.sleep(2)