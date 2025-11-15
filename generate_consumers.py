template = open("services/redis/consumer_template.py").read()

for i in range(10):
    with open(f"services/redis/consumer_{i}.py", "w") as f:
        f.write(template.replace("__STREAM_INDEX__", str(i)))

print("✅ consumer_0.py ~ consumer_9.py 생성 완료")

