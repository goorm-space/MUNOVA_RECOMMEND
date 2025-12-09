FROM python:3.11-slim

WORKDIR /app

# Protobuf 컴파일러 설치 (apt로 통일 - 더 안정적)
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    protoc --version && \
    rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Protobuf 파일 컴파일 (Python 모듈 생성)
RUN protoc --python_out=. user_action_log.proto

RUN chmod +x services/kafka/start_kafka_consumer.sh
RUN chmod +x services/start_server.sh
RUN chmod +x services/start_batch.sh

CMD ["bash", "services/start_server.sh"]
