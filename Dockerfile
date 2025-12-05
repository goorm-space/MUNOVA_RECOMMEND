FROM python:3.11-slim

WORKDIR /app

# Protobuf 컴파일러 설치
# ARM64는 apt로 설치, x86_64는 GitHub에서 다운로드
RUN apt-get update && \
    ARCH=$(uname -m) && \
    echo "Detected architecture: $ARCH" && \
    if [ "$ARCH" = "x86_64" ]; then \
        echo "📥 x86_64: GitHub에서 protoc 3.25.5 다운로드..." && \
        apt-get install -y wget unzip && \
        wget -q --timeout=30 --tries=3 \
            https://github.com/protocolbuffers/protobuf/releases/download/v3.25.5/protoc-3.25.5-linux-x86_64.zip \
            -O /tmp/protoc.zip && \
        unzip -q /tmp/protoc.zip -d /usr/local && \
        chmod +x /usr/local/bin/protoc && \
        rm /tmp/protoc.zip && \
        apt-get remove -y wget unzip && \
        apt-get autoremove -y; \
    elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        echo "📥 ARM64: apt로 protoc 설치..." && \
        apt-get install -y protobuf-compiler; \
    else \
        echo "❌ Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    /usr/local/bin/protoc --version 2>/dev/null || protoc --version && \
    rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Protobuf 파일 컴파일 (Python 모듈 생성)
RUN protoc --python_out=. user_action_log.proto

RUN chmod +x services/kafka/start_kafka_consumer.sh
RUN chmod +x services/start_server.sh

CMD ["bash", "services/start_server.sh"]
