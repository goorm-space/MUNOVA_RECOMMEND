#!/bin/bash

echo "🚀 FastAPI 서버 시작 중..."

cd /Users/kimseonghyun/recommend
export PYTHONPATH=/Users/kimseonghyun/recommend:$PYTHONPATH

# 의존성 확인
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "📦 FastAPI가 설치되지 않았습니다. 설치 중..."
    pip3 install -r requirements.txt
fi

# 서버 실행
echo "✅ 서버 시작: http://localhost:8001"
echo "📚 API 문서: http://localhost:8001/docs"
python3 -m uvicorn services.api_server:app --host 0.0.0.0 --port 8001 --reload

