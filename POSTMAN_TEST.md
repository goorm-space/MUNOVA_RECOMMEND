# Postman 테스트 가이드

## 서버 실행 방법

### 방법 1: Docker로 실행 (권장)
```bash
cd /Users/kimseonghyun/recommend
docker compose up -d --build
```

### 방법 2: 로컬에서 실행
```bash
cd /Users/kimseonghyun/recommend
pip install -r requirements.txt
python3 -m uvicorn services.api_server:app --host 0.0.0.0 --port 8001 --reload
```

## API 엔드포인트

### 1. 헬스 체크
**GET** `http://localhost:8001/`

**응답 예시:**
```json
{
  "status": "ok",
  "service": "recommend-server"
}
```

### 2. 헬스 체크 (상세)
**GET** `http://localhost:8001/health`

**응답 예시:**
```json
{
  "status": "healthy"
}
```

### 3. 사용자 기반 추천 상품 조회
**GET** `http://localhost:8001/api/recommend/user/{member_id}`

**파라미터:**
- `member_id` (path): 사용자 ID (예: 1, 100)
- `limit` (query, optional): 추천 상품 개수 (기본값: 8)

**예시:**
```
GET http://localhost:8001/api/recommend/user/1?limit=8
```

**응답 예시:**
```json
{
  "member_id": 1,
  "count": 0,
  "recommendations": [],
  "message": "데이터베이스가 연결되지 않았습니다."
}
```

### 4. 상품 기반 유사 상품 추천 조회
**GET** `http://localhost:8001/api/recommend/product/{product_id}`

**파라미터:**
- `product_id` (path): 상품 ID (예: 123)
- `limit` (query, optional): 추천 상품 개수 (기본값: 4)

**예시:**
```
GET http://localhost:8001/api/recommend/product/123?limit=4
```

**응답 예시:**
```json
{
  "product_id": 123,
  "count": 0,
  "recommendations": [],
  "message": "데이터베이스가 연결되지 않았습니다."
}
```

### 5. 추천 점수 조회
**GET** `http://localhost:8001/api/recommend/user/{member_id}/product/{product_id}/score`

**파라미터:**
- `member_id` (path): 사용자 ID
- `product_id` (path): 상품 ID

**예시:**
```
GET http://localhost:8001/api/recommend/user/1/product/123/score
```

**응답 예시:**
```json
{
  "member_id": 1,
  "product_id": 123,
  "score": 0.0
}
```

## Postman 설정

### 1. 새 Collection 생성
- Postman에서 "New" → "Collection" 선택
- 이름: "MUNOVA Recommend Server"

### 2. 환경 변수 설정 (선택사항)
- "Environments" → "New Environment"
- 변수 추가:
  - `base_url`: `http://localhost:8001`
  - `member_id`: `1`
  - `product_id`: `123`

### 3. 요청 생성
각 엔드포인트에 대해 새 Request를 생성하세요.

## 주의사항

1. **Redis 연결**: Redis Cluster가 실행 중이어야 합니다.
2. **데이터베이스**: DB가 연결되지 않아도 기본 엔드포인트는 동작하지만, 추천 데이터는 조회되지 않습니다.
3. **포트**: 기본 포트는 8001입니다. Docker로 실행하는 경우 `docker-compose.yml`의 포트 설정을 확인하세요.

## FastAPI 자동 문서

서버가 실행 중일 때 다음 URL에서 자동 생성된 API 문서를 확인할 수 있습니다:

- Swagger UI: `http://localhost:8001/docs`
- ReDoc: `http://localhost:8001/redoc`

