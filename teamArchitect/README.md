# Team Data Lake Platform Reference Implementation

현재 코드 구현은 아래 설계 결정에 따른다.

- `DD-02` 수집 파이프라인 내 비식별화 일괄 처리
- `DD-03` 플랫폼 서빙 레이어 중앙 접근 권한 통제
- `DD-04` API Gateway 기반 단일 진입점
- `DD-05` 커밋 이후 순차 이벤트 전파
- `DD-06` 수집용/소비용 MQ 이중 분리

## 구조

```
teamArchitect/
├── config/
│   └── consumer_policies.yaml  # 소비 시스템 정책 (허용 필드, 목적, 인증키)
├── team_data_platform/
│   ├── collection_layer/        # Knox 소스, Kafka 큐, 이벤트 발행
│   ├── processing_layer/        # Spark 파이프라인, 표준화, 비식별화
│   ├── serving_layer/           # 접근 권한 정책, 데이터 서빙
│   ├── storage_layer/           # Raw / Standardized / Serving 존 (파일 기반)
│   ├── entry_gateway_layer/     # API Gateway (포트 8080)
│   ├── ui_layer/                # 모니터링 대시보드 (포트 8090)
│   └── foundation_layer/        # 설정, 부트스트랩, 공통 모델
├── docker-compose.yml           # Kafka (KRaft 모드)
└── README.md
```

## 사전 요구사항

| 항목 | 버전 |
|------|------|
| Python | 3.10 이상 |
| Docker Desktop | 최신 버전 |
| kafka-python-ng | pip으로 설치 |

```bash
pip install kafka-python-ng
```

## 로컬 테스트 방법

### 1단계 — Kafka 실행

```bash
docker compose up -d
```

> Kafka가 완전히 뜨기까지 약 10초 소요. 처음 실행 시 이미지 다운로드로 수 분 걸릴 수 있다.

### 2단계 — Gateway 서버 실행 (터미널 1)

```bash
python -c "from team_data_platform.entry_gateway_layer.server import serve; serve()"
```

포트 `8080`에서 기동.

### 3단계 — UI 서버 실행 (터미널 2)

```bash
python -c "from team_data_platform.ui_layer.server import serve; serve()"
```

포트 `8090`에서 기동.

### 4단계 — 모니터링 대시보드 접속

브라우저에서 접속:

```
http://127.0.0.1:8090
```

**"🧪 테스트 데이터 주입"** 버튼을 클릭하면 파이프라인 전체 흐름을 확인할 수 있다.

```
Knox 소스 등록
  → Kafka Inbound 이벤트 발행
    → Spark Streaming 처리 (표준화 + 비식별화)
      → Raw / Standardized / Serving 존 적재
        → Kafka Outbound 전파
```

### 환경 초기화 (깨끗하게 리셋할 때)

Kafka 데이터와 로컬 데이터 존을 함께 초기화해야 한다. **둘 중 하나만 지우면 이벤트 재처리 불일치가 발생한다.**

```bash
# 1. 서버 프로세스 종료 (실행 중인 터미널에서 Ctrl+C)

# 2. Kafka 컨테이너 완전 삭제 (restart가 아닌 down)
docker compose down

# 3. 로컬 데이터 존 삭제
rm -rf var/          # macOS / Linux
rd /s /q var         # Windows

# 4. Kafka 재시작
docker compose up -d

# 5. 서버 재기동 (2단계, 3단계 반복)
```

> `docker compose restart`는 컨테이너만 재시작하고 Kafka 내부 데이터는 유지된다.
> 반드시 `docker compose down` → `docker compose up -d` 순서로 실행해야 한다.


## 샘플 API

**수집 (Gateway로 이벤트 발행):**

```bash
curl -X POST http://localhost:8080/api/v1/ingest/events ^
-H "Content-Type: application/json" ^
-H "X-Source-System: mail" ^
-H "X-Source-Key: source-mail-key" ^
-d "{\"event_id\":\"evt-001\",\"record_id\":\"mail-100\",\"entity_type\":\"mail\",\"operation\":\"UPSERT\",\"version\":1,\"occurred_at\":\"2026-05-11T09:00:00Z\",\"payload\":{\"subject\":\"문의\",\"body_html\":\"<p>홍길동 hong@example.com</p>\",\"user_name\":\"홍길동\",\"email\":\"hong@example.com\",\"phone\":\"01012345678\",\"attachments\":[{\"filename\":\"memo.txt\",\"text\":\"첨부\"}]}}"
```

**Spark Consume 트리거:**

```bash
curl -X POST http://localhost:8080/api/v1/admin/stream/consume ^
-H "Content-Type: application/json" ^
-d "{\"source_system\":\"mail\",\"limit\":100}"
```

**소비 시스템 조회:**

```bash
curl -X POST http://localhost:8080/api/v1/query/record ^
-H "Content-Type: application/json" ^
-H "X-Consumer-Id: search" ^
-H "X-Consumer-Key: consumer-search-key" ^
-d "{\"record_id\":\"mail-100\",\"purpose\":\"search\"}"
```

## 소비 시스템별 허용 필드

`config/consumer_policies.yaml`에서 관리한다. 새 소비 시스템 추가 시 이 파일에 항목을 추가하면 코드 변경 없이 반영된다.

| 소비 시스템 | 목적 | 특이사항 |
|------------|------|----------|
| `search` | 검색 색인 | 마스킹된 이름, body/attachment 텍스트 |
| `mobile` | 화면 표시 | 마스킹된 이름/전화번호, preview만 |
| `rag` | AI 검색 | context_text, 마스킹 이름 |
| `graph` | 관계 분석 | PII 없음, graph_nodes/edges만 |
| `analytics` | 통계 분석 | PII 없음, 토큰값(token)만 |

## 추가 구현 보강 필요

- 데이터 암호화 적용 필요