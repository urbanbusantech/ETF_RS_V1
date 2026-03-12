# 🚀 ETF 상대강도(RS) 모멘텀 대시보드

대한민국 상장 주식형 ETF를 대상으로 마크 미너비니(Mark Minervini)의 **상대강도(Relative Strength)** 방법론을 적용해 시장 주도 ETF를 자동 분석하고, Streamlit 대시보드 및 구글 블로그에 매일 자동 게시하는 프로젝트입니다.

[![Update ETF Data](https://github.com/{YOUR_USERNAME}/{YOUR_REPO}/actions/workflows/update.yml/badge.svg)](https://github.com/{YOUR_USERNAME}/{YOUR_REPO}/actions/workflows/update.yml)

---

## 📌 주요 기능

- **ETF 상대강도 계산**: 3개월(40%), 6개월(20%), 9개월(20%), 12개월(20%) 가중 수익률로 RS 점수(1~99) 산출
- **52주 신고가 주도주 분석**: 구글 스프레드시트와 연동하여 52주 최고가 돌파 종목 자동 선별
- **Streamlit 대시보드**: 네이버 차트 링크 포함, 컬럼 정렬 기능이 지원되는 인터랙티브 테이블
- **구글 블로그 자동 포스팅**: ETF 랭킹 및 주도주 뉴스를 HTML 리포트로 블로그에 자동 게시
- **GitHub Actions 자동화**: 평일 매일 장 마감 후(KST 16:30) 데이터 업데이트 및 전체 파이프라인 자동 실행

---

## 🗂️ 프로젝트 구조

```
ETF_RS_V1/
├── app.py                   # Streamlit 대시보드 메인 앱
├── update_data.py           # ETF 상대강도 계산 및 블로그 자동 포스팅
├── update_52w_high.py       # 52주 신고가 주도주 분석 및 블로그 자동 포스팅
├── etf_data.csv             # GitHub Actions가 자동 생성하는 ETF 데이터 파일
├── requirements.txt         # Python 패키지 의존성
└── .github/
    └── workflows/
        └── update.yml       # GitHub Actions 자동화 워크플로우
```

---

## ⚙️ 동작 방식

### 1. ETF 상대강도 파이프라인 (`update_data.py`)

1. 네이버 금융 API에서 국내 상장 주식형 ETF 목록 수집 (채권·원자재·인버스·레버리지 제외)
2. `FinanceDataReader`로 각 ETF의 1년치 주가 데이터 수집
3. 미너비니 RS 공식으로 가중 수익률 계산 및 백분위 점수(1~99) 산출
4. 벤치마크(KODEX 200, 069500) 대비 성과 비교
5. `etf_data.csv` 저장 및 구글 블로그 HTML 리포트 자동 게시

### 2. 52주 신고가 파이프라인 (`update_52w_high.py`)

1. 구글 스프레드시트(공개 CSV)에서 `🚀돌파` 상태인 종목 필터링
2. 스팩·리츠 제외 후 거래량 순 정렬
3. 거래량 상위 2개 종목의 구글 뉴스 RSS 수집
4. 섹터 모멘텀 브리핑 텍스트 자동 생성
5. 구글 블로그에 HTML 리포트 자동 게시

### 3. Streamlit 대시보드 (`app.py`)

- `etf_data.csv`를 읽어 인터랙티브 테이블로 시각화
- 상대강도 점수를 Progress Bar로 표시 (80점 이상 = 시장 상위 20% 주도주)
- 종목코드 클릭 시 네이버 차트로 바로 이동

---

## 🔐 GitHub Secrets 설정

레포지토리 `Settings > Secrets and variables > Actions`에서 아래 시크릿을 등록하세요.

| Secret 이름 | 설명 |
|---|---|
| `BLOGGER_BLOG_ID` | 구글 블로거 블로그 ID |
| `BLOGGER_CLIENT_ID` | OAuth 2.0 클라이언트 ID |
| `BLOGGER_CLIENT_SECRET` | OAuth 2.0 클라이언트 시크릿 |
| `BLOGGER_REFRESH_TOKEN` | Blogger API Refresh Token |
| `GH_PAT` | GitHub Personal Access Token (Secret 자동 갱신용) |
| `GOOGLE_SHEET_ID` | 52주 신고가 데이터가 담긴 구글 스프레드시트 ID |

> 💡 `BLOGGER_REFRESH_TOKEN`은 만료 시 스크립트가 자동으로 갱신하여 `GH_PAT`를 통해 시크릿을 업데이트합니다.

---

## 🚀 로컬 실행 방법

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정 (블로그 포스팅이 필요한 경우)

```bash
export BLOGGER_BLOG_ID="your_blog_id"
export BLOGGER_CLIENT_ID="your_client_id"
export BLOGGER_CLIENT_SECRET="your_client_secret"
export BLOGGER_REFRESH_TOKEN="your_refresh_token"
export GOOGLE_SHEET_ID="your_sheet_id"
```

### 3. ETF 데이터 업데이트

```bash
python update_data.py
```

### 4. 52주 신고가 분석

```bash
python update_52w_high.py
```

### 5. Streamlit 대시보드 실행

```bash
streamlit run app.py
```

---

## 📊 RS(상대강도) 계산 방법

마크 미너비니의 RS Rating은 시장 전체와 비교한 종목 퍼포먼스를 1~99점으로 환산합니다.

```
가중 수익률 = (3개월 수익률 × 0.4) + (6개월 × 0.2) + (9개월 × 0.2) + (12개월 × 0.2)
RS Rating   = 가중 수익률의 백분위 순위 × 99
```

| RS 점수 | 의미 |
|---|---|
| 80 이상 | 시장 상위 20% — 강력한 주도주 |
| 50 ~ 79 | 시장 평균 수준 |
| 50 미만 | 시장 대비 약세 |

---

## ⏰ 자동화 스케줄

GitHub Actions 워크플로우는 **평일(월~금) KST 16:30** (UTC 07:30)에 자동 실행됩니다.

```yaml
schedule:
  - cron: '30 7 * * 1-5'
```

실행 순서:
1. `update_data.py` — ETF 상대강도 계산 → `etf_data.csv` 커밋 & 푸시 → 블로그 포스팅
2. `update_52w_high.py` — 52주 신고가 분석 → 블로그 포스팅

---

## 📦 주요 의존성

| 패키지 | 용도 |
|---|---|
| `streamlit` | 웹 대시보드 |
| `pandas` | 데이터 처리 |
| `finance-datareader` | 주가 데이터 수집 |
| `google-api-python-client` | Blogger API 연동 |
| `google-auth` / `google-auth-oauthlib` | Google OAuth 인증 |
| `PyNaCl` | GitHub Secret 암호화 업데이트 |
| `requests` | HTTP 요청 (뉴스 RSS 등) |

---

## 📝 관련 링크

- 📊 **Streamlit 대시보드**: Streamlit Community Cloud 배포 후 URL 입력
- 📖 **분석 블로그 (모멘텀 인덱스 랩)**: [https://apple.journeywithgardens.com/](https://apple.journeywithgardens.com/)

---

## ⚠️ 면책 조항

본 프로젝트는 투자 참고용 정보 제공을 목적으로 하며, **투자 권유 또는 종목 추천이 아닙니다.** 모든 투자 결정과 그에 따른 결과는 투자자 본인의 책임입니다.
