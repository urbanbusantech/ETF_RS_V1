"""
update_data.py
══════════════════════════════════════════════════════════════
[기존 유지] ETF 상대강도 분석 (FinanceDataReader)
            → etf_data.csv 저장 + 블로거 포스팅

[신규 추가] 미너비니 SEPA + VCP 코스피 종목 스캐너 (한투 API)
            → 블로거 포스팅 + 텔레그램 전송

GitHub Secrets 추가 필요:
  KIS_APP_KEY           한투 실전 앱키
  KIS_APP_SECRET        한투 실전 시크릿
  TELEGRAM_TOKEN        텔레그램 봇 토큰
  TELEGRAM_CHAT_ID      수신 채팅 ID (선택 — 없으면 텔레그램 생략)
  TISTORY_ACCESS_TOKEN  티스토리 토큰 (선택)
  TISTORY_BLOG_NAME     티스토리 블로그명 (선택)
══════════════════════════════════════════════════════════════
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone
import FinanceDataReader as fdr
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


# ══════════════════════════════════════════════════════════════
# ── ① 기존 ETF 분석 (원본 그대로) ──
# ══════════════════════════════════════════════════════════════

def get_equity_etfs():
    """네이버 금융 API를 통해 국내 상장 주식형 ETF 목록을 수집합니다."""
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df = pd.DataFrame(data['result']['etfItemList'])

    target_codes = [1, 2, 4]
    equity_df = df[df['etfTabCode'].isin(target_codes)].copy()

    exclude_keywords = ['채권', '국고채', '금리', '원유', '골드', '금선물', '은선물',
                        '달러', '인버스', '레버리지', 'TR']
    pattern = '|'.join(exclude_keywords)
    equity_df = equity_df[~equity_df['itemname'].str.contains(pattern)]

    equity_df = equity_df[['itemcode', 'itemname', 'nowVal', 'quant']]
    return equity_df


def calculate_minervini_rs(equity_df):
    """최근 3/6/9/12개월 수익률에 가중치를 부여하여 상대강도를 계산합니다."""
    end_date   = datetime.now()
    start_date = end_date - pd.DateOffset(years=1)

    benchmark_data = fdr.DataReader('069500', start_date, end_date)
    if len(benchmark_data) >= 240:
        benchmark_now = float(benchmark_data['Close'].iloc[-1])
        benchmark_21d = float(benchmark_data['Close'].iloc[-21])
        benchmark_63d = float(benchmark_data['Close'].iloc[-63])
        benchmark_1y  = float(benchmark_data['Close'].iloc[0])
        benchmark_1m_ret = (benchmark_now / benchmark_21d) - 1
        benchmark_3m_ret = (benchmark_now / benchmark_63d) - 1
        benchmark_1y_ret = (benchmark_now / benchmark_1y)  - 1
    else:
        benchmark_1m_ret = benchmark_3m_ret = benchmark_1y_ret = 0

    scores = []
    codes  = equity_df['itemcode'].tolist()

    for i, code in enumerate(codes):
        if i % 50 == 0 and i > 0:
            time.sleep(0.5)
        try:
            df_hist = fdr.DataReader(code, start_date, end_date)
            if len(df_hist) < 240:
                scores.append({'itemcode': code, 'weighted_return': None,
                                '1m_ret': None, '3m_ret': None, '1y_ret': None})
                continue

            close = df_hist['Close']
            p0   = float(close.iloc[-1])
            p63  = float(close.iloc[-63])
            p126 = float(close.iloc[-126])
            p189 = float(close.iloc[-189])
            p240 = float(close.iloc[-240])

            weighted_ret = ((p0/p63  - 1) * 0.4 + (p0/p126 - 1) * 0.2 +
                            (p0/p189 - 1) * 0.2 + (p0/p240 - 1) * 0.2)

            scores.append({
                'itemcode':        code,
                'weighted_return': weighted_ret,
                '1m_ret':          (p0 / float(close.iloc[-21])) - 1,
                '3m_ret':          (p0 / p63)  - 1,
                '1y_ret':          (p0 / p240) - 1,
            })
        except Exception:
            scores.append({'itemcode': code, 'weighted_return': None,
                           '1m_ret': None, '3m_ret': None, '1y_ret': None})

    scores_df    = pd.DataFrame(scores)
    valid_scores = scores_df.dropna(subset=['weighted_return']).copy()
    valid_scores['RS_Rating'] = valid_scores['weighted_return'].rank(pct=True) * 99
    valid_scores['RS_Rating'] = valid_scores['RS_Rating'].apply(lambda x: int(round(x)))

    result_df = pd.merge(
        equity_df,
        valid_scores[['itemcode', '1m_ret', '3m_ret', '1y_ret', 'RS_Rating']],
        on='itemcode', how='inner'
    )
    result_df = result_df.sort_values(by='RS_Rating', ascending=False)
    result_df.columns = ['종목코드', '종목명', '현재가(원)', '거래량',
                         '1개월', '3개월', '1년', '상대강도']
    return result_df, benchmark_1m_ret, benchmark_3m_ret, benchmark_1y_ret


def post_to_blogger(title, html_content, labels=None):
    """Blogger API를 사용하여 글을 게시합니다. (기존 함수 원본 유지)"""
    blog_id       = os.environ.get('BLOGGER_BLOG_ID')
    client_id     = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([blog_id, client_id, client_secret, refresh_token]):
        print("💡 Blogger API 인증 정보가 없어 포스팅을 건너뜁니다.")
        return

    try:
        creds = Credentials(
            token=None, refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id, client_secret=client_secret
        )
        service = build('blogger', 'v3', credentials=creds)

        body = {"kind": "blogger#post", "title": title, "content": html_content}
        if labels:
            body["labels"] = labels

        service.posts().insert(blogId=blog_id, body=body, isDraft=False).execute()
        print("✅ 구글 블로그 포스팅 성공!")
    except Exception as e:
        print(f"❌ 구글 블로그 포스팅 실패: {e}")


def export_data(df, bm_1m, bm_3m, bm_1y):
    """데이터 가공 및 SEO 최적화된 HTML 리포트를 생성합니다. (기존 함수 원본 유지)"""
    df.to_csv('etf_data.csv', index=False, encoding='utf-8-sig')

    html_df = df.copy()
    html_df['현재가(원)'] = html_df['현재가(원)'].apply(lambda x: f"{x:,}")
    html_df['거래량']     = html_df['거래량'].apply(lambda x: f"{x:,}")
    html_df['1개월']      = (html_df['1개월'] * 100).round(2).astype(str) + '%'
    html_df['3개월']      = (html_df['3개월'] * 100).round(2).astype(str) + '%'
    html_df['1년']        = (html_df['1년']   * 100).round(2).astype(str) + '%'

    html_df['종목코드'] = html_df['종목코드'].apply(
        lambda x: f'<a href="https://finance.naver.com/item/fchart.naver?code={x}" '
                  f'target="_blank" style="color:#3498db;text-decoration:none;'
                  f'font-weight:bold;">{x}</a>'
    )
    html_df['상대강도'] = html_df['상대강도'].apply(
        lambda x: f'<span style="color:#c0392b;font-weight:bold;">{x}</span>'
        if x >= 80 else str(x)
    )

    kst          = timezone(timedelta(hours=9))
    now_kst      = datetime.now(kst)
    today_date   = now_kst.strftime('%Y-%m-%d')
    current_time = now_kst.strftime('%Y-%m-%d %H:%M')

    table_html = html_df.to_html(index=False, classes='etf-table', border=0,
                                  escape=False, justify='center')
    post_title = f"🚀 주식형 ETF 상대강도 모멘텀 랭킹({today_date})"

    html_content = f"""
    <div class="etf-container" style="font-family:'Helvetica Neue',Arial,sans-serif;
         line-height:1.6;color:#333;width:100%;max-width:1000px;
         margin:0 auto 30px auto;padding:0 10px;box-sizing:border-box;">
        <style>
            .etf-container h3 {{ color:#2c3e50;padding-left:10px;margin-top:25px;margin-bottom:10px; }}
            .etf-container .content-block {{ padding:10px 12px 10px 15px;border-left:4px solid #ccc;margin-bottom:20px; }}
            .etf-container .table-section {{ border-left:none;padding:0;margin-top:15px;width:100%;overflow-x:auto; }}
            .etf-container .content-block p {{ font-size:0.95em;color:#444;margin:0 0 10px 0; }}
            .technical-list {{ list-style:none;padding:0;margin:0;font-size:0.9em;color:#666; }}
            .technical-list li {{ margin-bottom:5px; }}
            .etf-table {{ width:100%;max-width:100%;border-collapse:collapse;
                          background-color:#ffffff;font-size:0.9em;
                          border:1px solid #e0e0e0;margin:0 auto; }}
            .etf-table th {{ background-color:#f8f9fa;color:#2c3e50;font-weight:600;
                             padding:10px;border:1px solid #e0e0e0; }}
            .etf-table td {{ padding:10px;border:1px solid #e0e0e0;
                             text-align:center;vertical-align:middle; }}
            .etf-table td:nth-child(2) {{ text-align:left; }}
            .etf-table tr:hover {{ background-color:#f1f4f8; }}
        </style>

        <h3>💡 개요: 시장 주도주를 찾는 모멘텀 분석</h3>
        <div class="content-block">
            <p>본 리포트는 대한민국 상장 주식형 ETF 중 현재 가장 강력한 상승 에너지를 보여주는 종목을 선별합니다.</p>
        </div>

        <h3>📈 상대강도(Relative Strength)란?</h3>
        <div class="content-block">
            <p>마크 미너비니의 <b>RS Rating</b>은 특정 종목의 퍼포먼스를 시장 전체와 비교하여 1~99점으로 환산한 지표입니다.</p>
            <p>- <b>계산 방식:</b> 최근 <b>3개월(40%)</b>, 6개월(20%), 9개월(20%), 12개월(20%) 가중치를 부여합니다.<br>
               - <b>해석 방법:</b> 80점 이상 = 시장 상위 20% 주도주군</p>
        </div>

        <h3>📊 분석 상세 정보</h3>
        <div class="content-block">
            <ul class="technical-list">
                <li>📅 <b>업데이트 일시:</b> {current_time} (KST 기준)</li>
                <li>🔍 <b>분석 대상:</b> 국내 상장 주식형 ETF {len(df)}개 (상장 1년 미만 제외)</li>
                <li>📉 <b>벤치마크(KODEX 200) 성과:</b> 1개월({bm_1m*100:.2f}%), 3개월({bm_3m*100:.2f}%), 1년({bm_1y*100:.2f}%)</li>
            </ul>
        </div>

        <h3>📋 주식형 ETF 상대강도 순위 TOP 리스트</h3>
        <div class="table-section">
            {table_html}
        </div>
    </div>
    """

    category_labels = ["상대강도"]
    post_to_blogger(post_title, html_content, category_labels)


# ══════════════════════════════════════════════════════════════
# ── ② 한투 API 공통 유틸 (미너비니 스캐너용) ──
# ══════════════════════════════════════════════════════════════

_KIS_TOKEN_CACHE: dict = {}

def _kis_get_token() -> str:
    """한투 OAuth 토큰 발급 (세션 내 캐시)"""
    if "token" in _KIS_TOKEN_CACHE:
        return _KIS_TOKEN_CACHE["token"]

    app_key    = os.environ.get("KIS_APP_KEY", "")
    app_secret = os.environ.get("KIS_APP_SECRET", "")
    base_url   = "https://openapi.koreainvestment.com:9443"

    if not app_key or not app_secret:
        raise ValueError("KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되지 않았습니다.")

    res = requests.post(
        f"{base_url}/oauth2/tokenP",
        json={"grant_type": "client_credentials",
              "appkey": app_key, "appsecret": app_secret},
        timeout=10
    )
    res.raise_for_status()
    _KIS_TOKEN_CACHE["token"] = res.json()["access_token"]
    return _KIS_TOKEN_CACHE["token"]


def _kis_get(path: str, tr_id: str, params: dict) -> dict:
    """한투 REST API GET 공통 호출"""
    app_key    = os.environ.get("KIS_APP_KEY", "")
    app_secret = os.environ.get("KIS_APP_SECRET", "")
    base_url   = "https://openapi.koreainvestment.com:9443"

    headers = {
        "Content-Type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {_kis_get_token()}",
        "appkey":        app_key,
        "appsecret":     app_secret,
        "tr_id":         tr_id,
        "custtype":      "P",
    }
    res = requests.get(f"{base_url}{path}", headers=headers,
                       params=params, timeout=15)
    res.raise_for_status()
    data = res.json()

    # 토큰 만료 시 자동 갱신
    if data.get("rt_cd") == "1" and "token" in data.get("msg1", "").lower():
        _KIS_TOKEN_CACHE.clear()
        return _kis_get(path, tr_id, params)

    return data


def _is_market_open_today() -> bool:
    """KIS API 휴장일 조회로 오늘 장 개시 여부 확인"""
    today = datetime.now().strftime("%Y%m%d")
    try:
        data = _kis_get(
            "/uapi/domestic-stock/v1/quotations/chk-holiday",
            "CTCA0903R",
            {"BASS_DT": today, "CTX_AREA_NK": "", "CTX_AREA_FK": ""}
        )
        output = data.get("output", [])
        if output:
            return output[0].get("opnd_yn", "N") == "Y"
    except Exception as e:
        print(f"⚠️ 장 운영일 API 오류({e}) — 주말 여부로 대체 판단")
    return datetime.now().weekday() < 5  # 주말이면 False


# ══════════════════════════════════════════════════════════════
# ── ③ 미너비니 스캐너 — 한투 API 데이터 수집 ──
# ══════════════════════════════════════════════════════════════

def _get_kospi_top(top_n: int = 200) -> list:
    """코스피 시가총액 상위 종목 목록 수집"""
    data = _kis_get(
        "/uapi/domestic-stock/v1/ranking/market-cap",
        "FHPST01710000",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE":  "20171",
            "FID_INPUT_ISCD":         "0001",
            "FID_DIV_CLS_CODE":       "0",
            "FID_BLNG_CLS_CODE":      "0",
            "FID_TRGT_CLS_CODE":      "0",
            "FID_TRGT_EXLS_CLS_CODE": "0",
            "FID_INPUT_PRICE_1":      "",
            "FID_INPUT_PRICE_2":      "",
            "FID_VOL_CNT":            "",
            "FID_INPUT_DATE_1":       "",
        }
    )
    stocks = []
    for item in data.get("output", []):
        stocks.append({
            "rank": item.get("data_rank", ""),
            "code": item.get("stck_shrn_iscd", ""),
            "name": item.get("hts_kor_isnm", ""),
        })
    return stocks[:top_n]


def _get_ohlcv_kis(code: str, days: int = 260) -> list:
    """
    한투 API 일별 OHLCV 조회
    반환: [{"date","open","high","low","close","volume"}, ...] 최신→과거 순
    """
    end_dt   = datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")

    data = _kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "FHKST03010100",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         code,
            "FID_INPUT_DATE_1":       start_dt,
            "FID_INPUT_DATE_2":       end_dt,
            "FID_PERIOD_DIV_CODE":    "D",
            "FID_ORG_ADJ_PRC":        "0",
        }
    )
    rows = []
    for item in data.get("output2", []):
        try:
            rows.append({
                "date":   item.get("stck_bsop_date", ""),
                "open":   float(item.get("stck_oprc", 0) or 0),
                "high":   float(item.get("stck_hgpr", 0) or 0),
                "low":    float(item.get("stck_lwpr", 0) or 0),
                "close":  float(item.get("stck_clpr", 0) or 0),
                "volume": float(item.get("acml_vol",  0) or 0),
            })
        except Exception:
            continue
    valid = [r for r in rows if r["close"] > 0]
    return valid[:days]


# ══════════════════════════════════════════════════════════════
# ── ④ 미너비니 전략 판별 로직 ──
# ══════════════════════════════════════════════════════════════

def _sma(closes: list, n: int):
    return sum(closes[:n]) / n if len(closes) >= n else None


def _check_trend_template(rows: list) -> dict:
    """
    미너비니 Trend Template 7조건 판별
    (RS 점수는 별도 계산 후 합산)

    조건:
      1. 현재가 > MA150, MA200
      2. MA150 > MA200
      3. MA200이 22거래일 전보다 상승
      4. MA50 > MA150 > MA200  (완전 정배열)
      5. 현재가 > MA50
      6. 현재가 ≥ 52주 저가 × 1.30
      7. 현재가 ≤ 52주 고가 × 1.25
    """
    if len(rows) < 200:
        return {"pass": False, "detail": {}}

    closes = [r["close"] for r in rows]
    highs  = [r["high"]  for r in rows]
    lows   = [r["low"]   for r in rows]
    price  = closes[0]

    ma50     = _sma(closes, 50)
    ma150    = _sma(closes, 150)
    ma200    = _sma(closes, 200)
    ma200_22 = _sma(closes[22:], 200)   # 22거래일 전 MA200

    high_52w = max(highs[:252])
    low_52w  = min(lows[:252])

    c = {
        1: price > ma150  and price > ma200,
        2: ma150 > ma200,
        3: (ma200 > ma200_22) if ma200_22 else False,
        4: ma50  > ma150  and ma50  > ma200,
        5: price > ma50,
        6: price >= low_52w  * 1.30,
        7: price <= high_52w * 1.25,
    }

    return {
        "pass": all(c.values()),
        "criteria": c,
        "detail": {
            "price":             round(price,    0),
            "ma50":              round(ma50,     0),
            "ma150":             round(ma150,    0),
            "ma200":             round(ma200,    0),
            "high_52w":          round(high_52w, 0),
            "low_52w":           round(low_52w,  0),
            "pct_from_52w_high": round((price / high_52w - 1) * 100, 2),
            "pct_from_52w_low":  round((price / low_52w  - 1) * 100, 2),
        }
    }


def _calc_rs(stock_rows: list, index_rows: list) -> float:
    """
    IBD RS 점수 근사 계산 (3/6/9/12개월 가중 수익률 비교)
    반환: 0~100
    """
    def ret(rows, p):
        return (rows[0]["close"] / rows[p]["close"] - 1) * 100 if len(rows) > p else 0

    periods = [63, 126, 189, 252]
    weights = [0.40, 0.20, 0.20, 0.20]

    s_ret = sum(w * ret(stock_rows, p) for w, p in zip(weights, periods))
    i_ret = sum(w * ret(index_rows,  p) for w, p in zip(weights, periods))

    # 상대 차이를 -30%~+30% 범위로 가정 후 0~100 정규화
    return round(min(max((s_ret - i_ret + 30) / 60 * 100, 0), 100), 1)


def _detect_vcp(rows: list, lookback: int = 60) -> dict:
    """
    VCP (Volatility Contraction Pattern) 탐지

    조건:
      ① 최소 3회 연속 되돌림 — 각 폭이 직전보다 작을 것
         (예: 18% → 12% → 6%)
      ② 수축 구간마다 평균 거래량도 감소 (공급 소멸)
      ③ 마지막 수축 폭 ≤ 10%  (타이트한 베이스)
      ④ 현재가가 피벗(최근 고점) 대비 -3% 이내 또는 돌파
      ⑤ 당일 거래량 ≥ 20일 평균 × 1.4  (돌파 확인용 — 별도 플래그)
    """
    if len(rows) < lookback + 20:
        return {"pass": False, "breakout_vol": False, "detail": {}}

    seg     = rows[:lookback]
    closes  = [r["close"]  for r in seg]
    highs   = [r["high"]   for r in seg]
    lows    = [r["low"]    for r in seg]
    volumes = [r["volume"] for r in seg]

    pivot     = max(highs)
    price_now = closes[0]

    # ── 수축 구간 탐지 ──
    contractions = []
    i = 0
    while i < len(highs) - 8:
        if highs[i] >= max(highs[max(0, i-3):i+4]):
            peak_val      = highs[i]
            trough_window = lows[i+1:i+16]
            if not trough_window:
                i += 4
                continue
            trough_val = min(trough_window)
            trough_idx = i + 1 + trough_window.index(trough_val)
            pct = (peak_val - trough_val) / peak_val * 100
            if pct >= 3:
                avg_vol = sum(volumes[i:trough_idx+1]) / max(1, trough_idx - i + 1)
                contractions.append({
                    "pct":     round(pct, 1),
                    "avg_vol": round(avg_vol, 0),
                })
            i = trough_idx + 2
        else:
            i += 1

    if len(contractions) < 3:
        return {"pass": False, "breakout_vol": False,
                "detail": {"reason": f"수축 {len(contractions)}회 (최소 3회 필요)",
                           "pullbacks": [c["pct"] for c in contractions]}}

    is_shrinking  = all(contractions[j]["pct"]     < contractions[j-1]["pct"]
                        for j in range(1, len(contractions)))
    vol_shrinking = all(contractions[j]["avg_vol"] < contractions[j-1]["avg_vol"]
                        for j in range(1, len(contractions)))
    tight_base    = contractions[-1]["pct"] <= 10.0
    near_pivot    = (price_now / pivot - 1) * 100 >= -3.0

    avg_vol_20    = sum(volumes[:20]) / 20
    breakout_vol  = volumes[0] >= avg_vol_20 * 1.4

    passed = is_shrinking and vol_shrinking and tight_base and near_pivot

    return {
        "pass":         passed,
        "breakout_vol": breakout_vol,
        "detail": {
            "contraction_count": len(contractions),
            "is_shrinking":      is_shrinking,
            "vol_shrinking":     vol_shrinking,
            "tight_base":        tight_base,
            "near_pivot":        near_pivot,
            "pivot":             round(pivot, 0),
            "last_pct":          contractions[-1]["pct"],
            "pullbacks":         [c["pct"] for c in contractions],
            "avg_vol_20":        round(avg_vol_20, 0),
            "today_vol":         round(volumes[0], 0),
        }
    }


def _near_52w_high(rows: list, threshold: float = 5.0) -> bool:
    if len(rows) < 10:
        return False
    h52 = max(r["high"] for r in rows[:252])
    return (h52 - rows[0]["close"]) / h52 * 100 <= threshold


def _volume_surge(rows: list, mult: float = 1.4) -> bool:
    if len(rows) < 21:
        return False
    avg = sum(r["volume"] for r in rows[1:21]) / 20
    return rows[0]["volume"] >= avg * mult


# ══════════════════════════════════════════════════════════════
# ── ⑤ 미너비니 스캔 실행 ──
# ══════════════════════════════════════════════════════════════

def run_minervini_scan(top_n: int = 200) -> dict:
    """
    코스피 시가총액 상위 top_n 종목 대상 미너비니 스캔
    반환: {date, scanned, trend_template, vcp, near_high_breakout}
    """
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n[미너비니 스캐너] {today} 스캔 시작 — 코스피 상위 {top_n}종목")

    # 코스피 지수 데이터 (RS 계산 기준)
    try:
        kospi_rows = _get_ohlcv_kis("0001", 260)
    except Exception as e:
        print(f"  ⚠️ 코스피 지수 조회 실패({e}) — RS 계산 스킵")
        kospi_rows = []

    stocks = _get_kospi_top(top_n)
    print(f"  종목 목록 수집 완료: {len(stocks)}개")

    tt_pass, vcp_pass, breakout_pass = [], [], []

    for i, st in enumerate(stocks):
        code, name = st["code"], st["name"]
        try:
            rows = _get_ohlcv_kis(code, 260)
            if len(rows) < 200:
                time.sleep(0.05)
                continue

            # Trend Template
            tt = _check_trend_template(rows)
            if not tt["pass"]:
                time.sleep(0.05)
                continue

            # RS 점수
            rs = _calc_rs(rows, kospi_rows) if kospi_rows else None
            if rs is not None and rs < 70:
                time.sleep(0.05)
                continue

            base = {**st, "rs": rs, "tt_detail": tt["detail"],
                    "criteria": tt["criteria"]}
            tt_pass.append(base)

            # VCP
            vcp = _detect_vcp(rows)
            base["vcp"] = vcp["detail"]
            if vcp["pass"]:
                vcp_pass.append({**base, "vcp_breakout_vol": vcp["breakout_vol"]})

            # 52주 신고가 근접 + 거래량 급증
            if _near_52w_high(rows) and _volume_surge(rows):
                breakout_pass.append({**base, "near_high": True, "vol_surge": True})

            if (i + 1) % 20 == 0:
                print(f"  진행: {i+1}/{len(stocks)} | "
                      f"TT:{len(tt_pass)} VCP:{len(vcp_pass)}")
            time.sleep(0.05)

        except Exception as e:
            print(f"  ⚠️ {name}({code}) 오류: {e}")
            time.sleep(0.1)

    print(f"  완료 — TT:{len(tt_pass)} | VCP:{len(vcp_pass)} | "
          f"신고가+거래량:{len(breakout_pass)}")

    return {
        "date":               today,
        "scanned":            len(stocks),
        "trend_template":     tt_pass,
        "vcp":                vcp_pass,
        "near_high_breakout": breakout_pass,
    }


# ══════════════════════════════════════════════════════════════
# ── ⑥ 미너비니 리포트 생성 + 배포 ──
# ══════════════════════════════════════════════════════════════

def _fmt_n(v) -> str:
    try:    return f"{float(v):,.0f}"
    except: return str(v)


def _build_minervini_html(result: dict) -> tuple:
    """(post_title, html_content) 반환"""
    date    = result["date"]
    scanned = result["scanned"]
    tt      = result["trend_template"]
    vcp     = result["vcp"]
    nh      = result["near_high_breakout"]

    post_title = f"📊 미너비니 SEPA 스캐너 ({date}) — VCP {len(vcp)}종목 · TT {len(tt)}종목"

    def stock_rows_html(stocks, show_vcp=False) -> str:
        rows = ""
        for s in stocks:
            d  = s.get("tt_detail", {})
            vd = s.get("vcp", {}) if show_vcp else {}
            pb = " → ".join(f"{p}%" for p in (vd.get("pullbacks") or [])[-4:])
            bv = "🔥" if s.get("vcp_breakout_vol") else ""
            vcp_td = (f"<td>{pb or '—'} / 마지막 {vd.get('last_pct','—')}%</td>"
                      if show_vcp else "")
            pct_color = "#c0392b" if float(d.get("pct_from_52w_high", 0)) > -5 else "#2c3e50"
            rows += f"""
            <tr>
              <td>{s.get('rank','—')}</td>
              <td style="text-align:left"><b>{s['name']}</b><br>
                <a href="https://finance.naver.com/item/fchart.naver?code={s['code']}"
                   target="_blank" style="color:#3498db;font-size:0.85em">{s['code']}</a>
              </td>
              <td style="font-family:monospace">{_fmt_n(d.get('price'))}원</td>
              <td style="color:{pct_color}">{d.get('pct_from_52w_high', 0):+.1f}%</td>
              <td style="font-family:monospace">{_fmt_n(d.get('ma50'))}</td>
              <td style="font-family:monospace">{_fmt_n(d.get('ma150'))}</td>
              <td style="font-family:monospace">{_fmt_n(d.get('ma200'))}</td>
              <td><b style="color:#c0392b">{s.get('rs','—')}</b></td>
              {vcp_td}
              <td>{bv}</td>
            </tr>"""
        return rows

    def section_html(heading, stocks, show_vcp=False) -> str:
        if not stocks:
            return f"<h3>{heading}</h3><p style='color:#888'>해당 없음</p>"
        extra_th = "<th>VCP 되돌림</th>" if show_vcp else ""
        return f"""
        <h3>{heading}</h3>
        <div style="overflow-x:auto">
        <table border="1" cellpadding="8" cellspacing="0"
               style="border-collapse:collapse;width:100%;font-size:0.9em;
                      min-width:700px;border:1px solid #e0e0e0">
          <thead style="background:#2c3e50;color:#fff">
            <tr>
              <th>순위</th><th>종목</th><th>현재가</th><th>52주고가대비</th>
              <th>MA50</th><th>MA150</th><th>MA200</th><th>RS</th>{extra_th}<th></th>
            </tr>
          </thead>
          <tbody>{stock_rows_html(stocks, show_vcp)}</tbody>
        </table>
        </div>"""

    html_content = f"""
    <div style="font-family:'Helvetica Neue',Arial,sans-serif;line-height:1.6;
                color:#333;max-width:1000px;margin:0 auto;padding:0 10px">

      <!-- KPI 요약 -->
      <div style="display:flex;gap:12px;flex-wrap:wrap;margin:16px 0">
        {''.join(f'''<div style="background:#f8f9fa;border:1px solid #e0e0e0;
                      border-top:3px solid {c};border-radius:6px;
                      padding:10px 18px;min-width:120px;text-align:center">
          <div style="font-size:1.8rem;font-weight:700;color:{c}">{n}</div>
          <div style="font-size:0.75rem;color:#888">{l}</div></div>'''
          for n, l, c in [
            (scanned,    "분석 종목",       "#3498db"),
            (len(tt),    "TrendTemplate",  "#8e44ad"),
            (len(vcp),   "VCP 패턴",       "#27ae60"),
            (len(nh),    "신고가+거래량",   "#e74c3c"),
          ])}
      </div>

      <!-- 전략 설명 -->
      <div style="background:#f0f7ff;border-left:4px solid #3498db;
                  padding:12px 16px;margin:16px 0;font-size:0.9em;line-height:1.9">
        <b>📌 미너비니 Trend Template 7+1조건</b><br>
        ① 현재가 &gt; MA150, MA200 &nbsp;
        ② MA150 &gt; MA200 &nbsp;
        ③ MA200 상승 중 (1개월 이상) &nbsp;
        ④ MA50 &gt; MA150 &gt; MA200 (완전 정배열) &nbsp;
        ⑤ 현재가 &gt; MA50<br>
        ⑥ 현재가 ≥ 52주 저가×1.30 &nbsp;
        ⑦ 현재가 ≤ 52주 고가×1.25 &nbsp;
        ⑧ RS 점수 ≥ 70<br><br>
        <b>🌀 VCP 패턴 조건</b><br>
        · 최소 3회 연속 되돌림, 각 폭이 직전보다 작을 것 (예: 18% → 12% → 6%)<br>
        · 수축 구간마다 거래량도 감소 (공급 소멸) · 마지막 수축 ≤ 10%<br>
        · 피벗 대비 현재가 -3% 이내 · 돌파 시 거래량 평균×1.4 이상 🔥
      </div>

      {section_html(f"🌀 VCP 패턴 감지 종목 ({len(vcp)}개)", vcp, show_vcp=True)}
      {section_html(f"🚀 52주 신고가 근접 + 거래량 급증 ({len(nh)}개)", nh)}
      {section_html(f"📋 Trend Template 통과 전체 ({len(tt)}개)", tt)}

      <div style="margin-top:24px;background:#fff3cd;border:1px solid #ffc107;
                  border-radius:4px;padding:10px 14px;font-size:0.8em;color:#666">
        ⚠️ 본 리포트는 한국투자증권 장마감 데이터 기반 자동 생성 정보이며,
        투자 권유가 아닙니다. 투자 판단 및 책임은 투자자 본인에게 있습니다.
      </div>
    </div>"""

    return post_title, html_content


def _build_telegram_message(result: dict) -> str:
    """텔레그램용 메시지 생성"""
    date  = result["date"]
    tt    = result["trend_template"]
    vcp   = result["vcp"]
    nh    = result["near_high_breakout"]

    lines = [
        "📊 <b>미너비니 SEPA 스캐너 리포트</b>",
        f"📅 {date}  |  코스피 상위 {result['scanned']}종목 분석",
        f"✅ TrendTemplate <b>{len(tt)}</b>  🌀 VCP <b>{len(vcp)}</b>  🚀 신고가+거래량 <b>{len(nh)}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if vcp:
        lines.append(f"\n🌀 <b>VCP 패턴 ({len(vcp)}종목)</b>")
        for s in vcp[:8]:
            d  = s.get("tt_detail", {})
            vd = s.get("vcp", {})
            pb = " → ".join(f"{p}%" for p in (vd.get("pullbacks") or [])[-4:])
            bv = "🔥" if s.get("vcp_breakout_vol") else ""
            lines.append(
                f"  • <b>{s['name']}</b>({s['code']}) "
                f"{_fmt_n(d.get('price'))}원  RS {s.get('rs','—')}\n"
                f"    되돌림: {pb}  |  마지막 {vd.get('last_pct','—')}%  {bv}"
            )
    else:
        lines.append("\n🌀 <b>VCP 패턴</b> — 해당 없음")

    if nh:
        lines.append(f"\n🚀 <b>52주 신고가 근접+거래량 ({len(nh)}종목)</b>")
        for s in nh[:8]:
            d = s.get("tt_detail", {})
            lines.append(
                f"  • <b>{s['name']}</b>({s['code']}) "
                f"{_fmt_n(d.get('price'))}원  "
                f"고가대비 {d.get('pct_from_52w_high',0):+.1f}%  "
                f"RS {s.get('rs','—')}"
            )
    else:
        lines.append("\n🚀 <b>신고가+거래량</b> — 해당 없음")

    lines.append(f"\n📋 <b>TrendTemplate 통과</b> ({len(tt)}종목)")
    for s in tt[:10]:
        d = s.get("tt_detail", {})
        lines.append(f"  {s.get('rank','—')}위 {s['name']}  "
                     f"{_fmt_n(d.get('price'))}원  RS {s.get('rs','—')}")
    if len(tt) > 10:
        lines.append(f"  … 외 {len(tt)-10}개")

    lines += [
        "\n━━━━━━━━━━━━━━━━━━━━",
        "⚠️ 참고용 정보. 투자 권유 아님.",
    ]
    return "\n".join(lines)


def _send_telegram(message: str) -> bool:
    """텔레그램 메시지 전송"""
    token   = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("💡 텔레그램 환경변수 없음 — 전송 스킵")
        return False
    # 4096자 초과 시 분할 전송
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"},
                timeout=15
            )
            r.raise_for_status()
        except Exception as e:
            print(f"❌ 텔레그램 전송 실패: {e}")
            return False
    print("✅ 텔레그램 전송 완료!")
    return True


def _post_tistory(title: str, html: str) -> bool:
    """티스토리 포스팅 (선택)"""
    token = os.environ.get("TISTORY_ACCESS_TOKEN", "")
    blog  = os.environ.get("TISTORY_BLOG_NAME", "")
    if not token or not blog:
        return False
    try:
        r = requests.post(
            "https://www.tistory.com/apis/post/write",
            params={
                "access_token": token, "output": "json",
                "blogName": blog, "title": title, "content": html,
                "visibility": "3", "tag": "미너비니,SEPA,VCP,코스피,주식스캐너",
            },
            timeout=20
        )
        r.raise_for_status()
        if r.json().get("tistory", {}).get("status") == "200":
            print("✅ 티스토리 포스팅 완료!")
            return True
    except Exception as e:
        print(f"❌ 티스토리 포스팅 실패: {e}")
    return False


def publish_minervini_report(result: dict):
    """미너비니 스캔 결과 → 블로거 + 텔레그램 + 티스토리 배포"""
    post_title, html_content = _build_minervini_html(result)

    # 블로거 포스팅 (기존 함수 재사용)
    post_to_blogger(post_title, html_content, labels=["미너비니", "SEPA", "VCP"])

    # 텔레그램 전송
    _send_telegram(_build_telegram_message(result))

    # 티스토리 (선택)
    _post_tistory(post_title, html_content)


# ══════════════════════════════════════════════════════════════
# ── 메인 실행 ──
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":

    # ── [기존] ETF 분석 ──────────────────────────────────────
    print("=" * 60)
    print("[1/2] ETF 상대강도 분석 시작")
    print("=" * 60)
    equity_df = get_equity_etfs()
    rs_df, bm_1m, bm_3m, bm_1y = calculate_minervini_rs(equity_df)
    export_data(rs_df, bm_1m, bm_3m, bm_1y)
    print("✅ ETF 분석 완료")

    # ── [신규] 미너비니 단위 종목 스캐너 ─────────────────────
    print("\n" + "=" * 60)
    print("[2/2] 미너비니 SEPA 종목 스캐너 시작")
    print("=" * 60)

    # 한투 API 키가 없으면 스캐너 건너뜀 (기존 ETF 작업에 영향 없음)
    if not os.environ.get("KIS_APP_KEY") or not os.environ.get("KIS_APP_SECRET"):
        print("💡 KIS_APP_KEY / KIS_APP_SECRET 없음 — 미너비니 스캐너 건너뜀")
    elif not _is_market_open_today():
        print("💡 오늘은 장 휴장일 — 미너비니 스캐너 건너뜀")
    else:
        try:
            scan_result = run_minervini_scan(top_n=200)
            publish_minervini_report(scan_result)
            print("✅ 미너비니 스캐너 완료")
        except Exception as e:
            # 스캐너 실패해도 기존 ETF 작업 결과에 영향 없음
            print(f"❌ 미너비니 스캐너 오류: {e}")
