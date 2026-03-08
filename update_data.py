"""
update_data.py
══════════════════════════════════════════════════════════════
[기존 유지] ETF 상대강도 분석 (FinanceDataReader)
            → etf_data.csv 저장 + 블로거 포스팅

[신규 추가] 미너비니 SEPA + VCP 스캐너 (한투 API & fdr)
            → 코스피 상위 200 + 코스닥 상위 200 각각 별도 포스팅
            → 블로거 포스팅 + 텔레그램 전송

※ 테스트 모드: 휴장일 체크 비활성화 (정상 동작 확인 후 재활성화)

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
# ── ① 기존 ETF 분석 (원본 그대로 유지하되 HTML 스타일 통일) ──
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
    """Blogger API를 사용하여 글을 게시합니다."""
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
    """데이터 가공 및 SEO 최적화된 HTML 리포트를 생성합니다. (모멘텀 인덱스 랩 양식 적용)"""
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

    table_html = html_df.to_html(index=False, classes='momentum-table', border=0,
                                  escape=False, justify='center')
    post_title = f"🚀 주식형 ETF 상대강도 모멘텀 랭킹 ({today_date})"

    html_content = f"""
    <div class="momentum-container">
        <style>
            .momentum-container {{ max-width:1000px; margin:0 auto; font-family:'Helvetica Neue', Arial, sans-serif; }}
            .momentum-container h3 {{ color:#2c3e50; padding-left:10px; text-align:left; margin-top:30px; margin-bottom:15px; }}
            .content-box {{ border-left:4px solid #ccc; padding:10px 12px 10px 15px; margin-bottom:20px; text-align:left; background:#fafafa; }}
            .momentum-list {{ list-style:none; padding:0; margin:0; font-size:0.9em; color:#666; text-align:left; }}
            .momentum-list li {{ margin-bottom:5px; }}
            .momentum-list li::before {{ content: '▪ '; color:#2c3e50; font-weight:bold; }}
            .momentum-table {{ width:100%; text-align:center; border:1px solid #e0e0e0; border-collapse:collapse; font-size:0.9em; margin-top:15px; }}
            .momentum-table th {{ background-color:#f8f9fa; color:#2c3e50; padding:10px; border:1px solid #e0e0e0; font-weight:bold; }}
            .momentum-table td {{ padding:10px; border:1px solid #e0e0e0; vertical-align:middle; }}
            .momentum-table td:nth-child(2) {{ text-align:left; }}
            .momentum-table tbody tr:hover {{ background-color:#f1f4f8; transition: background-color 0.2s; }}
        </style>

        <h3>💡 개요: 시장 주도주를 찾는 모멘텀 분석</h3>
        <div class="content-box">
            <p style="margin:0 0 10px 0; font-size:0.95em; color:#444;">본 리포트는 대한민국 상장 주식형 ETF 중 현재 가장 강력한 상승 에너지를 보여주는 종목을 선별합니다. 마크 미너비니의 RS Rating 방식으로 시장 대비 초과 수익 종목을 매일 자동 선별합니다.</p>
        </div>
        <h3>📈 상대강도(Relative Strength)란?</h3>
        <div class="content-box">
            <p style="margin:0 0 10px 0; font-size:0.95em; color:#444;">마크 미너비니의 <b>RS Rating</b>은 특정 종목의 퍼포먼스를 시장 전체와 비교하여 1~99점으로 환산한 지표입니다.</p>
            <ul class="momentum-list">
                <li><b>계산 방식:</b> 최근 <b>3개월(40%)</b>, 6개월(20%), 9개월(20%), 12개월(20%) 가중치를 부여합니다.</li>
                <li><b>해석 방법:</b> 80점 이상 = 시장 상위 20% 주도주군</li>
            </ul>
        </div>

        <h3>📊 분석 상세 정보</h3>
        <div class="content-box">
            <ul class="momentum-list">
                <li>📅 <b>업데이트 일시:</b> {current_time} (KST 기준)</li>
                <li>🔍 <b>분석 대상:</b> 국내 상장 주식형 ETF {len(df)}개 (상장 1년 미만 제외)</li>
                <li>📉 <b>벤치마크(KODEX 200) 성과:</b> 1개월({bm_1m*100:.2f}%), 3개월({bm_3m*100:.2f}%), 1년({bm_1y*100:.2f}%)</li>
            </ul>
        </div>

        <h3>📋 주식형 ETF 상대강도 순위 TOP 리스트</h3>
        <div style="overflow-x:auto;">
            {table_html}
        </div>
    </div>
    """

    category_labels = ["상대강도"]
    post_to_blogger(post_title, html_content, category_labels)


# ══════════════════════════════════════════════════════════════
# ── ② 한투 API 공통 유틸 및 FDR 지수 호출 (미너비니 스캐너용) ──
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
    return datetime.now().weekday() < 5


def _get_index_ohlcv_fdr(market: str, days: int = 260) -> list:
    """
    FinanceDataReader를 활용한 지수(KOSPI/KOSDAQ) 일별 데이터 조회.
    단순 지수 티커(KS11, KQ11) 호출 실패 시, 
    안정적으로 불러올 수 있는 대표 ETF(069500, 229200)로 우회(Fallback)합니다.
    """
    # [1순위: 지수 티커, 2순위: 지수 추종 대표 ETF]
    symbols = ['KS11', '069500'] if market == 'kospi' else ['KQ11', '229200']
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days*2) 
    
    for symbol in symbols:
        try:
            df = fdr.DataReader(symbol, start_date, end_date)
            if df is not None and not df.empty:
                df = df.sort_index(ascending=False).head(days)
                rows = []
                for date, row in df.iterrows():
                    rows.append({
                        "date": date.strftime("%Y%m%d"),
                        "close": float(row['Close'])
                    })
                if rows:
                    return rows
        except Exception as e:
            print(f"    ⚠️ 지수({symbol}) 조회 오류: {e}")
            continue
            
    # 모든 우회 시도 실패 시 빈 배열 반환
    return []


# ══════════════════════════════════════════════════════════════
# ── ③ 미너비니 스캐너 — 한투 API 개별 종목 데이터 수집 ──
# ══════════════════════════════════════════════════════════════

def _get_market_top(market: str = "kospi", top_n: int = 200) -> list:
    """시가총액 상위 종목 목록 수집 (최대 top_n개)"""
    market_label = "코스피" if market == "kospi" else "코스닥"
    sosok        = "0"     if market == "kospi" else "1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
    }
    stocks = []
    page   = 1

    print(f"  [{market_label}] 종목 목록 수집 중...")

    while len(stocks) < top_n:
        try:
            url  = "https://finance.naver.com/sise/sise_market_sum.nhn"
            r    = requests.get(url, headers=headers,
                                params={"sosok": sosok, "page": page}, timeout=10)
            r.raise_for_status()

            import re as _re
            found = _re.findall(
                r'code=(\d{6})[^"]*"[^>]*>\s*([^<]+?)\s*</a>',
                r.text
            )
            page_stocks = []
            seen_page   = set()
            for code, name in found:
                name = name.strip()
                if len(code) == 6 and name and code not in seen_page:
                    seen_page.add(code)
                    page_stocks.append((code, name))

            if not page_stocks:
                print(f"    {page}페이지에서 종목 없음 — 수집 종료")
                break

            for code, name in page_stocks:
                stocks.append({
                    "rank":   str(len(stocks) + 1),
                    "code":   code,
                    "name":   name,
                    "market": market,
                })
                if len(stocks) >= top_n:
                    break

            print(f"    {page}페이지 완료 — 누적 {len(stocks)}개")
            page  += 1
            time.sleep(0.3)

        except Exception as e:
            print(f"  ⚠️ 네이버 금융 {page}페이지 오류: {e}")
            break

    if not stocks:
        print(f"  ⚠️ 네이버 금융 실패 — 한투 API 폴백 (최대 30개)")
        iscd = "0001" if market == "kospi" else "1001"
        try:
            data = _kis_get(
                "/uapi/domestic-stock/v1/ranking/market-cap",
                "FHPST01710000",
                {
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_COND_SCR_DIV_CODE":  "20171",
                    "FID_INPUT_ISCD":         iscd,
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
            for item in data.get("output", []):
                stocks.append({
                    "rank":   item.get("data_rank", ""),
                    "code":   item.get("stck_shrn_iscd", ""),
                    "name":   item.get("hts_kor_isnm", ""),
                    "market": market,
                })
        except Exception as e:
            print(f"  ❌ 한투 API 폴백도 실패: {e}")

    seen, unique = set(), []
    for s in stocks:
        if s["code"] not in seen and s["code"]:
            seen.add(s["code"])
            s["rank"] = str(len(unique) + 1)
            unique.append(s)

    print(f"  [{market_label}] 최종 {len(unique[:top_n])}개 종목 수집 완료")
    return unique[:top_n]


def _parse_ohlcv_items(items: list) -> list:
    """한투 OHLCV 응답 아이템 → 표준 dict 변환"""
    rows = []
    for item in items:
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
    return [r for r in rows if r["close"] > 0]


def _get_ohlcv_kis(code: str, days: int = 260) -> list:
    """한투 API 일별 OHLCV 조회 (종목 전용)"""
    all_rows  = []
    end_dt    = datetime.now()
    CHUNK     = 100

    while len(all_rows) < days:
        end_str   = end_dt.strftime("%Y%m%d")
        start_str = (end_dt - timedelta(days=CHUNK * 2)).strftime("%Y%m%d")

        try:
            data = _kis_get(
                "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                "FHKST03010100",
                {
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD":         code,
                    "FID_INPUT_DATE_1":       start_str,
                    "FID_INPUT_DATE_2":       end_str,
                    "FID_PERIOD_DIV_CODE":    "D",
                    "FID_ORG_ADJ_PRC":        "0",
                }
            )
        except Exception as e:
            print(f"    ⚠️ OHLCV 조회 오류({code}): {e}")
            break

        items = data.get("output2") or data.get("output") or []
        if isinstance(items, dict):
            items = [items]

        chunk_rows = _parse_ohlcv_items(items)
        if not chunk_rows:
            break

        existing_dates = {r["date"] for r in all_rows}
        new_rows = [r for r in chunk_rows if r["date"] not in existing_dates]
        if not new_rows:
            break

        all_rows.extend(new_rows)
        oldest = min(all_rows, key=lambda x: x["date"])["date"]
        end_dt = datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)

        time.sleep(0.05)

    all_rows.sort(key=lambda x: x["date"], reverse=True)
    return all_rows[:days]


# ══════════════════════════════════════════════════════════════
# ── ④ 미너비니 전략 판별 로직 ──
# ══════════════════════════════════════════════════════════════

def _sma(closes: list, n: int):
    return sum(closes[:n]) / n if len(closes) >= n else None


def _check_trend_template(rows: list) -> dict:
    if len(rows) < 180:
        return {"pass": False, "criteria": {}, "detail": {}}

    closes = [r["close"] for r in rows]
    highs  = [r["high"]  for r in rows]
    lows   = [r["low"]   for r in rows]
    price  = closes[0]

    ma50     = _sma(closes, 50)
    ma150    = _sma(closes, 150)
    ma200    = _sma(closes, 200)
    ma200_22 = _sma(closes[22:], 200)

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
    if not stock_rows or not index_rows:
        return None

    index_dict = {r["date"]: r["close"] for r in index_rows}

    def get_stock_ret(rows, p):
        if len(rows) <= p: return 0
        return (rows[0]["close"] / rows[p]["close"] - 1) * 100

    def get_index_ret(rows, p):
        if len(rows) <= p: return 0
        now_date = rows[0]["date"]
        past_date = rows[p]["date"]
        
        now_idx_price = index_dict.get(now_date)
        past_idx_price = index_dict.get(past_date)
        
        if not now_idx_price or not past_idx_price:
            return 0
        return (now_idx_price / past_idx_price - 1) * 100

    periods = [63, 126, 189, 252]
    weights = [0.40, 0.20, 0.20, 0.20]

    s_ret = sum(w * get_stock_ret(stock_rows, p) for w, p in zip(weights, periods))
    i_ret = sum(w * get_index_ret(stock_rows, p) for w, p in zip(weights, periods))

    return round(min(max((s_ret - i_ret + 30) / 60 * 100, 0), 100), 1)


def _detect_vcp(rows: list, lookback: int = 60) -> dict:
    if len(rows) < lookback + 20:
        return {"pass": False, "breakout_vol": False, "detail": {}}

    seg     = rows[:lookback]
    closes  = [r["close"]  for r in seg]
    highs   = [r["high"]   for r in seg]
    lows    = [r["low"]    for r in seg]
    volumes = [r["volume"] for r in seg]

    pivot     = max(highs)
    price_now = closes[0]

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

def run_minervini_scan(market: str = "kospi", top_n: int = 200) -> dict:
    market_label = "코스피" if market == "kospi" else "코스닥"
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n[미너비니 스캐너] {today} {market_label} 스캔 시작 — 상위 {top_n}종목")

    index_rows = _get_index_ohlcv_fdr(market, 260)
    if not index_rows:
        print(f"  ⚠️ {market_label} 지수 조회 실패 — RS 계산 스킵됨")

    stocks = _get_market_top(market, top_n)
    print(f"  종목 목록 수집 완료: {len(stocks)}개")

    tt_pass, vcp_pass, breakout_pass, all_pass = [], [], [], []
    tt_codes = set()

    for i, st in enumerate(stocks):
        code, name = st["code"], st["name"]
        try:
            rows = _get_ohlcv_kis(code, 260)
            if i < 3:
                print(f"  [진단] {name}({code}) OHLCV 수신: {len(rows)}개")

            if len(rows) < 180:
                all_pass.append({
                    **st, "rs": None,
                    "tt_detail": {}, "criteria":  {},
                    "tt_pass":   False, "data_ok":   False,
                })
                time.sleep(0.05)
                continue

            tt = _check_trend_template(rows)
            rs = _calc_rs(rows, index_rows)

            all_pass.append({
                **st, "rs": rs,
                "tt_detail": tt.get("detail", {}),
                "criteria":  tt.get("criteria", {}),
                "tt_pass":   tt["pass"],
                "data_ok":   True,
            })

            if not tt["pass"]:
                time.sleep(0.05)
                continue

            if rs is not None and rs < 70:
                time.sleep(0.05)
                continue

            base = {**st, "rs": rs, "tt_detail": tt["detail"],
                    "criteria": tt["criteria"]}
            tt_pass.append(base)
            tt_codes.add(code)

            vcp = _detect_vcp(rows)
            base["vcp"] = vcp["detail"]
            if vcp["pass"]:
                vcp_pass.append({**base, "vcp_breakout_vol": vcp["breakout_vol"]})

            if _near_52w_high(rows) and _volume_surge(rows):
                breakout_pass.append({**base, "near_high": True, "vol_surge": True})

            if (i + 1) % 20 == 0:
                print(f"  진행: {i+1}/{len(stocks)} | "
                      f"원본:{len(all_pass)} TT:{len(tt_pass)} VCP:{len(vcp_pass)}")
            time.sleep(0.05)

        except Exception as e:
            print(f"  ⚠️ {name}({code}) 오류: {e}")
            all_pass.append({
                **st, "rs": None,
                "tt_detail": {}, "criteria": {},
                "tt_pass": False, "data_ok": False,
            })
            time.sleep(0.1)

    print(f"  완료 — 원본:{len(all_pass)} | TT:{len(tt_pass)} | "
          f"VCP:{len(vcp_pass)} | 신고가+거래량:{len(breakout_pass)}")

    return {
        "date":               today,
        "market":             market,
        "market_label":       market_label,
        "scanned":            len(stocks),
        "trend_template":     tt_pass,
        "vcp":                vcp_pass,
        "near_high_breakout": breakout_pass,
        "all_stocks":         all_pass,
        "tt_codes":           tt_codes,
    }


# ══════════════════════════════════════════════════════════════
# ── ⑥ 미너비니 리포트 생성 + 배포 (모멘텀 인덱스 랩 전용 양식 적용) ──
# ══════════════════════════════════════════════════════════════

def _fmt_n(v) -> str:
    try:    return f"{float(v):,.0f}"
    except: return str(v)

def _get_rs_html(rs_val) -> str:
    """RS 값 서식을 안전하게 처리합니다."""
    if rs_val is None:
        return '<span style="color:#999;">계산불가</span>'
    try:
        rs_float = float(rs_val)
        if rs_float >= 70:
            return f'<span style="color:#c0392b;font-weight:bold;">{rs_float:.1f}</span>'
        else:
            return f'{rs_float:.1f}'
    except (ValueError, TypeError):
        return '<span style="color:#999;">계산불가</span>'


def _build_minervini_html(result: dict) -> tuple:
    """모멘텀 인덱스 랩 블로그 규정에 완벽히 부합하는 포스팅 코드를 생성합니다."""
    date    = result["date"]
    scanned = result["scanned"]
    tt      = result["trend_template"]
    vcp     = result["vcp"]
    nh      = result["near_high_breakout"]

    market_label = result.get("market_label", "코스피")
    post_title = f"📊 미너비니 SEPA 스캐너 [{market_label}] ({date}) — VCP {len(vcp)}종목 · TT {len(tt)}종목"

    all_stocks = sorted(
        result.get("all_stocks", []),
        key=lambda x: int(x.get("rank", 9999)) if str(x.get("rank","")).isdigit() else 9999
    ) or tt

    def _raw_stock_row(s) -> str:
        d       = s.get("tt_detail", {})
        passed  = s.get("tt_pass", False)
        data_ok = s.get("data_ok", bool(d))

        if not data_ok or not d:
            return f"""
          <tr>
            <td>{s.get("rank","—")}</td>
            <td>
              <b>{s["name"]}</b><br>
              <a href="https://finance.naver.com/item/fchart.naver?code={s["code"]}" target="_blank" style="color:#3498db;font-size:0.82em;">{s["code"]}</a>
            </td>
            <td colspan="7" style="color:#ccc;">데이터 부족 (상장 1년 미만 또는 조회 오류)</td>
          </tr>"""

        pct_val   = d.get("pct_from_52w_high", 0) or 0
        pct_color = "#c0392b" if float(pct_val) > -5 else "#2c3e50"
        tt_badge  = '통과' if passed else '미통과'
        rs_html   = _get_rs_html(s.get("rs"))
                   
        return f"""
          <tr>
            <td>{s.get("rank","—")}</td>
            <td>
              <b>{s["name"]}</b><br>
              <a href="https://finance.naver.com/item/fchart.naver?code={s["code"]}" target="_blank" style="color:#3498db;font-size:0.82em;">{s["code"]}</a>
            </td>
            <td>{_fmt_n(d.get("price"))}원</td>
            <td style="color:{pct_color};">{float(pct_val):+.1f}%</td>
            <td>{_fmt_n(d.get("ma50"))}</td>
            <td>{_fmt_n(d.get("ma150"))}</td>
            <td>{_fmt_n(d.get("ma200"))}</td>
            <td>{rs_html}</td>
            <td>{tt_badge}</td>
          </tr>"""

    def stock_rows_html(stocks, show_vcp=False) -> str:
        rows = ""
        for s in stocks:
            d  = s.get("tt_detail", {})
            pct_color = "#c0392b" if float(d.get("pct_from_52w_high", 0)) > -5 else "#2c3e50"
            rs_html = _get_rs_html(s.get('rs'))
            
            # VCP 전용 셀을 show_vcp가 참일 때만 생성
            if show_vcp:
                vd = s.get("vcp", {})
                pb = " → ".join(f"{p}%" for p in (vd.get("pullbacks") or [])[-4:])
                bv = "🔥" if s.get("vcp_breakout_vol") else ""
                vcp_td = f"<td>{pb or '—'} / 마지막 {vd.get('last_pct','—')}%</td><td>{bv}</td>"
            else:
                vcp_td = ""

            rows += f"""
            <tr>
              <td>{s.get('rank','—')}</td>
              <td><b>{s['name']}</b><br>
                <a href="https://finance.naver.com/item/fchart.naver?code={s['code']}" target="_blank" style="color:#3498db;font-size:0.85em;">{s['code']}</a>
              </td>
              <td>{_fmt_n(d.get('price'))}원</td>
              <td style="color:{pct_color};">{d.get('pct_from_52w_high', 0):+.1f}%</td>
              <td>{_fmt_n(d.get('ma50'))}</td>
              <td>{_fmt_n(d.get('ma150'))}</td>
              <td>{_fmt_n(d.get('ma200'))}</td>
              <td>{rs_html}</td>
              {vcp_td}
            </tr>"""
        return rows

    def section_html(heading, stocks, show_vcp=False) -> str:
        if not stocks:
            return f"<h3>{heading}</h3><p style='color:#888'>해당 없음</p>"
        
        # VCP 테이블일 경우에만 추가 헤더를 붙여서 빈칸 방지
        extra_th = "<th>VCP 되돌림</th><th>돌파</th>" if show_vcp else ""
        
        return f"""
        <h3>{heading}</h3>
        <div style="overflow-x:auto;">
        <table class="momentum-table">
          <thead>
            <tr>
              <th>순위</th><th>종목</th><th>현재가</th><th>52주고가대비</th>
              <th>MA50</th><th>MA150</th><th>MA200</th><th>RS</th>{extra_th}
            </tr>
          </thead>
          <tbody>{stock_rows_html(stocks, show_vcp)}</tbody>
        </table>
        </div>"""

    html_content = f"""
    <div class="momentum-container">
        <style>
            .momentum-container {{ max-width:1000px; margin:0 auto; font-family:'Helvetica Neue', Arial, sans-serif; text-align:center; }}
            .momentum-container h3 {{ color:#2c3e50; padding-left:10px; text-align:left; margin-top:30px; margin-bottom:15px; }}
            .content-box {{ border-left:4px solid #ccc; padding:10px 12px 10px 15px; margin-bottom:20px; text-align:left; background:#fafafa; }}
            .momentum-list {{ list-style:none; padding:0; margin:0; font-size:0.9em; color:#666; text-align:left; }}
            .momentum-list li {{ margin-bottom:5px; }}
            .momentum-list li::before {{ content: '▪ '; color:#2c3e50; font-weight:bold; }}
            .momentum-table {{ width:100%; text-align:center; border:1px solid #e0e0e0; border-collapse:collapse; font-size:0.9em; margin-top:15px; margin-bottom:30px; }}
            .momentum-table th {{ background-color:#f8f9fa; color:#2c3e50; padding:10px; border:1px solid #e0e0e0; font-weight:bold; }}
            .momentum-table td {{ padding:10px; border:1px solid #e0e0e0; vertical-align:middle; }}
            .momentum-table td:nth-child(2) {{ text-align:left; }}
            .momentum-table tbody tr:hover {{ background-color:#f1f4f8; transition: background-color 0.2s; }}
        </style>

      <h3 style="margin-top:0;">💡 개요: {market_label} 시장 주도주 발굴 스캐너</h3>
      <div class="content-box">
        <p style="margin:0 0 10px 0; font-size:0.95em; color:#444;">
        미너비니 <b>SEPA(Specific Entry Point Analysis)</b> 전략 기반으로 {market_label} 시가총액 상위 {scanned}종목을 분석합니다.<br>
        Trend Template 조건을 통과한 <b>Stage 2 상승 추세</b> 종목 중 변동성이 수축되는 <b>VCP 패턴</b>을 자동 탐지하여 진입 타이밍 후보를 선별합니다.
        </p>
      </div>
      <h3>📌 미너비니 Trend Template 7+1 조건 & VCP 패턴</h3>
      <div class="content-box">
        <ul class="momentum-list">
          <li>현재가 &gt; MA150, MA200</li>
          <li>MA150 &gt; MA200</li>
          <li>MA200 상승 중 (1개월 이상)</li>
          <li>MA50 &gt; MA150 &gt; MA200 <b>(완전 정배열)</b></li>
          <li>현재가 &gt; MA50</li>
          <li>현재가 ≥ 52주 저가 × 1.30 및 52주 고가 × 1.25 이하</li>
          <li><b>RS 점수 ≥ 70</b></li>
          <li>(VCP 추가) 최소 3회 연속 되돌림 및 타이트한 베이스 (마지막 수축 ≤ 10%)</li>
        </ul>
      </div>

      {section_html(f"🌀 VCP 패턴 감지 종목 ({len(vcp)}개)", vcp, show_vcp=True)}
      {section_html(f"🚀 52주 신고가 근접 + 거래량 급증 ({len(nh)}개)", nh)}
      {section_html(f"📋 Trend Template 통과 전체 ({len(tt)}개)", tt)}

      <h3>📂 전체 분석 원본 데이터 ({len(all_stocks)}종목)</h3>
      <div class="content-box">
        <p style="margin:0; font-size:0.95em; color:#444;">스캔 대상 {market_label} 상위 {result["scanned"]}종목 중 분석을 완료한 전체 종목의 원본 데이터입니다.</p>
      </div>
      <div style="overflow-x:auto;">
      <table class="momentum-table">
        <thead>
          <tr>
            <th>순위</th><th>종목</th><th>현재가</th><th>52주고가대비</th>
            <th>MA50</th><th>MA150</th><th>MA200</th><th>RS</th><th>TT통과</th>
          </tr>
        </thead>
        <tbody>
          {"".join(_raw_stock_row(s) for s in all_stocks)}
        </tbody>
      </table>
      </div>
      
      <p style="text-align:left; font-size:0.8em; color:#888; margin-top:20px;">
        ⚠️ 본 리포트는 장마감 데이터 기반 자동 생성 정보이며, 투자 판단 및 책임은 투자자 본인에게 있습니다.
      </p>
    </div>"""

    return post_title, html_content


def _build_telegram_message(result: dict) -> str:
    date  = result["date"]
    tt    = result["trend_template"]
    vcp   = result["vcp"]
    nh    = result["near_high_breakout"]

    lines = [
        "📊 <b>미너비니 SEPA 스캐너 리포트</b>",
        f"📅 {date}  |  {result.get('market_label','코스피')} 상위 {result['scanned']}종목 분석",
        "",
        f"🌀 VCP 패턴 ({len(vcp)})",
        f"🚀 신고가+거래량 ({len(nh)})",
        f"📋 TrendTemplate ({len(tt)})",
    ]

    if vcp:
        lines.append("")
        lines.append(f"🌀 <b>VCP 패턴 ({len(vcp)}종목)</b>")
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

    if nh:
        lines.append("")
        lines.append(f"🚀 <b>52주 신고가 근접+거래량 ({len(nh)}종목)</b>")
        for s in nh[:8]:
            d = s.get("tt_detail", {})
            lines.append(
                f"  • <b>{s['name']}</b>({s['code']}) "
                f"{_fmt_n(d.get('price'))}원  "
                f"고가대비 {d.get('pct_from_52w_high',0):+.1f}%  "
                f"RS {s.get('rs','—')}"
            )

    return "\n".join(lines)


def _send_telegram(message: str) -> bool:
    token   = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("💡 텔레그램 환경변수 없음 — 전송 스킵")
        return False
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
    post_title, html_content = _build_minervini_html(result)
    post_to_blogger(post_title, html_content, labels=["미너비니"])
    _send_telegram(_build_telegram_message(result))
    _post_tistory(post_title, html_content)


# ══════════════════════════════════════════════════════════════
# ── 메인 실행 ──
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":

    print("=" * 60)
    print("[1/2] ETF 상대강도 분석 시작")
    print("=" * 60)
    equity_df = get_equity_etfs()
    rs_df, bm_1m, bm_3m, bm_1y = calculate_minervini_rs(equity_df)
    export_data(rs_df, bm_1m, bm_3m, bm_1y)
    print("✅ ETF 분석 완료")

    print("\n" + "=" * 60)
    print("[2/3] 미너비니 SEPA 종목 스캐너 시작")
    print("=" * 60)

    if not os.environ.get("KIS_APP_KEY") or not os.environ.get("KIS_APP_SECRET"):
        print("💡 KIS_APP_KEY / KIS_APP_SECRET 없음 — 미너비니 스캐너 건너뜀")
    else:
        # ※ 휴장일 체크 로직 주석 처리(테스트용)
        # if not _is_market_open_today():
        #     print("💡 오늘은 장 휴장일 — 미너비니 스캐너 건너뜀")
        # else:

        print("\n" + "-" * 40)
        print("[2/3] 코스피 상위 200종목 스캔")
        print("-" * 40)
        try:
            kospi_result = run_minervini_scan(market="kospi", top_n=200)
            publish_minervini_report(kospi_result)
            print("✅ 코스피 스캔 완료")
        except Exception as e:
            print(f"❌ 코스피 스캐너 오류: {e}")

        print("\n" + "-" * 40)
        print("[3/3] 코스닥 상위 200종목 스캔")
        print("-" * 40)
        try:
            kosdaq_result = run_minervini_scan(market="kosdaq", top_n=200)
            publish_minervini_report(kosdaq_result)
            print("✅ 코스닥 스캔 완료")
        except Exception as e:
            print(f"❌ 코스닥 스캐너 오류: {e}")
