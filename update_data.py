import requests
import pandas as pd
import json
import os
import base64
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import FinanceDataReader as fdr
import time

# 구글 인증 라이브러리
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ──────────────────────────────────────────────────────────────────
# 토큰 자동 갱신 + GitHub Secret 자동 업데이트 함수
# ──────────────────────────────────────────────────────────────────
def get_blogger_credentials():
    """
    환경변수에서 Blogger OAuth 인증 정보를 읽어 Credentials 객체를 반환합니다.
    Access Token이 만료된 경우 Refresh Token으로 자동 갱신하고,
    갱신된 최신 토큰을 GitHub Secret에 자동 저장합니다.
    """
    client_id     = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([client_id, client_secret, refresh_token]):
        print("💡 Blogger API 인증 정보가 없어 포스팅을 건너뜁니다.")
        return None

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret,
        scopes=['https://www.googleapis.com/auth/blogger']
    )

    try:
        creds.refresh(Request())
        print("✅ Access Token 자동 갱신 완료")

        if creds.refresh_token and creds.refresh_token != refresh_token:
            print("🔄 새 Refresh Token 감지 → GitHub Secret 자동 업데이트 시도")
            update_github_secret('BLOGGER_REFRESH_TOKEN', creds.refresh_token)

    except Exception as e:
        print(f"❌ Token 갱신 실패: {e}")
        print("👉 해결방법: 아래 '토큰 재발급 가이드'를 참고하여 BLOGGER_REFRESH_TOKEN을 새로 발급하세요.")
        return None

    return creds


def update_github_secret(secret_name: str, secret_value: str):
    """
    GitHub API를 통해 Repository Secret을 자동 업데이트합니다.
    """
    gh_pat = os.environ.get('GH_PAT')
    repo   = os.environ.get('GITHUB_REPOSITORY') 

    if not gh_pat or not repo:
        print("⚠️  GH_PAT 또는 GITHUB_REPOSITORY 환경변수가 없어 Secret 자동 업데이트를 건너뜁니다.")
        return

    headers = {
        'Authorization': f'token {gh_pat}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    key_resp = requests.get(
        f'https://api.github.com/repos/{repo}/actions/secrets/public-key',
        headers=headers
    )
    if key_resp.status_code != 200:
        print(f"⚠️  GitHub 공개키 조회 실패: {key_resp.status_code}")
        return

    key_data   = key_resp.json()
    public_key = key_data['key']
    key_id     = key_data['key_id']

    try:
        from nacl import encoding, public as nacl_public
        pk  = nacl_public.PublicKey(public_key.encode(), encoding.Base64Encoder())
        box = nacl_public.SealedBox(pk)
        encrypted = base64.b64encode(
            box.encrypt(secret_value.encode('utf-8'))
        ).decode('utf-8')
    except ImportError:
        print("⚠️  PyNaCl 미설치. requirements.txt에 'PyNaCl' 추가 후 재실행하세요.")
        return

    put_resp = requests.put(
        f'https://api.github.com/repos/{repo}/actions/secrets/{secret_name}',
        headers=headers,
        json={'encrypted_value': encrypted, 'key_id': key_id}
    )
    if put_resp.status_code in [201, 204]:
        print(f"✅ GitHub Secret '{secret_name}' 자동 업데이트 완료")
    else:
        print(f"⚠️  GitHub Secret 업데이트 실패: {put_resp.status_code} - {put_resp.text}")


# ──────────────────────────────────────────────────────────────────
# 데이터 수집 및 계산
# ──────────────────────────────────────────────────────────────────
def get_equity_etfs():
    """네이버 금융 API를 통해 국내 상장 주식형 ETF 목록을 수집합니다."""
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df = pd.DataFrame(data['result']['etfItemList'])

    target_codes = [1, 2, 4]
    equity_df = df[df['etfTabCode'].isin(target_codes)].copy()

    exclude_keywords = ['채권', '국고채', '금리', '원유', '골드', '금선물', '은선물', '달러', '인버스', '레버리지', 'TR']
    pattern = '|'.join(exclude_keywords)
    equity_df = equity_df[~equity_df['itemname'].str.contains(pattern)]

    equity_df = equity_df[['itemcode', 'itemname', 'nowVal', 'quant']]
    return equity_df


def calculate_minervini_rs(equity_df):
    """최근 3, 6, 9, 12개월 수익률에 가중치를 부여하여 상대강도를 계산합니다."""
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
        benchmark_1m_ret, benchmark_3m_ret, benchmark_1y_ret = 0, 0, 0

    scores = []
    codes  = equity_df['itemcode'].tolist()

    for i, code in enumerate(codes):
        if i % 50 == 0 and i > 0:
            time.sleep(0.5)

        try:
            df_hist = fdr.DataReader(code, start_date, end_date)
            if len(df_hist) < 240:
                scores.append({'itemcode': code, 'weighted_return': None, '1m_ret': None, '3m_ret': None, '1y_ret': None})
                continue

            close = df_hist['Close']
            p0   = float(close.iloc[-1])
            p63  = float(close.iloc[-63])
            p126 = float(close.iloc[-126])
            p189 = float(close.iloc[-189])
            p240 = float(close.iloc[-240])

            weighted_ret = (
                (p0/p63  - 1) * 0.4 +
                (p0/p126 - 1) * 0.2 +
                (p0/p189 - 1) * 0.2 +
                (p0/p240 - 1) * 0.2
            )

            scores.append({
                'itemcode': code,
                'weighted_return': weighted_ret,
                '1m_ret': (p0 / float(close.iloc[-21])) - 1,
                '3m_ret': (p0 / p63) - 1,
                '1y_ret': (p0 / p240) - 1
            })
        except Exception:
            scores.append({'itemcode': code, 'weighted_return': None, '1m_ret': None, '3m_ret': None, '1y_ret': None})

    scores_df   = pd.DataFrame(scores)
    valid_scores = scores_df.dropna(subset=['weighted_return']).copy()
    valid_scores['RS_Rating'] = valid_scores['weighted_return'].rank(pct=True) * 99
    valid_scores['RS_Rating'] = valid_scores['RS_Rating'].apply(lambda x: int(round(x)))

    result_df = pd.merge(equity_df, valid_scores[['itemcode', '1m_ret', '3m_ret', '1y_ret', 'RS_Rating']], on='itemcode', how='inner')
    result_df = result_df.sort_values(by='RS_Rating', ascending=False)
    result_df.columns = ['종목코드', '종목명', '현재가(원)', '거래량', '1개월', '3개월', '1년', '상대강도']

    return result_df, benchmark_1m_ret, benchmark_3m_ret, benchmark_1y_ret

# ──────────────────────────────────────────────────────────────────
# [신규] ETF 뉴스 검색 함수 추가
# ──────────────────────────────────────────────────────────────────
def get_etf_news(etf_name, limit=3):
    """구글 뉴스 RSS를 활용하여 특정 ETF의 최신 주요 뉴스를 수집합니다."""
    # ETF 이름과 함께 주가, 수익률, 특징주 등의 키워드를 조합하여 검색 정확도 상향
    query_str = f'"{etf_name}" (주가 OR 특징주 OR 수익률 OR 전망)'
    query = urllib.parse.quote(query_str)
    # 최근 2일 이내의 한국어 뉴스 검색
    url = f"https://news.google.com/rss/search?q={query}+when:2d&hl=ko&gl=KR&ceid=KR:ko"
    
    news_list = []
    try:
        res = requests.get(url, timeout=5)
        root = ET.fromstring(res.text)
        
        for item in root.findall('.//item')[:limit]:
            title = item.find('title').text
            link = item.find('link').text
            source_tag = item.find('source')
            source = source_tag.text if source_tag is not None else "관련뉴스"
            
            clean_title = title.rsplit(' - ', 1)[0]
            news_list.append({'title': clean_title, 'link': link, 'source': source})
            
    except Exception as e:
        print(f"⚠️ {etf_name} 뉴스 수집 중 오류: {e}")
        
    return news_list


# ──────────────────────────────────────────────────────────────────
# 블로그 포스팅 및 HTML 생성
# ──────────────────────────────────────────────────────────────────
def post_to_blogger(title, html_content, labels=None):
    """Blogger API를 사용하여 글을 게시합니다."""
    blog_id = os.environ.get('BLOGGER_BLOG_ID')

    if not blog_id:
        print("💡 BLOGGER_BLOG_ID가 없어 포스팅을 건너뜁니다.")
        return

    creds = get_blogger_credentials()
    if creds is None:
        return

    try:
        service = build('blogger', 'v3', credentials=creds)

        body = {
            "kind": "blogger#post",
            "title": title,
            "content": html_content
        }
        if labels:
            body["labels"] = labels

        service.posts().insert(blogId=blog_id, body=body, isDraft=False).execute()
        print("✅ 구글 블로그 포스팅 성공!")
    except Exception as e:
        print(f"❌ 구글 블로그 포스팅 실패: {e}")


def export_data(df, bm_1m, bm_3m, bm_1y):
    """데이터 가공 및 SEO 최적화된 HTML 리포트를 생성합니다."""
    df.to_csv('etf_data.csv', index=False, encoding='utf-8-sig')

    html_df = df.copy()
    html_df['현재가(원)'] = html_df['현재가(원)'].apply(lambda x: f"{x:,}")
    html_df['거래량']     = html_df['거래량'].apply(lambda x: f"{x:,}")
    html_df['1개월']      = (html_df['1개월'] * 100).round(2).astype(str) + '%'
    html_df['3개월']      = (html_df['3개월'] * 100).round(2).astype(str) + '%'
    html_df['1년']        = (html_df['1년']   * 100).round(2).astype(str) + '%'

    html_df['종목코드'] = html_df['종목코드'].apply(
        lambda x: f'<a href="https://finance.naver.com/item/fchart.naver?code={x}" target="_blank" style="color: #3498db; text-decoration: none; font-weight: bold;">{x}</a>'
    )
    html_df['상대강도'] = html_df['상대강도'].apply(
        lambda x: f'<span style="color: #c0392b; font-weight: bold;">{x}</span>' if x >= 80 else str(x)
    )

    kst          = timezone(timedelta(hours=9))
    now_kst      = datetime.now(kst)
    today_date   = now_kst.strftime('%Y-%m-%d')
    current_time = now_kst.strftime('%Y-%m-%d %H:%M')

    table_html = html_df.to_html(index=False, classes='etf-table', border=0, escape=False, justify='center')
    post_title  = f"🚀 주식형 ETF 상대강도 모멘텀 랭킹({today_date})"

    # ──────────────────────────────────────────────────────────────────
    # [신규] 주도 ETF Top 2에 대한 뉴스 수집 로직
    # ──────────────────────────────────────────────────────────────────
    news_html_blocks = ""
    if not html_df.empty:
        # 상대강도 기준 최상위 2개 ETF 추출
        top_etfs = html_df['종목명'].iloc[:2].tolist()
        print(f"📰 {', '.join(top_etfs)} ETF의 관련 뉴스를 검색합니다...")
        
        for etf in top_etfs:
            news_items = get_etf_news(etf, limit=3)
            if news_items:
                news_html_blocks += f"<h4 style='color: #d35400; margin-bottom: 5px; margin-top: 15px;'>🔥 주도 테마: {etf}</h4>\n"
                news_html_blocks += "<ul class='news-list'>\n"
                for item in news_items:
                    news_html_blocks += f"  <li><a href='{item['link']}' target='_blank'>[{item['source']}] {item['title']}</a></li>\n"
                news_html_blocks += "</ul>\n"
            else:
                news_html_blocks += f"<h4 style='color: #d35400; margin-bottom: 5px; margin-top: 15px;'>🔥 주도 테마: {etf}</h4>\n"
                news_html_blocks += "<ul class='news-list'><li>최근 2일 내 눈에 띄는 관련 뉴스가 없습니다.</li></ul>\n"

    # [수정] HTML 템플릿에 뉴스 CSS와 뉴스 박스 섹션 추가
    html_content = f"""
    <div class="etf-container" style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; width: 100%; max-width: 1000px; margin: 0 auto 30px auto; padding: 0 10px; box-sizing: border-box;">
        <style>
            .etf-container h3 {{ color: #2c3e50; padding-left: 10px; margin-top: 25px; margin-bottom: 10px; }}
            .etf-container .content-block {{ padding: 10px 12px 10px 15px; border-left: 4px solid #ccc; margin-bottom: 20px; }}
            .etf-container .table-section {{ border-left: none; padding: 0; margin-top: 15px; width: 100%; overflow-x: auto; }}
            .etf-container .content-block p {{ font-size: 0.95em; color: #444; margin: 0 0 10px 0; }}
            
            /* 뉴스 전용 CSS 추가 */
            .news-box {{ border-left: 4px solid #e67e22; padding: 15px; margin-bottom: 20px; background-color: #fff9f2; text-align: left; font-size: 0.95em; line-height: 1.6; }}
            ul.news-list, .technical-list {{ list-style: none; padding: 0; margin: 0; font-size: 0.9em; color: #666; }}
            ul.news-list li, .technical-list li {{ margin-bottom: 5px; }}
            ul.news-list li a {{ color: #2980b9; text-decoration: none; }}
            ul.news-list li a:hover {{ text-decoration: underline; color: #1a5276; }}

            .etf-table {{ width: 100%; max-width: 100%; border-collapse: collapse; background-color: #ffffff; font-size: 0.9em; border: 1px solid #e0e0e0; margin: 0 auto; }}
            .etf-table th {{ background-color: #f8f9fa; color: #2c3e50; font-weight: 600; padding: 10px; border: 1px solid #e0e0e0; }}
            .etf-table td {{ padding: 10px; border: 1px solid #e0e0e0; text-align: center; vertical-align: middle; }}
            .etf-table td:nth-child(2) {{ text-align: left; }}
            .etf-table tr:hover {{ background-color: #f1f4f8; }}
        </style>

        <h3>💡 개요: 시장 주도주를 찾는 모멘텀 분석</h3>
        <div class="content-block">
            <p>본 리포트는 대한민국 상장 주식형 ETF 중 현재 가장 강력한 상승 에너지를 보여주는 종목을 선별합니다. 단순 가격 상승률을 넘어 시장 대비 초과 수익을 내는 '진짜 주도주'를 확인하여 전략적 자산배분의 기초 자료로 활용해 보세요.</p>
        </div> <!--more-->
        
        <h3>📰 주도 ETF 관련 주요 뉴스 (Top 2)</h3>
        <div class="news-box">
            <p style="margin-top: 0; color: #555; font-size: 0.9em;">상대강도 기준 오늘 가장 돋보인 대장 ETF 2종목의 시장 이슈를 확인해 보세요.</p>
            {news_html_blocks}
        </div>

        <h3>📈 상대강도(Relative Strength)란?</h3>
        <div class="content-block">
            <p>마크 미너비니의 <b>RS Rating</b>은 특정 종목의 퍼포먼스를 시장 전체와 비교하여 1~99점으로 환산한 지표입니다.</p>
            <p>- <b>계산 방식:</b> 최근 <b>3개월(40%)</b>, 6개월(20%), 9개월(20%), 12개월(20%) 가중치를 부여하여 최신 트렌드를 강조합니다.<br>
               - <b>해석 방법:</b> 점수가 80점 이상인 종목은 현재 시장 상위 20% 이내의 주도주군임을 의미합니다.</p>
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


if __name__ == "__main__":
    equity_df = get_equity_etfs()
    rs_df, bm_1m, bm_3m, bm_1y = calculate_minervini_rs(equity_df)
    export_data(rs_df, bm_1m, bm_3m, bm_1y)
