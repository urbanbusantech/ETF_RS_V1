import requests
import pandas as pd
import json
import os
import base64
from datetime import datetime, timedelta, timezone
import FinanceDataReader as fdr
import time

# 구글 인증 라이브러리
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ──────────────────────────────────────────────────────────────────
# [수정 1] 토큰 자동 갱신 + GitHub Secret 자동 업데이트 함수
# 기존: Credentials만 생성하고 만료 여부 체크 없음 → invalid_grant 에러 반복 발생
# 개선: 만료 시 refresh_token으로 자동 갱신, 갱신된 토큰을 GitHub Secret에 즉시 저장
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

    # [핵심] token=None으로 설정 → 즉시 만료 상태로 인식하여 refresh 강제 실행
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret,
        scopes=['https://www.googleapis.com/auth/blogger']
    )

    # Access Token 갱신 시도
    try:
        creds.refresh(Request())
        print("✅ Access Token 자동 갱신 완료")

        # [수정 2] 갱신된 새 Refresh Token이 발급된 경우 GitHub Secret 자동 업데이트
        # Google은 경우에 따라 refresh 시 새 refresh_token을 발급할 수 있음
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
    필요한 Secret: GH_PAT (repo 권한을 가진 Personal Access Token)
    """
    gh_pat = os.environ.get('GH_PAT')
    repo   = os.environ.get('GITHUB_REPOSITORY')  # GitHub Actions에서 자동 주입

    if not gh_pat or not repo:
        print("⚠️  GH_PAT 또는 GITHUB_REPOSITORY 환경변수가 없어 Secret 자동 업데이트를 건너뜁니다.")
        return

    headers = {
        'Authorization': f'token {gh_pat}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    # Step 1. 저장소 공개키 조회 (Secret 암호화에 필요)
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

    # Step 2. PyNaCl로 Secret 값 암호화
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

    # Step 3. Secret 업데이트 요청
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
# 이하 기존 비즈니스 로직 (변경 없음)
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
# [수정 3] post_to_blogger: 인증을 get_blogger_credentials()로 위임
# 기존: 함수 내부에서 직접 Credentials 생성 (만료 체크 없음)
# 개선: 별도 함수에서 토큰 갱신까지 완료된 creds 객체를 받아 사용
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

    html_content = f"""
    <div class="etf-container" style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; width: 100%; max-width: 1000px; margin: 0 auto 30px auto; padding: 0 10px; box-sizing: border-box;">
        <style>
            .etf-container h3 {{ color: #2c3e50; padding-left: 10px; margin-top: 25px; margin-bottom: 10px; }}
            .etf-container .content-block {{ padding: 10px 12px 10px 15px; border-left: 4px solid #ccc; margin-bottom: 20px; }}
            .etf-container .table-section {{ border-left: none; padding: 0; margin-top: 15px; width: 100%; overflow-x: auto; }}
            .etf-container .content-block p {{ font-size: 0.95em; color: #444; margin: 0 0 10px 0; }}
            .technical-list {{ list-style: none; padding: 0; margin: 0; font-size: 0.9em; color: #666; }}
            .technical-list li {{ margin-bottom: 5px; }}
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
