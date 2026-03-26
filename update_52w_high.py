import pandas as pd
import numpy as np
import os
import urllib.parse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import io
import base64

# 차트 및 시각화 라이브러리
import mplfinance as mpf
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt

# 구글 API 라이브러리
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# 한국투자증권 API 공통 모듈 (동일 폴더에 위치해야 함)
import KIS_Common as kc

# 환경 변수 설정 (GitHub Secrets 또는 로컬 환경 변수)
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
GID = "0" 

def get_chart_html_block(ticker, name):
    """종목별 기술적 지표가 포함된 차트를 생성하여 HTML 이미지 태그로 반환"""
    print(f"📊 {name}({ticker}) 차트 생성 중...")
    
    # 1. 주가 데이터 가져오기 (최근 400일)
    df = kc.GetOhlcv(area="KR", stock_code=ticker, limit=400, adj_ok="1")
    if df is None or df.empty or len(df) < 260:
        return ""

    close = df['close']
    high = df['high']
    low = df['low']

    # 2. 기술적 지표 계산
    sma_20 = close.rolling(window=20).mean()
    sma_50 = close.rolling(window=50).mean()
    sma_150 = close.rolling(window=150).mean()
    sma_200 = close.rolling(window=200).mean()
    # 일목균형표 기준선 (26일간 최고+최저 / 2)
    kijun_sen = (high.rolling(window=26).max() + low.rolling(window=26).min()) / 2
    
    # 52주 최고/최저가 (차트 표시용)
    high_52w = close.iloc[-260:].max()
    low_52w = close.iloc[-260:].min()

    # 3. 차트용 데이터 슬라이싱 (최근 1년 260일)
    plot_df = df.iloc[-260:].copy()
    plot_df.columns = [col.capitalize() for col in plot_df.columns]
    plot_df.index = pd.to_datetime(plot_df.index)
    
    # 추가 지표 매핑
    plot_df['SMA20'] = sma_20.iloc[-260:].values
    plot_df['SMA50'] = sma_50.iloc[-260:].values
    plot_df['SMA150'] = sma_150.iloc[-260:].values
    plot_df['SMA200'] = sma_200.iloc[-260:].values
    plot_df['Kijun'] = kijun_sen.iloc[-260:].values
    plot_df['H52'] = float(high_52w)
    plot_df['L52'] = float(low_52w)

    # 4. 차트 레이아웃 구성
    ap = [
        mpf.make_addplot(plot_df['Kijun'], color='brown', width=1.5),         # 일목 기준선 (갈색)
        mpf.make_addplot(plot_df['SMA20'], color='gold', linestyle='--', width=1.0), # 20일선 (노란점선)
        mpf.make_addplot(plot_df['SMA50'], color='green', width=1.2), 
        mpf.make_addplot(plot_df['SMA150'], color='blue', width=1.2), 
        mpf.make_addplot(plot_df['SMA200'], color='red', width=1.8),
        mpf.make_addplot(plot_df['H52'], color='blue', linestyle=':', width=0.8),    # 52주 최고 (파란점선)
        mpf.make_addplot(plot_df['L52'], color='red', linestyle=':', width=0.8),     # 52주 최저 (빨간점선)
    ]

    mc = mpf.make_marketcolors(up='red', down='blue', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)
    
    # 5. 이미지 바이너리 변환
    buf = io.BytesIO()
    fig, axlist = mpf.plot(plot_df, type='candle', addplot=ap, style=s, returnfig=True, figsize=(10, 5))
    axlist[0].yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))
    
    # 여백 최적화 (쏠림 방지 및 블로그 가독성)
    fig.subplots_adjust(left=0.05, right=0.92, top=0.95, bottom=0.10)
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig) # 메모리 해제

    return f"""
    <div style="margin-top: 40px; border: 1px solid #eee; padding: 20px; border-radius: 12px; background-color: #fff; box-shadow: 0 2px 10px rgba(0,0,0,0.05);">
        <h4 style="margin: 0 0 15px 0; color: #2c3e50; border-left: 5px solid #3498db; padding-left: 10px;">📈 {name} ({ticker}) 상세 차트 분석</h4>
        <img src="data:image/png;base64,{img_str}" style="width: 100%; height: auto; display: block; border-radius: 6px;" alt="{name} 차트">
        <p style="font-size: 0.85em; color: #888; text-align: right; margin-top: 10px;">* 노란점선(20일), 갈색(일목기준선), 초록/파랑/빨강(50/150/200일선)</p>
    </div>
    """

def get_data_from_google_sheet():
    """공개된 구글 스프레드시트에서 돌파 종목 리스트 추출"""
    print("📌 구글 스프레드시트 DB 로드 중...")
    if not GOOGLE_SHEET_ID: return pd.DataFrame()
    csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GID}"
    
    try:
        df = pd.read_csv(csv_url, encoding='utf-8')
        if '상태' in df.columns:
            df = df[df['상태'] == '🚀돌파'].copy()
        
        col_code = '종목코드(정제)' if '종목코드(정제)' in df.columns else '종목코드'
        extract_cols = [col_code, '회사명', '현재가', '52주최고가', '거래량']
        if '업종' in df.columns: extract_cols.insert(2, '업종')
            
        result_df = df[extract_cols].copy()
        result_df.columns = ['종목코드', '종목명', '업종', '현재가(원)', '52주 최고가', '거래량'] if '업종' in df.columns else ['종목코드', '종목명', '현재가(원)', '52주 최고가', '거래량']
        
        # 필터링 및 정렬
        exclude_pattern = '스팩|리츠|제[0-9]+호'
        result_df = result_df[~result_df['종목명'].str.contains(exclude_pattern, regex=True, na=False)]
        result_df['정렬용_거래량'] = pd.to_numeric(result_df['거래량'].astype(str).str.replace(',', '').str.replace('₩', '').str.strip(), errors='coerce').fillna(0)
        result_df = result_df.sort_values(by='정렬용_거래량', ascending=False).drop(columns=['정렬용_거래량'])
        result_df['종목코드'] = result_df['종목코드'].apply(lambda x: str(x).replace('.0', '').zfill(6))
        
        return result_df
    except Exception as e:
        print(f"❌ 시트 오류: {e}"); return pd.DataFrame()

def get_stock_news(stock_name, limit=3):
    """구글 뉴스 RSS를 통한 특징주 뉴스 수집"""
    query = urllib.parse.quote(f'"{stock_name}" (주가 OR 특징주 OR 실적)')
    url = f"https://news.google.com/rss/search?q={query}+when:2d&hl=ko&gl=KR&ceid=KR:ko"
    news_list = []
    try:
        res = requests.get(url, timeout=5)
        root = ET.fromstring(res.text)
        for item in root.findall('.//item')[:limit]:
            title = item.find('title').text.rsplit(' - ', 1)[0]
            news_list.append({'title': title, 'link': item.find('link').text, 'source': item.find('source').text})
    except: pass
    return news_list

def post_to_blogger(title, html_content, labels=None):
    """구글 블로거 API를 이용한 자동 포스팅"""
    blog_id = os.environ.get('BLOGGER_BLOG_ID')
    client_id = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([blog_id, client_id, client_secret, refresh_token]):
        print("💡 API 정보 부족으로 포스팅을 건너뜁니다."); return

    try:
        creds = Credentials(token=None, refresh_token=refresh_token, token_uri='https://oauth2.googleapis.com/token', client_id=client_id, client_secret=client_secret)
        service = build('blogger', 'v3', credentials=creds)
        body = {"kind": "blogger#post", "title": title, "content": html_content}
        if labels: body["labels"] = labels
        service.posts().insert(blogId=blog_id, body=body, isDraft=False).execute()
        print("✅ 블로그 포스팅 완료!")
    except Exception as e: print(f"❌ 포스팅 실패: {e}")

def generate_market_summary(df):
    """시장 요약 브리핑 텍스트 생성"""
    if df.empty: return "금일은 52주 신고가 필터에 포착된 주도주가 없습니다."
    top_vol = df.iloc[0]['종목명']
    total = len(df)
    sector_info = "개별 종목 장세"
    if '업종' in df.columns:
        counts = df['업종'].value_counts()
        if not counts.empty:
            sector_info = f"<strong>[{counts.index[0]}]</strong> 섹터"
            if len(counts) > 1: sector_info += f" 및 <strong>[{counts.index[1]}]</strong> 섹터"
    
    return f"오늘 52주 신고가 필터를 통과한 주도주는 총 <strong>{total}개</strong>입니다. 특히 {sector_info}에 자금이 집중되었으며, 거래량 대장은 <strong>{top_vol}</strong>입니다."

def generate_html_report(df):
    """전체 HTML 포스트 본문 조립"""
    kst = timezone(timedelta(hours=9))
    today_str = datetime.now(kst).strftime('%Y-%m-%d')
    current_time = datetime.now(kst).strftime('%Y-%m-%d %H:%M')
    
    briefing = generate_market_summary(df)
    
    # 대장주 뉴스 섹션
    news_html = ""
    for stock in df['종목명'].iloc[:2]:
        news_items = get_stock_news(stock)
        news_html += f"<h4 style='color: #d35400; margin-top: 15px;'>🔥 특징주 브리핑: {stock}</h4><ul class='news-list'>"
        if news_items:
            for n in news_items: news_html += f"<li><a href='{n['link']}' target='_blank'>[{n['source']}] {n['title']}</a></li>"
        else: news_html += "<li>최근 2일 내 주요 뉴스가 없습니다.</li>"
        news_html += "</ul>"

    # 테이블 정제
    table_df = df.copy()
    for col in ['현재가(원)', '52주 최고가', '거래량']:
        table_df[col] = table_df[col].apply(lambda x: f"{int(float(str(x).replace('₩','').replace(',',''))):,}" if pd.notnull(x) else "-")
    
    table_df['종목코드'] = table_df['종목코드'].apply(lambda x: f'<a href="https://finance.naver.com/item/main.naver?code={x}" target="_blank" style="color:#3498db; font-weight:bold;">{x}</a>')
    display_table = table_df.drop(columns=['업종']) if '업종' in table_df.columns else table_df
    table_html = display_table.to_html(index=False, escape=False, border=0, classes='momentum-table')

    # 🚀 핵심: 종목별 차트 HTML 생성
    chart_html_list = ""
    for _, row in df.iterrows():
        chart_html_list += get_chart_html_block(row['종목코드'], row['종목명'])

    # 최종 조립
    html_content = f"""
    <div style="width:100%; max-width:900px; margin:0 auto; font-family:'Malgun Gothic', sans-serif; line-height:1.6;">
        <style>
            .momentum-table {{ width:100%; border-collapse:collapse; margin-top:20px; font-size:14px; text-align:center; }}
            .momentum-table th {{ background:#f8f9fa; padding:12px; border:1px solid #dee2e6; }}
            .momentum-table td {{ padding:10px; border:1px solid #dee2e6; }}
            .news-list {{ list-style:none; padding:0; font-size:14px; }}
            .news-list li {{ margin-bottom:6px; }}
            .news-list a {{ color:#2980b9; text-decoration:none; }}
        </style>
        
        <h2 style="color:#2c3e50;">🚀 오늘의 52주 신고가 주도주 분석</h2>
        <div style="background:#f0f7ff; padding:20px; border-left:5px solid #3498db; margin-bottom:25px;">
            {briefing}
        </div> <h3 style="border-bottom:2px solid #eee; padding-bottom:10px;">📊 모멘텀 돌파 종목 리스트</h3>
        {table_html}

        <h3 style="border-bottom:2px solid #eee; padding-bottom:10px; margin-top:40px;">📰 주도주 주요 이슈</h3>
        {news_html}

        <h3 style="border-bottom:2px solid #eee; padding-bottom:10px; margin-top:40px; text-align:center;">🔍 기술적 분석 차트</h3>
        {chart_html_list}
        
        <p style="margin-top:50px; color:#999; font-size:12px; text-align:center;">분석 기준 시간: {current_time} | 본 리포트는 투자 참고용이며 최종 결정은 본인에게 있습니다.</p>
    </div>
    """
    
    post_to_blogger(f"🚀 [주도주] 52주 신고가 돌파 종목 분석 ({today_str})", html_content, labels=["주식분석", "52주신고가"])

if __name__ == "__main__":
    df = get_data_from_google_sheet()
    if not df.empty:
        generate_html_report(df)
    else:
        print("⚠️ 돌파 종목이 없어 종료합니다.")
