import pandas as pd
import numpy as np
import os
import urllib.parse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import io
import base64

import mplfinance as mpf
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import KIS_Common as kc

# 환경 변수 (GitHub Secrets 활용)
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
BLOG_ID = os.environ.get('BLOGGER_BLOG_ID')
CLIENT_ID = os.environ.get('BLOGGER_CLIENT_ID')
CLIENT_SECRET = os.environ.get('BLOGGER_CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('BLOGGER_REFRESH_TOKEN')

def get_chart_base64(ticker, name):
    """지표(20/50/150/200, 일목기준선)가 포함된 차트 생성"""
    df = kc.GetOhlcv(area="KR", stock_code=ticker, limit=400, adj_ok="1")
    if df is None or df.empty or len(df) < 260: return None

    close, high, low = df['close'], df['high'], df['low']
    
    # 지표 계산
    sma_20 = close.rolling(window=20).mean()
    sma_50 = close.rolling(window=50).mean()
    sma_150 = close.rolling(window=150).mean()
    sma_200 = close.rolling(window=200).mean()
    kijun = (high.rolling(window=26).max() + low.rolling(window=26).min()) / 2
    h52, l52 = close.iloc[-260:].max(), close.iloc[-260:].min()

    plot_df = df.iloc[-260:].copy()
    plot_df.columns = [col.capitalize() for col in plot_df.columns]
    plot_df.index = pd.to_datetime(plot_df.index)
    
    ap = [
        mpf.make_addplot(kijun.iloc[-260:], color='brown', width=1.5),
        mpf.make_addplot(sma_20.iloc[-260:], color='gold', linestyle='--', width=1.0),
        mpf.make_addplot(sma_50.iloc[-260:], color='green', width=1.2),
        mpf.make_addplot(sma_150.iloc[-260:], color='blue', width=1.2),
        mpf.make_addplot(sma_200.iloc[-260:], color='red', width=1.8),
        mpf.make_addplot(pd.Series(h52, index=plot_df.index), color='blue', linestyle=':', width=0.8),
        mpf.make_addplot(pd.Series(l52, index=plot_df.index), color='red', linestyle=':', width=0.8),
    ]

    buf = io.BytesIO()
    fig, axlist = mpf.plot(plot_df, type='candle', addplot=ap, style='charles', returnfig=True, figsize=(10, 5))
    axlist[0].yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))
    
    # 📌 수정 완료: dpi=60 설정으로 이미지 용량을 줄여 API 400 에러(용량초과) 방지
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=60)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def calculate_rs_score(close):
    """상대강도(RS) 계산 로직"""
    returns = close.pct_change().dropna()
    avg_gain = returns[returns >= 0].mean()
    avg_loss = abs(returns[returns < 0].mean()) + 1e-10
    return avg_gain / avg_loss

def post_to_blogger(title, content, labels):
    if not all([BLOG_ID, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]): return
    creds = Credentials(None, refresh_token=REFRESH_TOKEN, token_uri='https://oauth2.googleapis.com/token', client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    service = build('blogger', 'v3', credentials=creds)
    
    # 📌 수정 완료: API 규격에 맞게 kind 와 isDraft 명시
    body = {
        "kind": "blogger#post",
        "title": title,
        "content": content,
        "labels": labels
    }
    service.posts().insert(blogId=BLOG_ID, body=body, isDraft=False).execute()

def process_market(market_label, df_market, index_rs_val):
    """시장별 필터링 및 포스팅 실행"""
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).strftime('%Y-%m-%d')
    passed_list = []

    print(f"🔍 {market_label} 시장 분석 시작 (RS 100점 이상 대상)...")

    for _, row in df_market.iterrows():
        ticker = str(row['종목코드']).zfill(6)
        name = row['회사명']
        df = kc.GetOhlcv("KR", ticker, limit=400, adj_ok="1")
        if df is None or len(df) < 260: continue
        
        c = df['close']
        # RS 점수 계산
        rs_score = (calculate_rs_score(c) / index_rs_val) * 100
        
        # 📌 필터 1: RS 점수 100점 이상 (시장 대비 강세)
        if rs_score < 100: continue
        
        # 📌 필터 2: 미너비니 기본 추세 템플릿 (정배열)
        price = c.iloc[-1]
        s50, s150, s200 = c.rolling(50).mean().iloc[-1], c.rolling(150).mean().iloc[-1], c.rolling(200).mean().iloc[-1]
        
        if (price > s150 and price > s200 and s150 > s200 and 
            s50 > s150 and price > s50 and price >= c.iloc[-260:].min() * 1.3):
            
            img = get_chart_base64(ticker, name)
            if img:
                passed_list.append({'name': name, 'ticker': ticker, 'rs': rs_score, 'img': img})

    if not passed_list:
        print(f"ℹ️ {market_label}: 조건 만족 종목 없음.")
        return

    # 본문 조립 (HTML 태그만)
    content = f"<h4>{market_label} 시장 상대강도(RS) 주도주 리포트</h4>"
    content += f"<p>지수(KODEX 200) 대비 강한 탄력을 보이며 정배열 추세를 유지하고 있는 {market_label} 종목군입니다.</p>"
    
    for s in sorted(passed_list, key=lambda x: x['rs'], reverse=True):
        url = f"https://finance.naver.com/item/main.naver?code={s['ticker']}"
        content += f"<div style='margin-bottom:50px; border-bottom:1px solid #ddd; padding-bottom:20px;'>"
        content += f"<h3><a href='{url}' target='_blank' style='color:#3498db;'>{s['name']} ({s['ticker']})</a> | RS점수: {s['rs']:.2f}</h3>"
        content += f"<img src='data:image/png;base64,{s['img']}' style='width:100%; max-width:850px; display:block; margin:10px 0;'>"
        content += "</div>"
    
    post_to_blogger(f"🔥 [{market_label}] 시장 대비 강세 주도주 (RS 100+) 분석 - {today}", content, [market_label, "상대강도"])
    print(f"✅ {market_label} 포스팅 완료!")

if __name__ == "__main__":
    # 구글 시트 데이터 로드
    csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid=0"
    df_sheet = pd.read_csv(csv_url, dtype={'종목코드': str})
    
    # 지수 RS 기준 (KODEX 200)
    idx_df = kc.GetOhlcv("KR", "069500", 400, "1")
    index_rs = calculate_rs_score(idx_df['close'])
    
    # 코스피/코스닥 분리 처리
    process_market("코스피", df_sheet[df_sheet['시장구분'] == '유가'], index_rs)
    process_market("코스닥", df_sheet[df_sheet['시장구분'] == '코스닥'], index_rs)
