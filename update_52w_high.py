import pandas as pd
import os
import urllib.parse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup  # 💡 웹 스캔을 위한 라이브러리
import time

# 구글 인증 라이브러리
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
GID = "0" 

def get_related_etfs(stock_code):
    """네이버 금융에서 해당 종목이 포함된 관련 ETF 상위 3개를 가져옵니다."""
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        etf_list = []
        # 1. 정석 방법: 'tb_etf' 클래스를 가진 테이블 찾기
        etf_table = soup.find('table', class_='tb_etf')
        if etf_table:
            rows = etf_table.find_all('tr')
            for row in rows:
                title_td = row.find('td', class_='title')
                if title_td and title_td.a:
                    etf_list.append(title_td.a.text.strip())
                    
        # 2. 폴백 방법: 우측 탭의 구조가 바뀌었을 경우를 대비해 ETF 브랜드명으로 찾기
        if not etf_list:
            etf_brands = ['KODEX', 'TIGER', 'ACE', 'SOL', 'KBSTAR', 'ARIRANG', 'HANARO', 'KOSEF', 'TIMEFOLIO', 'PLUS']
            for a in soup.find_all('a'):
                text = a.text.strip()
                if any(brand in text for brand in etf_brands) and len(text) > 4:
                    etf_list.append(text)
        
        # 0.2초 대기 (네이버 서버 과부하 방지)
        time.sleep(0.2)
        
        if etf_list:
            # 중복 제거 (순서 유지)
            seen = set()
            unique_etfs = [x for x in etf_list if not (x in seen or seen.add(x))]
            return ", ".join(unique_etfs[:3])
            
    except Exception as e:
        print(f"⚠️ ETF 파싱 에러({stock_code}): {e}")
        
    return "-"

def get_data_from_google_sheet():
    """공개된 구글 스프레드시트를 CSV 형태로 즉시 읽어옵니다."""
    print("📌 구글 스프레드시트 데이터베이스를 불러오는 중...")
    
    if not GOOGLE_SHEET_ID:
        print("❌ 오류: 구글 시트 ID가 설정되지 않았습니다. 깃허브 Secrets를 확인하세요.")
        return pd.DataFrame()
        
    csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GID}"
    
    try:
        df = pd.read_csv(csv_url, encoding='utf-8')
        
        if '상태' in df.columns:
            df = df[df['상태'] == '🚀돌파'].copy()
        
        col_code = '종목코드(정제)' if '종목코드(정제)' in df.columns else '종목코드'
        
        extract_cols = [col_code, '회사명', '현재가', '52주최고가', '거래량']
        has_sector = '업종' in df.columns
        if has_sector:
            extract_cols.insert(2, '업종')
            
        result_df = df[extract_cols].copy()
        
        if has_sector:
            result_df.columns = ['종목코드', '종목명', '업종', '현재가(원)', '52주 최고가', '거래량']
        else:
            result_df.columns = ['종목코드', '종목명', '현재가(원)', '52주 최고가', '거래량']
        
        exclude_keywords = ['스팩', '리츠', '제[0-9]+호']
        pattern = '|'.join(exclude_keywords)
        result_df = result_df[~result_df['종목명'].str.contains(pattern, regex=True, na=False)]
        
        result_df['정렬용_거래량'] = pd.to_numeric(
            result_df['거래량'].astype(str).str.replace(',', '').str.replace('₩', '').str.strip(), 
            errors='coerce'
        ).fillna(0)
        
        result_df = result_df.sort_values(by='정렬용_거래량', ascending=False)
        result_df = result_df.drop(columns=['정렬용_거래량'])
        
        result_df['종목코드'] = result_df['종목코드'].apply(lambda x: str(x).replace('.0', '').zfill(6))
        
        if not result_df.empty:
            print(f"🔍 {len(result_df)}개 종목에 대한 관련 ETF 정보를 수집합니다. (약 10~20초 소요)")
            result_df['포함 ETF (상위 3개)'] = result_df['종목코드'].apply(get_related_etfs)
        
        print(f"✅ 분석 완료! 스팩/리츠 제외 총 {len(result_df)}개의 종목 발견.")
        return result_df
        
    except Exception as e:
        print(f"❌ 구글 시트를 읽는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

def get_stock_news(stock_name, limit=3):
    query_str = f'"{stock_name}" (주가 OR 특징주 OR 실적)'
    query = urllib.parse.quote(query_str)
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
        print(f"⚠️ {stock_name} 뉴스 수집 중 오류: {e}")
        
    return news_list

def post_to_blogger(title, html_content, labels=None):
    blog_id = os.environ.get('BLOGGER_BLOG_ID')
    client_id = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([blog_id, client_id, client_secret, refresh_token]):
        print("💡 Blogger API 인증 정보가 없어 포스팅을 건너뜁니다.")
        return

    try:
        creds = Credentials(token=None, refresh_token=refresh_token, token_uri='https://oauth2.googleapis.com/token', client_id=client_id, client_secret=client_secret)
        service = build('blogger', 'v3', credentials=creds)
        body = {"kind": "blogger#post", "title": title, "content": html_content}
        if labels:
            body["labels"] = labels
        service.posts().insert(blogId=blog_id, body=body, isDraft=False).execute()
        print("✅ 구글 블로그 포스팅 성공!")
    except Exception as e:
        print(f"❌ 구글 블로그 포스팅 실패: {e}")

def generate_market_summary(df):
    if df.empty:
        return "금일은 52주 신고가 필터에 포착된 유의미한 주도주가 없습니다."
        
    top_vol_stock = df.iloc[0]['종목명']
    total_count = len(df)
    
    sector_text = "다양한 개별 이슈"
    if '업종' in df.columns:
        valid_sectors = df[df['업종'].notnull() & (df['업종'] != '')]['업종']
        if not valid_sectors.empty:
            sector_counts = valid_sectors.value_counts()
            top1_sector = sector_counts.index[0]
            top1_count = sector_counts.iloc[0]
            
            if len(sector_counts) > 1:
                top2_sector = sector_counts.index[1]
                top2_count = sector_counts.iloc[1]
                sector_text = f"<strong>[{top1_sector}]</strong>({top1_count}개) 및 <strong>[{top2_sector}]</strong>({top2_count}개) 섹터"
            else:
                sector_text = f"<strong>[{top1_sector}]</strong>({top1_count}개) 섹터"

    day_mod = datetime.now().day % 3
    
    if day_mod == 0:
        return (f"오늘 52주 신고가 필터에 포착된 주도주 후보는 총 <strong>{total_count}개</strong>입니다.<br>"
                f"업종별 데이터를 분석한 결과, 오늘은 특히 {sector_text}에 시장의 핵심 자금이 집중된 것으로 파악됩니다.<br>"
                f"가장 폭발적인 거래량을 동반하며 신고가에 안착한 대장주는 <strong>{top_vol_stock}</strong>입니다.<br>"
                f"위 주도 섹터들의 움직임이 단기적 자금 쏠림인지, 구조적 트렌드인지 주의 깊게 추적해 보시기 바랍니다.")
    elif day_mod == 1:
        return (f"강력한 모멘텀 필터를 통과한 오늘의 주도주 후보군은 총 <strong>{total_count}개</strong>입니다.<br>"
                f"시장 참여자들의 매수세가 돋보인 주요 섹터는 {sector_text} 위주로 형성되었습니다.<br>"
                f"오늘 거래량 1위를 차지하며 강한 시세를 분출한 종목은 <strong>{top_vol_stock}</strong>(으)로 확인되었습니다.<br>"
                f"신고가 돌파 종목들이 속한 업종의 전반적인 시황과 함께 개별 기업의 펀더멘탈을 교차 검증하시길 권장합니다.")
    else:
        return (f"금일 시장의 저항을 뚫고 52주 최고가 부근에 도달한 주도주 군은 총 <strong>{total_count}개</strong>로 집계되었습니다.<br>"
                f"눈여겨볼 만한 자금 유입 특징으로는 {sector_text} 관련주들의 동반 약진이 뚜렷했다는 점입니다.<br>"
                f"거래대금과 매수 강도 측면에서 오늘 가장 돋보인 리딩 주자는 <strong>{top_vol_stock}</strong>입니다.<br>"
                f"이러한 업종별 순환매 흐름을 파악하여 포트폴리오의 비중 조절과 편입 타이밍 전략에 활용해 보세요.")

def generate_html_report(df):
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    today_str = now_kst.strftime('%Y-%m-%d')
    current_time = now_kst.strftime('%Y-%m-%d %H:%M')
    
    briefing_text = generate_market_summary(df)
    
    news_html_blocks = ""
    if not df.empty:
        top_stocks = df['종목명'].iloc[:2].tolist()
        print(f"📰 {', '.join(top_stocks)} 종목의 관련 뉴스를 검색합니다...")
        
        for stock in top_stocks:
            news_items = get_stock_news(stock, limit=3)
            if news_items:
                news_html_blocks += f"<h4 style='color: #d35400; margin-bottom: 5px; margin-top: 15px;'>🔥 대장주: {stock}</h4>\n"
                news_html_blocks += "<ul class='news-list'>\n"
                for item in news_items:
                    news_html_blocks += f"  <li><a href='{item['link']}' target='_blank'>[{item['source']}] {item['title']}</a></li>\n"
                news_html_blocks += "</ul>\n"
            else:
                news_html_blocks += f"<h4 style='color: #d35400; margin-bottom: 5px; margin-top: 15px;'>🔥 대장주: {stock}</h4>\n"
                news_html_blocks += "<ul class='news-list'><li>최근 2일 내 눈에 띄는 특징주 뉴스가 없습니다.</li></ul>\n"
    
    table_df = df.drop(columns=['업종']) if '업종' in df.columns else df.copy()
    
    if table_df.empty:
        table_html = "<p style='text-align:center; padding: 20px; color: #666;'>금일 조건에 부합하는 종목이 없습니다.</p>"
    else:
        def clean_number(x):
            if pd.isnull(x):
                return "-"
            clean_str = str(x).replace('₩', '').replace(',', '').strip()
            try:
                return f"{int(float(clean_str)):,}"
            except ValueError:
                return str(x)

        table_df['현재가(원)'] = table_df['현재가(원)'].apply(clean_number)
        table_df['52주 최고가'] = table_df['52주 최고가'].apply(clean_number)
        table_df['거래량'] = table_df['거래량'].apply(clean_number)
        table_df['종목코드'] = table_df['종목코드'].apply(lambda x: f'<a href="https://finance.naver.com/item/fchart.naver?code={x}" target="_blank" style="color: #3498db; text-decoration: none; font-weight: bold;">{x}</a>')
        
        table_html = table_df.to_html(index=False, escape=False, border=0, classes='momentum-table', justify='center')

    # [핵심 수정] html, head, body 태그를 삭제하고 깔끔한 div 컨테이너로 감쌉니다.
    html_content = f"""
    <div class="momentum-container" style="width: 100%; max-width: 1000px; margin: 0 auto 30px auto; padding: 0 10px; box-sizing: border-box; text-align: center; font-family: 'Helvetica Neue', Arial, sans-serif;">
        <style>
            .momentum-container h3 {{
                color: #2c3e50;
                padding-left: 10px;
                text-align: left;
            }}
            .momentum-container .content-box {{
                border-left: 4px solid #ccc;
                padding: 15px;
                margin-bottom: 20px;
                background-color: #f8f9fa;
                text-align: left;
                font-size: 0.95em;
                line-height: 1.6;
            }}
            .momentum-container .briefing-box {{
                border-left: 4px solid #3498db;
                padding: 15px;
                margin-bottom: 20px;
                background-color: #f0f7ff;
                text-align: left;
                font-size: 0.95em;
                line-height: 1.7;
                color: #2c3e50;
            }}
            .momentum-container .news-box {{
                border-left: 4px solid #e67e22;
                padding: 15px;
                margin-bottom: 20px;
                background-color: #fff9f2;
                text-align: left;
                font-size: 0.95em;
                line-height: 1.6;
            }}
            ul.momentum-list, ul.news-list {{
                list-style-type: none;
                padding-left: 0;
                font-size: 0.9em;
                color: #666;
                text-align: left;
            }}
            ul.momentum-list li, ul.news-list li {{
                margin-bottom: 8px;
            }}
            ul.news-list li a {{
                color: #2980b9;
                text-decoration: none;
            }}
            ul.news-list li a:hover {{
                text-decoration: underline;
                color: #1a5276;
            }}
            .momentum-table {{
                width: 100%;
                max-width: 100%;
                text-align: center;
                border-collapse: collapse;
                margin-top: 20px;
                font-size: 0.9em;
            }}
            .momentum-table th {{
                background-color: #f8f9fa;
                color: #2c3e50;
                padding: 12px;
                border: 1px solid #e0e0e0;
                font-weight: bold;
                text-align: center !important; 
            }}
            .momentum-table td {{
                padding: 10px;
                border: 1px solid #e0e0e0;
                vertical-align: middle;
            }}
            .momentum-table td:last-child {{
                text-align: left;
            }}
            .momentum-table tr:hover {{
                background-color: #f1f4f8;
                transition: background-color 0.2s ease;
            }}
        </style>
        
        <h3>💡 시장의 주도주를 찾아라: 52주 신고가 리포트</h3>
        <div class="content-box">
            본 리포트는 강력한 모멘텀 추세에 기반하여, 1년(52주) 내 가장 폭발적인 상승 에너지를 보여주며 새로운 가격대를 개척하고 있는 핵심 주도주 후보군을 선별합니다. 매일 구글 파이낸스(Google Finance) 실시간 엔진과 연동되어 객관적이고 정확한 시세 데이터를 바탕으로 작성됩니다.
        </div>

        <h3>🤖 오늘의 섹터 모멘텀 브리핑</h3>
        <div class="briefing-box">
            {briefing_text}
        </div>

        <h3>📰 주도주 모멘텀 핵심 뉴스 (거래량 Top 2)</h3>
        <div class="news-box">
            <p style="margin-top: 0; color: #555; font-size: 0.9em;">해당 종목이 오늘 시장의 이목을 끈 재료(이슈)를 확인해 보세요.</p>
            {news_html_blocks}
        </div>

        <h3>📊 분석 기준 및 필터링 요건</h3>
        <ul class="momentum-list">
            <li>- <strong>기준일:</strong> {current_time} (KST)</li>
            <li>- <strong>조건:</strong> 현재가가 최근 52주 최고가에 근접하거나 돌파한 종목 (스팩, 리츠 제외)</li>
            <li>- <strong>정렬 기준:</strong> 당일 거래량이 가장 높은 시장 주도주 순으로 배열</li>
        </ul>

        <h3>🚀 오늘의 모멘텀 돌파 종목</h3>
        {table_html}
    </div>
    """
    
    post_title = f"🚀 [모멘텀 인댁스 랩] 52주 신고가 주도주 랭킹 ({today_str})"
    post_to_blogger(post_title, html_content, labels=["52주신고가", "모멘텀", "주도주뉴스"])
    
    file_name = f"52주_신고가_모멘텀_{today_str}.html"
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"📝 로컬 HTML 파일도 생성되었습니다: {os.path.abspath(file_name)}")

if __name__ == "__main__":
    result_df = get_data_from_google_sheet()
    if not result_df.empty:
        generate_html_report(result_df)
    else:
        print("⚠️ 처리할 데이터가 없어 리포트를 생성하지 않습니다.")
