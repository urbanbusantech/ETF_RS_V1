import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# 페이지 기본 설정
st.set_page_config(page_title="ETF 상대강도 대시보드", page_icon="🚀", layout="wide")

# 대시보드 웜톤 테마 주입
st.markdown("""
    <style>
    .main { background-color: #fffaf8; }
    .stDataFrame { border: 1px solid #d35400; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# 한국 시간으로 오늘 날짜 계산
kst = timezone(timedelta(hours=9))
today_date = datetime.now(kst).strftime('%Y-%m-%d')

st.title("🚀 대한민국 상장 주식형 ETF 모멘텀 대시보드")
st.markdown(f"""
마크 미너비니의 상대강도(RS)를 기준으로 시장의 주도주를 분석한 결과입니다.  
* 💡 **업데이트 일자:** {today_date} (매일 장 마감 후 자동 갱신)
* 📝 **이전 분석 이력 확인:** [제 블로그(모멘텀 인덱스 랩)를 방문해 주세요!](https://apple.journeywithgardens.com/)
""")

csv_path = 'etf_data.csv'

try:
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        
        # 1. 수익률 데이터 % 표기를 위해 100 곱하기 (문자열 방어 로직 포함)
        for col in ['1개월', '3개월', '1년']:
            if df[col].dtype == object: 
                df[col] = df[col].astype(str).str.replace('%', '').astype(float)
            else:
                df[col] = df[col] * 100
        
        # 2. 종목코드 6자리 문자열로 포맷팅 (069500 형태 유지)
        df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
        
        # 3. 네이버 차트 링크 생성
        df['차트'] = "https://finance.naver.com/item/fchart.naver?code=" + df['종목코드']
        
        # 4. 스트림릿 표 렌더링
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_order=['종목코드', '종목명', '현재가(원)', '거래량', '1개월', '3개월', '1년', '상대강도', '차트'],
            column_config={
                "종목코드": st.column_config.TextColumn("코드"),
                "현재가(원)": st.column_config.NumberColumn("현재가", format="%d 원"),
                "거래량": st.column_config.NumberColumn("거래량", format="%d"),
                "1개월": st.column_config.NumberColumn("1M (%)", format="%.2f%%"),
                "3개월": st.column_config.NumberColumn("3M (%)", format="%.2f%%"),
                "1년": st.column_config.NumberColumn("1Y (%)", format="%.2f%%"),
                "상대강도": st.column_config.ProgressColumn(
                    "상대강도",
                    help="1~99점. 80 이상 강력한 추세",
                    format="%d",
                    min_value=1,
                    max_value=99
                ),
                "차트": st.column_config.LinkColumn("네이버 차트", display_text="📈 보기")
            }
        )
        st.info("💡 표의 헤더(컬럼명)를 클릭하면 해당 기준으로 정렬할 수 있습니다.")
    else:
        st.warning("⚠️ 'etf_data.csv' 파일을 찾을 수 없습니다. GitHub Actions 실행을 기다려주세요.")

except Exception as e:
    st.error(f"🚨 앱 실행 중 오류 발생: {e}")
