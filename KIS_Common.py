'''

✅ 게만아 주식/코인 파이썬 퀀트 시스템 트레이딩의 모든 것
https://blog.naver.com/zacra/223910423439

🔥 게만아 컨텐츠 후기 등 안내
https://blog.naver.com/zacra/223568483228

📈 매일 매일 자동매매 봇 성과 공개 [게만아 투자 실험실]
https://blog.naver.com/PostList.naver?blogId=zacra&from=postList&categoryNo=55




임시 미완성 버전입니다.

관련 모듈 설치가 필요합니다.
한 줄 한 줄 실행해보세요 

pip install pandas
pip install finance-datareader
pip install plotly
pip install yfinance
pip install setuptools
pip install matplotlib
pip install requests

혹시나 그래도 ModuleNotFoundError 이런 식의 문구 (모듈을 못 찾겠다는 에러)가 보이면
이 포스팅을 참고하세요!
https://blog.naver.com/zacra/222508537156


파이썬을 모르신다면 이 코드는 수정하지 마세요 (그럴 필요가 없을 거예요!)

사용하다 막히시면
https://blog.naver.com/zacra/223799590835
이 포스팅에 댓글(비밀 댓글) 등으로 문의 주세요

혹은 아래 문의하기를 이용하세요
https://litt.ly/invest_gma

'''
import pandas as pd

from pytz import timezone
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import yfinance
import time


#나중에 완성된 KIS_Common 사용시 필요한 함수!
def SetChangeMode(dist = "REAL"):
    print("..") #사용하지 않음 

############################################################################################################################################################
############################################################################################################################################################
#한국인지 미국인지 구분해 현재 날짜정보를 리턴해 줍니다!
def GetNowDateStr(area = "KR", type= "NONE" ):
    timezone_info = timezone('Asia/Seoul')
    if area == "US":
        timezone_info = timezone('America/New_York')

    now = datetime.now(timezone_info)
    if type.upper() == "NONE":
        return now.strftime("%Y%m%d")
    else:
        return now.strftime("%Y-%m-%d")

#현재날짜에서 이전/이후 날짜를 구해서 리턴! (미래의 날짜를 구할 일은 없겠지만..)
def GetFromNowDateStr(area = "KR", type= "NONE" , days=100):
    timezone_info = timezone('Asia/Seoul')
    if area == "US":
        timezone_info = timezone('America/New_York')

    now = datetime.now(timezone_info)

    if days < 0:
        next = now - timedelta(days=abs(days))
    else:
        next = now + timedelta(days=days)

    if type.upper() == "NONE":
        return next.strftime("%Y%m%d")
    else:
        return next.strftime("%Y-%m-%d")
    
#일봉 정보 가져오기 1
def GetOhlcv1(area, stock_code, limit = 500, adj_ok = "1"):

    df = fdr.DataReader(stock_code,GetFromNowDateStr(area,"BAR",-limit),GetNowDateStr(area,"BAR"))

    if adj_ok == "1":
        
        try :
            df = df[[ 'Open', 'High', 'Low', 'Adj Close', 'Volume']]
        except Exception:
            df = df[[ 'Open', 'High', 'Low', 'Close', 'Volume']]

    else:
        df = df[[ 'Open', 'High', 'Low', 'Close', 'Volume']]



    df.columns = [ 'open', 'high', 'low', 'close', 'volume']
    df.index.name = "Date"

    #거래량과 시가,종가,저가,고가의 평균을 곱해 대략의 거래대금을 구해서 value 라는 항목에 넣는다 ㅎ
    df.insert(5,'value',((df['open'] + df['high'] + df['low'] + df['close'])/4.0) * df['volume'])


    df.insert(6,'change',(df['close'] - df['close'].shift(1)) / df['close'].shift(1))

    df[[ 'open', 'high', 'low', 'close', 'volume', 'change']] = df[[ 'open', 'high', 'low', 'close', 'volume', 'change']].apply(pd.to_numeric)

    #미국주식은 2초를 쉬어주자! 
    if area == "US":
        time.sleep(2.0)
    else:
        time.sleep(0.2)



    df.index = pd.to_datetime(df.index).strftime('%Y-%m-%d')


    return df

#일봉 정보 가져오기 2
def GetOhlcv2(area, stock_code, limit = 500, adj_ok = "1"):

    df = None

    if area == "KR":

        df = fdr.DataReader(stock_code, GetFromNowDateStr(area,"BAR",-limit), GetNowDateStr(area,"BAR"))


    else:
        df = yfinance.download(stock_code, period='max')
        print(df)

    if adj_ok == "1":
            
        try :
            df = df[[ 'Open', 'High', 'Low', 'Adj Close', 'Volume']]
        except Exception:
            df = df[[ 'Open', 'High', 'Low', 'Close', 'Volume']]

    else:
        df = df[[ 'Open', 'High', 'Low', 'Close', 'Volume']]

    
    df.columns = [ 'open', 'high', 'low', 'close', 'volume']
    df = df.astype({'open':float,'high':float,'low':float,'close':float,'volume':float})
    df.index.name = "Date"


    #거래량과 시가,종가,저가,고가의 평균을 곱해 대략의 거래대금을 구해서 value 라는 항목에 넣는다 ㅎ
    df.insert(5,'value',((df['open'] + df['high'] + df['low'] + df['close'])/4.0) * df['volume'])
    df.insert(6,'change',(df['close'] - df['close'].shift(1)) / df['close'].shift(1))

    df[[ 'open', 'high', 'low', 'close', 'volume', 'change']] = df[[ 'open', 'high', 'low', 'close', 'volume', 'change']].apply(pd.to_numeric)


    df.index = pd.to_datetime(df.index).strftime('%Y-%m-%d')

    time.sleep(0.2)
        

    return df

############################################################################################################################################################
############################################################################################################################################################



############################################################################################################################################################
#일봉 정보를 가져오는 함수 <- 이 함수만 사용하면 된다 1번에서 실패하면 2번에서 일봉정보를 가져오도록 구성 되어 있음
def GetOhlcv(area, stock_code, limit = 500, adj_ok = "1"):

    Adjlimit = limit * 1.7 #주말을 감안하면 5개를 가져오려면 적어도 7개는 뒤져야 된다. 1.4가 이상적이지만 혹시 모를 연속 공휴일 있을지 모르므로 1.7로 보정해준다

    df = None

    except_riase = False

    try:

        #print("----First try----")
        
        try:
            df = GetOhlcv1(area,stock_code,Adjlimit,adj_ok)
            
        except Exception as e:
            print("")
            
            if df is None or len(df) == 0:
                except_riase = False
                try:
                    #print("----Second try----")
                    df = GetOhlcv2(area,stock_code,Adjlimit,adj_ok)

                    if df is None or len(df) == 0:
                        except_riase = True
                    
                except Exception as e:
                    except_riase = True
                    

    except Exception as e:
        print(e)
        except_riase = True
    

    if except_riase == True:
        return df
    else:
        #print("---", limit)
        return df[-limit:]
############################################################################################################################################################