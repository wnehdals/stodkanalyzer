"""
주식 관련 데이터를 가져오는 스크립트
SaveTicker API를 사용하여 태그 정보와 뉴스 데이터를 가져옵니다.
"""

import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
import numpy as np
import re

from Opinion import Opinion
from StockDataFetcher import StockDataFetcher
from Ticker import Ticker
from Bank import Bank
import FinanceDataReader as fdr


def format_created_at(created_at_raw):
    """
    created_at 값을 yyyy.MM.dd HH:mm:ss 형태로 변환하는 함수
    
    Args:
        created_at_raw: 원본 날짜 문자열 (ISO 형식 또는 기타)
    
    Returns:
        str: yyyy.MM.dd HH:mm:ss 형태의 날짜 문자열
    """
    if not created_at_raw:
        return ''

    try:
        # ISO 형식의 날짜를 파싱
        if isinstance(created_at_raw, str) and 'T' in created_at_raw:
            # ISO 형식인 경우
            dt = datetime.fromisoformat(created_at_raw.replace('Z', '+00:00'))
            return dt.strftime('%Y.%m.%d %H:%M:%S')
        else:
            # 이미 다른 형식인 경우 그대로 반환
            return str(created_at_raw)
    except Exception as e:
        print(f"날짜 변환 오류: {e}, 원본: {created_at_raw}")
        return str(created_at_raw)


def update_save_db_xlsx(file_path: str = "save_db_001.xlsx", page_size: int = 20, delay: float = 0.1):
    """
    save_db_001.xlsx 파일을 업데이트하는 함수
    
    Args:
        file_path (str): Excel 파일 경로
        page_size (int): 페이지당 뉴스 수
        delay (float): API 호출 간격 (초)
    """
    print(f"=== {file_path} 파일 업데이트 시작 ===")

    # StockDataFetcher 인스턴스 생성
    fetcher = StockDataFetcher()

    # 1. 기존 Excel 파일이 있는지 확인하고 1번째 행의 ID 조회
    latest_id = None
    if os.path.exists(file_path):
        try:
            # 기존 파일 읽기
            existing_df = pd.read_excel(file_path)
            if not existing_df.empty:
                # 1번째 행의 ID (첫 번째 컬럼이 ID라고 가정)
                latest_id = existing_df.iloc[0, 0]  # 첫 번째 행, 첫 번째 컬럼
                print(f"기존 파일에서 최신 ID 발견: {latest_id}")
            else:
                print("기존 파일이 비어있습니다.")
        except Exception as e:
            print(f"기존 파일 읽기 오류: {e}")
            latest_id = None
    else:
        print("기존 파일이 없습니다. 새로 생성합니다.")

    # 2. 새로운 뉴스 데이터 수집
    new_news_data = []
    found_latest_id = False
    news_count = 0

    print("새로운 뉴스 데이터 수집 중...")
    print(f"페이지 크기: {page_size}, API 호출 간격: {delay}초")

    try:
        for news in fetcher.fetch_all_news(page_size=page_size, delay=delay):
            news_count += 1
            news_id = news.get('id')

            print(f"뉴스 {news_count} 처리 중 - ID: {news_id}")
            print(f"  제목: {news.get('title', 'N/A')[:50]}...")

            # 3. 기존 최신 ID와 같은 ID가 나오면 중지
            if latest_id and news_id == latest_id:
                print(f"기존 최신 ID ({latest_id})와 일치하는 뉴스를 발견했습니다. 수집을 중지합니다.")
                found_latest_id = True
                break

            # created_at 값을 yyyy.MM.dd HH:mm:ss 형태로 변환
            created_at_formatted = format_created_at(news.get('created_at', ''))

            # 새로운 뉴스 데이터 추가
            new_news_data.append({
                'id': news_id,
                'title': news.get('title', ''),
                'content': news.get('content', ''),
                'created_at': created_at_formatted,
                'tag_names': str(news.get('tag_names', []))  # 리스트를 문자열로 변환
            })

            print(f"새 뉴스 추가 완료 ({len(new_news_data)}개): {news.get('title', 'N/A')[:50]}...")

    except Exception as e:
        print(f"뉴스 수집 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n뉴스 수집 완료!")
    print(f"처리된 총 뉴스 수: {news_count}개")
    print(f"새로 추가할 뉴스 수: {len(new_news_data)}개")

    if not new_news_data:
        if found_latest_id:
            print("새로운 뉴스가 없습니다. 파일이 최신 상태입니다.")
        else:
            print("수집된 뉴스가 없습니다.")
        return

    # 4. 새로운 데이터를 DataFrame으로 변환
    new_df = pd.DataFrame(new_news_data)

    # 5. 기존 데이터와 새 데이터 결합
    if os.path.exists(file_path) and latest_id:
        try:
            # 기존 데이터 읽기
            existing_df = pd.read_excel(file_path)
            # 새 데이터를 기존 데이터 위에 추가 (created_at 내림차순 유지)
            combined_df = pd.concat([new_df, existing_df], ignore_index=True)
        except Exception as e:
            print(f"기존 데이터와 결합 중 오류: {e}")
            combined_df = new_df
    else:
        combined_df = new_df

    # 6. created_at 기준으로 내림차순 정렬
    try:
        # 기존 데이터의 created_at이 이미 yyyy.MM.dd HH:mm:ss 형태인지 확인
        if not combined_df.empty and 'created_at' in combined_df.columns:
            # 첫 번째 행의 created_at 형식 확인
            first_date = str(combined_df.iloc[0]['created_at'])
            if '.' in first_date and ':' in first_date and len(first_date) == 19:
                # 이미 yyyy.MM.dd HH:mm:ss 형태인 경우
                print("기존 데이터는 이미 올바른 날짜 형식입니다.")
            else:
                # ISO 형식인 경우 변환
                print("기존 데이터의 날짜 형식을 변환합니다.")
                combined_df['created_at'] = combined_df['created_at'].apply(format_created_at)

        # 날짜 기준으로 정렬 (문자열 정렬로도 가능)
        combined_df = combined_df.sort_values('created_at', ascending=False).reset_index(drop=True)

    except Exception as e:
        print(f"날짜 정렬 중 오류 발생: {e}")
        # 날짜 정렬 실패해도 계속 진행

    # 7. Excel 파일로 저장
    try:
        # 컬럼 순서: id, title, content, created_at, tag_names
        output_columns = ['id', 'title', 'content', 'created_at', 'tag_names']
        combined_df[output_columns].to_excel(file_path, index=False)

        print(f"파일이 성공적으로 업데이트되었습니다: {file_path}")
        print(f"총 {len(combined_df)}개의 뉴스가 저장되었습니다.")
        print(f"새로 추가된 뉴스: {len(new_news_data)}개")




    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")


def get_ticker(symbol: str, tickers: [Ticker]):
    for ticker in tickers:
        if ticker.symbol == symbol:
            return ticker
    return None


def get_bank(keyword: str):
    banks = [
        Bank(name="테러다인", nickNames=["테러다인"]),
        Bank(name="셀레스티카", nickNames=["셀레스티카", "세레스티카"]),
        Bank(name="미즈호", nickNames=["미즈호", "미즈호"]),
        Bank(name="Melius", nickNames=["Melius", "멜리어스"]),
        Bank(name="Susquehanna", nickNames=["Susquehanna", "스쿼시하난"]),
        Bank(name="Stephens", nickNames=["Stephens", "스테펜스"]),
        Bank(name="오펜하이머", nickNames=["오펜하이머", "오펜하이머"]),
        Bank(name="뱅크오브아메리카", nickNames=["뱅크오브아메리카", "뱅크오브아메리카"]),
        Bank(name="BITG", nickNames=["BITG", "BITG"]),
        Bank(name="KGI", nickNames=["KGI", "KGI"]),
        Bank(name="골드만삭스", nickNames=["골드만삭스", "골드만삭스"]),
        Bank(name="JP모건", nickNames=["JP모건", "JP모건"]),
        Bank(name="모건스탠리", nickNames=["모건스탠리", "모건스탠리"]),
        Bank(name="RBC", nickNames=["RBC", "RBC"]),
        Bank(name="키방크", nickNames=["키방크", "KeyBanc", "KeyBnac"]),
        Bank(name="바클레이스", nickNames=["바클레이스", "바클레이즈즈"]),
        Bank(name="에버코어", nickNames=["에버코어", "에버코어"]),
        Bank(name="CIBC", nickNames=["CIBC", "CIBC"]),
        Bank(name="제프리스", nickNames=["제프리스", "제프리스", "제피리스", "Jefferies"]),
        Bank(name="William", nickNames=["William"]),
        Bank(name="Neil", nickNames=["Neil"]),
        Bank(name="씨티", nickNames=["씨티", "Citi"]),
        Bank(name="씨트론", nickNames=["씨트론"]),
        Bank(name="Cantor", nickNames=["Cantor", "Cantor"]),
        Bank(name="MoffettNathanson", nickNames=["MoffettNathanson"]),
        Bank(name="TD", nickNames=["TD"]),
        Bank(name="Cowen", nickNames=["Cowen"]),
        Bank(name="Canaccord", nickNames=["Canaccord"]),
        Bank(name="HSBC", nickNames=["HSBC"]),
        Bank(name="BMO", nickNames=["BMO"]),
        Bank(name="Berenberg", nickNames=["Berenberg"]),
        Bank(name="Baird", nickNames=["Baird"]),
        Bank(name="Stifel", nickNames=["Stifel"]),
        Bank(name="구겐하임", nickNames=["구겐하임"]),
        Bank(name="DZ", nickNames=["DZ"]),
        Bank(name="Rothchild&Co", nickNames=["Rothchild&Co"]),
        Bank(name="UBS", nickNames=["UBS"]),
        Bank(name="도이치방크", nickNames=["도이치방크", "도이체방크"]),
        Bank(name="HC", nickNames=["HC"]),
        Bank(name="Wainwright", nickNames=["Wainwright"]),
        Bank(name="스코샤방크", nickNames=["스코샤방크"]),
        Bank(name="KBW", nickNames=["KBW"]),
        Bank(name="Loop", nickNames=["Loop", ]),
        Bank(name="Capital", nickNames=["Capital", "Capital"]),
        Bank(name="Arete", nickNames=["Arete"]),
        Bank(name="Ascendiant", nickNames=["Ascendiant"]),
        Bank(name="Wolfe", nickNames=["Wolfe"]),
        Bank(name="BTIG", nickNames=["BTIG"]),
        Bank(name="웰스파고", nickNames=["웰스파고", ]),
        Bank(name="벤치마크", nickNames=["벤치마크", "Benchmark"]),
        Bank(name="Needhma", nickNames=["Needhma"]),
        Bank(name="Seaport Global", nickNames=["Seaport Global"]),
        Bank(name="Needham", nickNames=["Needham"]),
        Bank(name="Wedbush", nickNames=["Wedbush", "웨드부쉬"]),
        Bank(name="BNP파리바스", nickNames=["BNP파리바스"]),
        Bank(name="Truist", nickNames=["Truist"]),
        Bank(name="Cannacord", nickNames=["Cannacord"]),
        Bank(name="Citizens", nickNames=["Citizens"]),
        Bank(name="Argus", nickNames=["Argus"]),
        Bank(name="파이퍼샌들러", nickNames=["파이퍼샌들러", "Piper Sandler"]),
        Bank(name="Rosenblatt", nickNames=["Rosenblatt"]),
        Bank(name="번스타인", nickNames=["번스타인", "번스타인"]),
        Bank(name="Leerink", nickNames=["Leerink"]),
        Bank(name="Craig - Hallum", nickNames=["Craig - Hallum"]),
        Bank(name="B.Riley", nickNames=["B.Riley", "B Riley"]),
        Bank(name="DA Davidson", nickNames=["DA Davidson", "DA Davidson"]),
        Bank(name="B Riley", nickNames=["B Riley", "B Riley"]),
        Bank(name="레드번", nickNames=["RedBurn", "레드번"]),
    ]

    for bank in banks:
        if any(nickName.strip().lower() in keyword.strip().lower() for nickName in bank.nickNames):
            return bank

    return Bank(name="", nickNames=[""])


def send_contents(contents: str):
    botToken = "bot8393594501:AAFmZV6H___YD-5usEXzfZsxKEUBjmvE2ag"
    chhenlId = -1003179740625
    sp500 = fdr.StockListing('SP500')

def get_news(ticker: Ticker, banks: [str] = []):
    """
    Ticker의 opinions에서 지정된 banks 목록에 있는 의견들의 news_id와 일치하는 뉴스를 출력하는 함수
    
    Args:
        ticker (Ticker): 종목 객체
        banks (list): 필터링할 은행 목록
    """
    # 1. Excel 파일 읽기
    df = pd.read_excel("save_db_001.xlsx")

    if df.empty:
        print("데이터가 없습니다.")
        return

    print(f"전체 뉴스 수: {len(df)}개")

    # 2. Ticker에 의견 데이터가 있는지 확인
    if not ticker.opinions:
        print(f"{ticker.symbol}에 대한 의견 데이터가 없습니다.")
        return

    # 3. 지정된 banks 목록에 있는 의견들만 필터링
    target_opinions = []
    if len(banks) == 0:
        target_opinions = ticker.opinions
    else:
        target_opinions = list(filter(lambda x: x.bank in banks, ticker.opinions))

    if not target_opinions:
        print("지정된 은행의 의견이 없습니다.")
        return

    # 4. ID 컬럼 확인
    id_column = df.columns[0]  # 첫 번째 열 (ID)

    # 5. 의견의 news_id와 일치하는 뉴스 찾기
    filtered_rows = []
    news_ids = [opinion.news_id for opinion in target_opinions]
    

    for idx, row in df.iterrows():
        news_id = str(row[id_column]) if pd.notna(row[id_column]) else ""
        
        if news_id in news_ids:
            filtered_rows.append(row)

    # 6. 종목 심볼 필터링 (뉴스 제목이나 내용에 종목 심볼이 포함되어 있는지 확인)
    symbol_filtered_rows = []
    symbol = ticker.symbol.upper()
    
    for row in filtered_rows:
        title = str(row.get('title', '')).upper()
        content = str(row.get('content', '')).upper()
        
        # 제목이나 내용에 종목 심볼이 포함되어 있는지 확인
        if symbol in title or symbol in content:
            symbol_filtered_rows.append(row)
    
    # 7. 결과 출력
    if not symbol_filtered_rows:
        print(f"조건에 맞는 뉴스가 없습니다. (종목 심볼 '{symbol}'이 포함된 뉴스 없음)")
        return
    
    print(f"\n=== {symbol} 관련 뉴스 {len(symbol_filtered_rows)}개 ===")
    for i, row in enumerate(symbol_filtered_rows):
        print(f"\n{i+1}")
        print(f"   제목: {row.get('title', 'N/A')}")
        print(f"   작성일: {row.get('created_at', 'N/A')}")
        print(f"   내용: {str(row.get('content', 'N/A'))[:100]}...")
    
    return target_opinions


def main():
    tickers = []
    """메인 실행 함수"""
    print("주식 관련 데이터 가져오기 시작...")

    # 1. 실제 뉴스 데이터로 Excel 업데이트
    print("\n=== 실제 뉴스 데이터로 Excel 업데이트 ===")
    # update_save_db_xlsx("save_db_001.xlsx", page_size=50, delay=0.1)
    print("\n=== 실제 뉴스 데이터로 Excel 업데이트 완료 ===")

    # 2. 투자 의견 뉴스 필터링
    print("\n=== 투자 의견 뉴스 업데이트 ===")
    filter_opinion_news(tickers)
    while True:
        print("--------------------------------")
        command = input("[1] 종목검색 [2] 그래프보기 [0] 종료: ")
        if command == "0":
            break
        elif command == "1":
            symbol = input("종목을 입력하세요: ")
            ticker = get_ticker(symbol.upper(), tickers)
            if ticker:
                print(f"종목: {ticker.symbol}")
                print(f"의견 수: {len(ticker.opinions)}개")
                get_news(ticker, ["JP모건","모건스탠리","뱅크오브아메리카","골드만삭스"])
            else:
                print("종목을 찾을 수 없습니다.")
        elif command == "2":
            symbol = input("그래프를 그릴 종목을 입력하세요: ")
            ticker = get_ticker(symbol.upper(), tickers)
            if ticker:
                draw_graph(ticker, ["JP모건","모건스탠리","뱅크오브아메리카","골드만삭스"])
            elif ticker:
                print("해당 종목에 대한 의견 데이터가 없습니다.")
            else:
                print("종목을 찾을 수 없습니다.")
        # filter_bank_news()


def draw_graph(ticker: Ticker, bankName: [str] = []):
    get_news(ticker,bankName)
    """
    Ticker 객체의 주식 데이터와 의견을 그래프로 그리는 함수
    
    Args:
        ticker (Ticker): 그래프를 그릴 Ticker 객체
    """
    if not ticker.opinions:
        print(f"{ticker.symbol}에 대한 의견 데이터가 없습니다.")
        return
    
    # 1. 의견 데이터에서 날짜 범위 설정
    # 의견들을 날짜순으로 정렬하여 가장 오래된 날짜를 start_date, 가장 최근 날짜를 end_date로 설정
    target_opinions = []
    target_opinions = get_news(ticker,bankName)
    sorted_opinions = sorted(target_opinions, key=lambda x: x.opinion_date)
    start_date = sorted_opinions[0].opinion_date  # 가장 오래된 날짜
    end_date = sorted_opinions[-1].opinion_date   # 가장 최근 날짜
    
    print(f"그래프 그리기: {ticker.symbol}")
    print(f"시작 날짜: {start_date}")
    print(f"종료 날짜: {end_date}")
    for i in target_opinions:
        print(i.bank)
    
    try:
        # 2. FinanceDataReader로 주식 데이터 가져오기
        # 날짜 형식을 변환 (yyyy.MM.dd -> yyyy-MM-dd)
        start_date_formatted = start_date.replace('.', '-')
        end_date_formatted = end_date.replace('.', '-')
        
        df = fdr.DataReader(ticker.symbol, start_date_formatted, end_date_formatted)

        if df.empty:
            print(f"{ticker.symbol}의 주식 데이터를 가져올 수 없습니다.")
            return
        plt.rc('font', family='Apple SD Gothic Neo')
        #plt.rc('font', family='Malgun Gothic')
        # 3. 그래프 설정
        fig, ax = plt.subplots(figsize=(15, 8))
        
        # 4. 주가 그래프 그리기
        ax.plot(df.index, df['Close'], label='Close Price', color='blue', linewidth=2)
        
        # 5. 의견이 있는 날짜에 점으로 표시
        opinion_colors = {'상향': 'red', '하향': 'green', '중립': 'orange'}
        legend_added = set()  # 범례에 추가된 의견 타입 추적
        opinion_data = []  # 클릭 이벤트를 위한 의견 데이터 저장
        
        for opinion in target_opinions:
            # opinion_date를 datetime으로 변환
            opinion_date_str = opinion.opinion_date.split(" ")[0].replace('.', '-')
            try:
                opinion_date = pd.to_datetime(opinion_date_str)

                # 해당 날짜에 주가 데이터가 있는지 확인

                if opinion_date in df.index:
                    close_price = df.loc[opinion_date, 'Close']
                    color = opinion_colors.get(opinion.opinion, 'gray')
                    
                    # 범례에 추가할지 결정
                    show_legend = opinion.opinion not in legend_added
                    if show_legend:
                        legend_added.add(opinion.opinion)
                    
                    scatter = ax.scatter(opinion_date, close_price, 
                              color=color, s=50, alpha=0.8,
                              label=f'{opinion.opinion}' if show_legend else "")
                    
                    # 의견 데이터 저장 (클릭 이벤트용)
                    opinion_data.append({
                        'scatter': scatter,
                        'opinion': opinion,
                        'date': opinion_date,
                        'price': close_price
                    })
                    
                    # 의견 텍스트 추가 (숨겨진 텍스트)
                    ax.annotate(f'',
                               xy=(opinion_date, close_price),
                               xytext=(10, 10), textcoords='offset points',
                               fontsize=8, alpha=0.8)
                else:
                    print(f"의견 날짜 {opinion_date_str}에 해당하는 주가 데이터가 없습니다.")
            except Exception as e:
                print(f"날짜 변환 오류: {opinion_date_str} - {e}")
        
        # 6. 클릭 이벤트 핸들러 정의
        def on_click(event):
            if event.inaxes != ax:
                return
            
            # 클릭한 위치에서 가장 가까운 의견 찾기
            if event.xdata is not None and event.ydata is not None:
                # matplotlib의 내부 날짜 형식을 올바른 날짜로 변환
                click_date = mdates.num2date(event.xdata)
                click_price = event.ydata
                print(f"클릭된 날짜: {click_date.strftime('%Y-%m-%d')} / 가격: ${click_price:.2f}")
                min_distance = float('inf')
                closest_opinion = None
                
                for data in opinion_data:
                    # click_date를 pandas datetime으로 변환
                    click_date_pd = pd.to_datetime(click_date)
                    
                    # 날짜 비교 (날짜 부분만)
                    data_date_str = data['date'].strftime('%Y-%m-%d')
                    click_date_str = click_date_pd.strftime('%Y-%m-%d')
                    

                    if data_date_str == click_date_str:
                        opinion = data['opinion']
                        print(f"\n=== 클릭된 의견 정보 ===")
                        print(f"의견: {opinion.opinion}")
                        print(f"은행: {opinion.bank}")
                        print(f"날짜: {opinion.opinion_date}")
                        print(f"뉴스 ID: {opinion.news_id}")
                        print(f"주가: ${data['price']:.2f}")
                        break
                else:
                    print("클릭한 위치 근처에 의견이 없습니다.")
        
        # 클릭 이벤트 연결
        fig.canvas.mpl_connect('button_press_event', on_click)
        
        # 7. 그래프 설정
        ax.set_title(f'{ticker.symbol} 주가 및 투자 의견', fontsize=16, fontweight='bold')
        ax.set_xlabel('날짜', fontsize=12)
        ax.set_ylabel('주가 (USD)', fontsize=12)
        
        # x축 날짜 형식 설정
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y.%m.%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax.get_xticklabels(), rotation=45)
        
        # 범례 설정
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), loc='upper left')
        
        # 그리드 추가
        ax.grid(True, alpha=0.3)
        
        # 레이아웃 조정
        plt.tight_layout()
        
        # 8. 그래프 표시
        print("그래프에서 점을 클릭하면 해당 의견의 은행 정보가 표시됩니다.")
        plt.show()
        
        print(f"{ticker.symbol} 그래프가 성공적으로 생성되었습니다.")
        
    except Exception as e:
        print(f"그래프 생성 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()


def filter_bank_news():
    """투자 의견 관련 뉴스만 필터링하여 저장하는 함수"""
    print("=== 투자 의견 뉴스 필터링 시작 ===")

    # 필터링할 키워드 목록
    opinion_keywords = get_bank()
    print(opinion_keywords)
    try:
        # 1. save_db_001.xlsx 파일 읽기
        print("save_db_001.xlsx 파일을 읽는 중...")
        df = pd.read_excel("save_db_001.xlsx")

        if df.empty:
            print("데이터가 없습니다.")
            return

        # 2. 2번째 열(title)에서 키워드 필터링
        title_column = df.columns[1]  # 2번째 열 (0-based index)

        # 기존 opinion_db_001.xlsx 파일 병합 및 업데이트 방식으로 저장
        # 1. 필터: 제목에 Bank nickname이 포함된 뉴스만 추출 (vectorized)
        opinion_nicknames = [nickname.strip()
                             for bank in opinion_keywords
                             for nickname in bank.nickNames if nickname.strip()]
        pattern = '|'.join(map(pd.re.escape, opinion_nicknames))
        mask = df[title_column].astype(str).str.contains(pattern, na=False)
        filtered_df = df.loc[mask].copy()
        print(f"필터링된 뉴스 수: {len(filtered_df)}개")
        output_file = "opinion_db_001.xlsx"

        # 2. 기존 파일 병합 (중복 ID 제거)
        if os.path.exists(output_file):
            existing_df = pd.read_excel(output_file)
            combined_df = pd.concat([filtered_df, existing_df], ignore_index=True)
            # 첫 번째 칼럼(뉴스 ID)이 중복 제거 기준이라고 가정
            dedup_col = existing_df.columns[0] if not existing_df.empty else filtered_df.columns[0]
            combined_df = combined_df.drop_duplicates(subset=dedup_col, keep='first')
            final_df = combined_df
        else:
            final_df = filtered_df

        final_df.to_excel(output_file, index=False)
        print(f"파일이 성공적으로 업데이트되었습니다: {output_file}")

        # 5. 저장된 파일 미리보기


    except FileNotFoundError:
        print("save_db_001.xlsx 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


def thinking_opinion(title: str):
    opinion_up = ["상향", "매수", "투자 의견", "투자의견", "Buy", "목표가 상향", "중립→매수", "매도→중립"]
    opinion_down = ["하향", "매도", "Sell", "목표가 하향", "중립→매도", "매수->중립"]
    if any(keyword in title for keyword in opinion_up):
        return "상향"
    elif any(keyword in title for keyword in opinion_down):
        return "하향"
    else:
        return "중립"


def filter_opinion_news(tickers: [Ticker]):
    """투자 의견 관련 뉴스만 필터링하여 저장하는 함수"""
    print("=== 투자 의견 뉴스 필터링 시작 ===")

    # 필터링할 키워드 목록
    opinion_keywords = ["목표가 상향", "목표가 하향", "투자 의견", "투자의견", "목표가", "매수의견", "매도의견", "매수 의견", "매도 의견", "중립→매수", "매도→중립",
                        "중립→매도", "매수->중립", "Buy", "Sell"]

    try:
        # 1. save_db_001.xlsx 파일 읽기
        print("save_db_001.xlsx 파일을 읽는 중...")
        df = pd.read_excel("save_db_001.xlsx")

        if df.empty:
            print("데이터가 없습니다.")
            return

        print(f"전체 뉴스 수: {len(df)}개")

        # 2. 2번째 열(title)에서 키워드 필터링
        title_column = df.columns[1]  # 2번째 열 (0-based index)

        # 키워드가 포함된 행 찾기
        filtered_rows = []
        for idx, row in df.iterrows():
            title = str(row[title_column]) if pd.notna(row[title_column]) else ""

            # 키워드 중 하나라도 포함되어 있는지 확인
            if any(keyword in title for keyword in opinion_keywords):
                filtered_rows.append(row)

        if not filtered_rows:
            print("조건에 맞는 뉴스가 없습니다.")
            return

        print(f"투자 의견 뉴스 수: {len(filtered_rows)}개")

        # 3. NYSE 종목 목록 가져오기 (한 번만)
        if not tickers:  # tickers가 비어있을 때만 초기화
            print("NYSE 종목 목록을 가져오는 중...")
            nasdaq_df = fdr.StockListing('NYSE')
            for idx, row in nasdaq_df.iterrows():
                tickers.append(Ticker(symbol=row['Symbol']))
            print(f"총 {len(tickers)}개 종목 로드 완료")

        # 4. 종목별 의견 데이터 생성
        opinion_count = 0
        for news in filtered_rows:
            news_title = news['title']
            news_id = news['id']
            news_date = news['created_at']
            
            # 뉴스 제목에 포함된 종목 찾기 (정확한 매칭)
            for ticker in tickers:
                # 정확한 종목 심볼 매칭 (단어 경계 고려)
                pattern = r'\b' + re.escape(ticker.symbol) + r'\b'
                if re.search(pattern, news_title):
                    # 중복 의견 체크 (같은 news_id가 이미 있는지 확인)
                    opinion = Opinion(
                        symbol=ticker.symbol,
                        opinion=thinking_opinion(news_title),
                        opinion_date=news_date,
                        bank=get_bank(news_title).name,
                        news_id=news_id,
                    )
                    ticker.opinions.append(opinion)
                    opinion_count += 1
                    break  # 한 뉴스당 하나의 종목에만 매칭

        print(f"총 {opinion_count}개의 의견이 추가되었습니다.")

        # 5. 의견이 있는 종목들만 필터링
        tickers_with_opinions = [ticker for ticker in tickers if ticker.opinions]
        print(f"의견이 있는 종목 수: {len(tickers_with_opinions)}개")

        # 6. opinion_db_001.xlsx 파일로 저장
        output_file = "opinion_db_001.xlsx"
        filtered_df = pd.DataFrame(filtered_rows)
        filtered_df.to_excel(output_file, index=False)

        print(f"파일이 성공적으로 저장되었습니다: {output_file}")
        print(f"총 {len(filtered_df)}개의 투자 의견 뉴스가 저장되었습니다.")

    except FileNotFoundError:
        print("save_db_001.xlsx 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


def update_excel_only():
    """Excel 파일만 업데이트하는 함수"""
    print("=== Excel 파일 업데이트만 실행 ===")
    update_save_db_xlsx("save_db_001.xlsx", page_size=50, delay=0.2)


if __name__ == '__main__':
    # 기본 실행
    main()

    # 개별 실행 옵션들 (주석 해제하여 사용)
    # update_excel_only()  # Excel 파일만 업데이트
    # filter_opinion_only()  # 투자 의견 필터링만 실행
