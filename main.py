"""
주식 관련 데이터를 가져오는 스크립트
SaveTicker API를 사용하여 태그 정보와 뉴스 데이터를 가져옵니다.
"""

import pandas as pd
import os
from datetime import datetime
from StockDataFetcher import StockDataFetcher
from Ticker import Ticker
from Bank import Bank
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
        
        # 최신 5개 뉴스 미리보기
        print("\n=== 최신 뉴스 5개 ===")
        for idx, row in combined_df.head(5).iterrows():
            print(f"{idx + 1}. {row['title'][:50]}...")
            print(f"   작성일: {row['created_at']}")
            print(f"   태그: {row['tag_names']}")
            print()
            
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")

def get_ticker(keyword: str):
    """미국 나스닥 시가총액 상위 1~100위 주식 TICKER 리턴 함수 (간단 매칭)"""
    tickers = [
        Ticker("AAPL"),   # Apple
        Ticker("MSFT"),   # Microsoft
        Ticker("GOOG"),   # Alphabet Class C
        Ticker("GOOGL"),  # Alphabet Class A
        Ticker("NVDA"),   # NVIDIA
        Ticker("META"),   # Meta Platforms
        Ticker("AVGO"),   # Broadcom
        Ticker("TSLA"),   # Tesla
        Ticker("COST"),   # Costco
        Ticker("PEP"),    # PepsiCo
        Ticker("ADBE"),   # Adobe
        Ticker("CSCO"),   # Cisco
        Ticker("AMD"),    # AMD
        Ticker("TMUS"),   # T-Mobile US
        Ticker("AMZN"),   # Amazon
        Ticker("NFLX"),   # Netflix
        Ticker("INTC"),   # Intel
        Ticker("TXN"),    # Texas Instruments
        Ticker("QCOM"),   # Qualcomm
        Ticker("AMGN"),   # Amgen
        Ticker("HON"),    # Honeywell
        Ticker("BKNG"),   # Booking Holdings
        Ticker("AMAT"),   # Applied Materials
        Ticker("INTU"),   # Intuit
        Ticker("SBUX"),   # Starbucks
        Ticker("ISRG"),   # Intuitive Surgical
        Ticker("REGN"),   # Regeneron
        Ticker("ADI"),    # Analog Devices
        Ticker("MU"),     # Micron Tech
        Ticker("MDLZ"),   # Mondelez
        Ticker("GILD"),   # Gilead Sciences
        Ticker("FISV"),   # Fiserv
        Ticker("VRTX"),   # Vertex Pharmaceuticals
        Ticker("SNPS"),   # Synopsys
        Ticker("LRCX"),   # Lam Research
        Ticker("MAR"),    # Marriott
        Ticker("ADP"),    # Automatic Data Processing
        Ticker("PDD"),    # PDD Holdings
        Ticker("PANW"),   # Palo Alto Networks
        Ticker("MRNA"),   # Moderna
        Ticker("KDP"),    # Keurig Dr Pepper
        Ticker("AEP"),    # American Electric Power
        Ticker("ORLY"),   # O'Reilly Automotive
        Ticker("CTAS"),   # Cintas
        Ticker("MNST"),   # Monster Beverage
        Ticker("IDXX"),   # IDEXX Laboratories
        Ticker("KHC"),    # Kraft Heinz
        Ticker("EXC"),    # Exelon
        Ticker("CSX"),    # CSX Corporation
        Ticker("MCHP"),   # Microchip Technology
        Ticker("SIRI"),   # Sirius XM Holdings
        Ticker("PAYX"),   # Paychex
        Ticker("KLAC"),   # KLA Corp
        Ticker("CDNS"),   # Cadence Design
        Ticker("CHTR"),   # Charter Communications
        Ticker("DXCM"),   # Dexcom
        Ticker("ODFL"),   # Old Dominion Freight
        Ticker("FAST"),   # Fastenal
        Ticker("EA"),     # Electronic Arts
        Ticker("PCAR"),   # PACCAR
        Ticker("CEG"),    # Constellation Energy
        Ticker("WBD"),    # Warner Bros Discovery
        Ticker("FTNT"),   # Fortinet
        Ticker("ON"),     # ON Semiconductor
        Ticker("BKR"),    # Baker Hughes
        Ticker("CDW"),    # CDW
        Ticker("ROST"),   # Ross Stores
        Ticker("DDOG"),   # Datadog
        Ticker("XEL"),    # Xcel Energy
        Ticker("DLTR"),   # Dollar Tree
        Ticker("ANSS"),   # Ansys
        Ticker("VRSK"),   # Verisk Analytics
        Ticker("CPRT"),   # Copart
        Ticker("TTD"),    # Trade Desk
        Ticker("WBA"),    # Walgreens Boots Alliance
        Ticker("GFS"),    # GlobalFoundries
        Ticker("ALGN"),   # Align Technology
        Ticker("CTSH"),   # Cognizant
        Ticker("ZS"),     # Zscaler
        Ticker("CBOE"),   # Cboe Global Markets
        Ticker("INCY"),   # Incyte
        Ticker("MTCH"),   # Match Group
        Ticker("TEAM"),   # Atlassian
        Ticker("GEN"),    # Gen Digital
        Ticker("SPLK"),   # Splunk
        Ticker("TCOM"),   # Trip.com Group
        Ticker("LULU"),   # Lululemon Athletica
        Ticker("MDB"),    # MongoDB
        Ticker("ALNY"),   # Alnylam Pharma
        Ticker("QRVO"),   # Qorvo
        Ticker("BIIB"),   # Biogen
        Ticker("WDAY"),   # Workday
        Ticker("DDOG"),   # (Duplicate allowed for ticker coverage)
        Ticker("OKTA"),   # Okta
        Ticker("DXCM"),   # (Duplicate allowed for ticker coverage)
        Ticker("ROKU"),   # Roku
        Ticker("CRWD"),   # CrowdStrike
        Ticker("NTES"),   # NetEase
        Ticker("ZM"),     # Zoom Video
        Ticker("DOCU"),   # DocuSign
        Ticker("JD"),     # JD.com
        Ticker("SGEN"),   # Seagen
    ]
    filtered = [t for t in tickers if keyword.upper() in t.id.upper()]
    return filtered
    tickers = [
        Ticker(name="AAPL", nickNames=["AAPL", "Apple"]),
        Ticker(name="GOOG", nickNames=["GOOG", "Google"]),
        Ticker(name="MSFT", nickNames=["MSFT", "Microsoft"]),
    ]
    return tickers  

def get_bank(keyword: str):
    banks = [
        Bank(name="테러다인", nickNames=["테러다인", ]),
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
        Bank(name="모건스탠리", nickNames=["모건스탠리", "모건스탠리"] ),
        Bank(name="RBC", nickNames=["RBC", "RBC"]),
        Bank(name="키방크", nickNames=["키방크", "KeyBanc", "KeyBnac"]),
        Bank(name="바클레이스", nickNames=["바클레이스", "바클레이즈즈"]),
        Bank(name="에버코어", nickNames=["에버코어", "에버코어"]),
        Bank(name="CIBC", nickNames=["CIBC", "CIBC"]),
        Bank(name="제프리스", nickNames=["제프리스", "제프리스", "제피리스","Jefferies"]),
        Bank(name="William", nickNames=["William"]),
        Bank(name="Neil", nickNames=["Neil"]),
        Bank(name="씨티", nickNames=["씨티", "Citi"]),
        Bank(name="씨트론", nickNames=["씨트론"]),
        Bank(name="Cantor", nickNames=["Cantor", "Cantor"]),
        Bank(name="MoffettNathanson", nickNames=["MoffettNathanson"]),
        Bank(name="TD", nickNames=["TD",]),
        Bank(name="Cowen", nickNames=["Cowen", ]),
        Bank(name="Canaccord", nickNames=["Canaccord", ]),
        Bank(name="HSBC", nickNames=["HSBC", ]),
        Bank(name="BMO", nickNames=["BMO", ]),
        Bank(name="Berenberg", nickNames=["Berenberg", ]),
        Bank(name="Baird", nickNames=["Baird", ]),
        Bank(name="Stifel", nickNames=["Stifel", ]),
        Bank(name="구겐하임", nickNames=["구겐하임", ]),
        Bank(name="DZ", nickNames=["DZ", ]),
        Bank(name="Rothchild&Co", nickNames=["Rothchild&Co", ]),
        Bank(name="UBS", nickNames=["UBS", ]),
        Bank(name="도이치방크", nickNames=["도이치방크", "도이체방크"]),
        Bank(name="HC", nickNames=["HC", ]),
        Bank(name="Wainwright", nickNames=["Wainwright", ]),
        Bank(name="스코샤방크", nickNames=["스코샤방크", ]),
        Bank(name="KBW", nickNames=["KBW", ]),
        Bank(name="Loop", nickNames=["Loop", ]),
        Bank(name="Capital", nickNames=["Capital", "Capital"]),
        Bank(name="Arete", nickNames=["Arete", ]),
        Bank(name="Ascendiant", nickNames=["Ascendiant", ]),
        Bank(name="Wolfe", nickNames=["Wolfe", ]),
        Bank(name="BTIG", nickNames=["BTIG", ]),  
        Bank(name="웰스파고", nickNames=["웰스파고", ]),
        Bank(name="벤치마크", nickNames=["벤치마크","Benchmark" ]),
        Bank(name="Needhma", nickNames=["Needhma", ]),
        Bank(name="Seaport Global", nickNames=["Seaport Global", ]),
        Bank(name="Needham", nickNames=["Needham", ]),
        Bank(name="Wedbush", nickNames=["Wedbush", "웨드부쉬"]),
        Bank(name="BNP파리바스", nickNames=["BNP파리바스", ]),
        Bank(name="Truist", nickNames=["Truist", ]),
        Bank(name="Cannacord", nickNames=["Cannacord", ]),
        Bank(name="Citizens", nickNames=["Citizens", ]),
        Bank(name="Argus", nickNames=["Argus", ]),
        Bank(name="파이퍼샌들러", nickNames=["파이퍼샌들러", "Piper Sandler"]),
        Bank(name="Rosenblatt", nickNames=["Rosenblatt", ]),
        Bank(name="번스타인", nickNames=["번스타인", "번스타인"]),
        Bank(name="Leerink", nickNames=["Leerink", ]),
        Bank(name="Craig - Hallum", nickNames=["Craig - Hallum", ]),
        Bank(name="B.Riley", nickNames=["B.Riley", "B Riley"]),
        Bank(name="DA Davidson", nickNames=["DA Davidson", "DA Davidson"]),
        Bank(name="B Riley", nickNames=["B Riley", "B Riley"]),
        Bank(name="레드번", nickNames=["RedBurn", "레드번"]),
    ]

    for bank in banks:
        if any(nickName.strip().lower() == keyword.strip().lower() for nickName in bank.nickNames):
            return bank
    return Bank()

def main():
    """메인 실행 함수"""
    print("주식 관련 데이터 가져오기 시작...")
    
    # 1. 실제 뉴스 데이터로 Excel 업데이트
    print("\n=== 실제 뉴스 데이터로 Excel 업데이트 ===")
    #update_save_db_xlsx("save_db_001.xlsx", page_size=50, delay=0.1)
    
    # 2. 투자 의견 뉴스 필터링
    print("\n=== 투자 의견 뉴스 필터링 ===")
    #filter_opinion_news()
    filter_bank_news()
    banks = [
        Bank(name="테러다인"),
        Bank(name="셀레스티카"),
        Bank(name="미즈호"),
        Bank(name="Melius"),
        Bank(name="Susquehanna"),
        Bank(name="Stephens"),
        Bank(name="오펜하이머"),
        Bank(name="뱅크오브아메리카"),
        Bank(name="BITG"),
        Bank(name="KGI"),
        Bank(name="골드만삭스"),
        Bank(name="JP모건"),
        Bank(name="모건스탠리"),
        Bank(name="RBC"),
        Bank(name="키방크"),
        Bank(name="바클레이스"),
        Bank(name="에버코어"),
        Bank(name="CIBC"),
        Bank(name="제프리스"),
        Bank(name="William"),
        Bank(name="O"),
        Bank(name="Neil"),
        Bank(name="씨티"),
        Bank(name="Cantor"),
        Bank(name="MoffettNathanson"),
        Bank(name="TD"),
        Bank(name="Cowen"),
        Bank(name="Canaccord"),
        Bank(name="HSBC"),
        Bank(name="BMO"),
        Bank(name="Berenberg"),
        Bank(name="Baird"),
        Bank(name="Stifel"),
        Bank(name="구겐하임"),
        Bank(name="DZ"),
        Bank(name="Bank"),
        Bank(name="Rothchild&Co"),
        Bank(name="UBS"),
        Bank(name="도이치방크"),
        Bank(name="HC"),
        Bank(name="Wainwright"),
        Bank(name="스코샤방크"),
        Bank(name="KBW"),
        Bank(name="Loop"),
        Bank(name="Capital"),
        Bank(name="Arete"),
        Bank(name="Ascendiant"),
        Bank(name="Wolfe"),
        Bank(name="BTIG"),  
        Bank(name="웰스파고"),
        Bank(name="벤치마크"),
        Bank(name="Needhma"),
        Bank(name="Seaport Global"),
        Bank(name="Needham"),
        Bank(name="Wedbush"),
        Bank(name="BNP파리바스"),
        Bank(name="Truist"),
        Bank(name="Cannacord"),
        Bank(name="Citizens"),
        Bank(name="Argus"),
        Bank(name="파이퍼샌들러"),
        Bank(name="웨드부시"),
        Bank(name="Rosenblatt"),
        Bank(name="번스타인"),
        Bank(name="Leerink"),
        Bank(name="파이퍼샌들러"),
        Bank(name="Craig - Hallum"),
        Bank(name="B.Riley"),
        Bank(name="Benchmark"),
        Bank(name="파이퍼 샌들러"),
        Bank(name="Jefferies"),
        Bank(name="DA Davidson"),
    ]


def filter_bank_news():
    """투자 의견 관련 뉴스만 필터링하여 저장하는 함수"""
    print("=== 투자 의견 뉴스 필터링 시작 ===")

    # 필터링할 키워드 목록
    opinion_keywords = [
    "테러다인",
    "셀레스티카",
    "미즈호",
    "Melius",
    "멜리어스",
    "Susquehanna",
    "Stephens",
    "오펜하이머",
    "뱅크오브아메리카",
    "BITG",
    "KGI",
    "골드만삭스",
    "JP모건",
    "모건스탠리",
    "RBC",
    "키방크",           
    "바클레이스",
    "에버코어",
    "CIBC",
    "제프리스",
    "William",
    "O",
    "Neil",
    "씨티",
    "Cantor",
    "MoffettNathanson",
    "TD",
    "Cowen",
    "Canaccord",
    "HSBC",
    "BMO",
    "Berenberg",
    "Baird",
    "Stifel",
    "구겐하임",
    "DZ",
    "Bank",
    "Rothchild&Co",
    "UBS",
    "도이치방크",
    "HC",
    "Wainwright",
    "스코샤방크",
    "KBW",
    "Loop",
    "Capital",
    "Arete",
    "Ascendiant",
    "Wolfe",
    "BTIG",
    "웰스파고",
    "벤치마크",
    "Needhma",
    "Seaport Global",
    "Needham",
    "Wedbush",
    "BNP파리바스",
    "Truist",
    "Cannacord",
    "Citizens",
    "Argus",
    "파이퍼샌들러",
    "웨드부시",
    "Rosenblatt",
    "번스타인",
    "Leerink",
    "파이퍼샌들러",
    "Craig - Hallum",
    "B.Riley",
    "Benchmark",
    "파이퍼 샌들러",
    "Jefferies",
    "DA Davidson",

    ]

    try:
        # 1. save_db_001.xlsx 파일 읽기
        print("save_db_001.xlsx 파일을 읽는 중...")
        df = pd.read_excel("opinion_db_001.xlsx")

        if df.empty:
            print("데이터가 없습니다.")
            return


        # 2. 2번째 열(title)에서 키워드 필터링
        title_column = df.columns[1]  # 2번째 열 (0-based index)
        print(f"제목 컬럼: {title_column}")

        # 키워드가 포함된 행 찾기
        filtered_rows = []
        for idx, row in df.iterrows():
            title = str(row[title_column]) if pd.notna(row[title_column]) else ""

            # 키워드 중 하나라도 포함되어 있는지 확인
            if any(keyword in title for keyword in opinion_keywords):
                #filtered_rows.append(row)
                print(f"매칭된 뉴스: {title[:50]}...")
            else:
                filtered_rows.append(row)
                print(f"매칭된 뉴스: {title[:50]}...")

        if not filtered_rows:
            print("조건에 맞는 뉴스가 없습니다.")
            return

        # 3. 필터링된 데이터를 DataFrame으로 변환
        filtered_df = pd.DataFrame(filtered_rows)
        print(f"필터링된 뉴스 수: {len(filtered_df)}개")

        # 4. opinion_db_001.xlsx 파일로 저장
        output_file = "opinion_db_001.xlsx"
        filtered_df.to_excel(output_file, index=False)

        print(f"파일이 성공적으로 저장되었습니다: {output_file}")

        # 5. 저장된 파일 미리보기


    except FileNotFoundError:
        print("save_db_001.xlsx 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


def filter_opinion_news():
    """투자 의견 관련 뉴스만 필터링하여 저장하는 함수"""
    print("=== 투자 의견 뉴스 필터링 시작 ===")
    
    # 필터링할 키워드 목록
    opinion_keywords = ["목표가 상향", "목표가 하향", "투자 의견", "투자의견", "목표가"]
    
    try:
        # 1. save_db_001.xlsx 파일 읽기
        print("save_db_001.xlsx 파일을 읽는 중...")
        df = pd.read_excel("save_db_001.xlsx")
        
        if df.empty:
            print("데이터가 없습니다.")
            return
        
        print(f"전체 뉴스 수: {len(df)}개")
        print(f"컬럼 목록: {list(df.columns)}")
        
        # 2. 2번째 열(title)에서 키워드 필터링
        title_column = df.columns[1]  # 2번째 열 (0-based index)
        print(f"제목 컬럼: {title_column}")
        
        # 키워드가 포함된 행 찾기
        filtered_rows = []
        for idx, row in df.iterrows():
            title = str(row[title_column]) if pd.notna(row[title_column]) else ""
            
            # 키워드 중 하나라도 포함되어 있는지 확인
            if any(keyword in title for keyword in opinion_keywords):
                filtered_rows.append(row)
                print(f"매칭된 뉴스: {title[:50]}...")
        
        if not filtered_rows:
            print("조건에 맞는 뉴스가 없습니다.")
            return
        
        # 3. 필터링된 데이터를 DataFrame으로 변환
        filtered_df = pd.DataFrame(filtered_rows)
        print(f"필터링된 뉴스 수: {len(filtered_df)}개")
        
        # 4. opinion_db_001.xlsx 파일로 저장
        output_file = "opinion_db_001.xlsx"
        filtered_df.to_excel(output_file, index=False)
        
        print(f"파일이 성공적으로 저장되었습니다: {output_file}")
        
        # 5. 저장된 파일 미리보기
        print("\n=== 저장된 데이터 미리보기 ===")
        print(f"총 {len(filtered_df)}개의 투자 의견 뉴스가 저장되었습니다.")
        
        # 각 키워드별 개수 확인
        keyword_counts = {}
        for keyword in opinion_keywords:
            count = sum(1 for title in filtered_df[title_column] 
                       if pd.notna(title) and keyword in str(title))
            if count > 0:
                keyword_counts[keyword] = count
        
        print("\n키워드별 매칭 개수:")
        for keyword, count in keyword_counts.items():
            print(f"  {keyword}: {count}개")
        
        # 처음 5개 뉴스 미리보기
        print("\n처음 5개 뉴스 미리보기:")
        for idx, row in filtered_df.head(5).iterrows():
            print(f"{idx + 1}. {row[title_column][:60]}...")
            print(f"   작성일: {row.get('created_at', 'N/A')}")
            print()
            
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


def filter_opinion_only():
    """투자 의견 필터링만 실행하는 함수"""
    print("=== 투자 의견 필터링만 실행 ===")
    filter_opinion_news()


if __name__ == '__main__':
    # 기본 실행
    main()
    
    # 개별 실행 옵션들 (주석 해제하여 사용)
    # update_excel_only()  # Excel 파일만 업데이트
    # filter_opinion_only()  # 투자 의견 필터링만 실행
