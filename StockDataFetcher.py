"""
주식 관련 데이터를 가져오는 클래스
SaveTicker API를 사용하여 태그 정보와 뉴스 데이터를 가져옵니다.
"""

import requests
import pandas as pd
import json
import time
from typing import Dict, List, Optional, Generator
from datetime import datetime


class StockDataFetcher:
    """주식 관련 데이터를 가져오는 클래스"""
    
    def __init__(self, base_url: str = "https://api.saveticker.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'StockDataFetcher/1.0',
            'Accept': 'application/json'
        })
    
    def fetch_tags(self) -> Optional[Dict]:
        """태그 목록을 가져옵니다."""
        try:
            url = f"{self.base_url}/api/tags/list"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API 호출 중 오류 발생: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류: {e}")
            return None
    
    def get_ticker_tags(self) -> pd.DataFrame:
        """티커 태그들만 필터링하여 DataFrame으로 반환합니다."""
        data = self.fetch_tags()
        if not data or 'tags' not in data:
            return pd.DataFrame()
        
        tags = data['tags']
        ticker_tags = [tag for tag in tags if tag.get('is_ticker', False)]
        
        df = pd.DataFrame(ticker_tags)
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['updated_at'] = pd.to_datetime(df['updated_at'])
        
        return df
    
    def get_category_tags(self) -> pd.DataFrame:
        """카테고리 태그들만 필터링하여 DataFrame으로 반환합니다."""
        data = self.fetch_tags()
        if not data or 'tags' not in data:
            return pd.DataFrame()
        
        tags = data['tags']
        category_tags = [tag for tag in tags if not tag.get('is_ticker', False)]
        
        df = pd.DataFrame(category_tags)
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['updated_at'] = pd.to_datetime(df['updated_at'])
        
        return df
    
    def get_required_tags(self) -> pd.DataFrame:
        """필수 태그들만 필터링하여 DataFrame으로 반환합니다."""
        data = self.fetch_tags()
        if not data or 'tags' not in data:
            return pd.DataFrame()
        
        tags = data['tags']
        required_tags = [tag for tag in tags if tag.get('is_required', False)]
        
        df = pd.DataFrame(required_tags)
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['updated_at'] = pd.to_datetime(df['updated_at'])
        
        return df
    
    def analyze_tags(self) -> Dict:
        """태그 데이터를 분석하여 통계 정보를 반환합니다."""
        data = self.fetch_tags()
        if not data or 'tags' not in data:
            return {}
        
        tags = data['tags']
        total_tags = len(tags)
        ticker_tags = len([tag for tag in tags if tag.get('is_ticker', False)])
        category_tags = total_tags - ticker_tags
        required_tags = len([tag for tag in tags if tag.get('is_required', False)])
        
        return {
            'total_tags': total_tags,
            'ticker_tags': ticker_tags,
            'category_tags': category_tags,
            'required_tags': required_tags,
            'optional_tags': total_tags - required_tags
        }
    
    def fetch_news_page(self, page: int = 1, page_size: int = 20, sort: str = "created_at_desc") -> Optional[Dict]:
        """특정 페이지의 뉴스 목록을 가져옵니다."""
        try:
            url = f"{self.base_url}/api/news/list"
            params = {
                'page': page,
                'page_size': page_size,
                'sort': sort
            }
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"뉴스 API 호출 중 오류 발생 (페이지 {page}): {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류 (페이지 {page}): {e}")
            return None
    
    def fetch_all_news(self, page_size: int = 20, delay: float = 0.1) -> Generator[Dict, None, None]:
        """전체 뉴스를 페이지별로 가져오는 제너레이터입니다."""
        page = 1
        total_count = None
        
        while True:

            print(f"페이지 {page} 로딩 중...")
            data = self.fetch_news_page(page, page_size)
            
            if not data:
                print(f"페이지 {page}에서 데이터를 가져올 수 없습니다.")
                break
            
            # 첫 번째 페이지에서 total_count 확인
            if total_count is None:
                total_count = data.get('total_count', 0)
                print(f"전체 뉴스 수: {total_count}")
            
            news_list = data.get('news_list', [])
            if not news_list:
                print("더 이상 가져올 뉴스가 없습니다.")
                break
            
            # 각 뉴스 아이템을 yield
            for news in news_list:
                yield news
            
            # 마지막 페이지인지 확인
            if len(news_list) < page_size:
                print("마지막 페이지에 도달했습니다.")
                break
            
            page += 1
            
            # API 호출 간격 조절 (서버 부하 방지)
            if delay > 0:
                time.sleep(delay)
    
    def get_all_news_dataframe(self, page_size: int = 20, delay: float = 0.1) -> pd.DataFrame:
        """전체 뉴스를 DataFrame으로 반환합니다."""
        all_news = []
        
        for news in self.fetch_all_news(page_size, delay):
            all_news.append(news)
        
        if not all_news:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_news)
        
        # 날짜 컬럼 변환
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        return df
    
    def get_news_by_tag(self, tag_name: str, page_size: int = 20, delay: float = 0.1) -> pd.DataFrame:
        """특정 태그가 포함된 뉴스만 필터링하여 반환합니다."""
        all_news_df = self.get_all_news_dataframe(page_size, delay)
        
        if all_news_df.empty:
            return pd.DataFrame()
        
        # tag_names 컬럼에서 특정 태그가 포함된 뉴스 필터링
        filtered_df = all_news_df[
            all_news_df['tag_names'].apply(
                lambda tags: tag_name in tags if isinstance(tags, list) else False
            )
        ]
        
        return filtered_df
    
    def analyze_news(self, page_size: int = 20, delay: float = 0.1) -> Dict:
        """뉴스 데이터를 분석하여 통계 정보를 반환합니다."""
        df = self.get_all_news_dataframe(page_size, delay)
        
        if df.empty:
            return {}
        
        # 기본 통계
        total_news = len(df)
        
        # 태그별 통계
        all_tags = []
        for tags in df['tag_names'].dropna():
            if isinstance(tags, list):
                all_tags.extend(tags)
        
        tag_counts = pd.Series(all_tags).value_counts()
        
        # 날짜별 통계
        if 'created_at' in df.columns:
            df['date'] = df['created_at'].dt.date
            daily_counts = df['date'].value_counts().sort_index()
        else:
            daily_counts = pd.Series()
        
        # 조회수 통계
        view_stats = df['view_count'].describe() if 'view_count' in df.columns else pd.Series()
        
        # 좋아요 통계
        like_stats = df['like_stats'].apply(
            lambda x: x.get('like_count', 0) if isinstance(x, dict) else 0
        ).describe() if 'like_stats' in df.columns else pd.Series()
        
        return {
            'total_news': total_news,
            'top_tags': tag_counts.head(10).to_dict(),
            'daily_counts': daily_counts.to_dict(),
            'view_stats': view_stats.to_dict() if not view_stats.empty else {},
            'like_stats': like_stats.to_dict() if not like_stats.empty else {}
        }
