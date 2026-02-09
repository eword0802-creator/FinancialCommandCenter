"""
Pre-Market Command Center v8.2
Institutional-Grade Market Prep Dashboard
AI Expert Analysis · Earnings Intelligence · Whale Tracker · Support/Resistance

v8.2 Updates:
- Fixed futures/indices data loading (NQ=F, ES=F, etc.)
- Enhanced institutional activity analysis with:
  * Smart Money Score (0-100)
  * Squeeze Potential indicator
  * Accumulation/Distribution phase detection
  * Institutional Momentum signals
  * Enhanced dark pool sentiment analysis
- Improved chart data cleaning (more lenient, handles edge cases)
- Added None checks for chart rendering
- All indicators preserved: RSI, MACD, Bollinger Bands, Volume, MAs

v8.1 Updates:
- Code quality improvements and bug fixes
- Enhanced error handling
- Optimized caching strategy
- Removed duplicate CSS
- Added safe division utilities

Features:
- 🐋 Institutional Activity & Whale Tracker (enhanced)
- 📅 Earnings Center (calendar, analyzer, news)
- 📰 News Flow Analysis in Market Brief
- 📈 Advanced Options Screener with time-of-day weighting
- 🎯 AI-generated macro analysis
- 🧠 Smart Money indicators
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import warnings
from typing import Optional, Tuple, List, Dict, Any
warnings.filterwarnings('ignore')

# === UTILITY FUNCTIONS ===

def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero or invalid."""
    try:
        if denominator is None or denominator == 0 or pd.isna(denominator):
            return default
        result = numerator / denominator
        return default if pd.isna(result) else result
    except (TypeError, ZeroDivisionError):
        return default

def safe_pct_change(current: float, previous: float, default: float = 0.0) -> float:
    """Calculate percentage change safely."""
    return safe_div((current - previous), previous, default) * 100

def safe_get(data: dict, key: str, default: Any = None) -> Any:
    """Safely get a value from a dict, handling None and NaN."""
    try:
        val = data.get(key, default)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return val
    except (AttributeError, TypeError):
        return default

def format_large_number(num: float, precision: int = 2) -> str:
    """Format large numbers with B/M/K suffixes."""
    if num is None or pd.isna(num):
        return "N/A"
    try:
        num = float(num)
        if abs(num) >= 1e12:
            return f"${num/1e12:.{precision}f}T"
        elif abs(num) >= 1e9:
            return f"${num/1e9:.{precision}f}B"
        elif abs(num) >= 1e6:
            return f"${num/1e6:.{precision}f}M"
        elif abs(num) >= 1e3:
            return f"${num/1e3:.{precision}f}K"
        else:
            return f"${num:.{precision}f}"
    except (ValueError, TypeError):
        return "N/A"

# === STREAMLIT CONFIG ===

st.set_page_config(page_title="Pre-Market Command Center", page_icon="📈", layout="wide", initial_sidebar_state="collapsed")

if 'selected_stock' not in st.session_state: st.session_state.selected_stock = None
if 'show_stock_report' not in st.session_state: st.session_state.show_stock_report = False
if 'chart_tf' not in st.session_state: st.session_state.chart_tf = '5D'

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap');
    .stApp { background: linear-gradient(180deg, #0d1117 0%, #161b22 100%); }
    .main-title { font-family: 'Inter', sans-serif; font-size: 2.5rem; font-weight: 700; color: #ffffff; margin-bottom: 0; }
    .subtitle { font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; color: #8b949e; margin-bottom: 1.5rem; }
    .metric-card { background: linear-gradient(145deg, #21262d 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.25rem; margin: 0.5rem 0; transition: all 0.2s; }
    .metric-card:hover { border-color: #58a6ff; }
    .metric-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.1em; }
    .metric-value { font-family: 'Inter', sans-serif; font-size: 1.75rem; font-weight: 600; color: #ffffff; }
    .positive { color: #3fb950 !important; }
    .negative { color: #f85149 !important; }
    .neutral { color: #8b949e !important; }
    .summary-section { background: linear-gradient(145deg, #161b22 0%, #0d1117 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.5rem; margin: 1rem 0; }
    .summary-header { font-family: 'Inter', sans-serif; font-size: 1.1rem; font-weight: 600; color: #58a6ff; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid #30363d; }
    .news-item { background: #21262d; border-left: 3px solid #58a6ff; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: 0 8px 8px 0; }
    .news-item:hover { background: #30363d; }
    .news-title { font-family: 'Inter', sans-serif; font-size: 0.9rem; color: #ffffff; margin-bottom: 0.25rem; }
    .news-meta { font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #8b949e; }
    .sentiment-bullish { background: linear-gradient(90deg, #238636 0%, #2ea043 100%); color: white; padding: 0.5rem 1rem; border-radius: 8px; font-weight: 600; }
    .sentiment-bearish { background: linear-gradient(90deg, #da3633 0%, #f85149 100%); color: white; padding: 0.5rem 1rem; border-radius: 8px; font-weight: 600; }
    .sentiment-neutral { background: linear-gradient(90deg, #6e7681 0%, #8b949e 100%); color: white; padding: 0.5rem 1rem; border-radius: 8px; font-weight: 600; }
    .event-card { background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 1rem; margin: 0.5rem 0; }
    .event-time { font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #58a6ff; margin-bottom: 0.25rem; }
    .event-title { font-family: 'Inter', sans-serif; font-size: 0.9rem; color: #ffffff; }
    .event-impact-high { border-left: 3px solid #f85149; }
    .event-impact-medium { border-left: 3px solid #d29922; }
    .event-impact-low { border-left: 3px solid #3fb950; }
    .calls-card { background: linear-gradient(145deg, #0d2818 0%, #0d1117 100%); border: 1px solid #238636; border-radius: 12px; padding: 1.25rem; }
    .puts-card { background: linear-gradient(145deg, #2d1215 0%, #0d1117 100%); border: 1px solid #da3633; border-radius: 12px; padding: 1.25rem; }
    .options-pick-card { background: linear-gradient(145deg, #21262d 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.25rem; margin: 0.75rem 0; }
    .pick-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; padding-bottom: 0.5rem; border-bottom: 1px solid #30363d; }
    .pick-symbol { font-family: 'IBM Plex Mono', monospace; font-size: 1.3rem; font-weight: 700; color: #ffffff; }
    .pick-score { font-family: 'IBM Plex Mono', monospace; font-size: 1rem; padding: 0.3rem 0.8rem; border-radius: 6px; font-weight: 600; }
    .score-excellent { background: linear-gradient(135deg, #238636 0%, #2ea043 100%); color: #ffffff; }
    .score-good { background: linear-gradient(135deg, #1f6feb 0%, #388bfd 100%); color: #ffffff; }
    .score-fair { background: linear-gradient(135deg, #9e6a03 0%, #d29922 100%); color: #ffffff; }
    .score-weak { background: linear-gradient(135deg, #6e7681 0%, #8b949e 100%); color: #ffffff; }
    .timestamp { font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #6e7681; text-align: right; }
    .market-status { display: inline-block; padding: 0.4rem 1rem; border-radius: 20px; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; font-weight: 500; }
    .status-premarket { background: #9e6a0320; color: #d29922; border: 1px solid #9e6a03; }
    .status-open { background: #23863620; color: #3fb950; border: 1px solid #238636; }
    .status-afterhours { background: #a371f720; color: #a371f7; border: 1px solid #8957e5; }
    .status-closed { background: #6e768120; color: #8b949e; border: 1px solid #6e7681; }
    .fear-greed-bar { height: 8px; border-radius: 4px; background: linear-gradient(90deg, #f85149 0%, #d29922 50%, #3fb950 100%); position: relative; margin: 1rem 0; }
    .fear-greed-indicator { width: 4px; height: 16px; background: white; position: absolute; top: -4px; border-radius: 2px; }
    .report-section { background: linear-gradient(145deg, #1c2128 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.5rem; margin: 1rem 0; }
    .earnings-inline { border-left: 3px solid #d29922; }
    .key-takeaway { background: #21262d; border-left: 3px solid #58a6ff; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: 0 8px 8px 0; font-size: 0.9rem; color: #c9d1d9; }
    .analyst-rating { display: inline-block; padding: 0.3rem 0.8rem; border-radius: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; font-weight: 600; }
    .rating-buy { background: #238636; color: white; }
    .rating-hold { background: #9e6a03; color: white; }
    .rating-sell { background: #da3633; color: white; }
    .company-stat { text-align: center; padding: 0.75rem; }
    .stat-value { font-family: 'Inter', sans-serif; font-size: 1.4rem; font-weight: 700; color: #ffffff; }
    .stat-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem; color: #8b949e; text-transform: uppercase; }
    .econ-indicator { background: #21262d; border-radius: 8px; padding: 0.75rem; margin: 0.25rem; text-align: center; }
    .econ-value { font-size: 1.1rem; font-weight: 600; color: #ffffff; }
    .econ-label { font-size: 0.65rem; color: #8b949e; }
    .econ-change { font-size: 0.75rem; }
    .risk-item { padding: 0.5rem; margin: 0.25rem 0; border-radius: 6px; font-size: 0.85rem; }
    .risk-high { background: #da363320; border-left: 3px solid #f85149; }
    .risk-medium { background: #9e6a0320; border-left: 3px solid #d29922; }
    .opportunity-item { padding: 0.5rem; margin: 0.25rem 0; border-radius: 6px; font-size: 0.85rem; background: #1f6feb20; border-left: 3px solid #58a6ff; }
    .signal-card { background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem; margin: 0.5rem 0; }
    .signal-bullish { border-left: 3px solid #3fb950; }
    .signal-bearish { border-left: 3px solid #f85149; }
    .signal-neutral { border-left: 3px solid #8b949e; }
    .signal-title { font-weight: 600; color: #ffffff; font-size: 0.9rem; }
    .signal-detail { color: #8b949e; font-size: 0.8rem; margin-top: 0.25rem; }
    .sr-level { display: flex; justify-content: space-between; padding: 0.4rem 0.75rem; margin: 0.25rem 0; border-radius: 6px; font-size: 0.85rem; }
    .resistance-level { background: #da363320; border-left: 3px solid #f85149; }
    .support-level { background: #23863620; border-left: 3px solid #3fb950; }
    .current-price-level { background: #1f6feb30; border-left: 3px solid #58a6ff; font-weight: 600; }
    .expert-analysis { background: linear-gradient(145deg, #1a1f2e 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.5rem; margin: 1rem 0; }
    .expert-header { font-family: 'Inter', sans-serif; font-size: 1.1rem; font-weight: 600; color: #a371f7; margin-bottom: 1rem; }
    .expert-verdict { font-size: 1.5rem; font-weight: 700; margin: 0.5rem 0; }
    .expert-text { color: #c9d1d9; line-height: 1.6; font-size: 0.95rem; }
    .earnings-card { background: linear-gradient(145deg, #21262d 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 10px; padding: 1rem; margin: 0.5rem 0; transition: all 0.2s; }
    .earnings-card:hover { border-color: #a371f7; }
    .earnings-beat { border-left: 4px solid #3fb950; }
    .earnings-miss { border-left: 4px solid #f85149; }
    .earnings-meet { border-left: 4px solid #d29922; }
    .whale-signal { background: rgba(163,113,247,0.1); border: 1px solid rgba(163,113,247,0.3); border-radius: 8px; padding: 0.5rem 1rem; margin: 0.25rem; display: inline-block; }
</style>
""", unsafe_allow_html=True)

FUTURES_SYMBOLS = {"S&P 500": "ES=F", "Nasdaq 100": "NQ=F", "Dow Jones": "YM=F", "Russell 2000": "RTY=F", "Crude Oil": "CL=F", "Gold": "GC=F", "Silver": "SI=F", "Natural Gas": "NG=F", "VIX": "^VIX", "Dollar Index": "DX=F", "10Y Treasury": "^TNX", "Bitcoin": "BTC-USD"}
SECTOR_ETFS = {"Technology": {"symbol": "XLK", "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "AMD", "CRM", "ORCL", "ADBE"]}, "Financial": {"symbol": "XLF", "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW"]}, "Energy": {"symbol": "XLE", "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO"]}, "Healthcare": {"symbol": "XLV", "stocks": ["UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT"]}, "Consumer Disc.": {"symbol": "XLY", "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX"]}, "Consumer Staples": {"symbol": "XLP", "stocks": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL"]}, "Industrials": {"symbol": "XLI", "stocks": ["CAT", "GE", "RTX", "UNP", "BA", "HON", "DE", "LMT"]}, "Materials": {"symbol": "XLB", "stocks": ["LIN", "APD", "SHW", "FCX", "NEM", "NUE", "DOW", "ECL"]}, "Utilities": {"symbol": "XLU", "stocks": ["NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL"]}, "Real Estate": {"symbol": "XLRE", "stocks": ["AMT", "PLD", "CCI", "EQIX", "SPG", "PSA", "O", "WELL"]}, "Communication": {"symbol": "XLC", "stocks": ["META", "GOOGL", "NFLX", "DIS", "CMCSA", "VZ", "T", "TMUS"]}}
FINANCE_CATEGORIES = {"Major Banks": ["JPM", "BAC", "WFC", "C", "USB", "PNC"], "Investment Banks": ["GS", "MS", "SCHW", "RJF"], "Insurance": ["BRK-B", "AIG", "MET", "PRU", "AFL", "TRV"], "Payments": ["V", "MA", "AXP", "PYPL", "SQ"], "Asset Managers": ["BLK", "BX", "KKR", "APO", "TROW"], "Fintech": ["PYPL", "SQ", "SOFI", "HOOD", "COIN"]}
OPTIONS_UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "GLD", "SLV", "TLT", "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO", "JPM", "BAC", "GS", "MS", "C", "WFC", "XOM", "CVX", "COP", "SLB", "UNH", "JNJ", "LLY", "PFE", "ABBV", "HD", "MCD", "NKE", "SBUX", "COST", "NFLX", "CRM", "ORCL", "V", "MA", "DIS"]
GLOBAL_INDICES = {"FTSE 100": "^FTSE", "DAX": "^GDAXI", "CAC 40": "^FCHI", "Nikkei 225": "^N225", "Hang Seng": "^HSI", "Shanghai": "000001.SS"}
NEWS_FEEDS = {"Reuters": "https://feeds.reuters.com/reuters/businessNews", "CNBC": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "MarketWatch": "http://feeds.marketwatch.com/marketwatch/topstories"}
TIMEFRAMES = {"1D": ("1d", "5m"), "5D": ("5d", "15m"), "1M": ("1mo", "1h"), "3M": ("3mo", "1d"), "6M": ("6mo", "1d"), "1Y": ("1y", "1d"), "YTD": ("ytd", "1d")}

# === TECHNICAL ANALYSIS CONSTANTS ===
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
RSI_BULLISH = 60
RSI_BEARISH = 40
RSI_PERIOD = 14

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2

MA_SHORT = 20
MA_LONG = 50

VIX_HIGH = 25
VIX_ELEVATED = 20
VIX_LOW = 15

VOLUME_HIGH_MULTIPLIER = 2.0
VOLUME_EXTREME_MULTIPLIER = 3.0

SHORT_INTEREST_HIGH = 20
SHORT_INTEREST_ELEVATED = 10

INSTITUTIONAL_OWNERSHIP_HIGH = 70
INSTITUTIONAL_OWNERSHIP_VERY_HIGH = 90
INSTITUTIONAL_OWNERSHIP_LOW = 20

# Cache TTLs (in seconds)
CACHE_SHORT = 120    # 2 minutes - real-time data
CACHE_MEDIUM = 300   # 5 minutes - moderate updates
CACHE_LONG = 600     # 10 minutes - slower updates
CACHE_VERY_LONG = 1800  # 30 minutes - rarely changing data

def get_market_status():
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    premarket, market_open, market_close, afterhours = now.replace(hour=4, minute=0), now.replace(hour=9, minute=30), now.replace(hour=16, minute=0), now.replace(hour=20, minute=0)
    if now.weekday() >= 5: return "closed", "Weekend", "Opens Monday"
    if now < premarket: return "closed", "Closed", f"Pre-market in {(premarket-now).seconds//3600}h"
    elif now < market_open: return "premarket", "Pre-Market", f"Opens in {(market_open-now).seconds//3600}h {((market_open-now).seconds%3600)//60}m"
    elif now < market_close: return "open", "Market Open", f"Closes in {(market_close-now).seconds//3600}h {((market_close-now).seconds%3600)//60}m"
    elif now < afterhours: return "afterhours", "After Hours", "Until 8PM"
    return "closed", "Closed", "Opens 4AM"

@st.cache_data(ttl=CACHE_SHORT)
def fetch_stock_data(symbol: str, period: str = "5d", interval: str = "15m") -> Tuple[Optional[pd.DataFrame], dict]:
    """Fetch stock data with proper error handling."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval, prepost=False)
        info = ticker.info or {}
        return hist, info
    except requests.exceptions.RequestException as e:
        # Network errors
        return None, {}
    except Exception as e:
        # Other yfinance errors
        return None, {}

@st.cache_data(ttl=180)
def fetch_rss_news(feed_url, limit=10):
    try:
        resp = requests.get(feed_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            return [{'title': item.find('title').text, 'link': item.find('link').text if item.find('link') is not None else '', 'published': item.find('pubDate').text if item.find('pubDate') is not None else ''} for item in root.findall('.//item')[:limit] if item.find('title') is not None]
    except: pass
    return []

@st.cache_data(ttl=180)
def fetch_all_news():
    all_news = []
    for source, url in NEWS_FEEDS.items():
        for item in fetch_rss_news(url, 5):
            item['source'] = source
            all_news.append(item)
    try:
        for sym in ["^GSPC", "SPY"]:
            yf_news = yf.Ticker(sym).news
            if yf_news:
                for item in yf_news[:3]:
                    all_news.append({'title': item.get('title', ''), 'link': item.get('link', ''), 'source': item.get('publisher', 'Yahoo'), 'published': datetime.fromtimestamp(item.get('providerPublishTime', 0)).strftime('%H:%M') if item.get('providerPublishTime') else ''})
    except: pass
    seen = set()
    return [n for n in all_news if n.get('title', '')[:40] not in seen and not seen.add(n.get('title', '')[:40])][:25]

@st.cache_data(ttl=300)
def fetch_stock_news_direct(symbol):
    """Fetch news directly for a stock using multiple methods."""
    news_items = []
    
    # Method 1: Try yfinance
    try:
        ticker = yf.Ticker(symbol)
        raw_news = ticker.news
        if raw_news and isinstance(raw_news, list):
            for item in raw_news[:10]:
                if isinstance(item, dict):
                    title = item.get('title', item.get('headline', ''))
                    if title:
                        news_items.append({
                            'title': title,
                            'link': item.get('link', item.get('url', '')),
                            'publisher': item.get('publisher', item.get('source', 'Yahoo Finance')),
                            'providerPublishTime': item.get('providerPublishTime', item.get('publishTime', 0)),
                        })
    except:
        pass
    
    # Method 2: If no news, try Google News RSS
    if len(news_items) < 3:
        try:
            search_term = f"{symbol}+stock"
            rss_url = f"https://news.google.com/rss/search?q={search_term}&hl=en-US&gl=US&ceid=US:en"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            resp = requests.get(rss_url, headers=headers, timeout=8)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for item in root.findall('.//item')[:8]:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_elem = item.find('pubDate')
                    source_elem = item.find('source')
                    
                    if title_elem is not None and title_elem.text:
                        # Check if not duplicate
                        title = title_elem.text
                        if not any(n['title'] == title for n in news_items):
                            news_items.append({
                                'title': title,
                                'link': link_elem.text if link_elem is not None else '',
                                'publisher': source_elem.text if source_elem is not None else 'Google News',
                                'providerPublishTime': 0,
                                'published': pub_elem.text if pub_elem is not None else ''
                            })
        except:
            pass
    
    # Method 3: Try Finviz RSS as another fallback
    if len(news_items) < 3:
        try:
            finviz_url = f"https://finviz.com/quote.ashx?t={symbol}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            resp = requests.get(finviz_url, headers=headers, timeout=8)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                news_table = soup.find('table', {'id': 'news-table'})
                if news_table:
                    rows = news_table.find_all('tr')[:8]
                    for row in rows:
                        link = row.find('a')
                        if link:
                            title = link.text.strip()
                            href = link.get('href', '')
                            source_span = row.find('span')
                            source = source_span.text.strip() if source_span else 'Finviz'
                            
                            if title and not any(n['title'] == title for n in news_items):
                                news_items.append({
                                    'title': title,
                                    'link': href,
                                    'publisher': source,
                                    'providerPublishTime': 0,
                                    'published': ''
                                })
        except:
            pass
    
    return news_items[:10]

def fetch_finviz_insider_data(symbol):
    """
    Fetch insider trading data from Finviz as a fallback/additional source.
    Returns a list of insider transactions.
    """
    insider_data = []
    
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml',
        }
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Find insider trading table
            insider_table = soup.find('table', class_='body-table')
            if not insider_table:
                # Try alternate table selector
                tables = soup.find_all('table')
                for table in tables:
                    if 'Insider Trading' in str(table) or 'insider' in str(table).lower():
                        insider_table = table
                        break
            
            if insider_table:
                rows = insider_table.find_all('tr')[1:]  # Skip header
                for row in rows[:10]:
                    cols = row.find_all('td')
                    if len(cols) >= 6:
                        try:
                            insider_data.append({
                                'owner': cols[0].text.strip()[:25],
                                'relationship': cols[1].text.strip() if len(cols) > 1 else '',
                                'date': cols[2].text.strip() if len(cols) > 2 else '',
                                'transaction': cols[3].text.strip() if len(cols) > 3 else '',
                                'cost': cols[4].text.strip() if len(cols) > 4 else '',
                                'shares': cols[5].text.strip() if len(cols) > 5 else '',
                                'value': cols[6].text.strip() if len(cols) > 6 else '',
                            })
                        except:
                            continue
            
            # Also try to get additional metrics
            metrics = {}
            snapshot_table = soup.find('table', class_='snapshot-table2')
            if snapshot_table:
                rows = snapshot_table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    for i in range(0, len(cols) - 1, 2):
                        try:
                            label = cols[i].text.strip()
                            value = cols[i+1].text.strip()
                            if label and value:
                                metrics[label] = value
                        except:
                            continue
            
            return {'transactions': insider_data, 'metrics': metrics}
    except:
        pass
    
    return {'transactions': [], 'metrics': {}}

@st.cache_data(ttl=300)
def fetch_comprehensive_data(symbol):
    """
    Fetch comprehensive data for any symbol type (stocks, ETFs, futures, indices, crypto).
    Handles different instrument types gracefully with appropriate fallbacks.
    """
    data = {
        'info': {}, 
        'hist_1d': None, 
        'hist_5d': None, 
        'hist_1mo': None, 
        'hist_3mo': None, 
        'hist_6mo': None, 
        'hist_1y': None, 
        'news': [], 
        'earnings': None, 
        'recommendations': None, 
        'calendar': None, 
        'holders': None,
        'insider_transactions': None,
        'insider_roster': None,
        'major_holders': None,
        'options_data': None,
        'finviz_data': {},
    }
    
    try:
        ticker = yf.Ticker(symbol)
        
        # === FETCH INFO (with fallback) ===
        try:
            info = ticker.info
            if info and isinstance(info, dict):
                data['info'] = info
        except Exception:
            data['info'] = {'symbol': symbol}
        
        # === FETCH HISTORICAL DATA (most important - try multiple timeframes) ===
        # This is the critical part - we need at least some price data
        timeframes = [
            ('hist_5d', '5d', '15m'),
            ('hist_1d', '1d', '5m'),
            ('hist_1mo', '1mo', '1h'),
            ('hist_3mo', '3mo', '1d'),
            ('hist_6mo', '6mo', '1d'),
            ('hist_1y', '1y', '1d'),
        ]
        
        has_any_data = False
        for key, period, interval in timeframes:
            try:
                hist = ticker.history(period=period, interval=interval, prepost=False)
                if hist is not None and not hist.empty and len(hist) >= 2:
                    data[key] = hist
                    has_any_data = True
            except Exception:
                continue
        
        # If no data at all, try one more time with simpler params
        if not has_any_data:
            try:
                hist = ticker.history(period="5d")
                if hist is not None and not hist.empty:
                    data['hist_5d'] = hist
                    has_any_data = True
            except Exception:
                pass
        
        # Return None only if we have absolutely no price data
        if not has_any_data:
            return None
        
        # Detect instrument type for conditional data fetching
        quote_type = data['info'].get('quoteType', '').upper()
        is_stock = quote_type in ['EQUITY', ''] and '=F' not in symbol and not symbol.startswith('^')
        is_etf = quote_type == 'ETF'
        is_future = '=F' in symbol
        is_index = symbol.startswith('^')
        is_crypto = quote_type == 'CRYPTOCURRENCY' or '-USD' in symbol
        
        # === FETCH NEWS (works for most symbols) ===
        try:
            data['news'] = fetch_stock_news_direct(symbol)
        except Exception:
            data['news'] = []
        
        # === STOCK-SPECIFIC DATA (skip for futures/indices) ===
        if is_stock or is_etf:
            # Earnings data
            for method in ['earnings_history', 'quarterly_earnings', 'earnings_dates']:
                if data['earnings'] is not None:
                    break
                try:
                    earnings_data = getattr(ticker, method, None)
                    if earnings_data is not None and hasattr(earnings_data, 'empty') and not earnings_data.empty:
                        if method == 'earnings_dates' and 'Reported EPS' in earnings_data.columns:
                            earnings_data = earnings_data.rename(columns={'Reported EPS': 'epsActual', 'EPS Estimate': 'epsEstimate'})
                        data['earnings'] = earnings_data
                except Exception:
                    continue
            
            # Recommendations
            for method in ['recommendations', 'recommendations_summary']:
                if data['recommendations'] is not None:
                    break
                try:
                    recs = getattr(ticker, method, None)
                    if recs is not None and hasattr(recs, 'empty') and not recs.empty:
                        data['recommendations'] = recs.tail(30) if len(recs) > 30 else recs
                except Exception:
                    continue
            
            # Calendar
            try:
                data['calendar'] = ticker.calendar
            except Exception:
                pass
            
            # Holders data
            try:
                h = ticker.institutional_holders
                if h is not None and not h.empty:
                    data['holders'] = h
            except Exception:
                pass
            
            # === INSTITUTIONAL ACTIVITY DATA ===
            try:
                insider_txns = ticker.insider_transactions
                if insider_txns is not None and not insider_txns.empty:
                    data['insider_transactions'] = insider_txns
            except Exception:
                pass
            
            try:
                insider_roster = ticker.insider_roster_holders
                if insider_roster is not None and not insider_roster.empty:
                    data['insider_roster'] = insider_roster
            except Exception:
                pass
            
            try:
                major = ticker.major_holders
                if major is not None and not major.empty:
                    data['major_holders'] = major
            except Exception:
                pass
            
            # Options data
            try:
                if ticker.options:
                    nearest_exp = ticker.options[0]
                    chain = ticker.option_chain(nearest_exp)
                    data['options_data'] = {
                        'expiration': nearest_exp,
                        'calls': chain.calls if hasattr(chain, 'calls') else None,
                        'puts': chain.puts if hasattr(chain, 'puts') else None
                    }
            except Exception:
                pass
            
            # Finviz data (stocks only)
            if is_stock:
                try:
                    finviz_data = fetch_finviz_insider_data(symbol)
                    if finviz_data:
                        data['finviz_data'] = finviz_data
                except Exception:
                    pass
        
        return data
        
    except Exception as e:
        # Last resort - return minimal data structure
        return None

@st.cache_data(ttl=600)
def fetch_economic_indicators():
    indicators = {}
    for name, sym in [("10Y Treasury", "^TNX"), ("5Y Treasury", "^FVX"), ("VIX", "^VIX"), ("Dollar Index", "DX=F")]:
        try:
            hist = yf.Ticker(sym).history(period="5d")
            if not hist.empty:
                indicators[name] = {'value': hist['Close'].iloc[-1], 'prev': hist['Close'].iloc[-2] if len(hist) > 1 else hist['Close'].iloc[-1], 'unit': '%' if 'Treasury' in name else ''}
        except: pass
    return indicators

def calculate_rsi(prices: pd.Series, period: int = RSI_PERIOD) -> Tuple[float, str]:
    """Calculate RSI with proper handling of edge cases."""
    if len(prices) < period + 1: 
        return 50.0, "neutral"
    
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    
    # Safe division - avoid divide by zero
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
    rsi_val = 100 - (100 / (1 + rs)) if rs != 100 else 100
    
    val = rsi_val if not pd.isna(rsi_val) else 50.0
    
    if val > RSI_OVERBOUGHT:
        cond = "overbought"
    elif val < RSI_OVERSOLD:
        cond = "oversold"
    elif val > RSI_BULLISH:
        cond = "bullish"
    elif val < RSI_BEARISH:
        cond = "bearish"
    else:
        cond = "neutral"
    
    return val, cond

def calculate_macd(prices: pd.Series) -> Tuple[float, float, float, str]:
    """Calculate MACD with proper handling."""
    min_periods = MACD_SLOW + MACD_SIGNAL
    if len(prices) < min_periods: 
        return 0.0, 0.0, 0.0, "neutral"
    
    ema_fast = prices.ewm(span=MACD_FAST).mean()
    ema_slow = prices.ewm(span=MACD_SLOW).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL).mean()
    hist = macd_line - signal_line
    
    current_hist = hist.iloc[-1]
    prev_hist = hist.iloc[-2]
    
    if current_hist > 0 and current_hist > prev_hist:
        sig = "bullish"
    elif current_hist < 0 and current_hist < prev_hist:
        sig = "bearish"
    else:
        sig = "neutral"
    
    return macd_line.iloc[-1], signal_line.iloc[-1], current_hist, sig

def calculate_bollinger(prices: pd.Series, period: int = BOLLINGER_PERIOD) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """Calculate Bollinger Bands with proper handling."""
    if len(prices) < period: 
        return None, None, None, "neutral"
    
    sma = prices.rolling(period).mean()
    std = prices.rolling(period).std()
    upper = sma + (std * BOLLINGER_STD)
    lower = sma - (std * BOLLINGER_STD)
    curr = prices.iloc[-1]
    
    if curr > upper.iloc[-1]:
        pos = "above_upper"
    elif curr < lower.iloc[-1]:
        pos = "below_lower"
    elif curr > sma.iloc[-1]:
        pos = "upper_half"
    else:
        pos = "lower_half"
    
    return upper.iloc[-1], sma.iloc[-1], lower.iloc[-1], pos

def calculate_support_resistance(hist, current_price):
    """Calculate key support and resistance levels using multiple methods."""
    if hist is None or len(hist) < 20:
        return [], []
    
    levels = {'support': [], 'resistance': []}
    highs, lows, closes = hist['High'].values, hist['Low'].values, hist['Close'].values
    
    # Method 1: Pivot Points (Daily)
    if len(hist) >= 2:
        prev_high, prev_low, prev_close = highs[-2], lows[-2], closes[-2]
        pivot = (prev_high + prev_low + prev_close) / 3
        r1 = 2 * pivot - prev_low
        r2 = pivot + (prev_high - prev_low)
        s1 = 2 * pivot - prev_high
        s2 = pivot - (prev_high - prev_low)
        
        if r1 > current_price: levels['resistance'].append(('Pivot R1', r1))
        if r2 > current_price: levels['resistance'].append(('Pivot R2', r2))
        if s1 < current_price: levels['support'].append(('Pivot S1', s1))
        if s2 < current_price: levels['support'].append(('Pivot S2', s2))
    
    # Method 2: Recent Swing Highs/Lows
    for i in range(5, len(highs) - 5):
        if highs[i] == max(highs[i-5:i+5]):
            if highs[i] > current_price:
                levels['resistance'].append(('Swing High', highs[i]))
            else:
                levels['support'].append(('Prior Resistance', highs[i]))
        if lows[i] == min(lows[i-5:i+5]):
            if lows[i] < current_price:
                levels['support'].append(('Swing Low', lows[i]))
            else:
                levels['resistance'].append(('Prior Support', lows[i]))
    
    # Method 3: Moving Averages as dynamic S/R
    if len(closes) >= 50:
        ma20 = pd.Series(closes).rolling(20).mean().iloc[-1]
        ma50 = pd.Series(closes).rolling(50).mean().iloc[-1]
        if ma20 < current_price:
            levels['support'].append(('MA20', ma20))
        else:
            levels['resistance'].append(('MA20', ma20))
        if ma50 < current_price:
            levels['support'].append(('MA50', ma50))
        else:
            levels['resistance'].append(('MA50', ma50))
    
    # Method 4: 52-week high/low
    high_52w = max(highs)
    low_52w = min(lows)
    if high_52w > current_price:
        levels['resistance'].append(('52W High', high_52w))
    if low_52w < current_price:
        levels['support'].append(('52W Low', low_52w))
    
    # Deduplicate and sort - cluster nearby levels
    def cluster_levels(level_list, threshold=0.02):
        if not level_list: return []
        sorted_levels = sorted(level_list, key=lambda x: x[1])
        clustered = []
        current_cluster = [sorted_levels[0]]
        
        for i in range(1, len(sorted_levels)):
            if abs(sorted_levels[i][1] - current_cluster[0][1]) / current_cluster[0][1] < threshold:
                current_cluster.append(sorted_levels[i])
            else:
                avg_price = sum(l[1] for l in current_cluster) / len(current_cluster)
                labels = [l[0] for l in current_cluster]
                clustered.append((labels[0] if len(labels) == 1 else f"{labels[0]} Zone", avg_price, len(current_cluster)))
                current_cluster = [sorted_levels[i]]
        
        if current_cluster:
            avg_price = sum(l[1] for l in current_cluster) / len(current_cluster)
            labels = [l[0] for l in current_cluster]
            clustered.append((labels[0] if len(labels) == 1 else f"{labels[0]} Zone", avg_price, len(current_cluster)))
        
        return clustered
    
    support = cluster_levels(levels['support'])
    resistance = cluster_levels(levels['resistance'])
    
    # Sort and return top levels
    support = sorted(support, key=lambda x: x[1], reverse=True)[:4]
    resistance = sorted(resistance, key=lambda x: x[1])[:4]
    
    return support, resistance

def calculate_metrics(hist: pd.DataFrame, info: dict) -> Optional[dict]:
    """Calculate key metrics from historical data with safe division."""
    if hist is None or hist.empty: 
        return None
    
    latest = hist.iloc[-1]
    price = latest['Close']
    prev = safe_get(info, 'regularMarketPreviousClose', price)
    
    change_pct = safe_pct_change(price, prev)
    vol = latest['Volume']
    avg_vol = hist['Volume'].rolling(20).mean().iloc[-1] if len(hist) > 20 else vol
    vol_vs_avg = safe_div(vol, avg_vol, 1.0) * 100
    
    first_close = hist['Close'].iloc[0] if len(hist) > 1 else price
    momentum = safe_pct_change(price, first_close)
    
    rsi, rsi_cond = calculate_rsi(hist['Close'])
    _, _, _, macd_sig = calculate_macd(hist['Close'])
    
    return {
        'current_price': price, 
        'prev_close': prev, 
        'overnight_change': price - prev, 
        'overnight_change_pct': change_pct, 
        'volume': vol, 
        'volume_vs_avg': vol_vs_avg, 
        'high': latest['High'], 
        'low': latest['Low'], 
        'momentum_5d': momentum, 
        'rsi': rsi, 
        'rsi_condition': rsi_cond, 
        'macd_signal': macd_sig
    }

def generate_detailed_signals(hist, info):
    """Generate institutional-grade detailed technical signals."""
    if hist is None or len(hist) < 50:
        return []
    
    signals = []
    prices = hist['Close']
    current = prices.iloc[-1]
    
    # RSI Analysis
    rsi, rsi_cond = calculate_rsi(prices)
    if rsi_cond == "overbought":
        signals.append({
            'name': 'RSI Overbought Warning',
            'direction': 'bearish',
            'value': f'{rsi:.1f}',
            'detail': f'RSI at {rsi:.1f} indicates overextended buying pressure. Historically, readings above 70 precede mean reversion. Monitor for bearish divergence or distribution patterns. Consider reducing position size or tightening stops.',
            'strength': 'strong' if rsi > 80 else 'moderate'
        })
    elif rsi_cond == "oversold":
        signals.append({
            'name': 'RSI Oversold - Potential Reversal',
            'direction': 'bullish',
            'value': f'{rsi:.1f}',
            'detail': f'RSI at {rsi:.1f} suggests oversold conditions. Watch for bullish divergence on price action. Historically, RSI below 30 with stabilizing price presents accumulation opportunity. Volume confirmation critical.',
            'strength': 'strong' if rsi < 20 else 'moderate'
        })
    elif rsi_cond == "bullish":
        signals.append({
            'name': 'RSI Bullish Momentum',
            'direction': 'bullish',
            'value': f'{rsi:.1f}',
            'detail': f'RSI at {rsi:.1f} shows healthy bullish momentum without overextension. Trend following setups favored. Monitor for continuation above 60 level.',
            'strength': 'moderate'
        })
    elif rsi_cond == "bearish":
        signals.append({
            'name': 'RSI Bearish Momentum',
            'direction': 'bearish',
            'value': f'{rsi:.1f}',
            'detail': f'RSI at {rsi:.1f} indicates weakening momentum. Price vulnerable to further downside. Watch for failed rallies and lower highs.',
            'strength': 'moderate'
        })
    
    # MACD Analysis
    macd_line, signal_line, macd_hist, macd_sig = calculate_macd(prices)
    if macd_sig == "bullish":
        signals.append({
            'name': 'MACD Bullish Expansion',
            'direction': 'bullish',
            'value': f'{macd_hist:.3f}',
            'detail': f'MACD histogram expanding positively at {macd_hist:.3f}. This indicates accelerating upward momentum with buying pressure exceeding selling. Optimal for trend-following entries. Target next resistance level.',
            'strength': 'strong' if macd_hist > 0.5 else 'moderate'
        })
    elif macd_sig == "bearish":
        signals.append({
            'name': 'MACD Bearish Expansion',
            'direction': 'bearish',
            'value': f'{macd_hist:.3f}',
            'detail': f'MACD histogram contracting/negative at {macd_hist:.3f}. Momentum deteriorating with sellers in control. Avoid new longs; consider hedging existing positions. Wait for histogram to stabilize before re-entry.',
            'strength': 'strong' if macd_hist < -0.5 else 'moderate'
        })
    
    # Moving Average Analysis
    if len(prices) >= 50:
        ma20 = prices.rolling(20).mean().iloc[-1]
        ma50 = prices.rolling(50).mean().iloc[-1]
        ma20_prev = prices.rolling(20).mean().iloc[-2]
        ma50_prev = prices.rolling(50).mean().iloc[-2]
        
        # Golden/Death Cross
        if ma20 > ma50 and ma20_prev <= ma50_prev:
            signals.append({
                'name': 'Golden Cross Formation',
                'direction': 'bullish',
                'value': 'Confirmed',
                'detail': 'MA20 has crossed above MA50, a classic bullish signal indicating potential trend reversal. Institutional buying often follows. Historical win rate ~65% over 3-month horizon. Consider scaling into position.',
                'strength': 'strong'
            })
        elif ma20 < ma50 and ma20_prev >= ma50_prev:
            signals.append({
                'name': 'Death Cross Warning',
                'direction': 'bearish',
                'value': 'Confirmed',
                'detail': 'MA20 has crossed below MA50, signaling potential downtrend initiation. Historically precedes 10-15% declines. Reduce exposure; implement stop losses. Wait for stabilization before accumulating.',
                'strength': 'strong'
            })
        
        # Price vs MAs
        if current > ma20 > ma50:
            pct_above_ma20 = ((current - ma20) / ma20) * 100
            signals.append({
                'name': 'Bullish Trend Structure',
                'direction': 'bullish',
                'value': f'+{pct_above_ma20:.1f}% from MA20',
                'detail': f'Price trading above both key moving averages with MA20 > MA50. This is the strongest trend configuration. Current price {pct_above_ma20:.1f}% above MA20. Pullbacks to MA20 (~${ma20:.2f}) present buying opportunities.',
                'strength': 'strong'
            })
        elif current < ma20 < ma50:
            pct_below_ma20 = ((ma20 - current) / ma20) * 100
            signals.append({
                'name': 'Bearish Trend Structure',
                'direction': 'bearish',
                'value': f'-{pct_below_ma20:.1f}% from MA20',
                'detail': f'Price below both key moving averages with MA20 < MA50. Bearish configuration favors shorts/puts. Rallies to MA20 (~${ma20:.2f}) likely to face resistance. Avoid bottom-fishing until structure improves.',
                'strength': 'strong'
            })
    
    # Bollinger Band Analysis
    bb_upper, bb_mid, bb_lower, bb_pos = calculate_bollinger(prices)
    if bb_upper:
        bb_width = ((bb_upper - bb_lower) / bb_mid) * 100
        if bb_pos == "above_upper":
            signals.append({
                'name': 'Bollinger Band Breakout',
                'direction': 'neutral',
                'value': f'Above Upper Band',
                'detail': f'Price trading above upper Bollinger Band (${bb_upper:.2f}). This can indicate either strong momentum continuation OR overextension. Band width at {bb_width:.1f}%. If width narrow, expect volatility expansion. Take partial profits; trail stops.',
                'strength': 'moderate'
            })
        elif bb_pos == "below_lower":
            signals.append({
                'name': 'Bollinger Band Oversold',
                'direction': 'bullish',
                'value': f'Below Lower Band',
                'detail': f'Price below lower Bollinger Band (${bb_lower:.2f}), statistically rare. Mean reversion likely but timing uncertain. Wait for price to close back inside bands before entry. Band width {bb_width:.1f}%.',
                'strength': 'moderate'
            })
    
    # Volume Analysis
    vol = hist['Volume'].iloc[-1]
    avg_vol = hist['Volume'].rolling(20).mean().iloc[-1]
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1
    
    if vol_ratio > 2:
        price_change = prices.iloc[-1] - prices.iloc[-2]
        if price_change > 0:
            signals.append({
                'name': 'High Volume Accumulation',
                'direction': 'bullish',
                'value': f'{vol_ratio:.1f}x Avg',
                'detail': f'Volume {vol_ratio:.1f}x above average on up move. Institutional accumulation likely. This validates upward price action and suggests continuation. Monitor for follow-through in coming sessions.',
                'strength': 'strong'
            })
        else:
            signals.append({
                'name': 'High Volume Distribution',
                'direction': 'bearish',
                'value': f'{vol_ratio:.1f}x Avg',
                'detail': f'Volume {vol_ratio:.1f}x above average on down move. Institutional distribution/selling pressure. This is a warning sign even in uptrends. Reduce position size; tighten stops.',
                'strength': 'strong'
            })
    elif vol_ratio < 0.5:
        signals.append({
            'name': 'Low Volume Warning',
            'direction': 'neutral',
            'value': f'{vol_ratio:.1f}x Avg',
            'detail': f'Volume {vol_ratio:.1f}x below average. Low conviction move - susceptible to reversal. Wait for volume confirmation before acting on price signals.',
            'strength': 'weak'
        })
    
    # Momentum/ROC
    if len(prices) >= 10:
        roc_5 = safe_pct_change(prices.iloc[-1], prices.iloc[-5])
        roc_10 = safe_pct_change(prices.iloc[-1], prices.iloc[-10])
        
        if roc_5 > 5 and roc_10 > 8:
            signals.append({
                'name': 'Strong Momentum Surge',
                'direction': 'bullish',
                'value': f'+{roc_5:.1f}% (5d)',
                'detail': f'5-day ROC +{roc_5:.1f}%, 10-day ROC +{roc_10:.1f}%. Exceptional momentum rarely sustainable. Expect consolidation but trend likely continues. Use pullbacks for entry, not chasing.',
                'strength': 'strong'
            })
        elif roc_5 < -5 and roc_10 < -8:
            signals.append({
                'name': 'Momentum Collapse',
                'direction': 'bearish',
                'value': f'{roc_5:.1f}% (5d)',
                'detail': f'5-day ROC {roc_5:.1f}%, 10-day ROC {roc_10:.1f}%. Sharp decline suggests capitulation or fundamental concerns. Watch for stabilization before catching falling knife. Bounces likely to be sold.',
                'strength': 'strong'
            })
    
    return signals

def analyze_institutional_activity(data, current_price):
    """
    Comprehensive institutional activity analysis including:
    - Insider transactions with sentiment scoring
    - Options flow analysis (unusual activity, sweeps, block trades)
    - Dark pool activity estimation
    - Short interest and squeeze potential
    - Institutional ownership trends
    - Smart money indicators
    
    Returns a dictionary with whale/institutional signals and detailed metrics.
    """
    activity = {
        'insider_sentiment': 'neutral',
        'insider_transactions': [],
        'insider_buy_count': 0,
        'insider_sell_count': 0,
        'insider_net_value': 0,
        'institutional_ownership': 0,
        'insider_ownership': 0,
        'unusual_options': [],
        'options_sentiment': 'neutral',
        'call_volume': 0,
        'put_volume': 0,
        'put_call_ratio': 0,
        'whale_signals': [],
        'overall_signal': 'neutral',
        'short_interest': 0,
        'short_ratio': 0,
        'avg_volume': 0,
        'relative_volume': 0,
        'dark_pool_estimate': 0,
        'dark_pool_sentiment': 'neutral',
        'block_trades': [],
        'finviz_data': {},
        'squeeze_potential': 0,
        'smart_money_score': 0,
        'accumulation_distribution': 'neutral',
        'institutional_momentum': 'neutral',
    }
    
    info = data.get('info', {})
    
    # === OWNERSHIP DATA ===
    activity['institutional_ownership'] = info.get('heldPercentInstitutions', 0) * 100 if info.get('heldPercentInstitutions') else 0
    activity['insider_ownership'] = info.get('heldPercentInsiders', 0) * 100 if info.get('heldPercentInsiders') else 0
    
    # === SHORT INTEREST DATA & SQUEEZE POTENTIAL ===
    activity['short_interest'] = info.get('shortPercentOfFloat', 0) * 100 if info.get('shortPercentOfFloat') else 0
    activity['short_ratio'] = info.get('shortRatio', 0) if info.get('shortRatio') else 0
    
    # Calculate squeeze potential score (0-100)
    squeeze_score = 0
    if activity['short_interest'] > 20:
        squeeze_score += 40
    elif activity['short_interest'] > 15:
        squeeze_score += 30
    elif activity['short_interest'] > 10:
        squeeze_score += 20
    
    if activity['short_ratio'] > 5:
        squeeze_score += 30
    elif activity['short_ratio'] > 3:
        squeeze_score += 20
    elif activity['short_ratio'] > 2:
        squeeze_score += 10
    
    # === VOLUME ANALYSIS ===
    activity['avg_volume'] = info.get('averageVolume', 0)
    current_volume = info.get('volume', 0)
    if activity['avg_volume'] > 0:
        activity['relative_volume'] = current_volume / activity['avg_volume']
        # High volume with high short interest = higher squeeze potential
        if activity['relative_volume'] > 2 and activity['short_interest'] > 10:
            squeeze_score += 30
    
    activity['squeeze_potential'] = min(squeeze_score, 100)
    
    # === DARK POOL ANALYSIS (Enhanced) ===
    # Dark pools handle ~35-45% of equity volume for large caps
    # Estimate based on multiple factors
    base_dp = 32  # Base dark pool estimate
    
    # Adjust for institutional ownership
    if activity['institutional_ownership'] > 80:
        base_dp += 8  # Very high inst ownership = more dark pool
    elif activity['institutional_ownership'] > 60:
        base_dp += 5
    elif activity['institutional_ownership'] > 40:
        base_dp += 2
    
    # Adjust for market cap (larger caps = more dark pool)
    market_cap = info.get('marketCap', 0)
    if market_cap > 100e9:  # >$100B
        base_dp += 5
    elif market_cap > 10e9:  # >$10B
        base_dp += 3
    
    # Adjust for average volume
    if activity['avg_volume'] > 10000000:
        base_dp += 3
    elif activity['avg_volume'] > 1000000:
        base_dp += 1
    
    activity['dark_pool_estimate'] = min(base_dp, 45)  # Cap at 45%
    
    # Dark pool sentiment inference
    # High relative volume with price stability suggests dark pool accumulation
    hist_1d = data.get('hist_1d')
    if hist_1d is not None and len(hist_1d) > 1:
        daily_range = (hist_1d['High'].max() - hist_1d['Low'].min()) / current_price * 100
        if activity['relative_volume'] > 1.5 and daily_range < 2:
            activity['dark_pool_sentiment'] = 'accumulation'
            activity['whale_signals'].append(('🐋', 'High volume with low volatility - possible dark pool accumulation'))
        elif activity['relative_volume'] > 1.5 and daily_range > 4:
            activity['dark_pool_sentiment'] = 'distribution'
            activity['whale_signals'].append(('🐋', 'High volume with high volatility - possible dark pool distribution'))
    
    # === SMART MONEY SCORE ===
    smart_score = 50  # Start neutral
    
    # Major holders breakdown
    major_holders = data.get('major_holders')
    if major_holders is not None and not major_holders.empty:
        try:
            for idx, row in major_holders.iterrows():
                if 'insider' in str(row.values).lower():
                    activity['insider_ownership'] = float(str(row.iloc[0]).replace('%', '')) if '%' in str(row.iloc[0]) else 0
                elif 'institution' in str(row.values).lower():
                    activity['institutional_ownership'] = float(str(row.iloc[0]).replace('%', '')) if '%' in str(row.iloc[0]) else 0
        except:
            pass
    
    # === INSIDER TRANSACTIONS ===
    insider_txns = data.get('insider_transactions')
    has_insider_data = insider_txns is not None and not insider_txns.empty
    
    # If no Yahoo data, try Finviz as fallback
    finviz_data = data.get('finviz_data', {})
    
    if has_insider_data:
        buy_value = 0
        sell_value = 0
        recent_txns = []
        
        for idx, row in insider_txns.head(15).iterrows():
            try:
                # Get transaction details
                insider_name = row.get('Insider', row.get('insider', row.get('Name', 'Unknown')))
                txn_type = str(row.get('Transaction', row.get('transaction', row.get('Text', '')))).lower()
                shares = abs(float(row.get('Shares', row.get('shares', row.get('Value', 0)))))
                value = abs(float(row.get('Value', row.get('value', shares * current_price))))
                
                # Determine if buy or sell
                is_buy = any(word in txn_type for word in ['buy', 'purchase', 'acquisition', 'exercise'])
                is_sell = any(word in txn_type for word in ['sell', 'sale', 'disposition'])
                
                if is_buy:
                    activity['insider_buy_count'] += 1
                    buy_value += value
                    recent_txns.append({
                        'name': str(insider_name)[:20],
                        'type': 'BUY',
                        'shares': shares,
                        'value': value,
                        'color': '#3fb950'
                    })
                elif is_sell:
                    activity['insider_sell_count'] += 1
                    sell_value += value
                    recent_txns.append({
                        'name': str(insider_name)[:20],
                        'type': 'SELL',
                        'shares': shares,
                        'value': value,
                        'color': '#f85149'
                    })
            except:
                continue
        
        activity['insider_transactions'] = recent_txns[:6]
        activity['insider_net_value'] = buy_value - sell_value
        
        # Determine insider sentiment
        if activity['insider_buy_count'] > activity['insider_sell_count'] * 2:
            activity['insider_sentiment'] = 'strongly bullish'
            activity['whale_signals'].append(('🟢', 'Heavy insider buying detected'))
        elif activity['insider_buy_count'] > activity['insider_sell_count']:
            activity['insider_sentiment'] = 'bullish'
            activity['whale_signals'].append(('🟢', 'Net insider buying'))
        elif activity['insider_sell_count'] > activity['insider_buy_count'] * 2:
            activity['insider_sentiment'] = 'strongly bearish'
            activity['whale_signals'].append(('🔴', 'Heavy insider selling detected'))
        elif activity['insider_sell_count'] > activity['insider_buy_count']:
            activity['insider_sentiment'] = 'bearish'
            activity['whale_signals'].append(('🟡', 'Net insider selling'))
    
    elif finviz_data.get('transactions'):
        # Use Finviz data as fallback
        recent_txns = []
        for txn in finviz_data['transactions'][:10]:
            try:
                txn_type = txn.get('transaction', '').lower()
                is_buy = 'buy' in txn_type or 'purchase' in txn_type
                is_sell = 'sale' in txn_type or 'sell' in txn_type
                
                # Parse value
                value_str = txn.get('value', '0').replace('$', '').replace(',', '')
                try:
                    value = float(value_str) if value_str else 0
                except:
                    value = 0
                
                if is_buy:
                    activity['insider_buy_count'] += 1
                    recent_txns.append({
                        'name': txn.get('owner', 'Unknown')[:20],
                        'type': 'BUY',
                        'shares': 0,
                        'value': value,
                        'color': '#3fb950'
                    })
                elif is_sell:
                    activity['insider_sell_count'] += 1
                    recent_txns.append({
                        'name': txn.get('owner', 'Unknown')[:20],
                        'type': 'SELL',
                        'shares': 0,
                        'value': value,
                        'color': '#f85149'
                    })
            except:
                continue
        
        activity['insider_transactions'] = recent_txns[:6]
        
        if activity['insider_buy_count'] > activity['insider_sell_count']:
            activity['insider_sentiment'] = 'bullish'
            activity['whale_signals'].append(('🟢', 'Net insider buying (Finviz)'))
        elif activity['insider_sell_count'] > activity['insider_buy_count']:
            activity['insider_sentiment'] = 'bearish'
            activity['whale_signals'].append(('🟡', 'Net insider selling (Finviz)'))
    
    # === UNUSUAL OPTIONS ACTIVITY ===
    options_data = data.get('options_data')
    if options_data:
        calls = options_data.get('calls')
        puts = options_data.get('puts')
        
        unusual_options = []
        total_call_volume = 0
        total_put_volume = 0
        
        if calls is not None and not calls.empty:
            for idx, row in calls.iterrows():
                try:
                    volume = int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0
                    open_interest = int(row.get('openInterest', 1)) if pd.notna(row.get('openInterest')) and row.get('openInterest', 0) > 0 else 1
                    strike = float(row.get('strike', 0))
                    
                    total_call_volume += volume
                    
                    # Unusual activity: volume > 2x open interest
                    if volume > open_interest * 2 and volume > 1000:
                        unusual_options.append({
                            'type': 'CALL',
                            'strike': strike,
                            'volume': volume,
                            'oi': open_interest,
                            'ratio': volume / open_interest,
                            'otm': strike > current_price,
                            'color': '#3fb950'
                        })
                except:
                    continue
        
        if puts is not None and not puts.empty:
            for idx, row in puts.iterrows():
                try:
                    volume = int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0
                    open_interest = int(row.get('openInterest', 1)) if pd.notna(row.get('openInterest')) and row.get('openInterest', 0) > 0 else 1
                    strike = float(row.get('strike', 0))
                    
                    total_put_volume += volume
                    
                    # Unusual activity: volume > 2x open interest
                    if volume > open_interest * 2 and volume > 1000:
                        unusual_options.append({
                            'type': 'PUT',
                            'strike': strike,
                            'volume': volume,
                            'oi': open_interest,
                            'ratio': volume / open_interest,
                            'otm': strike < current_price,
                            'color': '#f85149'
                        })
                except:
                    continue
        
        # Sort by volume and take top unusual options
        unusual_options.sort(key=lambda x: x['volume'], reverse=True)
        activity['unusual_options'] = unusual_options[:5]
        
        activity['call_volume'] = total_call_volume
        activity['put_volume'] = total_put_volume
        
        # Put/Call ratio
        if total_call_volume > 0:
            activity['put_call_ratio'] = total_put_volume / total_call_volume
        
        # Options sentiment
        if activity['put_call_ratio'] < 0.5:
            activity['options_sentiment'] = 'bullish'
            activity['whale_signals'].append(('🟢', f'Low put/call ratio ({activity["put_call_ratio"]:.2f})'))
        elif activity['put_call_ratio'] > 1.5:
            activity['options_sentiment'] = 'bearish'
            activity['whale_signals'].append(('🔴', f'High put/call ratio ({activity["put_call_ratio"]:.2f})'))
        
        # Check for unusual call buying (bullish)
        unusual_calls = [o for o in unusual_options if o['type'] == 'CALL' and o['otm']]
        unusual_puts = [o for o in unusual_options if o['type'] == 'PUT' and o['otm']]
        
        if len(unusual_calls) >= 2:
            activity['whale_signals'].append(('🟢', f'{len(unusual_calls)} unusual OTM call sweeps'))
        if len(unusual_puts) >= 2:
            activity['whale_signals'].append(('🔴', f'{len(unusual_puts)} unusual OTM put sweeps'))
    
    # === INSTITUTIONAL OWNERSHIP SIGNALS ===
    if activity['institutional_ownership'] > 90:
        activity['whale_signals'].append(('🟡', f'Very high institutional ownership ({activity["institutional_ownership"]:.1f}%)'))
    elif activity['institutional_ownership'] > 70:
        activity['whale_signals'].append(('🟢', f'Strong institutional ownership ({activity["institutional_ownership"]:.1f}%)'))
    elif activity['institutional_ownership'] < 20:
        activity['whale_signals'].append(('🟡', f'Low institutional ownership ({activity["institutional_ownership"]:.1f}%)'))
    
    if activity['insider_ownership'] > 20:
        activity['whale_signals'].append(('🟢', f'High insider ownership ({activity["insider_ownership"]:.1f}%) - aligned interests'))
        smart_score += 10
    
    # === SHORT INTEREST SIGNALS ===
    if activity['short_interest'] > 20:
        activity['whale_signals'].append(('🔴', f'High short interest ({activity["short_interest"]:.1f}%) - potential squeeze or bearish thesis'))
        smart_score -= 5  # Could go either way
    elif activity['short_interest'] > 10:
        activity['whale_signals'].append(('🟡', f'Elevated short interest ({activity["short_interest"]:.1f}%)'))
    
    if activity['short_ratio'] > 5:
        activity['whale_signals'].append(('🟡', f'High days-to-cover ({activity["short_ratio"]:.1f} days) - squeeze fuel if momentum shifts'))
    
    # === SQUEEZE POTENTIAL SIGNAL ===
    if activity['squeeze_potential'] > 70:
        activity['whale_signals'].append(('🚀', f'HIGH squeeze potential ({activity["squeeze_potential"]}%) - high SI + high DTC'))
    elif activity['squeeze_potential'] > 50:
        activity['whale_signals'].append(('🟡', f'Moderate squeeze potential ({activity["squeeze_potential"]}%)'))
    
    # === VOLUME SIGNALS ===
    if activity['relative_volume'] > 3:
        activity['whale_signals'].append(('🔥', f'Extreme volume ({activity["relative_volume"]:.1f}x avg) - major institutional activity'))
        smart_score += 15 if activity.get('dark_pool_sentiment') == 'accumulation' else -5
    elif activity['relative_volume'] > 2:
        activity['whale_signals'].append(('🟢', f'High relative volume ({activity["relative_volume"]:.1f}x avg)'))
        smart_score += 5
    elif activity['relative_volume'] < 0.5:
        activity['whale_signals'].append(('🟡', f'Low volume ({activity["relative_volume"]:.1f}x avg) - lack of institutional interest'))
        smart_score -= 5
    
    # === ACCUMULATION/DISTRIBUTION ===
    hist_5d = data.get('hist_5d')
    if hist_5d is not None and len(hist_5d) > 5:
        # Simple A/D line approximation
        closes = hist_5d['Close'].values
        volumes = hist_5d['Volume'].values
        highs = hist_5d['High'].values
        lows = hist_5d['Low'].values
        
        ad_sum = 0
        for i in range(len(closes)):
            if highs[i] != lows[i]:
                clv = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / (highs[i] - lows[i])
                ad_sum += clv * volumes[i]
        
        if ad_sum > 0:
            activity['accumulation_distribution'] = 'accumulation'
            if activity['relative_volume'] > 1.2:
                activity['whale_signals'].append(('🟢', 'A/D line positive - accumulation phase'))
                smart_score += 10
        else:
            activity['accumulation_distribution'] = 'distribution'
            if activity['relative_volume'] > 1.2:
                activity['whale_signals'].append(('🔴', 'A/D line negative - distribution phase'))
                smart_score -= 10
    
    # === FINALIZE SMART MONEY SCORE ===
    # Incorporate insider sentiment
    if activity['insider_sentiment'] in ['bullish', 'strongly bullish']:
        smart_score += 15
    elif activity['insider_sentiment'] in ['bearish', 'strongly bearish']:
        smart_score -= 10
    
    # Incorporate options sentiment
    if activity['options_sentiment'] == 'bullish':
        smart_score += 10
    elif activity['options_sentiment'] == 'bearish':
        smart_score -= 10
    
    activity['smart_money_score'] = max(0, min(100, smart_score))
    
    # === OVERALL SIGNAL (Enhanced) ===
    bullish_signals = sum(1 for s in activity['whale_signals'] if s[0] in ['🟢', '🐋', '🚀'])
    bearish_signals = sum(1 for s in activity['whale_signals'] if s[0] == '🔴')
    fire_signals = sum(1 for s in activity['whale_signals'] if s[0] == '🔥')
    
    # Fire signals are strong but need context from price action
    if fire_signals > 0:
        hist = data.get('hist_1d')
        if hist is not None and len(hist) > 1:
            first_close = hist['Close'].iloc[0]
            price_change = safe_pct_change(hist['Close'].iloc[-1], first_close)
            if price_change > 1:
                bullish_signals += fire_signals
            elif price_change < -1:
                bearish_signals += fire_signals
    
    # Factor in smart money score
    if activity['smart_money_score'] > 65:
        bullish_signals += 1
    elif activity['smart_money_score'] < 35:
        bearish_signals += 1
    
    if bullish_signals > bearish_signals + 1:
        activity['overall_signal'] = 'bullish'
    elif bearish_signals > bullish_signals + 1:
        activity['overall_signal'] = 'bearish'
    else:
        activity['overall_signal'] = 'neutral'
    
    # Set institutional momentum
    if activity['overall_signal'] == 'bullish' and activity['smart_money_score'] > 60:
        activity['institutional_momentum'] = 'strong_bullish'
    elif activity['overall_signal'] == 'bullish':
        activity['institutional_momentum'] = 'bullish'
    elif activity['overall_signal'] == 'bearish' and activity['smart_money_score'] < 40:
        activity['institutional_momentum'] = 'strong_bearish'
    elif activity['overall_signal'] == 'bearish':
        activity['institutional_momentum'] = 'bearish'
    else:
        activity['institutional_momentum'] = 'neutral'
    
    return activity

def generate_expert_analysis(symbol, data, signals, support_levels, resistance_levels, news_sentiment):
    """Generate AI expert analysis synthesizing all available data."""
    info = data.get('info', {})
    hist = data.get('hist_5d')
    
    if hist is None or hist.empty:
        return None
    
    price = hist['Close'].iloc[-1]
    prev = safe_get(info, 'regularMarketPreviousClose', price)
    change_pct = safe_pct_change(price, prev)
    
    # Determine overall bias
    bullish_count = sum(1 for s in signals if s['direction'] == 'bullish')
    bearish_count = sum(1 for s in signals if s['direction'] == 'bearish')
    strong_bullish = sum(1 for s in signals if s['direction'] == 'bullish' and s.get('strength') == 'strong')
    strong_bearish = sum(1 for s in signals if s['direction'] == 'bearish' and s.get('strength') == 'strong')
    
    # Calculate technical score
    tech_score = (bullish_count * 10 + strong_bullish * 15) - (bearish_count * 10 + strong_bearish * 15)
    
    # Incorporate fundamentals
    pe = info.get('trailingPE', 0)
    sector = info.get('sector', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    # Generate verdict
    if tech_score >= 30:
        verdict = "STRONG BUY"
        verdict_color = "#3fb950"
        bias = "bullish"
    elif tech_score >= 15:
        verdict = "BUY"
        verdict_color = "#3fb950"
        bias = "bullish"
    elif tech_score >= 5:
        verdict = "LEAN BULLISH"
        verdict_color = "#58a6ff"
        bias = "neutral_bullish"
    elif tech_score <= -30:
        verdict = "STRONG SELL"
        verdict_color = "#f85149"
        bias = "bearish"
    elif tech_score <= -15:
        verdict = "SELL"
        verdict_color = "#f85149"
        bias = "bearish"
    elif tech_score <= -5:
        verdict = "LEAN BEARISH"
        verdict_color = "#d29922"
        bias = "neutral_bearish"
    else:
        verdict = "NEUTRAL"
        verdict_color = "#8b949e"
        bias = "neutral"
    
    # Build analysis text
    name = info.get('shortName', symbol)
    
    # Opening assessment
    if change_pct > 0:
        opening = f"{name} is trading at ${price:.2f}, up {change_pct:.2f}% from previous close."
    else:
        opening = f"{name} is trading at ${price:.2f}, down {abs(change_pct):.2f}% from previous close."
    
    # Technical summary
    tech_signals = []
    for s in signals[:3]:
        tech_signals.append(f"{s['name']} ({s['value']})")
    tech_summary = f"Key technical signals: {', '.join(tech_signals)}." if tech_signals else ""
    
    # Support/Resistance context
    nearest_support = support_levels[0] if support_levels else None
    nearest_resistance = resistance_levels[0] if resistance_levels else None
    
    sr_context = ""
    if nearest_support and nearest_resistance and price > 0:
        support_dist = safe_div((price - nearest_support[1]), price) * 100
        resist_dist = safe_div((nearest_resistance[1] - price), price) * 100
        sr_context = f"Price is {support_dist:.1f}% above key support at ${nearest_support[1]:.2f} and {resist_dist:.1f}% below resistance at ${nearest_resistance[1]:.2f}. "
        
        if support_dist < 2:
            sr_context += "Proximity to support offers favorable risk/reward for longs with tight stops. "
        elif resist_dist < 2:
            sr_context += "Approaching resistance - watch for rejection or breakout confirmation. "
    
    # Risk assessment
    rsi, _ = calculate_rsi(hist['Close'])
    vol_ratio = hist['Volume'].iloc[-1] / hist['Volume'].rolling(20).mean().iloc[-1] if len(hist) > 20 else 1
    
    risks = []
    if rsi > 70:
        risks.append("overbought RSI conditions")
    elif rsi < 30:
        risks.append("oversold but potentially catching a falling knife")
    if vol_ratio < 0.7:
        risks.append("low volume suggesting weak conviction")
    if change_pct < -3:
        risks.append("significant recent decline may continue")
    
    risk_text = f"Key risks: {', '.join(risks)}." if risks else ""
    
    # News sentiment integration
    news_context = ""
    if news_sentiment:
        if news_sentiment['overall'] == 'bullish':
            news_context = f"News sentiment is positive with {news_sentiment['bullish']} bullish signals detected, providing fundamental tailwind. "
        elif news_sentiment['overall'] == 'bearish':
            news_context = f"News flow is negative with {news_sentiment['bearish']} bearish signals, creating headline risk. "
    
    # Trading recommendation
    support_price = nearest_support[1] if nearest_support else price * 0.98
    resistance_price = nearest_resistance[1] if nearest_resistance else price * 1.05
    
    if bias == "bullish":
        recommendation = f"For active traders, consider entries on pullbacks to ${support_price:.2f} with stops 2-3% below. Initial target at ${resistance_price:.2f}."
    elif bias == "bearish":
        support_critical = nearest_support[1] if nearest_support else price * 0.95
        recommendation = f"Defensive positioning warranted. Consider reducing exposure or implementing hedges. Support at ${support_critical:.2f} is critical - breach opens downside."
    else:
        recommendation = "Wait for clearer signal before committing capital. Range-bound action likely until technical picture clarifies."
    
    analysis = {
        'verdict': verdict,
        'verdict_color': verdict_color,
        'tech_score': tech_score,
        'text': f"{opening} {tech_summary} {sr_context}{news_context}{risk_text} {recommendation}",
        'bias': bias
    }
    
    return analysis

def analyze_news_sentiment(news_items):
    if not news_items: return {"overall": "neutral", "score": 0, "bullish": 0, "bearish": 0, "items": []}
    bullish_words = ['surge', 'rally', 'beat', 'upgrade', 'record', 'strong', 'growth', 'buy', 'soar', 'gain', 'profit', 'positive', 'bullish', 'outperform', 'rise', 'exceeds', 'breakthrough', 'jumps', 'climbs', 'wins', 'success', 'higher', 'boost']
    bearish_words = ['drop', 'fall', 'miss', 'downgrade', 'weak', 'cut', 'sell', 'crash', 'warning', 'decline', 'loss', 'negative', 'bearish', 'underperform', 'fear', 'concern', 'risk', 'lawsuit', 'investigation', 'plunge', 'tumble', 'slump', 'lower', 'fails']
    total_b, total_bear, items = 0, 0, []
    
    for item in news_items:
        # Get title from various possible keys
        title = item.get('title', item.get('headline', ''))
        if not title:
            continue
            
        title_lower = title.lower()
        b = sum(1 for w in bullish_words if w in title_lower)
        bear = sum(1 for w in bearish_words if w in title_lower)
        total_b += b
        total_bear += bear
        sent = "bullish" if b > bear else "bearish" if bear > b else "neutral"
        
        # Categorize
        cats = []
        if any(w in title_lower for w in ['earnings', 'revenue', 'profit', 'eps', 'quarter', 'results']): cats.append("Earnings")
        if any(w in title_lower for w in ['fed', 'rate', 'inflation', 'economy', 'gdp', 'jobs', 'employment']): cats.append("Economic")
        if any(w in title_lower for w in ['merger', 'acquisition', 'deal', 'buyout', 'takeover']): cats.append("M&A")
        if any(w in title_lower for w in ['analyst', 'upgrade', 'downgrade', 'price target', 'rating']): cats.append("Analyst")
        if any(w in title_lower for w in ['product', 'launch', 'new', 'innovation', 'release', 'announce']): cats.append("Product")
        if any(w in title_lower for w in ['ai', 'artificial intelligence', 'technology', 'tech']): cats.append("Tech")
        if not cats: cats.append("General")
        
        # Parse time - handle multiple formats
        time_str = ""
        pub_time = item.get('providerPublishTime', 0)
        
        if pub_time and pub_time > 0:
            try:
                pub_datetime = datetime.fromtimestamp(pub_time)
                diff = datetime.now() - pub_datetime
                if diff.days > 0:
                    time_str = f"{diff.days}d ago"
                elif diff.seconds > 3600:
                    time_str = f"{diff.seconds//3600}h ago"
                elif diff.seconds > 60:
                    time_str = f"{diff.seconds//60}m ago"
                else:
                    time_str = "Just now"
            except:
                time_str = ""
        
        if not time_str:
            # Try published field
            published = item.get('published', '')
            if published:
                try:
                    # Parse RSS date format
                    if ',' in published:
                        time_str = published.split(',')[1].strip()[:12]
                    else:
                        time_str = published[:16]
                except:
                    time_str = "Recent"
        
        if not time_str:
            time_str = "Recent"
        
        # Get source/publisher
        source = item.get('publisher', item.get('source', ''))
        if not source or source == '':
            source = 'News'
        
        items.append({
            'title': title,
            'source': source,
            'link': item.get('link', item.get('url', '')),
            'sentiment': sent,
            'categories': cats,
            'time': time_str
        })
    
    score = (total_b - total_bear) / max(total_b + total_bear, 1)
    overall = "bullish" if score > 0.2 else "bearish" if score < -0.2 else "neutral"
    return {"overall": overall, "score": score, "bullish": total_b, "bearish": total_bear, "items": items}

def generate_assessment(market_data, news_sentiment, econ_ind):
    assessment = {'sentiment': 'Neutral', 'sentiment_score': 50, 'key_themes': [], 'risks': [], 'opportunities': [], 'trading_bias': 'neutral', 'confidence': 'medium'}
    bullish, bearish = 0, 0
    es = market_data.get('futures', {}).get('S&P 500', {})
    nq = market_data.get('futures', {}).get('Nasdaq 100', {})
    es_ch, nq_ch = es.get('overnight_change_pct', 0), nq.get('overnight_change_pct', 0)
    if es_ch > 0.5: bullish += 3; assessment['opportunities'].append(f"Strong S&P futures (+{es_ch:.2f}%)")
    elif es_ch > 0.2: bullish += 1
    elif es_ch < -0.5: bearish += 3; assessment['risks'].append(f"Weak S&P futures ({es_ch:.2f}%)")
    elif es_ch < -0.2: bearish += 1
    if nq_ch > 0.5: bullish += 2; assessment['key_themes'].append("Tech leadership")
    elif nq_ch < -0.5: bearish += 2; assessment['key_themes'].append("Tech weakness")
    vix = market_data.get('futures', {}).get('VIX', {})
    vix_level, vix_ch = vix.get('current_price', 20), vix.get('overnight_change_pct', 0)
    if vix_level > 25: bearish += 3; assessment['risks'].append(f"High VIX ({vix_level:.1f})")
    elif vix_level > 20: bearish += 1
    elif vix_level < 15: bullish += 2; assessment['opportunities'].append(f"Low VIX ({vix_level:.1f})")
    if vix_ch > 10: bearish += 2; assessment['key_themes'].append("VIX spiking")
    oil = market_data.get('futures', {}).get('Crude Oil', {})
    if oil:
        oil_ch = oil.get('overnight_change_pct', 0)
        if oil_ch > 3: assessment['key_themes'].append("Oil rallying")
        elif oil_ch < -3: assessment['key_themes'].append("Oil weakness")
    for _, m in market_data.get('global', {}).items():
        ch = m.get('overnight_change_pct', 0)
        if ch > 1.5: bullish += 1
        elif ch < -1.5: bearish += 1
    if news_sentiment['overall'] == 'bullish': bullish += 2; assessment['key_themes'].append("Positive news flow")
    elif news_sentiment['overall'] == 'bearish': bearish += 2; assessment['key_themes'].append("Negative headlines")
    sectors = [(n, d.get('metrics', {}).get('overnight_change_pct', 0)) for n, d in market_data.get('sectors', {}).items()]
    sectors.sort(key=lambda x: x[1], reverse=True)
    if sectors:
        assessment['opportunities'].append(f"Leader: {sectors[0][0]} ({sectors[0][1]:+.2f}%)")
        if sectors[-1][1] < -0.5: assessment['risks'].append(f"Laggard: {sectors[-1][0]} ({sectors[-1][1]:+.2f}%)")
    net = bullish - bearish
    if net >= 6: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'], assessment['confidence'] = 'Strongly Bullish', 85, 'long', 'high'
    elif net >= 3: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'], assessment['confidence'] = 'Bullish', 70, 'long', 'medium'
    elif net >= 1: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'] = 'Slightly Bullish', 58, 'neutral_bullish'
    elif net <= -6: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'], assessment['confidence'] = 'Strongly Bearish', 15, 'short', 'high'
    elif net <= -3: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'], assessment['confidence'] = 'Bearish', 30, 'short', 'medium'
    elif net <= -1: assessment['sentiment'], assessment['sentiment_score'], assessment['trading_bias'] = 'Slightly Bearish', 42, 'neutral_bearish'
    return assessment

def generate_expert_macro_summary(market_data, news_sentiment, econ_ind, assessment):
    """
    Generate an expert-level macro analyst summary paragraph.
    Written in the voice of a senior strategist from a top-tier institution (Goldman Sachs/IMF caliber).
    Data-driven, skeptical, focused on key drivers without hype or speculation.
    Returns tuple: (market_summary, news_analysis)
    """
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    market_hour = now.hour
    
    # Determine market session context
    if market_hour < 9 or (market_hour == 9 and now.minute < 30):
        session_context = "pre-market"
    elif market_hour < 16:
        session_context = "regular session"
    else:
        session_context = "post-market"
    
    # Extract all relevant data
    es = market_data.get('futures', {}).get('S&P 500', {})
    nq = market_data.get('futures', {}).get('Nasdaq 100', {})
    dj = market_data.get('futures', {}).get('Dow Jones', {})
    vix = market_data.get('futures', {}).get('VIX', {})
    gold = market_data.get('futures', {}).get('Gold', {})
    oil = market_data.get('futures', {}).get('Crude Oil', {})
    tnx = market_data.get('futures', {}).get('10Y Treasury', {})
    
    es_price = es.get('current_price', 0)
    es_ch = es.get('overnight_change_pct', 0)
    nq_price = nq.get('current_price', 0)
    nq_ch = nq.get('overnight_change_pct', 0)
    dj_price = dj.get('current_price', 0)
    dj_ch = dj.get('overnight_change_pct', 0)
    vix_level = vix.get('current_price', 20)
    vix_ch = vix.get('overnight_change_pct', 0)
    gold_price = gold.get('current_price', 0)
    gold_ch = gold.get('overnight_change_pct', 0)
    oil_price = oil.get('current_price', 0)
    oil_ch = oil.get('overnight_change_pct', 0)
    tnx_level = tnx.get('current_price', 4.5) if tnx else 4.5
    tnx_ch = tnx.get('overnight_change_pct', 0) if tnx else 0
    
    # Economic indicators
    treasury_10y = econ_ind.get('10Y Treasury', {}).get('value', 4.5)
    dollar_idx = econ_ind.get('Dollar Index', {}).get('value', 0)
    
    # Global markets analysis
    global_data = market_data.get('global', {})
    global_performance = [(n, m.get('overnight_change_pct', 0)) for n, m in global_data.items()]
    global_up = sum(1 for _, ch in global_performance if ch > 0)
    global_down = len(global_performance) - global_up
    avg_global_ch = sum(ch for _, ch in global_performance) / max(len(global_performance), 1)
    
    # Sectors analysis
    sectors = [(n, d.get('metrics', {}).get('overnight_change_pct', 0)) for n, d in market_data.get('sectors', {}).items()]
    sectors.sort(key=lambda x: x[1], reverse=True)
    top_sectors = sectors[:3] if sectors else []
    bottom_sectors = sectors[-3:] if sectors else []
    sectors_up = sum(1 for _, ch in sectors if ch > 0)
    sector_dispersion = max(ch for _, ch in sectors) - min(ch for _, ch in sectors) if sectors else 0
    
    # News sentiment metrics
    news_bull = news_sentiment.get('bullish', 0)
    news_bear = news_sentiment.get('bearish', 0)
    news_overall = news_sentiment.get('overall', 'neutral')
    news_items = news_sentiment.get('items', [])
    
    # === BUILD THE MARKET SUMMARY ===
    
    # Opening statement with indices
    if abs(es_ch) >= 2:
        magnitude = "sharp"
    elif abs(es_ch) >= 1:
        magnitude = "notable"
    elif abs(es_ch) >= 0.5:
        magnitude = "moderate"
    else:
        magnitude = "modest"
    
    direction = "gains" if es_ch > 0 else "losses" if es_ch < 0 else "flat trading"
    
    # Determine if this is a significant session
    significant_move = abs(es_ch) >= 1.5 or vix_level >= 25 or abs(vix_ch) >= 10
    
    if significant_move and es_ch < -1.5:
        opening = f"Markets are under significant pressure in {session_context} trading, with the S&P 500 at {es_price:,.0f} ({es_ch:+.2f}%), the Nasdaq 100 declining to {nq_price:,.0f} ({nq_ch:+.2f}%), and the Dow at {dj_price:,.0f} ({dj_ch:+.2f}%)—marking one of the more pronounced risk-off sessions in recent weeks."
    elif significant_move and es_ch > 1.5:
        opening = f"Risk appetite has returned forcefully in {session_context} trading, with the S&P 500 surging to {es_price:,.0f} ({es_ch:+.2f}%), the Nasdaq 100 at {nq_price:,.0f} ({nq_ch:+.2f}%), and the Dow climbing to {dj_price:,.0f} ({dj_ch:+.2f}%)."
    else:
        opening = f"U.S. equities are showing {magnitude} {direction} in {session_context} trading, with the S&P 500 at {es_price:,.0f} ({es_ch:+.2f}%), Nasdaq 100 at {nq_price:,.0f} ({nq_ch:+.2f}%), and Dow at {dj_price:,.0f} ({dj_ch:+.2f}%)."
    
    # Volatility and risk metrics
    if vix_level >= 30:
        vol_analysis = f"The VIX has spiked to {vix_level:.1f}, a level historically associated with capitulation events—while painful, such readings often precede tactical bottoms for patient investors."
    elif vix_level >= 25:
        vol_analysis = f"Implied volatility is elevated with VIX at {vix_level:.1f}, suggesting hedging demand remains robust and options markets are pricing meaningful near-term risk."
    elif vix_level >= 20:
        vol_analysis = f"The VIX at {vix_level:.1f} reflects a market transitioning from complacency to caution, though not yet at levels suggesting acute stress."
    elif vix_level >= 15:
        vol_analysis = f"Volatility metrics remain contained with VIX at {vix_level:.1f}, indicating institutional positioning is not overly defensive despite recent price action."
    else:
        vol_analysis = f"The subdued VIX ({vix_level:.1f}) suggests complacency may be building—historically, such low readings warrant attention as they often precede volatility expansion."
    
    if abs(vix_ch) >= 15:
        vol_analysis += f" Today's {vix_ch:+.1f}% move in volatility is noteworthy."
    
    # Sector rotation insight
    if sectors:
        top_name, top_ch = top_sectors[0] if top_sectors else ('N/A', 0)
        bottom_name, bottom_ch = bottom_sectors[-1] if bottom_sectors else ('N/A', 0)
        
        # Detect rotation patterns
        defensive_sectors = ['Utilities', 'Consumer Staples', 'Healthcare', 'Real Estate']
        cyclical_sectors = ['Technology', 'Consumer Disc.', 'Communication', 'Financial', 'Industrials']
        
        top_is_defensive = any(d in top_name for d in defensive_sectors)
        top_is_cyclical = any(c in top_name for c in cyclical_sectors)
        
        if top_is_defensive and es_ch < 0:
            rotation_insight = f"Classic defensive rotation is evident with {top_name} ({top_ch:+.2f}%) outperforming while {bottom_name} ({bottom_ch:+.2f}%) lags—a pattern consistent with late-cycle positioning."
        elif top_is_cyclical and es_ch > 0:
            rotation_insight = f"Cyclical leadership via {top_name} ({top_ch:+.2f}%) suggests growth expectations remain intact, with {bottom_name} ({bottom_ch:+.2f}%) as the laggard."
        else:
            rotation_insight = f"Sector dispersion of {sector_dispersion:.2f}% shows {top_name} leading ({top_ch:+.2f}%) versus {bottom_name} ({bottom_ch:+.2f}%), with {sectors_up}/11 sectors positive."
    else:
        rotation_insight = "Sector data unavailable for rotation analysis."
    
    # Global context
    if global_up > global_down + 2:
        global_insight = f"Global risk appetite is constructive with {global_up}/{len(global_performance)} major indices higher, providing a supportive backdrop for U.S. equities."
    elif global_down > global_up + 2:
        global_insight = f"International markets are contributing to the risk-off tone, with {global_down}/{len(global_performance)} indices lower—weakness is broad-based rather than U.S.-specific."
    else:
        global_insight = "Global markets are mixed, offering limited directional conviction from overseas flows."
    
    # Commodities and safe havens
    commodity_signals = []
    if gold_ch > 1:
        commodity_signals.append(f"gold surging {gold_ch:.1f}% (safe-haven bid)")
    elif gold_ch < -1:
        commodity_signals.append(f"gold declining {abs(gold_ch):.1f}% (risk appetite returning)")
    
    if oil_ch > 2:
        commodity_signals.append(f"crude rallying {oil_ch:.1f}% (demand/supply dynamics)")
    elif oil_ch < -2:
        commodity_signals.append(f"crude dropping {abs(oil_ch):.1f}% (demand concerns)")
    
    if commodity_signals:
        commodity_insight = "Cross-asset signals show " + " and ".join(commodity_signals) + "."
    else:
        commodity_insight = ""
    
    # Primary driver identification
    themes = assessment.get('key_themes', [])
    
    if vix_ch > 20:
        primary_driver = "Today's volatility spike appears to be the dominant factor, triggering systematic deleveraging and momentum-based selling."
    elif 'Tech weakness' in themes and nq_ch < es_ch - 0.3:
        primary_driver = "Technology sector weakness is the primary drag, with growth-sensitive names under pressure as rate expectations potentially shift."
    elif 'Tech leadership' in themes and nq_ch > es_ch + 0.3:
        primary_driver = "Mega-cap technology is providing market leadership, with AI-adjacent names continuing to attract institutional flows."
    elif gold_ch > 2 and es_ch < 0:
        primary_driver = "Safe-haven rotation into gold suggests geopolitical or policy uncertainty is driving positioning, not just technical factors."
    elif news_overall == 'bearish' and es_ch < -0.5:
        primary_driver = "Negative headline flow is pressuring sentiment, though fundamentals appear secondary to positioning dynamics."
    elif news_overall == 'bullish' and es_ch > 0.5:
        primary_driver = "Constructive news flow is supporting risk appetite, with momentum strategies likely adding to positioning."
    else:
        primary_driver = "Price action appears primarily technical in nature, with no single macro catalyst dominating the tape."
    
    # Forward-looking assessment
    bias = assessment.get('trading_bias', 'neutral')
    confidence = assessment.get('confidence', 'medium')
    
    if bias in ['long', 'neutral_bullish']:
        if confidence == 'high':
            outlook = "For tomorrow's session, the path of least resistance appears higher—though we would use strength to rebalance rather than chase. Key resistance levels and overnight futures activity warrant monitoring for confirmation."
        else:
            outlook = "Near-term momentum is constructive, but conviction is moderate. We favor tactical long exposure with defined risk parameters rather than aggressive positioning."
    elif bias in ['short', 'neutral_bearish']:
        if confidence == 'high':
            outlook = "Tomorrow's open warrants caution. Support levels are being tested, and a breach could trigger accelerated selling from systematic strategies. We favor defensive positioning and reduced gross exposure until stabilization is evident."
        else:
            outlook = "Risk management takes priority in the current environment. While oversold bounces are possible, the burden of proof is on the bulls to demonstrate demand at these levels."
    else:
        outlook = "The market is in a consolidation phase with balanced risks. We maintain neutral positioning and await clearer directional signals before committing capital. Range-bound trading strategies may be optimal."
    
    # Combine into final market summary
    summary_parts = [opening, vol_analysis, rotation_insight, global_insight]
    if commodity_insight:
        summary_parts.append(commodity_insight)
    summary_parts.extend([primary_driver, outlook])
    
    market_summary = " ".join(summary_parts)
    
    # === BUILD THE NEWS ANALYSIS PARAGRAPH ===
    news_analysis = generate_news_analysis_paragraph(news_items, news_sentiment, es_ch, sectors, assessment)
    
    return market_summary, news_analysis

def generate_news_analysis_paragraph(news_items, news_sentiment, market_change, sectors, assessment):
    """
    Generate an institutional-grade analysis of top news events impacting the market.
    """
    if not news_items:
        return "News flow is light today with no significant headlines driving price action. Market participants should monitor overnight developments and pre-market announcements for potential catalysts."
    
    # Categorize news by theme
    earnings_news = [n for n in news_items if 'Earnings' in n.get('categories', [])]
    economic_news = [n for n in news_items if 'Economic' in n.get('categories', [])]
    tech_news = [n for n in news_items if 'Tech' in n.get('categories', [])]
    analyst_news = [n for n in news_items if 'Analyst' in n.get('categories', [])]
    ma_news = [n for n in news_items if 'M&A' in n.get('categories', [])]
    
    bullish_count = news_sentiment.get('bullish', 0)
    bearish_count = news_sentiment.get('bearish', 0)
    overall_sentiment = news_sentiment.get('overall', 'neutral')
    
    # Build news analysis
    analysis_parts = []
    
    # Opening - overall news sentiment
    if overall_sentiment == 'bullish':
        analysis_parts.append(f"Today's news flow skews constructive with {bullish_count} bullish signals versus {bearish_count} bearish indicators across monitored headlines.")
    elif overall_sentiment == 'bearish':
        analysis_parts.append(f"Headlines are tilting negative with {bearish_count} cautionary signals outweighing {bullish_count} positive data points, contributing to risk-off positioning.")
    else:
        analysis_parts.append(f"News sentiment is balanced with {bullish_count} bullish and {bearish_count} bearish signals, suggesting no dominant narrative from today's headlines.")
    
    # Top stories analysis
    top_stories = news_items[:5]
    if top_stories:
        # Find most impactful stories (earnings and economic tend to move markets most)
        key_stories = []
        
        if earnings_news:
            for story in earnings_news[:2]:
                sentiment = story.get('sentiment', 'neutral')
                title_short = story['title'][:80]
                if sentiment == 'bullish':
                    key_stories.append(f"positive earnings momentum ('{title_short}...')")
                elif sentiment == 'bearish':
                    key_stories.append(f"earnings disappointment weighing on sentiment ('{title_short}...')")
        
        if economic_news:
            for story in economic_news[:2]:
                sentiment = story.get('sentiment', 'neutral')
                title_short = story['title'][:80]
                if 'fed' in story['title'].lower() or 'rate' in story['title'].lower():
                    key_stories.append(f"Federal Reserve/rate expectations shifting based on '{title_short}...'")
                elif 'inflation' in story['title'].lower():
                    key_stories.append(f"inflation data impacting rate trajectory ('{title_short}...')")
                elif 'jobs' in story['title'].lower() or 'employment' in story['title'].lower():
                    key_stories.append(f"labor market data influencing growth expectations")
        
        if tech_news and not key_stories:
            for story in tech_news[:1]:
                key_stories.append(f"technology sector developments ('{story['title'][:60]}...')")
        
        if ma_news:
            for story in ma_news[:1]:
                key_stories.append(f"M&A activity signaling corporate confidence ('{story['title'][:60]}...')")
        
        if key_stories:
            analysis_parts.append("Key drivers include: " + "; ".join(key_stories[:3]) + ".")
    
    # Market implications based on news
    if earnings_news and len(earnings_news) >= 2:
        bullish_earnings = sum(1 for e in earnings_news if e.get('sentiment') == 'bullish')
        bearish_earnings = sum(1 for e in earnings_news if e.get('sentiment') == 'bearish')
        if bullish_earnings > bearish_earnings:
            analysis_parts.append("Earnings releases are tracking above expectations, supporting the case for sustained corporate profit growth and potentially higher price targets.")
        elif bearish_earnings > bullish_earnings:
            analysis_parts.append("Earnings misses are dominating the tape, raising questions about forward guidance and the sustainability of current valuations.")
    
    if economic_news:
        analysis_parts.append("Economic data releases are shaping rate expectations and growth forecasts—traders should monitor Fed commentary for policy guidance.")
    
    # Geopolitical/macro considerations
    geopolitical_keywords = ['tariff', 'china', 'trade', 'war', 'sanctions', 'russia', 'ukraine', 'geopolitical', 'trump', 'biden', 'election', 'congress']
    geopolitical_news = [n for n in news_items if any(kw in n.get('title', '').lower() for kw in geopolitical_keywords)]
    
    if geopolitical_news:
        analysis_parts.append("Geopolitical headlines are adding to uncertainty—institutional portfolios may see increased hedging activity until clarity emerges.")
    
    # AI/Tech focus
    ai_keywords = ['ai', 'artificial intelligence', 'nvidia', 'openai', 'chatgpt', 'microsoft', 'google', 'meta', 'semiconductor', 'chip']
    ai_news = [n for n in news_items if any(kw in n.get('title', '').lower() for kw in ai_keywords)]
    
    if ai_news:
        analysis_parts.append("AI and technology themes continue to dominate headlines, with implications for semiconductor demand, cloud infrastructure spending, and mega-cap valuations.")
    
    # Forward-looking
    if overall_sentiment == 'bullish' and market_change > 0:
        analysis_parts.append("The alignment of positive headlines with constructive price action suggests institutional buyers are engaging—momentum may persist into tomorrow's session absent negative overnight developments.")
    elif overall_sentiment == 'bearish' and market_change < 0:
        analysis_parts.append("Negative news flow is being confirmed by price action, warranting tactical caution. Watch for dip-buyers to emerge at key technical support levels.")
    elif overall_sentiment != 'neutral' and ((overall_sentiment == 'bullish' and market_change < 0) or (overall_sentiment == 'bearish' and market_change > 0)):
        analysis_parts.append("The divergence between news sentiment and price action suggests positioning dynamics may be overriding fundamentals—this tension typically resolves within 2-3 sessions.")
    
    return " ".join(analysis_parts)

@st.cache_data(ttl=1800)
def get_upcoming_earnings():
    """Fetch comprehensive upcoming earnings calendar."""
    earnings = []
    
    # Extended watchlist of major stocks
    earnings_watchlist = [
        # Mega caps
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "BRK-B",
        # Financials
        "JPM", "BAC", "WFC", "GS", "MS", "C", "AXP", "BLK",
        # Healthcare
        "JNJ", "UNH", "PFE", "ABBV", "MRK", "LLY", "TMO", "ABT",
        # Consumer
        "PG", "KO", "PEP", "WMT", "COST", "MCD", "NKE", "SBUX",
        # Tech
        "V", "MA", "CRM", "ORCL", "ADBE", "NOW", "INTU", "IBM",
        "AMD", "INTC", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX",
        # Media/Comm
        "DIS", "NFLX", "CMCSA", "T", "VZ",
        # Industrials
        "BA", "CAT", "GE", "RTX", "LMT", "NOC", "HON", "UPS", "FDX",
        # Energy
        "XOM", "CVX", "COP", "SLB", "EOG",
    ]
    
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).date()
    
    for symbol in earnings_watchlist:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            company_name = info.get('shortName', info.get('longName', symbol))
            
            cal = ticker.calendar
            if cal and isinstance(cal, dict):
                ed = cal.get('Earnings Date', [])
                if isinstance(ed, list) and ed:
                    earnings_date = ed[0]
                    if hasattr(earnings_date, 'date'):
                        earnings_date = earnings_date.date()
                    elif isinstance(earnings_date, str):
                        try:
                            earnings_date = datetime.strptime(earnings_date[:10], '%Y-%m-%d').date()
                        except:
                            continue
                    
                    days_until = (earnings_date - today).days
                    if 0 <= days_until <= 14:
                        # Determine timing (BMO/AMC)
                        timing = ""
                        if 'Earnings Average' in cal:
                            timing = "Estimated"
                        else:
                            timing = "TBD"
                        
                        # Try to get EPS estimate
                        est_eps = info.get('forwardEps', info.get('trailingEps', 'N/A'))
                        if est_eps and est_eps != 'N/A':
                            est_eps = f"${est_eps:.2f}"
                        
                        earnings.append({
                            'symbol': symbol,
                            'name': company_name[:30],
                            'date': earnings_date.strftime('%m/%d'),
                            'days_until': days_until,
                            'is_today': days_until == 0,
                            'timing': timing,
                            'est_eps': est_eps,
                        })
        except:
            continue
    
    return sorted(earnings, key=lambda x: x['days_until'])[:20]

@st.cache_data(ttl=600)
def analyze_earnings_history(symbol):
    """Analyze a stock's earnings history and generate AI insights."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        company_name = info.get('shortName', info.get('longName', symbol))
        
        # Get earnings history
        earnings_hist = None
        try:
            earnings_hist = ticker.earnings_history
        except:
            pass
        
        if earnings_hist is None or earnings_hist.empty:
            try:
                earnings_hist = ticker.quarterly_earnings
            except:
                pass
        
        # Get earnings dates
        earnings_dates = None
        try:
            earnings_dates = ticker.earnings_dates
        except:
            pass
        
        # Build track record
        beats = 0
        misses = 0
        meets = 0
        recent_quarters = []
        price_reactions = []
        
        if earnings_hist is not None and not earnings_hist.empty:
            for idx, row in earnings_hist.head(8).iterrows():
                try:
                    actual = row.get('epsActual', row.get('Reported EPS', row.get('Actual', None)))
                    estimate = row.get('epsEstimate', row.get('EPS Estimate', row.get('Estimate', None)))
                    
                    if actual is not None and estimate is not None:
                        actual = float(actual)
                        estimate = float(estimate)
                        
                        surprise_pct = ((actual - estimate) / abs(estimate) * 100) if estimate != 0 else 0
                        
                        if surprise_pct > 2:
                            result = 'beat'
                            beats += 1
                        elif surprise_pct < -2:
                            result = 'miss'
                            misses += 1
                        else:
                            result = 'meet'
                            meets += 1
                        
                        # Format quarter name
                        quarter_name = str(idx)[:10] if hasattr(idx, 'strftime') else str(idx)
                        
                        recent_quarters.append({
                            'quarter': quarter_name,
                            'actual_eps': actual,
                            'est_eps': estimate,
                            'surprise_pct': surprise_pct,
                            'result': result,
                        })
                        
                        # Simulate price reaction (would need historical data for accuracy)
                        price_reactions.append({
                            'quarter': quarter_name,
                            'move': surprise_pct * 0.5 + np.random.uniform(-2, 2),  # Simplified estimation
                        })
                except:
                    continue
        
        # Get next earnings date
        next_earnings = "TBD"
        try:
            cal = ticker.calendar
            if cal and isinstance(cal, dict) and 'Earnings Date' in cal:
                ed = cal['Earnings Date']
                if isinstance(ed, list) and ed:
                    next_date = ed[0]
                    if hasattr(next_date, 'strftime'):
                        next_earnings = next_date.strftime('%b %d, %Y')
                    else:
                        next_earnings = str(next_date)[:10]
        except:
            pass
        
        # Generate AI analysis
        beat_rate = (beats / (beats + misses + meets) * 100) if (beats + misses + meets) > 0 else 0
        avg_surprise = np.mean([q['surprise_pct'] for q in recent_quarters]) if recent_quarters else 0
        
        # Build analysis paragraph
        if beat_rate >= 80:
            consistency = "exceptional track record of consistently beating analyst expectations"
        elif beat_rate >= 60:
            consistency = "solid history of meeting or exceeding estimates"
        elif beat_rate >= 40:
            consistency = "mixed earnings track record"
        else:
            consistency = "challenging history of meeting analyst expectations"
        
        if avg_surprise > 5:
            surprise_assessment = f"The company has delivered substantial positive surprises, averaging {avg_surprise:.1f}% above consensus—a pattern that suggests conservative guidance or analyst underestimation."
        elif avg_surprise > 0:
            surprise_assessment = f"Earnings have modestly exceeded expectations on average ({avg_surprise:+.1f}%), indicating reasonable predictability."
        elif avg_surprise > -5:
            surprise_assessment = f"Results have tracked close to estimates with occasional shortfalls ({avg_surprise:+.1f}% avg), typical for a mature company."
        else:
            surprise_assessment = f"The company has frequently disappointed relative to expectations ({avg_surprise:+.1f}% avg), warranting caution heading into reports."
        
        # Forward-looking
        pe_ratio = info.get('forwardPE', info.get('trailingPE', 0))
        if pe_ratio and pe_ratio > 0:
            if pe_ratio > 40:
                valuation_context = f"With a forward P/E of {pe_ratio:.1f}x, expectations are elevated—any guidance disappointment could trigger meaningful multiple compression."
            elif pe_ratio > 20:
                valuation_context = f"The forward P/E of {pe_ratio:.1f}x reflects growth expectations that require continued execution to sustain."
            else:
                valuation_context = f"At {pe_ratio:.1f}x forward earnings, the valuation provides some cushion for modest disappointments."
        else:
            valuation_context = "Valuation metrics suggest the stock is priced for its current growth trajectory."
        
        ai_analysis = f"{company_name} demonstrates a {consistency}, having beaten estimates {beats} out of the last {beats + misses + meets} quarters. {surprise_assessment} {valuation_context} Investors should monitor management commentary on forward guidance, margin trends, and any changes to full-year outlooks during the upcoming report."
        
        return {
            'company_name': company_name,
            'next_earnings': next_earnings,
            'track_record': {
                'beats': beats,
                'misses': misses,
                'meets': meets,
            },
            'recent_quarters': recent_quarters[:4],
            'price_reactions': price_reactions[:4],
            'ai_analysis': ai_analysis,
        }
    except Exception as e:
        return None

def analyze_earnings_content(text, title):
    """Analyze earnings call transcript or earnings news content."""
    text_lower = text.lower()
    
    # Extract key metrics
    key_metrics = []
    
    # Revenue patterns
    revenue_match = re.search(r'revenue[s]?\s+(?:of|was|reached|totaled)?\s*\$?([\d,.]+)\s*(billion|million|B|M)?', text, re.IGNORECASE)
    if revenue_match:
        key_metrics.append({'label': 'Revenue', 'value': f"${revenue_match.group(1)} {revenue_match.group(2) or ''}"})
    
    # EPS patterns
    eps_match = re.search(r'(?:eps|earnings per share)[:\s]+\$?([\d.]+)', text, re.IGNORECASE)
    if eps_match:
        key_metrics.append({'label': 'EPS', 'value': f"${eps_match.group(1)}"})
    
    # Guidance patterns
    guidance_match = re.search(r'(?:guidance|outlook|expect)[:\s]+.*?(\$?[\d,.]+\s*(?:billion|million|%)?)', text, re.IGNORECASE)
    if guidance_match:
        key_metrics.append({'label': 'Guidance', 'value': guidance_match.group(1)})
    
    # Growth patterns
    growth_match = re.search(r'(?:growth|grew|increase)[d]?\s+(?:of|by)?\s*([\d.]+)\s*%', text, re.IGNORECASE)
    if growth_match:
        key_metrics.append({'label': 'Growth', 'value': f"{growth_match.group(1)}%"})
    
    # Margin patterns
    margin_match = re.search(r'(?:margin|margins)[:\s]+(?:of|at|was)?\s*([\d.]+)\s*%', text, re.IGNORECASE)
    if margin_match:
        key_metrics.append({'label': 'Margin', 'value': f"{margin_match.group(1)}%"})
    
    # Sentiment analysis
    bullish_keywords = ['beat', 'exceeded', 'strong', 'growth', 'record', 'raised', 'optimistic', 'confident', 'accelerating', 'outperformed', 'momentum', 'robust']
    bearish_keywords = ['miss', 'missed', 'weak', 'decline', 'lowered', 'disappointing', 'challenging', 'headwinds', 'pressure', 'soft', 'below', 'cut']
    
    bullish_count = sum(1 for word in bullish_keywords if word in text_lower)
    bearish_count = sum(1 for word in bearish_keywords if word in text_lower)
    
    if bullish_count > bearish_count + 3:
        sentiment = 'bullish'
    elif bearish_count > bullish_count + 3:
        sentiment = 'bearish'
    else:
        sentiment = 'neutral'
    
    # Guidance tone
    if any(word in text_lower for word in ['raised guidance', 'increased outlook', 'raised forecast', 'above expectations']):
        guidance_tone = 'Raised'
    elif any(word in text_lower for word in ['lowered guidance', 'reduced outlook', 'cut forecast', 'below expectations']):
        guidance_tone = 'Lowered'
    elif any(word in text_lower for word in ['maintained guidance', 'reaffirmed', 'unchanged']):
        guidance_tone = 'Maintained'
    else:
        guidance_tone = 'Not Specified'
    
    # Key takeaways extraction
    takeaways = []
    
    # Find sentences with important keywords
    sentences = text.replace('\n', ' ').split('.')
    important_keywords = ['revenue', 'earnings', 'guidance', 'margin', 'growth', 'outlook', 'expect', 'beat', 'miss', 'record', 'forecast']
    
    for sent in sentences:
        sent = sent.strip()
        if 30 < len(sent) < 200:
            if any(kw in sent.lower() for kw in important_keywords):
                # Clean and add
                clean_sent = re.sub(r'\s+', ' ', sent).strip()
                if clean_sent and clean_sent not in takeaways:
                    takeaways.append(clean_sent)
                    if len(takeaways) >= 5:
                        break
    
    # If not enough takeaways, use generic extraction
    if len(takeaways) < 3:
        for sent in sentences[:20]:
            sent = sent.strip()
            if 50 < len(sent) < 150 and sent not in takeaways:
                takeaways.append(sent)
                if len(takeaways) >= 5:
                    break
    
    # Generate summary
    if 'beat' in text_lower and ('revenue' in text_lower or 'earnings' in text_lower):
        summary_opening = "The company delivered results that exceeded analyst expectations"
    elif 'miss' in text_lower:
        summary_opening = "Results fell short of consensus estimates"
    else:
        summary_opening = "The company reported quarterly results"
    
    if guidance_tone == 'Raised':
        guidance_text = "Management raised forward guidance, signaling confidence in the business trajectory."
    elif guidance_tone == 'Lowered':
        guidance_text = "Notably, management lowered guidance, citing headwinds that warrant investor attention."
    else:
        guidance_text = "Forward guidance remained largely in line with prior expectations."
    
    # Sector/theme detection
    themes = []
    if any(word in text_lower for word in ['ai', 'artificial intelligence', 'machine learning']):
        themes.append("AI momentum")
    if any(word in text_lower for word in ['cloud', 'saas', 'subscription']):
        themes.append("cloud/subscription growth")
    if any(word in text_lower for word in ['margin expansion', 'cost cutting', 'efficiency']):
        themes.append("margin improvement")
    
    theme_text = f"Key themes include {', '.join(themes)}." if themes else ""
    
    summary = f"{summary_opening}. {guidance_text} {theme_text} Investors should focus on management's commentary around forward demand, competitive positioning, and any changes to capital allocation priorities. The market's reaction will depend on whether results and guidance meet the elevated (or lowered) bar set by recent trading patterns."
    
    # Trading implications
    trading_implications = []
    
    if sentiment == 'bullish':
        trading_implications.append("Strong results may support continuation of bullish momentum")
        if guidance_tone == 'Raised':
            trading_implications.append("Raised guidance is typically a positive catalyst for sustained buying")
    elif sentiment == 'bearish':
        trading_implications.append("Weak results may pressure shares in near-term trading")
        if guidance_tone == 'Lowered':
            trading_implications.append("Lowered guidance often triggers analyst estimate revisions")
    else:
        trading_implications.append("Mixed results suggest range-bound trading until clearer signals emerge")
    
    trading_implications.append("Monitor options implied volatility for expected move around earnings")
    
    return {
        'key_metrics': key_metrics,
        'sentiment': sentiment,
        'guidance_tone': guidance_tone,
        'takeaways': takeaways,
        'summary': summary,
        'trading_implications': trading_implications,
    }

@st.cache_data(ttl=1800)
def get_earnings_today():
    """Fetch stocks with earnings today/this week."""
    earnings = []
    # Check major stocks for upcoming earnings
    earnings_watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "BAC", "WFC", 
                          "JNJ", "UNH", "PG", "V", "MA", "HD", "DIS", "NFLX", "CRM", "ORCL",
                          "AMD", "INTC", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX", "KLAC",
                          "XOM", "CVX", "COP", "SLB", "BA", "CAT", "GE", "RTX", "LMT", "NOC"]
    
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).date()
    
    for symbol in earnings_watchlist[:20]:  # Limit to avoid too many API calls
        try:
            ticker = yf.Ticker(symbol)
            cal = ticker.calendar
            if cal and isinstance(cal, dict) and 'Earnings Date' in cal:
                ed = cal['Earnings Date']
                if isinstance(ed, list) and ed:
                    earnings_date = ed[0]
                    if hasattr(earnings_date, 'date'):
                        earnings_date = earnings_date.date()
                    elif isinstance(earnings_date, str):
                        earnings_date = datetime.strptime(earnings_date[:10], '%Y-%m-%d').date()
                    
                    days_until = (earnings_date - today).days
                    if 0 <= days_until <= 7:
                        earnings.append({
                            'symbol': symbol,
                            'date': earnings_date,
                            'days_until': days_until
                        })
        except:
            pass
    
    return sorted(earnings, key=lambda x: x['days_until'])

def get_economic_calendar():
    """Get comprehensive economic calendar with real dates."""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    today = now.date()
    weekday = now.weekday()
    day_of_month = now.day
    month = now.month
    
    events = []
    
    # === REGULAR WEEKLY EVENTS ===
    weekly_events = {
        0: [  # Monday
            {"time": "10:00 AM", "event": "ISM Manufacturing (1st Mon of month)", "impact": "high", "condition": day_of_month <= 7},
            {"time": "Various", "event": "Fed Speaker Events", "impact": "medium", "condition": True},
        ],
        1: [  # Tuesday
            {"time": "10:00 AM", "event": "JOLTS Job Openings", "impact": "medium", "condition": True},
            {"time": "10:00 AM", "event": "Consumer Confidence", "impact": "medium", "condition": day_of_month > 20},
            {"time": "1:00 PM", "event": "Treasury Auction (2Y/5Y)", "impact": "medium", "condition": True},
        ],
        2: [  # Wednesday
            {"time": "8:15 AM", "event": "ADP Employment Report", "impact": "medium", "condition": day_of_month <= 7},
            {"time": "10:00 AM", "event": "ISM Services (1st Wed of month)", "impact": "high", "condition": day_of_month <= 7},
            {"time": "10:30 AM", "event": "EIA Crude Oil Inventories", "impact": "medium", "condition": True},
            {"time": "2:00 PM", "event": "FOMC Minutes (3 weeks after meeting)", "impact": "high", "condition": 14 <= day_of_month <= 21},
            {"time": "2:00 PM", "event": "Beige Book", "impact": "medium", "condition": day_of_month in [10, 11, 12, 13, 14, 15]},
        ],
        3: [  # Thursday
            {"time": "8:30 AM", "event": "Initial Jobless Claims", "impact": "high", "condition": True},
            {"time": "8:30 AM", "event": "Continuing Claims", "impact": "medium", "condition": True},
            {"time": "10:30 AM", "event": "EIA Natural Gas Storage", "impact": "low", "condition": True},
            {"time": "1:00 PM", "event": "Treasury Auction (7Y)", "impact": "medium", "condition": day_of_month > 20},
        ],
        4: [  # Friday
            {"time": "8:30 AM", "event": "Non-Farm Payrolls", "impact": "high", "condition": day_of_month <= 7},
            {"time": "8:30 AM", "event": "Unemployment Rate", "impact": "high", "condition": day_of_month <= 7},
            {"time": "10:00 AM", "event": "Consumer Sentiment (Prelim)", "impact": "medium", "condition": 10 <= day_of_month <= 15},
            {"time": "10:00 AM", "event": "Consumer Sentiment (Final)", "impact": "medium", "condition": day_of_month > 20},
        ],
    }
    
    # Add weekly events that match conditions
    for evt in weekly_events.get(weekday, []):
        if evt.get('condition', True):
            events.append({"time": evt['time'], "event": evt['event'], "impact": evt['impact']})
    
    # === MONTHLY EVENTS (approximate dates) ===
    monthly_events = [
        # CPI - usually around 10th-14th
        {"day_range": (10, 14), "time": "8:30 AM", "event": "CPI Inflation Report", "impact": "high"},
        # PPI - usually day after CPI
        {"day_range": (11, 15), "time": "8:30 AM", "event": "PPI Producer Prices", "impact": "high"},
        # Retail Sales - around 15th
        {"day_range": (13, 17), "time": "8:30 AM", "event": "Retail Sales", "impact": "high"},
        # Industrial Production - around 15th-17th
        {"day_range": (15, 18), "time": "9:15 AM", "event": "Industrial Production", "impact": "medium"},
        # Housing Starts - around 17th-19th
        {"day_range": (16, 20), "time": "8:30 AM", "event": "Housing Starts", "impact": "medium"},
        # Existing Home Sales - around 20th-22nd
        {"day_range": (19, 23), "time": "10:00 AM", "event": "Existing Home Sales", "impact": "medium"},
        # New Home Sales - around 23rd-26th
        {"day_range": (23, 27), "time": "10:00 AM", "event": "New Home Sales", "impact": "medium"},
        # Durable Goods - around 25th-27th
        {"day_range": (24, 28), "time": "8:30 AM", "event": "Durable Goods Orders", "impact": "high"},
        # GDP - end of month (quarterly)
        {"day_range": (26, 30), "time": "8:30 AM", "event": "GDP (Quarterly)", "impact": "high"},
        # PCE - last Friday of month
        {"day_range": (26, 31), "time": "8:30 AM", "event": "PCE Price Index (Fed's preferred)", "impact": "high"},
    ]
    
    for evt in monthly_events:
        if evt['day_range'][0] <= day_of_month <= evt['day_range'][1]:
            # Check if not already added
            if not any(e['event'] == evt['event'] for e in events):
                events.append({"time": evt['time'], "event": evt['event'], "impact": evt['impact']})
    
    # === FOMC MEETING DATES 2024-2025 ===
    # These are actual scheduled FOMC dates
    fomc_dates = [
        # 2025 FOMC Dates
        (1, 28, 29), (3, 18, 19), (5, 6, 7), (6, 17, 18),
        (7, 29, 30), (9, 16, 17), (11, 4, 5), (12, 16, 17)
    ]
    
    for fomc_month, day1, day2 in fomc_dates:
        if month == fomc_month:
            if day_of_month == day1:
                events.append({"time": "All Day", "event": "FOMC Meeting Day 1", "impact": "high"})
            elif day_of_month == day2:
                events.append({"time": "2:00 PM", "event": "FOMC Rate Decision & Statement", "impact": "high"})
                events.append({"time": "2:30 PM", "event": "Fed Chair Press Conference", "impact": "high"})
    
    # === EARNINGS RELEASES ===
    earnings = get_earnings_today()
    today_earnings = [e for e in earnings if e['days_until'] == 0]
    upcoming_earnings = [e for e in earnings if 0 < e['days_until'] <= 3]
    
    if today_earnings:
        symbols = ", ".join([e['symbol'] for e in today_earnings[:5]])
        extra = f" +{len(today_earnings)-5} more" if len(today_earnings) > 5 else ""
        events.insert(0, {"time": "Pre/Post Mkt", "event": f"Earnings: {symbols}{extra}", "impact": "high"})
    
    if upcoming_earnings:
        symbols = ", ".join([e['symbol'] for e in upcoming_earnings[:3]])
        events.append({"time": "This Week", "event": f"Upcoming: {symbols}", "impact": "medium"})
    
    # === MARKET HOURS EVENTS ===
    if weekday == 4:  # Friday
        events.append({"time": "4:00 PM", "event": "Weekly Options Expiration", "impact": "medium"})
    
    # Check for monthly options expiration (3rd Friday)
    if weekday == 4:
        # Find 3rd Friday
        first_day = today.replace(day=1)
        first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
        third_friday = first_friday + timedelta(days=14)
        if today == third_friday:
            events.append({"time": "4:00 PM", "event": "Monthly Options Expiration (OpEx)", "impact": "high"})
    
    # === BOND MARKET ===
    if weekday == 3:  # Thursday
        events.append({"time": "1:00 PM", "event": "30Y Treasury Auction (monthly)", "impact": "medium"})
    
    # Sort by impact then time
    impact_order = {"high": 0, "medium": 1, "low": 2, "varies": 3}
    events.sort(key=lambda x: (impact_order.get(x['impact'], 3), x['time']))
    
    # If no events, add placeholder
    if not events:
        events.append({"time": "Today", "event": "Light Calendar Day", "impact": "low"})
    
    return events[:10]  # Limit to top 10 events

def create_chart(hist, symbol, tf="5D", show_ind=True, support=None, resistance=None):
    """Create a professional-grade financial chart with technical indicators."""
    if hist is None or hist.empty: 
        return None
    
    # Determine max range based on timeframe
    if tf in ['1D']:
        max_range_pct = 12  # More lenient
    elif tf in ['5D']:
        max_range_pct = 15  # More lenient
    else:
        max_range_pct = 20  # More lenient
    
    # Clean the data using helper function
    try:
        hist = clean_chart_data(hist, max_range_pct=max_range_pct)
    except Exception:
        pass  # Use original data if cleaning fails
    
    if hist is None or len(hist) < 2:  # Reduced from 5 to 2
        return None
    
    # Determine number of subplot rows
    rows = 4 if show_ind and len(hist) >= 26 else 2
    heights = [0.55, 0.15, 0.15, 0.15] if rows == 4 else [0.75, 0.25]
    
    # Create subplots
    fig = make_subplots(
        rows=rows, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.02,
        row_heights=heights,
        subplot_titles=None
    )
    
    # === MAIN PRICE CHART ===
    # Candlestick with improved styling
    fig.add_trace(
        go.Candlestick(
            x=hist.index,
            open=hist['Open'],
            high=hist['High'],
            low=hist['Low'],
            close=hist['Close'],
            increasing=dict(line=dict(color='#00C805', width=1), fillcolor='#00C805'),
            decreasing=dict(line=dict(color='#FF3B30', width=1), fillcolor='#FF3B30'),
            name='Price',
            whiskerwidth=0.5,
            opacity=0.95
        ),
        row=1, col=1
    )
    
    # Moving Averages with better styling
    if len(hist) >= 20:
        ma20 = hist['Close'].rolling(20).mean()
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=ma20, 
                name='MA20', 
                line=dict(color='#00BFFF', width=1.5),
                hovertemplate='MA20: $%{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Bollinger Bands with subtle fill
        bb_std = hist['Close'].rolling(20).std()
        bb_upper = ma20 + bb_std * 2
        bb_lower = ma20 - bb_std * 2
        
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=bb_upper,
                name='BB Upper',
                line=dict(color='rgba(100,100,100,0.4)', width=1, dash='dot'),
                hoverinfo='skip'
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=bb_lower,
                name='BB Lower',
                line=dict(color='rgba(100,100,100,0.4)', width=1, dash='dot'),
                fill='tonexty',
                fillcolor='rgba(100,100,100,0.08)',
                hoverinfo='skip'
            ),
            row=1, col=1
        )
    
    if len(hist) >= 50:
        ma50 = hist['Close'].rolling(50).mean()
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=ma50,
                name='MA50',
                line=dict(color='#FFA500', width=1.5),
                hovertemplate='MA50: $%{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Support/Resistance levels with better styling
    current_price = hist['Close'].iloc[-1]
    
    if support:
        for i, s in enumerate(support[:2]):
            fig.add_hline(
                y=s[1], 
                line_dash="dash", 
                line_color="rgba(0,200,5,0.6)",
                line_width=1,
                annotation_text=f"S{i+1}: ${s[1]:.2f}",
                annotation_position="left",
                annotation_font_size=10,
                annotation_font_color="#00C805",
                row=1, col=1
            )
    
    if resistance:
        for i, r in enumerate(resistance[:2]):
            fig.add_hline(
                y=r[1],
                line_dash="dash",
                line_color="rgba(255,59,48,0.6)",
                line_width=1,
                annotation_text=f"R{i+1}: ${r[1]:.2f}",
                annotation_position="left",
                annotation_font_size=10,
                annotation_font_color="#FF3B30",
                row=1, col=1
            )
    
    # Current price line
    fig.add_hline(
        y=current_price,
        line_dash="solid",
        line_color="#00BFFF",
        line_width=1,
        annotation_text=f"${current_price:.2f}",
        annotation_position="right",
        annotation_font_size=11,
        annotation_font_color="#00BFFF",
        annotation_bgcolor="rgba(0,0,0,0.7)",
        row=1, col=1
    )
    
    # === TECHNICAL INDICATORS ===
    if rows == 4:
        # RSI
        delta = hist['Close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # RSI line with gradient coloring based on value
        rsi_colors = ['#FF3B30' if v > 70 else '#00C805' if v < 30 else '#00BFFF' for v in rsi]
        
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=rsi,
                name='RSI',
                line=dict(color='#A855F7', width=1.5),
                hovertemplate='RSI: %{y:.1f}<extra></extra>'
            ),
            row=2, col=1
        )
        
        # RSI zones
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(255,59,48,0.1)", line_width=0, row=2, col=1)
        fig.add_hrect(y0=0, y1=30, fillcolor="rgba(0,200,5,0.1)", line_width=0, row=2, col=1)
        fig.add_hline(y=70, line_dash="dot", line_color="rgba(255,59,48,0.5)", line_width=1, row=2, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="rgba(0,200,5,0.5)", line_width=1, row=2, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="rgba(100,100,100,0.3)", line_width=1, row=2, col=1)
        
        # MACD
        ema12 = hist['Close'].ewm(span=12, adjust=False).mean()
        ema26 = hist['Close'].ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - signal_line
        
        # MACD histogram with gradient colors
        hist_colors = ['#00C805' if h >= 0 else '#FF3B30' for h in macd_hist]
        
        fig.add_trace(
            go.Bar(
                x=hist.index, y=macd_hist,
                name='MACD Hist',
                marker_color=hist_colors,
                opacity=0.6,
                hovertemplate='Hist: %{y:.4f}<extra></extra>'
            ),
            row=3, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=macd_line,
                name='MACD',
                line=dict(color='#00BFFF', width=1.5),
                hovertemplate='MACD: %{y:.4f}<extra></extra>'
            ),
            row=3, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=signal_line,
                name='Signal',
                line=dict(color='#FFA500', width=1.5),
                hovertemplate='Signal: %{y:.4f}<extra></extra>'
            ),
            row=3, col=1
        )
        fig.add_hline(y=0, line_dash="solid", line_color="rgba(100,100,100,0.3)", line_width=1, row=3, col=1)
        
        vol_row = 4
    else:
        vol_row = 2
    
    # === VOLUME ===
    vol_colors = ['#00C805' if c >= o else '#FF3B30' for c, o in zip(hist['Close'], hist['Open'])]
    
    fig.add_trace(
        go.Bar(
            x=hist.index, y=hist['Volume'],
            name='Volume',
            marker_color=vol_colors,
            opacity=0.5,
            hovertemplate='Vol: %{y:,.0f}<extra></extra>'
        ),
        row=vol_row, col=1
    )
    
    # Add volume moving average
    if len(hist) >= 20:
        vol_ma = hist['Volume'].rolling(20).mean()
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=vol_ma,
                name='Vol MA20',
                line=dict(color='#FFA500', width=1),
                hoverinfo='skip'
            ),
            row=vol_row, col=1
        )
    
    # === LAYOUT ===
    eastern = pytz.timezone('US/Eastern')
    
    # Calculate price range for better Y axis
    price_min = hist['Low'].min()
    price_max = hist['High'].max()
    price_padding = (price_max - price_min) * 0.05
    
    fig.update_layout(
        title=dict(
            text=f"<b>{symbol}</b> · {tf} · {datetime.now(eastern).strftime('%I:%M %p ET')}",
            font=dict(size=14, color='#ffffff'),
            x=0.5,
            xanchor='center'
        ),
        template='plotly_dark',
        paper_bgcolor='rgba(13,17,23,1)',
        plot_bgcolor='rgba(22,27,34,1)',
        font=dict(family='Inter, sans-serif', color='#8b949e', size=10),
        showlegend=False,
        xaxis_rangeslider_visible=False,
        height=550 if rows == 4 else 380,
        margin=dict(l=60, r=60, t=50, b=30),
        hovermode='x unified',
        hoverlabel=dict(
            bgcolor='rgba(30,35,42,0.95)',
            font_size=11,
            font_family='Inter, sans-serif'
        )
    )
    
    # Update all x-axes to hide gaps (weekends, after hours)
    for i in range(1, rows + 1):
        fig.update_xaxes(
            row=i, col=1,
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(48,54,61,0.5)',
            showline=True,
            linewidth=1,
            linecolor='rgba(48,54,61,0.8)',
            zeroline=False,
            showticklabels=(i == rows),
            tickfont=dict(size=9),
            rangebreaks=[
                dict(bounds=["sat", "mon"]),  # Hide weekends
                dict(bounds=[20, 4], pattern="hour") if tf in ['1D', '5D'] else {}  # Hide after hours for intraday
            ]
        )
    
    # Update Y-axes
    fig.update_yaxes(
        row=1, col=1,
        title_text="Price ($)",
        title_font=dict(size=10),
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(48,54,61,0.3)',
        showline=True,
        linewidth=1,
        linecolor='rgba(48,54,61,0.8)',
        tickformat='$,.2f',
        tickfont=dict(size=9),
        range=[price_min - price_padding, price_max + price_padding]
    )
    
    if rows == 4:
        fig.update_yaxes(row=2, col=1, title_text="RSI", title_font=dict(size=9), range=[0, 100], 
                        showgrid=True, gridcolor='rgba(48,54,61,0.2)', tickfont=dict(size=8))
        fig.update_yaxes(row=3, col=1, title_text="MACD", title_font=dict(size=9),
                        showgrid=True, gridcolor='rgba(48,54,61,0.2)', tickfont=dict(size=8))
        fig.update_yaxes(row=4, col=1, title_text="Vol", title_font=dict(size=9),
                        showgrid=False, tickfont=dict(size=8), tickformat='.2s')
    else:
        fig.update_yaxes(row=2, col=1, title_text="Volume", title_font=dict(size=9),
                        showgrid=False, tickfont=dict(size=8), tickformat='.2s')
    
    return fig

def clean_chart_data(hist, max_range_pct=10):
    """Clean OHLC data to remove bad wicks and outliers.
    
    More lenient cleaning to ensure charts render for most tickers.
    """
    if hist is None or hist.empty:
        return None
    
    try:
        # Make a copy to avoid modifying original
        hist = hist.copy()
        
        # Remove rows with NaN in essential columns
        essential_cols = ['Open', 'High', 'Low', 'Close']
        for col in essential_cols:
            if col not in hist.columns:
                return None
        
        hist = hist.dropna(subset=essential_cols)
        
        if len(hist) < 2:
            return hist if len(hist) > 0 else None
        
        # Remove obviously invalid candles (High < Low)
        valid_hlc = hist['High'] >= hist['Low']
        if valid_hlc.sum() > 0:
            hist = hist[valid_hlc]
        
        if len(hist) < 2:
            return hist if len(hist) > 0 else None
        
        # Only filter extreme range candles if we have enough data
        if len(hist) >= 10:
            # Calculate candle range as percentage of close price
            close_prices = hist['Close'].replace(0, np.nan)  # Avoid division by zero
            candle_range_pct = (hist['High'] - hist['Low']) / close_prices * 100
            candle_range_pct = candle_range_pct.fillna(0)
            
            # Only filter if most candles are within normal range
            valid_range = candle_range_pct <= max_range_pct
            if valid_range.sum() >= len(hist) * 0.5:  # More lenient: 50% instead of 70%
                hist = hist[valid_range]
        
        if len(hist) < 2:
            return hist if len(hist) > 0 else None
        
        # IQR outlier removal - only if we have enough data points
        if len(hist) >= 20:
            try:
                q1 = hist['Close'].quantile(0.02)  # More lenient: 2% instead of 5%
                q3 = hist['Close'].quantile(0.98)  # More lenient: 98% instead of 95%
                iqr = q3 - q1
                if iqr > 0:
                    price_valid = (hist['Close'] >= q1 - 3*iqr) & (hist['Close'] <= q3 + 3*iqr)  # 3x instead of 2x
                    if price_valid.sum() >= len(hist) * 0.7:
                        hist = hist[price_valid]
            except Exception:
                pass  # Skip IQR filtering if it fails
        
        # Cap extreme wicks - only for charts with enough data
        if len(hist) >= 5:
            try:
                for idx in hist.index:
                    open_price = hist.loc[idx, 'Open']
                    close_price = hist.loc[idx, 'Close']
                    high_price = hist.loc[idx, 'High']
                    low_price = hist.loc[idx, 'Low']
                    
                    # Skip if any values are invalid
                    if pd.isna(open_price) or pd.isna(close_price) or pd.isna(high_price) or pd.isna(low_price):
                        continue
                    if close_price <= 0:
                        continue
                    
                    body_high = max(open_price, close_price)
                    body_low = min(open_price, close_price)
                    body_size = body_high - body_low
                    
                    # Allow larger wicks (5x body or 5% of price)
                    max_wick = max(body_size * 5, close_price * 0.05)
                    
                    if high_price > body_high + max_wick:
                        hist.loc[idx, 'High'] = body_high + max_wick
                    if low_price < body_low - max_wick:
                        hist.loc[idx, 'Low'] = max(body_low - max_wick, 0.01)  # Don't go below 0
            except Exception:
                pass  # Skip wick capping if it fails
        
        return hist if len(hist) >= 2 else None
        
    except Exception as e:
        # If anything fails, return original data
        return hist if hist is not None and len(hist) >= 2 else None

def create_mini_chart(hist, symbol, show_volume=True):
    """Create a simplified mini chart for dashboard views."""
    if hist is None or hist.empty:
        return None
    
    # Clean data using helper - more lenient for mini charts
    try:
        hist = clean_chart_data(hist, max_range_pct=15)
    except Exception:
        pass  # Use original data if cleaning fails
    
    if hist is None or len(hist) < 2:  # Reduced from 5 to 2
        return None
    
    rows = 2 if show_volume else 1
    heights = [0.8, 0.2] if show_volume else [1.0]
    
    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=heights)
    
    # Simple candlestick
    fig.add_trace(
        go.Candlestick(
            x=hist.index,
            open=hist['Open'],
            high=hist['High'],
            low=hist['Low'],
            close=hist['Close'],
            increasing=dict(line=dict(color='#00C805', width=1), fillcolor='#00C805'),
            decreasing=dict(line=dict(color='#FF3B30', width=1), fillcolor='#FF3B30'),
            name='Price',
            whiskerwidth=0.5
        ),
        row=1, col=1
    )
    
    # Simple MA
    if len(hist) >= 20:
        ma20 = hist['Close'].rolling(20).mean()
        fig.add_trace(
            go.Scatter(x=hist.index, y=ma20, name='MA20', line=dict(color='#00BFFF', width=1)),
            row=1, col=1
        )
    
    # Volume
    if show_volume:
        vol_colors = ['#00C805' if c >= o else '#FF3B30' for c, o in zip(hist['Close'], hist['Open'])]
        fig.add_trace(
            go.Bar(x=hist.index, y=hist['Volume'], marker_color=vol_colors, opacity=0.4),
            row=2, col=1
        )
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(22,27,34,1)',
        font=dict(family='Inter, sans-serif', color='#8b949e', size=9),
        showlegend=False,
        xaxis_rangeslider_visible=False,
        height=250,
        margin=dict(l=40, r=40, t=20, b=20),
        hovermode='x unified'
    )
    
    # Hide gaps
    fig.update_xaxes(
        showgrid=False,
        showticklabels=False,
        rangebreaks=[dict(bounds=["sat", "mon"])]
    )
    fig.update_yaxes(showgrid=True, gridcolor='rgba(48,54,61,0.3)', tickformat='$,.2f', tickfont=dict(size=8))
    
    return fig

def render_stock_report(symbol):
    st.markdown(f"## 📊 {symbol} - Institutional Analysis")
    if st.button("← Back to Dashboard", key="back_btn"): st.session_state.selected_stock = None; st.session_state.show_stock_report = False; st.rerun()
    
    with st.spinner(f"Loading comprehensive data for {symbol}..."):
        data = fetch_comprehensive_data(symbol)
    
    if not data: st.error(f"Could not load {symbol}"); return
    
    info = data.get('info', {})
    hist_5d = data.get('hist_5d')
    hist_3mo = data.get('hist_3mo')
    
    # Detect instrument type
    quote_type = info.get('quoteType', '').upper()
    is_etf = quote_type == 'ETF' or symbol in [s['symbol'] for s in SECTOR_ETFS.values()]
    is_future = '=F' in symbol or symbol.startswith('^')
    is_index = symbol.startswith('^')
    is_crypto = quote_type == 'CRYPTOCURRENCY' or '-USD' in symbol
    
    # Get appropriate label
    if is_etf:
        instrument_type = "ETF"
        type_color = "#a371f7"
    elif is_future:
        instrument_type = "Future" if '=F' in symbol else "Index"
        type_color = "#d29922"
    elif is_crypto:
        instrument_type = "Crypto"
        type_color = "#f7931a"
    else:
        instrument_type = "Stock"
        type_color = "#58a6ff"
    
    name = info.get('longName', info.get('shortName', symbol))
    sector = info.get('sector', info.get('category', 'N/A'))
    industry = info.get('industry', info.get('fundFamily', 'N/A'))
    
    # For ETFs, show category info
    if is_etf:
        sector = info.get('category', 'ETF')
        industry = info.get('fundFamily', info.get('exchange', 'N/A'))
    
    # For futures/indices
    if is_future or is_index:
        sector = info.get('exchange', 'Futures')
        industry = quote_type if quote_type else 'Derivative'
    
    price = info.get('currentPrice', info.get('regularMarketPrice', 0))
    if price == 0 and hist_5d is not None and not hist_5d.empty:
        price = hist_5d['Close'].iloc[-1]
    prev = safe_get(info, 'regularMarketPreviousClose', price)
    ch_pct = safe_pct_change(price, prev)
    ch_color = "#3fb950" if ch_pct >= 0 else "#f85149"
    
    # Calculate S/R levels
    support_levels, resistance_levels = calculate_support_resistance(hist_3mo, price)
    
    # Generate detailed signals
    signals = generate_detailed_signals(hist_3mo if hist_3mo is not None and len(hist_3mo) > 50 else hist_5d, info)
    
    # Analyze news
    news = data.get('news', [])
    news_sentiment = analyze_news_sentiment(news)
    
    # Generate expert analysis
    expert = generate_expert_analysis(symbol, data, signals, support_levels, resistance_levels, news_sentiment)
    
    # Header with instrument type badge
    type_badge = f'<span style="background: {type_color}; color: white; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.75rem; margin-left: 0.5rem;">{instrument_type}</span>'
    st.markdown(f'<div class="report-section"><div style="display:flex;justify-content:space-between;align-items:start;"><div><h2 style="margin:0;color:#fff;">{name}{type_badge}</h2><p style="color:#8b949e;margin:0.5rem 0;">{sector} · {industry}</p></div><div style="text-align:right;"><div style="font-size:2rem;font-weight:700;color:#fff;">${price:,.2f}</div><div style="color:{ch_color};font-size:1.1rem;">{ch_pct:+.2f}%</div></div></div></div>', unsafe_allow_html=True)
    
    # Key Stats - adapt based on instrument type
    cols = st.columns(6)
    
    if is_etf:
        # ETF-specific stats
        aum = info.get('totalAssets', info.get('marketCap', 0))
        aum_str = f"${aum/1e12:.2f}T" if aum >= 1e12 else f"${aum/1e9:.1f}B" if aum >= 1e9 else f"${aum/1e6:.0f}M" if aum else "N/A"
        expense = info.get('annualReportExpenseRatio', info.get('expenseRatio', 0))
        expense_str = f"{expense*100:.2f}%" if expense else "N/A"
        ytd_ret = info.get('ytdReturn', 0)
        ytd_str = f"{ytd_ret*100:.1f}%" if ytd_ret else "N/A"
        div_yield = info.get('yield', info.get('dividendYield', 0))
        div_str = f"{div_yield*100:.2f}%" if div_yield else "N/A"
        stats = [("AUM", aum_str), ("Expense", expense_str), ("52W Hi", f"${info.get('fiftyTwoWeekHigh', 0):.2f}"), ("52W Lo", f"${info.get('fiftyTwoWeekLow', 0):.2f}"), ("YTD Return", ytd_str), ("Div Yield", div_str)]
    elif is_future or is_index:
        # Future/Index stats
        open_price = info.get('regularMarketOpen', info.get('open', 0))
        day_high = info.get('dayHigh', info.get('regularMarketDayHigh', 0))
        day_low = info.get('dayLow', info.get('regularMarketDayLow', 0))
        vol = info.get('regularMarketVolume', info.get('volume', 0))
        vol_str = f"{vol/1e6:.1f}M" if vol and vol >= 1e6 else f"{vol/1e3:.0f}K" if vol else "N/A"
        stats = [("Open", f"${open_price:.2f}" if open_price else "N/A"), ("Day High", f"${day_high:.2f}" if day_high else "N/A"), ("Day Low", f"${day_low:.2f}" if day_low else "N/A"), ("52W Hi", f"${info.get('fiftyTwoWeekHigh', 0):.2f}"), ("52W Lo", f"${info.get('fiftyTwoWeekLow', 0):.2f}"), ("Volume", vol_str)]
    else:
        # Stock stats
        mc = info.get('marketCap', 0)
        mc_str = f"${mc/1e12:.2f}T" if mc >= 1e12 else f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M" if mc else "N/A"
        pe = info.get('trailingPE', info.get('forwardPE', 0))
        pe_str = f"{pe:.1f}" if pe and pe > 0 else "N/A"
        stats = [("Mkt Cap", mc_str), ("P/E", pe_str), ("52W Hi", f"${info.get('fiftyTwoWeekHigh', 0):.2f}"), ("52W Lo", f"${info.get('fiftyTwoWeekLow', 0):.2f}"), ("Avg Vol", f"{info.get('averageVolume', 0)/1e6:.1f}M" if info.get('averageVolume') else "N/A"), ("Beta", f"{info.get('beta', 1):.2f}" if info.get('beta') else "N/A")]
    
    for i, (l, v) in enumerate(stats):
        with cols[i]: st.markdown(f'<div class="company-stat"><div class="stat-value">{v}</div><div class="stat-label">{l}</div></div>', unsafe_allow_html=True)
    
    # Expert Analysis Section
    if expert:
        st.markdown(f"""
        <div class="expert-analysis">
            <div class="expert-header">🤖 AI Expert Analysis</div>
            <div class="expert-verdict" style="color: {expert['verdict_color']};">{expert['verdict']}</div>
            <p style="color: #8b949e; font-size: 0.8rem; margin-bottom: 1rem;">Technical Score: {expert['tech_score']:+d}</p>
            <p class="expert-text">{expert['text']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Chart Section
    st.markdown("### 📈 Price Chart with S/R Levels")
    tf_cols = st.columns(7)
    sel_tf = st.session_state.get('chart_tf', '5D')
    for i, tf in enumerate(TIMEFRAMES.keys()):
        with tf_cols[i]:
            if st.button(tf, key=f"tf_{tf}", use_container_width=True): sel_tf = tf; st.session_state.chart_tf = tf
    show_ind = st.checkbox("Show Technical Indicators", value=True, key="show_ind")
    tf_map = {'1D': 'hist_1d', '5D': 'hist_5d', '1M': 'hist_1mo', '3M': 'hist_3mo', '6M': 'hist_6mo', '1Y': 'hist_1y', 'YTD': 'hist_1y'}
    ch_hist = data.get(tf_map.get(sel_tf, 'hist_5d'))
    if ch_hist is not None and not ch_hist.empty:
        fig = create_chart(ch_hist, symbol, sel_tf, show_ind, support_levels, resistance_levels)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"Unable to render chart for {symbol}. The data may be incomplete or contain invalid values.")
    
    # Support/Resistance Display
    sr_col1, sr_col2 = st.columns(2)
    with sr_col1:
        st.markdown("#### 🟢 Support Levels")
        if support_levels:
            for name, level, strength in support_levels:
                dist = safe_div((price - level), price) * 100
                st.markdown(f'<div class="sr-level support-level"><span>{name}</span><span>${level:.2f} ({dist:.1f}% below)</span></div>', unsafe_allow_html=True)
        else:
            st.info("No clear support levels identified")
    with sr_col2:
        st.markdown("#### 🔴 Resistance Levels")
        if resistance_levels:
            for name, level, strength in resistance_levels:
                dist = safe_div((level - price), price) * 100
                st.markdown(f'<div class="sr-level resistance-level"><span>{name}</span><span>${level:.2f} ({dist:.1f}% above)</span></div>', unsafe_allow_html=True)
        else:
            st.info("No clear resistance levels identified")
    
    st.markdown("---")
    
    # Detailed Signals Section
    st.markdown("### 📊 Technical Signals (Institutional Detail)")
    if signals:
        for sig in signals:
            dir_class = f"signal-{sig['direction']}"
            dir_emoji = "📈" if sig['direction'] == 'bullish' else "📉" if sig['direction'] == 'bearish' else "➡️"
            strength_badge = f"<span style='background: {'#238636' if sig.get('strength') == 'strong' else '#9e6a03'}; color: white; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.7rem; margin-left: 0.5rem;'>{sig.get('strength', 'moderate').upper()}</span>"
            st.markdown(f"""
            <div class="signal-card {dir_class}">
                <div class="signal-title">{dir_emoji} {sig['name']} · {sig['value']}{strength_badge}</div>
                <div class="signal-detail">{sig['detail']}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Insufficient data for detailed signal analysis")
    
    st.markdown("---")
    
    # Two column layout for News and Fundamentals
    col_l, col_r = st.columns([1.5, 1])
    
    with col_l:
        st.markdown("### 📰 Latest News")
        if news_sentiment['items']:
            sent_color = "#3fb950" if news_sentiment['overall'] == 'bullish' else "#f85149" if news_sentiment['overall'] == 'bearish' else "#8b949e"
            st.markdown(f"<div style='margin-bottom:1rem;'><span style='color:{sent_color};font-weight:600;'>News Sentiment: {news_sentiment['overall'].upper()}</span> <span style='color:#8b949e;'>({news_sentiment['bullish']} bullish / {news_sentiment['bearish']} bearish signals)</span></div>", unsafe_allow_html=True)
            
            for item in news_sentiment['items'][:8]:
                c = "#3fb950" if item['sentiment'] == 'bullish' else "#f85149" if item['sentiment'] == 'bearish' else "#58a6ff"
                cats = " · ".join(item['categories'][:2])
                st.markdown(f"""
                <div class="news-item" style="border-left-color:{c};">
                    <div class="news-title">{item['title'][:100]}{'...' if len(item['title']) > 100 else ''}</div>
                    <div class="news-meta">{item['source']} · {item['time']} · {cats}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No recent news available for this stock")
        
        # Institutional Holders
        h = data.get('holders')
        if h is not None and not h.empty:
            with st.expander("🏛️ Top Institutional Holders", expanded=False):
                st.dataframe(h.head(8), use_container_width=True)
    
    with col_r:
        # Different content based on instrument type
        if is_etf:
            # ETF-specific info
            st.markdown("### 📊 ETF Details")
            
            # Holdings info
            top_holdings = info.get('holdings', [])
            if top_holdings:
                st.markdown("**Top Holdings:**")
                for holding in top_holdings[:5]:
                    name = holding.get('holdingName', holding.get('symbol', 'Unknown'))
                    pct = holding.get('holdingPercent', 0) * 100
                    st.markdown(f"<div style='display:flex;justify-content:space-between;font-size:0.85rem;padding:0.2rem 0;'><span style='color:#c9d1d9;'>{name}</span><span style='color:#58a6ff;'>{pct:.1f}%</span></div>", unsafe_allow_html=True)
            
            # Performance metrics
            st.markdown("### 📈 Performance")
            perf_metrics = [
                ("1Y Return", info.get('yearReturn', 0)),
                ("3Y Return", info.get('threeYearReturn', 0)),
                ("5Y Return", info.get('fiveYearReturn', 0)),
                ("YTD", info.get('ytdReturn', 0)),
            ]
            for label, val in perf_metrics:
                if val:
                    color = "#3fb950" if val > 0 else "#f85149"
                    st.markdown(f"<div style='display:flex;justify-content:space-between;padding:0.2rem 0;font-size:0.85rem;'><span style='color:#8b949e;'>{label}</span><span style='color:{color};'>{val*100:+.1f}%</span></div>", unsafe_allow_html=True)
            
            # Fund info
            st.markdown("### 📋 Fund Info")
            fund_info = [
                ("Category", info.get('category', 'N/A')),
                ("Family", info.get('fundFamily', 'N/A')),
                ("Expense Ratio", f"{info.get('annualReportExpenseRatio', 0)*100:.2f}%" if info.get('annualReportExpenseRatio') else "N/A"),
                ("Div Yield", f"{info.get('yield', 0)*100:.2f}%" if info.get('yield') else "N/A"),
                ("Beta", f"{info.get('beta3Year', info.get('beta', 0)):.2f}" if info.get('beta3Year') or info.get('beta') else "N/A"),
            ]
            for label, val in fund_info:
                st.markdown(f"<div style='display:flex;justify-content:space-between;padding:0.2rem 0;font-size:0.85rem;'><span style='color:#8b949e;'>{label}</span><span style='color:#fff;'>{val}</span></div>", unsafe_allow_html=True)
        
        elif is_future or is_index:
            # Futures/Index-specific info
            st.markdown("### 📊 Contract Details")
            
            contract_info = [
                ("Exchange", info.get('exchange', 'N/A')),
                ("Currency", info.get('currency', 'USD')),
                ("Quote Type", info.get('quoteType', 'N/A')),
            ]
            for label, val in contract_info:
                st.markdown(f"<div style='display:flex;justify-content:space-between;padding:0.2rem 0;font-size:0.85rem;'><span style='color:#8b949e;'>{label}</span><span style='color:#fff;'>{val}</span></div>", unsafe_allow_html=True)
            
            # Trading info
            st.markdown("### 📈 Trading Info")
            day_range = f"${info.get('dayLow', 0):.2f} - ${info.get('dayHigh', 0):.2f}"
            week_range = f"${info.get('fiftyTwoWeekLow', 0):.2f} - ${info.get('fiftyTwoWeekHigh', 0):.2f}"
            
            trading_info = [
                ("Day Range", day_range),
                ("52W Range", week_range),
                ("Prev Close", f"${info.get('regularMarketPreviousClose', 0):.2f}"),
                ("Open", f"${info.get('regularMarketOpen', 0):.2f}"),
                ("Volume", f"{info.get('regularMarketVolume', 0):,}" if info.get('regularMarketVolume') else "N/A"),
            ]
            for label, val in trading_info:
                st.markdown(f"<div style='display:flex;justify-content:space-between;padding:0.2rem 0;font-size:0.85rem;'><span style='color:#8b949e;'>{label}</span><span style='color:#fff;'>{val}</span></div>", unsafe_allow_html=True)
            
            # For indices, show what it tracks
            if is_index:
                st.markdown("### ℹ️ About")
                st.info("This is an index that tracks a basket of securities. It cannot be traded directly - use ETFs or futures for exposure.")
        
        else:
            # Standard stock info
            # Earnings
            st.markdown("### 💰 Earnings History")
            earn = data.get('earnings')
            earnings_displayed = False
            
            if earn is not None and not earn.empty:
                recent_earn = earn.tail(4)
                
                for idx, row in recent_earn.iterrows():
                    act = row.get('epsActual', row.get('Reported EPS', row.get('reportedEPS', row.get('Actual', None))))
                    est = row.get('epsEstimate', row.get('EPS Estimate', row.get('estimatedEPS', row.get('Estimate', None))))
                    
                    try:
                        act = float(act) if act is not None and str(act) not in ['nan', 'None', ''] else None
                        est = float(est) if est is not None and str(est) not in ['nan', 'None', ''] else None
                    except:
                        continue
                    
                    if act is not None and est is not None and est != 0:
                        surp = ((act - est) / abs(est) * 100)
                        cls = "earnings-beat" if surp > 2 else "earnings-miss" if surp < -2 else "earnings-inline"
                        em = "✅" if surp > 2 else "❌" if surp < -2 else "➖"
                        date_str = str(idx)[:10] if idx else ""
                        
                        st.markdown(f"""
                        <div class="earnings-card {cls}">
                            <div style="display:flex;justify-content:space-between;">
                                <span style="color:#8b949e;">{date_str}</span>
                                <span>{em}</span>
                            </div>
                            <div style="margin-top:0.5rem;">
                                <span style="color:#8b949e;">EPS:</span> ${act:.2f} vs ${est:.2f} 
                                <span style="color:{'#3fb950' if surp > 0 else '#f85149'};">({surp:+.1f}%)</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        earnings_displayed = True
            
            if not earnings_displayed:
                trailing_eps = info.get('trailingEps')
                if trailing_eps:
                    st.markdown(f"""
                    <div class="earnings-card earnings-inline">
                        <div style="color:#8b949e;">Trailing EPS (TTM)</div>
                        <div style="font-size:1.2rem;font-weight:600;color:#fff;margin-top:0.25rem;">${trailing_eps:.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.info("No earnings data available")
            
            # Events
            st.markdown("### 📅 Upcoming Events")
            cal = data.get('calendar')
            if cal and isinstance(cal, dict):
                if 'Earnings Date' in cal:
                    ed = cal['Earnings Date']
                    st.markdown(f'<div class="event-card event-impact-high"><div class="event-time">📊 Earnings</div><div class="event-title">{ed[0] if isinstance(ed, list) and ed else ed}</div></div>', unsafe_allow_html=True)
                if 'Ex-Dividend Date' in cal:
                    st.markdown(f'<div class="event-card event-impact-medium"><div class="event-time">💰 Ex-Dividend</div><div class="event-title">{cal["Ex-Dividend Date"]}</div></div>', unsafe_allow_html=True)
            else:
                st.info("No scheduled events")
            
            # Analyst Ratings (stocks only)
            st.markdown("### 📊 Analyst Consensus")
            
            # First try to get from info dict (more reliable)
            rec_key = info.get('recommendationKey', '')
            num_analysts = info.get('numberOfAnalystOpinions', 0)
            rec_mean = info.get('recommendationMean', 0)  # 1=Strong Buy, 5=Sell
            
            analyst_displayed = False
            
            if rec_key or rec_mean:
                # Map recommendation key to display
                if rec_key:
                    rating = rec_key.replace('_', ' ').title()
                elif rec_mean:
                    if rec_mean <= 1.5: rating = "Strong Buy"
                    elif rec_mean <= 2.5: rating = "Buy"
                    elif rec_mean <= 3.5: rating = "Hold"
                    elif rec_mean <= 4.5: rating = "Sell"
                    else: rating = "Strong Sell"
                
                cls = "rating-buy" if 'Buy' in rating else "rating-sell" if 'Sell' in rating else "rating-hold"
                analyst_txt = f"{num_analysts} analysts" if num_analysts else ""
                mean_txt = f" (Score: {rec_mean:.1f}/5)" if rec_mean else ""
                
                st.markdown(f'<div style="text-align:center;"><span class="analyst-rating {cls}">{rating}</span><p style="color:#8b949e;font-size:0.8rem;">{analyst_txt}{mean_txt}</p></div>', unsafe_allow_html=True)
                analyst_displayed = True
            
            # Also try recommendations dataframe for breakdown
            recs = data.get('recommendations')
            if recs is not None and not recs.empty:
                recent = recs.tail(20)
                col_n = None
                for possible_col in ['To Grade', 'toGrade', 'grade', 'Grade', 'rating', 'Rating', 'action', 'Action']:
                    if possible_col in recent.columns:
                        col_n = possible_col
                        break
                
                if col_n:
                    g = recent[col_n].value_counts()
                    buy = sum(g.get(t, 0) for t in ['Buy', 'Outperform', 'Overweight', 'Strong Buy', 'Positive', 'Strong-Buy', 'Market Outperform', 'Sector Outperform'])
                    hold = sum(g.get(t, 0) for t in ['Hold', 'Neutral', 'Equal-Weight', 'Market Perform', 'Sector Perform', 'Equal Weight', 'In-Line', 'Inline'])
                    sell = sum(g.get(t, 0) for t in ['Sell', 'Underperform', 'Underweight', 'Reduce', 'Negative', 'Strong Sell', 'Market Underperform', 'Sector Underperform'])
                    total = buy + hold + sell
                    
                    if total > 0:
                        if not analyst_displayed:
                            rating = 'Strong Buy' if buy > total * 0.7 else 'Buy' if buy > hold and buy > sell else 'Sell' if sell > hold else 'Hold'
                            cls = "rating-buy" if 'Buy' in rating else "rating-sell" if rating == 'Sell' else "rating-hold"
                            st.markdown(f'<div style="text-align:center;"><span class="analyst-rating {cls}">{rating}</span><p style="color:#8b949e;font-size:0.8rem;">{total} recent ratings</p></div>', unsafe_allow_html=True)
                        
                        st.markdown(f"""
                        <div style="font-size:0.85rem; margin-top:0.5rem;">
                            <div style="display:flex;justify-content:space-between;"><span style="color:#3fb950;">Buy/Outperform</span><span>{buy} ({buy/total*100:.0f}%)</span></div>
                            <div style="display:flex;justify-content:space-between;"><span style="color:#d29922;">Hold/Neutral</span><span>{hold} ({hold/total*100:.0f}%)</span></div>
                            <div style="display:flex;justify-content:space-between;"><span style="color:#f85149;">Sell/Underperform</span><span>{sell} ({sell/total*100:.0f}%)</span></div>
                        </div>
                        """, unsafe_allow_html=True)
                        analyst_displayed = True
            
            if not analyst_displayed:
                st.info("No analyst ratings available")
            
            # Price Targets
            st.markdown("### 🎯 Price Targets")
            tl, tm, th = info.get('targetLowPrice', 0), info.get('targetMeanPrice', 0), info.get('targetHighPrice', 0)
            if tm and price:
                up = safe_pct_change(tm, price)
                st.markdown(f"""
                <div style="font-size:0.9rem;">
                    <div style="display:flex;justify-content:space-between;"><span style="color:#f85149;">Low</span><span>${tl:.2f}</span></div>
                    <div style="display:flex;justify-content:space-between;"><span style="color:#58a6ff;">Consensus</span><span style="font-weight:600;">${tm:.2f} ({up:+.1f}%)</span></div>
                    <div style="display:flex;justify-content:space-between;"><span style="color:#3fb950;">High</span><span>${th:.2f}</span></div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No price targets available")
            
            # Key Fundamentals
            st.markdown("### 📈 Fundamentals")
            for l, v in [("EPS (TTM)", f"${info.get('trailingEps', 0):.2f}" if info.get('trailingEps') else "N/A"), ("Fwd EPS", f"${info.get('forwardEps', 0):.2f}" if info.get('forwardEps') else "N/A"), ("Rev Growth", f"{info.get('revenueGrowth', 0)*100:.1f}%" if info.get('revenueGrowth') else "N/A"), ("Profit Margin", f"{info.get('profitMargins', 0)*100:.1f}%" if info.get('profitMargins') else "N/A"), ("ROE", f"{info.get('returnOnEquity', 0)*100:.1f}%" if info.get('returnOnEquity') else "N/A"), ("Debt/Equity", f"{info.get('debtToEquity', 0)/100:.2f}" if info.get('debtToEquity') else "N/A")]:
                st.markdown(f"<div style='display:flex;justify-content:space-between;padding:0.2rem 0;font-size:0.85rem;'><span style='color:#8b949e;'>{l}</span><span style='color:#fff;'>{v}</span></div>", unsafe_allow_html=True)
    
    # === INSTITUTIONAL ACTIVITY / WHALE TRACKER SECTION ===
    st.markdown("---")
    st.markdown("### 🐋 Institutional Activity & Whale Tracker")
    st.markdown("<p style='color: #8b949e; font-size: 0.8rem;'>Insider transactions, options flow, and institutional ownership signals</p>", unsafe_allow_html=True)
    
    # Analyze institutional activity
    inst_activity = analyze_institutional_activity(data, price)
    
    # Overall signal banner
    signal = inst_activity['overall_signal']
    if signal == 'bullish':
        signal_color = '#3fb950'
        signal_bg = 'rgba(63,185,80,0.1)'
        signal_text = '🟢 BULLISH INSTITUTIONAL FLOW'
    elif signal == 'bearish':
        signal_color = '#f85149'
        signal_bg = 'rgba(248,81,73,0.1)'
        signal_text = '🔴 BEARISH INSTITUTIONAL FLOW'
    else:
        signal_color = '#d29922'
        signal_bg = 'rgba(210,153,34,0.1)'
        signal_text = '🟡 NEUTRAL INSTITUTIONAL FLOW'
    
    st.markdown(f"""
    <div style="background: {signal_bg}; border: 1px solid {signal_color}; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem; text-align: center;">
        <span style="color: {signal_color}; font-weight: 700; font-size: 1rem;">{signal_text}</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Whale signals summary
    if inst_activity['whale_signals']:
        st.markdown("#### 🎯 Key Whale Signals")
        signal_cols = st.columns(min(3, len(inst_activity['whale_signals'])))
        for i, (emoji, signal_text) in enumerate(inst_activity['whale_signals'][:6]):
            with signal_cols[i % 3]:
                bg_color = 'rgba(63,185,80,0.15)' if emoji == '🟢' else 'rgba(248,81,73,0.15)' if emoji == '🔴' else 'rgba(210,153,34,0.15)'
                st.markdown(f"""
                <div style="background: {bg_color}; border-radius: 6px; padding: 0.5rem; margin: 0.25rem 0; text-align: center;">
                    <span style="font-size: 0.8rem;">{emoji} {signal_text}</span>
                </div>
                """, unsafe_allow_html=True)
    
    # Three columns: Ownership | Insider Activity | Options Flow
    whale_col1, whale_col2, whale_col3 = st.columns(3)
    
    with whale_col1:
        st.markdown("#### 🏛️ Ownership")
        inst_own = inst_activity['institutional_ownership']
        insider_own = inst_activity['insider_ownership']
        
        # Institutional ownership bar
        inst_color = '#3fb950' if inst_own > 60 else '#d29922' if inst_own > 30 else '#f85149'
        st.markdown(f"""
        <div style="margin-bottom: 0.75rem;">
            <div style="display: flex; justify-content: space-between; font-size: 0.8rem; margin-bottom: 0.25rem;">
                <span style="color: #8b949e;">Institutional</span>
                <span style="color: {inst_color}; font-weight: 600;">{inst_own:.1f}%</span>
            </div>
            <div style="background: rgba(48,54,61,0.5); border-radius: 4px; height: 8px; overflow: hidden;">
                <div style="background: {inst_color}; width: {min(inst_own, 100)}%; height: 100%;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Insider ownership bar
        insider_color = '#3fb950' if insider_own > 10 else '#d29922' if insider_own > 3 else '#8b949e'
        st.markdown(f"""
        <div style="margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; font-size: 0.8rem; margin-bottom: 0.25rem;">
                <span style="color: #8b949e;">Insider</span>
                <span style="color: {insider_color}; font-weight: 600;">{insider_own:.1f}%</span>
            </div>
            <div style="background: rgba(48,54,61,0.5); border-radius: 4px; height: 8px; overflow: hidden;">
                <div style="background: {insider_color}; width: {min(insider_own * 2, 100)}%; height: 100%;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Top institutional holders
        holders = data.get('holders')
        if holders is not None and not holders.empty:
            st.markdown("<p style='color: #8b949e; font-size: 0.7rem; margin-top: 0.5rem;'>Top Holders:</p>", unsafe_allow_html=True)
            for idx, row in holders.head(3).iterrows():
                holder_name = str(row.get('Holder', row.get('holder', 'Unknown')))[:25]
                st.markdown(f"<p style='color: #c9d1d9; font-size: 0.75rem; margin: 0.1rem 0;'>• {holder_name}</p>", unsafe_allow_html=True)
    
    with whale_col2:
        st.markdown("#### 👔 Insider Activity")
        
        buy_count = inst_activity['insider_buy_count']
        sell_count = inst_activity['insider_sell_count']
        net_value = inst_activity['insider_net_value']
        
        # Buy/Sell summary
        st.markdown(f"""
        <div style="display: flex; justify-content: space-around; margin-bottom: 0.75rem;">
            <div style="text-align: center;">
                <div style="color: #3fb950; font-size: 1.2rem; font-weight: 700;">{buy_count}</div>
                <div style="color: #8b949e; font-size: 0.7rem;">Buys</div>
            </div>
            <div style="text-align: center;">
                <div style="color: #f85149; font-size: 1.2rem; font-weight: 700;">{sell_count}</div>
                <div style="color: #8b949e; font-size: 0.7rem;">Sells</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Net value
        net_color = '#3fb950' if net_value > 0 else '#f85149' if net_value < 0 else '#8b949e'
        net_text = f"+${net_value/1e6:.1f}M" if net_value >= 1e6 else f"+${net_value/1e3:.0f}K" if net_value > 0 else f"-${abs(net_value)/1e6:.1f}M" if net_value <= -1e6 else f"-${abs(net_value)/1e3:.0f}K" if net_value < 0 else "$0"
        
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 0.5rem;">
            <span style="color: #8b949e; font-size: 0.75rem;">Net Insider Flow: </span>
            <span style="color: {net_color}; font-weight: 600;">{net_text}</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Recent transactions
        if inst_activity['insider_transactions']:
            st.markdown("<p style='color: #8b949e; font-size: 0.7rem;'>Recent Transactions:</p>", unsafe_allow_html=True)
            for txn in inst_activity['insider_transactions'][:4]:
                val_str = f"${txn['value']/1e6:.1f}M" if txn['value'] >= 1e6 else f"${txn['value']/1e3:.0f}K"
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; font-size: 0.7rem; padding: 0.15rem 0; border-bottom: 1px solid rgba(48,54,61,0.5);">
                    <span style="color: {txn['color']};">{txn['type']}</span>
                    <span style="color: #8b949e;">{val_str}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("<p style='color: #6e7681; font-size: 0.75rem; font-style: italic;'>No recent insider transactions</p>", unsafe_allow_html=True)
    
    with whale_col3:
        st.markdown("#### 📊 Options Flow")
        
        call_vol = inst_activity['call_volume']
        put_vol = inst_activity['put_volume']
        pc_ratio = inst_activity['put_call_ratio']
        
        # Volume summary
        st.markdown(f"""
        <div style="display: flex; justify-content: space-around; margin-bottom: 0.75rem;">
            <div style="text-align: center;">
                <div style="color: #3fb950; font-size: 1.1rem; font-weight: 700;">{call_vol/1e3:.0f}K</div>
                <div style="color: #8b949e; font-size: 0.7rem;">Call Vol</div>
            </div>
            <div style="text-align: center;">
                <div style="color: #f85149; font-size: 1.1rem; font-weight: 700;">{put_vol/1e3:.0f}K</div>
                <div style="color: #8b949e; font-size: 0.7rem;">Put Vol</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Put/Call ratio
        pc_color = '#3fb950' if pc_ratio < 0.7 else '#f85149' if pc_ratio > 1.3 else '#d29922'
        pc_sentiment = 'Bullish' if pc_ratio < 0.7 else 'Bearish' if pc_ratio > 1.3 else 'Neutral'
        
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 0.5rem;">
            <span style="color: #8b949e; font-size: 0.75rem;">P/C Ratio: </span>
            <span style="color: {pc_color}; font-weight: 600;">{pc_ratio:.2f}</span>
            <span style="color: {pc_color}; font-size: 0.7rem;"> ({pc_sentiment})</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Unusual options activity
        if inst_activity['unusual_options']:
            st.markdown("<p style='color: #8b949e; font-size: 0.7rem;'>🔥 Unusual Activity:</p>", unsafe_allow_html=True)
            for opt in inst_activity['unusual_options'][:3]:
                otm_label = "OTM" if opt['otm'] else "ITM"
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; font-size: 0.7rem; padding: 0.15rem 0; border-bottom: 1px solid rgba(48,54,61,0.5);">
                    <span style="color: {opt['color']};">{opt['type']} ${opt['strike']:.0f} {otm_label}</span>
                    <span style="color: #8b949e;">{opt['volume']/1e3:.1f}K vol ({opt['ratio']:.1f}x OI)</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("<p style='color: #6e7681; font-size: 0.75rem; font-style: italic;'>No unusual options activity</p>", unsafe_allow_html=True)
    
    # === DARK POOL & SHORT INTEREST ROW ===
    st.markdown("#### 🌑 Dark Pool & Short Interest Estimates")
    dp_col1, dp_col2, dp_col3, dp_col4 = st.columns(4)
    
    with dp_col1:
        dark_pool_est = inst_activity['dark_pool_estimate']
        dp_sentiment = inst_activity.get('dark_pool_sentiment', 'neutral')
        dp_color = '#3fb950' if dp_sentiment == 'accumulation' else '#f85149' if dp_sentiment == 'distribution' else '#a371f7'
        dp_label = '📈 Accum' if dp_sentiment == 'accumulation' else '📉 Distr' if dp_sentiment == 'distribution' else 'Est.'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {dp_color}; font-size: 1.3rem; font-weight: 700;">{dark_pool_est}%</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Dark Pool Volume</div>
            <div style="color: {dp_color}; font-size: 0.6rem; font-style: italic;">{dp_label}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with dp_col2:
        short_int = inst_activity['short_interest']
        short_color = '#f85149' if short_int > 15 else '#d29922' if short_int > 8 else '#3fb950'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {short_color}; font-size: 1.3rem; font-weight: 700;">{short_int:.1f}%</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Short Interest</div>
            <div style="color: #6e7681; font-size: 0.6rem;">% of float shorted</div>
        </div>
        """, unsafe_allow_html=True)
    
    with dp_col3:
        short_ratio = inst_activity['short_ratio']
        sr_color = '#f85149' if short_ratio > 5 else '#d29922' if short_ratio > 3 else '#3fb950'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {sr_color}; font-size: 1.3rem; font-weight: 700;">{short_ratio:.1f}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Days to Cover</div>
            <div style="color: #6e7681; font-size: 0.6rem;">Short ratio</div>
        </div>
        """, unsafe_allow_html=True)
    
    with dp_col4:
        rel_vol = inst_activity['relative_volume']
        rv_color = '#3fb950' if rel_vol > 1.5 else '#d29922' if rel_vol > 0.8 else '#f85149'
        rv_label = "High" if rel_vol > 1.5 else "Normal" if rel_vol > 0.8 else "Low"
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {rv_color}; font-size: 1.3rem; font-weight: 700;">{rel_vol:.2f}x</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Relative Volume</div>
            <div style="color: #6e7681; font-size: 0.6rem;">{rv_label} vs avg</div>
        </div>
        """, unsafe_allow_html=True)
    
    # NEW: Smart Money & Squeeze Potential Row
    st.markdown("#### 🧠 Smart Money Indicators")
    sm_col1, sm_col2, sm_col3, sm_col4 = st.columns(4)
    
    with sm_col1:
        smart_score = inst_activity.get('smart_money_score', 50)
        sm_color = '#3fb950' if smart_score > 60 else '#f85149' if smart_score < 40 else '#d29922'
        sm_label = 'Bullish' if smart_score > 60 else 'Bearish' if smart_score < 40 else 'Neutral'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {sm_color}; font-size: 1.3rem; font-weight: 700;">{smart_score}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Smart Money Score</div>
            <div style="color: {sm_color}; font-size: 0.6rem;">{sm_label}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with sm_col2:
        squeeze = inst_activity.get('squeeze_potential', 0)
        sq_color = '#f7931a' if squeeze > 70 else '#d29922' if squeeze > 40 else '#8b949e'
        sq_label = '🚀 HIGH' if squeeze > 70 else 'Moderate' if squeeze > 40 else 'Low'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {sq_color}; font-size: 1.3rem; font-weight: 700;">{squeeze}%</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Squeeze Potential</div>
            <div style="color: {sq_color}; font-size: 0.6rem;">{sq_label}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with sm_col3:
        ad_phase = inst_activity.get('accumulation_distribution', 'neutral')
        ad_color = '#3fb950' if ad_phase == 'accumulation' else '#f85149' if ad_phase == 'distribution' else '#8b949e'
        ad_icon = '📈' if ad_phase == 'accumulation' else '📉' if ad_phase == 'distribution' else '➡️'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {ad_color}; font-size: 1.3rem; font-weight: 700;">{ad_icon}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">A/D Phase</div>
            <div style="color: {ad_color}; font-size: 0.6rem;">{ad_phase.title()}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with sm_col4:
        momentum = inst_activity.get('institutional_momentum', 'neutral')
        mom_color = '#3fb950' if 'bullish' in momentum else '#f85149' if 'bearish' in momentum else '#8b949e'
        mom_icon = '🟢' if 'bullish' in momentum else '🔴' if 'bearish' in momentum else '🟡'
        mom_label = 'Strong' if 'strong' in momentum else 'Moderate' if momentum != 'neutral' else 'Neutral'
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 0.75rem;">
            <div style="color: {mom_color}; font-size: 1.3rem; font-weight: 700;">{mom_icon}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">Inst. Momentum</div>
            <div style="color: {mom_color}; font-size: 0.6rem;">{mom_label}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Whale Activity Interpretation
    st.markdown("#### 📊 Institutional Flow Analysis")
    
    # Build interpretation based on signals
    interpretations = []
    
    # Smart money interpretation
    smart_score = inst_activity.get('smart_money_score', 50)
    if smart_score > 65:
        interpretations.append(f"🧠 **Smart Money Bullish** (Score: {smart_score}): Multiple institutional indicators suggest accumulation. Whales appear to be building positions.")
    elif smart_score < 35:
        interpretations.append(f"🧠 **Smart Money Bearish** (Score: {smart_score}): Institutional indicators suggest distribution or avoidance. Exercise caution.")
    
    # Squeeze potential
    squeeze = inst_activity.get('squeeze_potential', 0)
    if squeeze > 70:
        interpretations.append(f"🚀 **High Squeeze Potential** ({squeeze}%): High short interest + high days-to-cover creates explosive squeeze conditions if momentum turns positive.")
    elif squeeze > 50:
        interpretations.append(f"⚡ **Moderate Squeeze Risk** ({squeeze}%): Elevated short positioning could accelerate price moves in either direction.")
    
    if inst_activity['short_interest'] > 15:
        interpretations.append(f"⚠️ **High Short Interest** ({inst_activity['short_interest']:.1f}%): Significant bearish bets against this stock. Watch for short squeeze potential if positive catalysts emerge.")
    elif inst_activity['short_interest'] > 8:
        interpretations.append(f"📊 **Elevated Short Interest** ({inst_activity['short_interest']:.1f}%): Moderate short positioning indicates some bearish sentiment among institutional traders.")
    
    if inst_activity['relative_volume'] > 2:
        interpretations.append(f"🔥 **Unusual Volume** ({inst_activity['relative_volume']:.1f}x avg): Heavy institutional activity detected. Large players are actively trading this name.")
    
    # A/D phase interpretation
    ad_phase = inst_activity.get('accumulation_distribution', 'neutral')
    if ad_phase == 'accumulation' and inst_activity['relative_volume'] > 1.2:
        interpretations.append("📈 **Accumulation Phase**: Money flow analysis shows net buying pressure with institutional participation.")
    elif ad_phase == 'distribution' and inst_activity['relative_volume'] > 1.2:
        interpretations.append("📉 **Distribution Phase**: Money flow analysis indicates selling pressure - institutions may be reducing positions.")
    
    if inst_activity['insider_buy_count'] > inst_activity['insider_sell_count'] and inst_activity['insider_buy_count'] > 0:
        interpretations.append(f"✅ **Net Insider Buying**: Insiders have made {inst_activity['insider_buy_count']} purchase(s) vs {inst_activity['insider_sell_count']} sale(s). Management showing confidence.")
    elif inst_activity['insider_sell_count'] > inst_activity['insider_buy_count'] * 2:
        interpretations.append(f"🚨 **Heavy Insider Selling**: {inst_activity['insider_sell_count']} insider sales detected. May indicate reduced confidence or planned diversification.")
    
    if inst_activity['put_call_ratio'] < 0.5:
        interpretations.append(f"📈 **Bullish Options Flow**: P/C ratio of {inst_activity['put_call_ratio']:.2f} indicates options traders are positioned for upside.")
    elif inst_activity['put_call_ratio'] > 1.5:
        interpretations.append(f"📉 **Bearish Options Flow**: P/C ratio of {inst_activity['put_call_ratio']:.2f} shows heavy put buying—either hedging or bearish speculation.")
    
    if inst_activity['institutional_ownership'] > 80:
        interpretations.append(f"🏛️ **Heavily Institutionalized** ({inst_activity['institutional_ownership']:.1f}%): Stock movements likely driven by institutional rebalancing and fund flows.")
    
    if not interpretations:
        interpretations.append("📊 **Neutral Flow**: No significant whale signals detected. Institutional activity appears normal for this security.")
    
    st.markdown(f"""
    <div style="background: rgba(33,38,45,0.5); border-radius: 8px; padding: 1rem; margin-top: 0.5rem;">
        {'<br>'.join(interpretations)}
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander("📖 About", expanded=False):
        st.markdown(info.get('longBusinessSummary', info.get('description', 'No description available.')))

def render_clickable_stock(sym, price, ch_pct, col, prefix=""):
    with col:
        if st.button(f"{sym}\n${price:.2f}\n{ch_pct:+.2f}%", key=f"stk_{prefix}_{sym}_{abs(hash(str(price)+str(ch_pct)))}", use_container_width=True):
            st.session_state.selected_stock = sym; st.session_state.show_stock_report = True; st.rerun()

@st.cache_data(ttl=300)
def get_market_summary():
    data = {'futures': {}, 'global': {}, 'sectors': {}, 'news': []}
    for n, s in FUTURES_SYMBOLS.items():
        h, i = fetch_stock_data(s, "5d", "15m")
        m = calculate_metrics(h, i)
        if m: data['futures'][n] = m
    for n, i in SECTOR_ETFS.items():
        h, inf = fetch_stock_data(i['symbol'], "5d", "15m")
        m = calculate_metrics(h, inf)
        if m: data['sectors'][n] = {'symbol': i['symbol'], 'metrics': m}
    for n, s in GLOBAL_INDICES.items():
        h, i = fetch_stock_data(s, "5d", "15m")
        m = calculate_metrics(h, i)
        if m: data['global'][n] = m
    data['news'] = fetch_all_news()
    return data

def calc_opt_score(sym, direction='calls'):
    """
    Advanced options scoring system that considers:
    - Technical indicators (RSI, MACD, moving averages, Bollinger Bands)
    - Volume analysis (relative volume, unusual activity)
    - Momentum (multiple timeframes)
    - Time of day factors
    - Market context (VIX, sector performance)
    - Options flow signals
    - Support/Resistance proximity
    - Earnings proximity
    """
    data = fetch_comprehensive_data(sym)
    if not data: return None
    
    h = data.get('hist_5d')
    h1m = data.get('hist_1mo')
    h3m = data.get('hist_3mo')
    info = data.get('info', {})
    
    if h is None or h.empty: return None
    
    # Current price and basic metrics
    price = h['Close'].iloc[-1]
    prev = safe_get(info, 'regularMarketPreviousClose', price)
    overnight = safe_pct_change(price, prev)
    
    # === TIME OF DAY CONTEXT ===
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    market_hour = now.hour
    market_minute = now.minute
    
    # Time-based adjustments
    if market_hour < 9 or (market_hour == 9 and market_minute < 30):
        # Pre-market: Focus on overnight moves and gap potential
        time_context = 'premarket'
        time_weight = 1.2  # Boost overnight signals
    elif market_hour < 10:
        # First 30 min: High volatility, momentum matters most
        time_context = 'open'
        time_weight = 1.1
    elif market_hour < 12:
        # Morning session: Good trend follow opportunities
        time_context = 'morning'
        time_weight = 1.0
    elif market_hour < 14:
        # Midday: Lower volume, mean reversion plays
        time_context = 'midday'
        time_weight = 0.9  # Reduce conviction
    elif market_hour < 15:
        # Afternoon: Institutional positioning
        time_context = 'afternoon'
        time_weight = 1.0
    else:
        # Power hour: Momentum acceleration
        time_context = 'close'
        time_weight = 1.15
    
    # === TECHNICAL INDICATORS ===
    score = 0
    signals = []
    
    # 1. RSI Analysis (0-15 points)
    rsi = 50
    if h1m is not None and len(h1m) > 14:
        rsi, rsi_signal = calculate_rsi(h1m['Close'])
        
        if direction == 'calls':
            if rsi < 30:  # Oversold - great for calls
                score += 15
                signals.append(('🟢', f'Oversold RSI ({rsi:.0f})'))
            elif rsi < 40:
                score += 12
                signals.append(('🟢', f'Low RSI ({rsi:.0f})'))
            elif rsi < 50:
                score += 8
            elif rsi < 60:
                score += 5
            elif rsi < 70:
                score += 3
            else:  # Overbought - risky for calls
                score += 0
                signals.append(('🟡', f'Overbought RSI ({rsi:.0f})'))
        else:  # puts
            if rsi > 70:  # Overbought - great for puts
                score += 15
                signals.append(('🔴', f'Overbought RSI ({rsi:.0f})'))
            elif rsi > 60:
                score += 12
                signals.append(('🔴', f'High RSI ({rsi:.0f})'))
            elif rsi > 50:
                score += 8
            elif rsi > 40:
                score += 5
            elif rsi > 30:
                score += 3
            else:  # Oversold - risky for puts
                score += 0
                signals.append(('🟡', f'Oversold RSI ({rsi:.0f})'))
    
    # 2. Momentum Analysis - Multiple Timeframes (0-20 points)
    mom_1d = overnight
    mom_5d = ((price - h['Close'].iloc[0]) / h['Close'].iloc[0] * 100) if len(h) > 1 else 0
    mom_1m = 0
    if h1m is not None and len(h1m) > 5:
        mom_1m = ((price - h1m['Close'].iloc[0]) / h1m['Close'].iloc[0] * 100)
    
    if direction == 'calls':
        # For calls, we want positive momentum but not overextended
        if mom_1d > 0 and mom_5d > 0 and mom_1m > 0:
            # All timeframes aligned bullish
            score += min(15, (mom_1d + mom_5d/2 + mom_1m/4) * 2)
            signals.append(('🟢', 'Multi-timeframe bullish momentum'))
        elif mom_1d > 1:  # Strong overnight gap up
            score += min(10, mom_1d * 3)
            if time_context == 'premarket':
                signals.append(('🟢', f'Gap up +{mom_1d:.1f}%'))
        elif mom_5d < -5 and mom_1d > 0:  # Bounce play
            score += 12
            signals.append(('🟢', 'Potential bounce setup'))
        
        # Penalize overextension
        if mom_5d > 10:
            score -= 5
            signals.append(('🟡', 'Extended - caution'))
    else:  # puts
        if mom_1d < 0 and mom_5d < 0 and mom_1m < 0:
            score += min(15, (abs(mom_1d) + abs(mom_5d)/2 + abs(mom_1m)/4) * 2)
            signals.append(('🔴', 'Multi-timeframe bearish momentum'))
        elif mom_1d < -1:  # Strong overnight gap down
            score += min(10, abs(mom_1d) * 3)
            if time_context == 'premarket':
                signals.append(('🔴', f'Gap down {mom_1d:.1f}%'))
        elif mom_5d > 5 and mom_1d < 0:  # Reversal play
            score += 12
            signals.append(('🔴', 'Potential reversal setup'))
        
        if mom_5d < -10:
            score -= 5
            signals.append(('🟡', 'Oversold - caution'))
    
    # 3. Volume Analysis (0-15 points)
    current_vol = h['Volume'].iloc[-1] if len(h) > 0 else 0
    avg_vol = info.get('averageVolume', h['Volume'].mean() if len(h) > 5 else current_vol)
    avg_vol = avg_vol if avg_vol > 0 else 1
    
    rel_volume = current_vol / avg_vol
    
    if rel_volume > 3:
        score += 15
        signals.append(('🔥', f'Extreme volume ({rel_volume:.1f}x)'))
    elif rel_volume > 2:
        score += 12
        signals.append(('🟢', f'High volume ({rel_volume:.1f}x)'))
    elif rel_volume > 1.5:
        score += 10
    elif rel_volume > 1:
        score += 7
    elif rel_volume > 0.7:
        score += 5
    else:
        score += 2
        signals.append(('🟡', 'Low volume'))
    
    # 4. Volatility/Range Analysis (0-10 points)
    if len(h) > 0 and price > 0:
        day_range = safe_div((h['High'].iloc[-1] - h['Low'].iloc[-1]), price) * 100
        avg_range = ((h['High'] - h['Low']) / h['Close'].replace(0, np.nan)).mean() * 100 if len(h) > 3 else day_range
        avg_range = avg_range if not pd.isna(avg_range) else day_range
        
        range_expansion = safe_div(day_range, avg_range, 1.0)
        
        if range_expansion > 1.5:
            score += 10
            signals.append(('🔥', 'Range expansion'))
        elif range_expansion > 1.2:
            score += 8
        elif range_expansion > 0.8:
            score += 5
        else:
            score += 3
    
    # 5. Moving Average Analysis (0-10 points)
    if h1m is not None and len(h1m) > 20:
        ma_20 = h1m['Close'].rolling(20).mean().iloc[-1]
        ma_50 = h1m['Close'].rolling(50).mean().iloc[-1] if len(h1m) > 50 else ma_20
        
        price_vs_ma20 = ((price - ma_20) / ma_20 * 100) if ma_20 else 0
        
        if direction == 'calls':
            if price > ma_20 and price > ma_50:
                score += 8
                signals.append(('🟢', 'Above key MAs'))
            elif price > ma_20:
                score += 5
            elif price < ma_20 and price_vs_ma20 > -3:
                # Near MA - potential support
                score += 6
                signals.append(('🟢', 'Testing MA support'))
            else:
                score += 2
        else:  # puts
            if price < ma_20 and price < ma_50:
                score += 8
                signals.append(('🔴', 'Below key MAs'))
            elif price < ma_20:
                score += 5
            elif price > ma_20 and price_vs_ma20 < 3:
                # Near MA - potential resistance
                score += 6
                signals.append(('🔴', 'Testing MA resistance'))
            else:
                score += 2
    
    # 6. Options Flow / Institutional Activity (0-10 points)
    options_data = data.get('options_data')
    if options_data:
        calls_df = options_data.get('calls')
        puts_df = options_data.get('puts')
        
        total_call_vol = calls_df['volume'].sum() if calls_df is not None and 'volume' in calls_df.columns else 0
        total_put_vol = puts_df['volume'].sum() if puts_df is not None and 'volume' in puts_df.columns else 0
        
        # Clean NaN values
        total_call_vol = 0 if pd.isna(total_call_vol) else total_call_vol
        total_put_vol = 0 if pd.isna(total_put_vol) else total_put_vol
        
        if total_call_vol + total_put_vol > 0:
            pc_ratio = total_put_vol / total_call_vol if total_call_vol > 0 else 1
            
            if direction == 'calls':
                if pc_ratio < 0.5:  # Heavy call buying
                    score += 10
                    signals.append(('🟢', f'Bullish options flow (P/C: {pc_ratio:.2f})'))
                elif pc_ratio < 0.8:
                    score += 7
                elif pc_ratio > 1.5:  # Contrarian - lots of puts could mean oversold
                    score += 5
                else:
                    score += 3
            else:  # puts
                if pc_ratio > 1.5:  # Heavy put buying
                    score += 10
                    signals.append(('🔴', f'Bearish options flow (P/C: {pc_ratio:.2f})'))
                elif pc_ratio > 1.2:
                    score += 7
                elif pc_ratio < 0.5:  # Contrarian - lots of calls could mean overbought
                    score += 5
                else:
                    score += 3
    
    # 7. Sector Context (0-5 points)
    sector = info.get('sector', '')
    # Would ideally check sector ETF performance here
    score += 3  # Baseline
    
    # 8. Earnings Proximity (adjust score)
    try:
        cal = info.get('calendar', {})
        if cal and 'Earnings Date' in str(cal):
            # Near earnings - higher IV, adjust expectations
            signals.append(('📅', 'Earnings approaching'))
            if time_context in ['premarket', 'open']:
                score += 3  # Earnings plays can work pre-market
    except:
        pass
    
    # 9. VIX Context (0-5 points)
    # Higher VIX = higher premiums, favor puts or high-conviction calls only
    try:
        vix_data = yf.Ticker('^VIX').history(period='1d')
        if not vix_data.empty:
            vix_level = vix_data['Close'].iloc[-1]
            
            if direction == 'calls':
                if vix_level < 15:
                    score += 5  # Low fear, good for calls
                elif vix_level < 20:
                    score += 3
                elif vix_level > 25:
                    score += 0  # High fear, calls risky
                    signals.append(('🟡', 'Elevated VIX'))
            else:  # puts
                if vix_level > 25:
                    score += 5  # High fear, puts have momentum
                    signals.append(('🔴', 'High VIX environment'))
                elif vix_level > 20:
                    score += 3
                else:
                    score += 1
    except:
        pass
    
    # === APPLY TIME WEIGHT ===
    score = score * time_weight
    
    # === FINAL SCORING ===
    # Max theoretical score ~100
    # Grade thresholds
    if score >= 75:
        grade = "A+"
        gr_cls = "score-excellent"
    elif score >= 65:
        grade = "A"
        gr_cls = "score-excellent"
    elif score >= 55:
        grade = "B+"
        gr_cls = "score-good"
    elif score >= 45:
        grade = "B"
        gr_cls = "score-good"
    elif score >= 35:
        grade = "C+"
        gr_cls = "score-fair"
    elif score >= 25:
        grade = "C"
        gr_cls = "score-fair"
    else:
        grade = "D"
        gr_cls = "score-weak"
    
    return {
        'symbol': sym,
        'total_score': round(score, 1),
        'grade': grade,
        'grade_class': gr_cls,
        'current_price': price,
        'overnight_change_pct': overnight,
        'momentum_5d': mom_5d,
        'momentum_1m': mom_1m,
        'rsi': rsi,
        'relative_volume': rel_volume,
        'time_context': time_context,
        'signals': signals[:4],  # Top 4 signals
    }

@st.cache_data(ttl=180)  # Shorter cache for more responsive updates
def get_top_options():
    """
    Get top options candidates with comprehensive scoring.
    Returns top calls and puts sorted by score.
    """
    calls, puts = [], []
    
    # Get market context for filtering
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    market_hour = now.hour
    
    # Adjust universe based on time of day
    if market_hour < 10:
        # Pre-market/early: Focus on high-volume names with overnight moves
        universe = OPTIONS_UNIVERSE[:35]
    else:
        # Regular hours: Full universe
        universe = OPTIONS_UNIVERSE[:40]
    
    for s in universe:
        try:
            c = calc_opt_score(s, 'calls')
            if c and c['total_score'] > 20:  # Minimum threshold
                calls.append(c)
            
            p = calc_opt_score(s, 'puts')
            if p and p['total_score'] > 20:
                puts.append(p)
        except:
            continue
    
    # Sort by score
    calls = sorted(calls, key=lambda x: x['total_score'], reverse=True)
    puts = sorted(puts, key=lambda x: x['total_score'], reverse=True)
    
    return calls[:5], puts[:5]  # Return top 5 each

def main():
    if st.session_state.show_stock_report and st.session_state.selected_stock: render_stock_report(st.session_state.selected_stock); return
    col_t, col_s = st.columns([3, 1])
    with col_t: st.markdown('<h1 class="main-title">📈 Pre-Market Command Center</h1>', unsafe_allow_html=True); st.markdown('<p class="subtitle">Institutional Analysis · AI Insights · Click Any Stock</p>', unsafe_allow_html=True)
    with col_s:
        sk, st_txt, cd = get_market_status()
        eastern = pytz.timezone('US/Eastern')
        st.markdown(f'<div style="text-align:right;"><span class="market-status status-{sk}">{st_txt}</span><p class="timestamp">{cd}</p><p class="timestamp">{datetime.now(eastern).strftime("%I:%M %p ET")}</p></div>', unsafe_allow_html=True)
    st.markdown("---")
    tabs = st.tabs(["🎯 Market Brief", "🌍 Futures", "📊 Stocks", "🏢 Sectors", "📈 Options", "📅 Earnings", "🔍 Research"])
    
    with tabs[0]:
        st.markdown("## 🎯 Daily Intelligence")
        if st.button("🔄 Refresh", key="ref", type="primary"): st.cache_data.clear(); st.rerun()
        with st.spinner("Loading..."):
            md = get_market_summary()
            news = md.get('news', [])
            ns = analyze_news_sentiment(news)
            econ = fetch_economic_indicators()
            assess = generate_assessment(md, ns, econ)
        c1, c2, c3, c4 = st.columns([1.5, 1, 1, 1])
        with c1:
            sent, score, bias, conf = assess['sentiment'], assess['sentiment_score'], assess['trading_bias'], assess['confidence']
            cls = "sentiment-bullish" if 'Bullish' in sent else "sentiment-bearish" if 'Bearish' in sent else "sentiment-neutral"
            st.markdown(f'<div class="summary-section"><div class="summary-header">📊 Assessment</div><div style="text-align:center;padding:1rem;"><span class="{cls}">{sent}</span><p style="color:#8b949e;margin:0.5rem 0;font-size:0.8rem;">Bias: {bias.replace("_"," ").title()} · Conf: {conf.title()}</p></div><div class="fear-greed-bar"><div class="fear-greed-indicator" style="left:{score}%;"></div></div><div style="display:flex;justify-content:space-between;font-size:0.7rem;color:#8b949e;"><span>Fear</span><span>Greed</span></div></div>', unsafe_allow_html=True)
        with c2:
            es = md['futures'].get('S&P 500', {})
            ch = es.get('overnight_change_pct', 0)
            st.markdown(f'<div class="metric-card"><div class="metric-label">S&P Futures</div><div class="metric-value">${es.get("current_price", 0):,.2f}</div><div class="{"positive" if ch >= 0 else "negative"}">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        with c3:
            nq = md['futures'].get('Nasdaq 100', {})
            ch = nq.get('overnight_change_pct', 0)
            st.markdown(f'<div class="metric-card"><div class="metric-label">Nasdaq Futures</div><div class="metric-value">${nq.get("current_price", 0):,.2f}</div><div class="{"positive" if ch >= 0 else "negative"}">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        with c4:
            vix = md['futures'].get('VIX', {})
            vl, vc = vix.get('current_price', 0), vix.get('overnight_change_pct', 0)
            st.markdown(f'<div class="metric-card"><div class="metric-label">VIX</div><div class="metric-value {"negative" if vl > 20 else "positive" if vl < 15 else "neutral"}">{vl:.2f}</div><div class="{"positive" if vc <= 0 else "negative"}">{vc:+.2f}%</div></div>', unsafe_allow_html=True)
        st.markdown("### 📉 Economic Indicators")
        ec_cols = st.columns(4)
        for i, (n, d) in enumerate(list(econ.items())[:4]):
            with ec_cols[i]:
                v, p = d.get('value', 0), d.get('prev', d.get('value', 0))
                ch = ((v - p) / p * 100) if p else 0
                st.markdown(f'<div class="econ-indicator"><div class="econ-value">{v:.2f}{d.get("unit","")}</div><div class="econ-label">{n}</div><div class="econ-change" style="color:{"#3fb950" if ch >= 0 else "#f85149"};">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        st.markdown("---")
        col_a, col_c = st.columns([2, 1])
        with col_a:
            st.markdown("### 📋 Analysis")
            es_ch, nq_ch = md['futures'].get('S&P 500', {}).get('overnight_change_pct', 0), md['futures'].get('Nasdaq 100', {}).get('overnight_change_pct', 0)
            vix_v = md['futures'].get('VIX', {}).get('current_price', 20)
            st.markdown(f"**Overview:** S&P {'up' if es_ch > 0 else 'down'} **{abs(es_ch):.2f}%**, Nasdaq {'up' if nq_ch > 0 else 'down'} **{abs(nq_ch):.2f}%**. VIX at **{vix_v:.1f}** {'(elevated)' if vix_v > 25 else '(moderate)' if vix_v > 18 else '(calm)'}. News **{ns['overall']}** ({ns['bullish']} bull/{ns['bearish']} bear).")
            if assess['key_themes']: st.markdown("**Themes:** " + ", ".join(assess['key_themes']))
            oc, rc = st.columns(2)
            with oc:
                st.markdown("**✅ Opportunities:**")
                for o in assess.get('opportunities', [])[:3]: st.markdown(f'<div class="opportunity-item">{o}</div>', unsafe_allow_html=True)
            with rc:
                st.markdown("**⚠️ Risks:**")
                for i, r in enumerate(assess.get('risks', [])[:3]): st.markdown(f'<div class="risk-item risk-{"high" if i == 0 else "medium"}">{r}</div>', unsafe_allow_html=True)
            
            # Expert Macro Analyst Summary
            st.markdown("---")
            market_summary, news_analysis = generate_expert_macro_summary(md, ns, econ, assess)
            
            # Determine market sentiment for styling
            sent_color = "#3fb950" if 'Bullish' in assess['sentiment'] else "#f85149" if 'Bearish' in assess['sentiment'] else "#d29922"
            
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(163,113,247,0.1) 0%, rgba(88,166,255,0.05) 100%); border: 1px solid rgba(163,113,247,0.3); border-radius: 12px; padding: 1.5rem; margin: 1rem 0;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem; padding-bottom: 0.75rem; border-bottom: 1px solid rgba(163,113,247,0.2);">
                    <div>
                        <span style="font-size: 1.1rem; font-weight: 700; color: #ffffff;">🎩 Chief Strategist's Market Brief</span>
                        <span style="margin-left: 0.75rem; background: rgba(163,113,247,0.2); color: #a371f7; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.65rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;">Institutional Grade</span>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 0.7rem; color: #6e7681;">{datetime.now(eastern).strftime('%I:%M %p ET')}</div>
                        <div style="font-size: 0.65rem; color: #484f58;">{datetime.now(eastern).strftime('%B %d, %Y')}</div>
                    </div>
                </div>
                <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin: 0 0 1rem 0; font-family: 'Georgia', serif;">{market_summary}</p>
                <div style="background: rgba(88,166,255,0.05); border-left: 3px solid #58a6ff; padding: 1rem; margin: 1rem 0; border-radius: 0 8px 8px 0;">
                    <div style="font-size: 0.75rem; font-weight: 600; color: #58a6ff; margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 0.05em;">📰 News Flow Analysis</div>
                    <p style="color: #c9d1d9; font-size: 0.85rem; line-height: 1.7; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{news_analysis}</p>
                </div>
                <div style="display: flex; align-items: center; margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid rgba(163,113,247,0.2);">
                    <span style="font-size: 0.7rem; color: #6e7681;">Market Bias:</span>
                    <span style="margin-left: 0.5rem; color: {sent_color}; font-weight: 600; font-size: 0.75rem;">{assess['sentiment']}</span>
                    <span style="margin-left: 1rem; font-size: 0.7rem; color: #6e7681;">Confidence:</span>
                    <span style="margin-left: 0.5rem; color: #8b949e; font-size: 0.75rem;">{assess['confidence'].title()}</span>
                    <span style="margin-left: auto; font-size: 0.65rem; color: #484f58; font-style: italic;">Analysis generated from real-time market data</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        with col_c:
            eastern = pytz.timezone('US/Eastern')
            st.markdown(f"### 📅 Today's Calendar")
            st.markdown(f"<p style='color: #8b949e; font-size: 0.75rem; margin-bottom: 0.5rem;'>{datetime.now(eastern).strftime('%A, %B %d, %Y')}</p>", unsafe_allow_html=True)
            calendar_events = get_economic_calendar()
            for e in calendar_events[:8]:
                cls = f"event-impact-{e['impact']}" if e['impact'] in ['high', 'medium', 'low'] else ""
                em = "🔴" if e['impact'] == 'high' else "🟡" if e['impact'] == 'medium' else "🟢" if e['impact'] == 'low' else "⚪"
                st.markdown(f'<div class="event-card {cls}"><div class="event-time">{em} {e["time"]}</div><div class="event-title">{e["event"]}</div></div>', unsafe_allow_html=True)
            if not calendar_events:
                st.info("Light calendar day - no major scheduled events")
        st.markdown("---")
        st.markdown("### 🌍 Global Markets")
        g_cols = st.columns(6)
        for i, (n, m) in enumerate(list(md.get('global', {}).items())[:6]):
            ch = m.get('overnight_change_pct', 0)
            with g_cols[i]: st.markdown(f'<div class="metric-card" style="padding:0.75rem;"><div class="metric-label" style="font-size:0.55rem;">{n}</div><div class="metric-value" style="font-size:1rem;">{m.get("current_price", 0):,.0f}</div><div class="{"positive" if ch >= 0 else "negative"}" style="font-size:0.8rem;">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        st.markdown("### 📊 Sectors")
        sectors = sorted(md.get('sectors', {}).items(), key=lambda x: x[1].get('metrics', {}).get('overnight_change_pct', 0), reverse=True)
        s_cols = st.columns(6)
        for i, (n, info) in enumerate(sectors[:6]):
            m = info.get('metrics', {})
            render_clickable_stock(info['symbol'], m.get('current_price', 0), m.get('overnight_change_pct', 0), s_cols[i], "sum_sec")
        st.markdown("### 📰 News")
        if ns['items']:
            n_cols = st.columns(2)
            for i, item in enumerate(ns['items'][:8]):
                c = "#3fb950" if item['sentiment'] == 'bullish' else "#f85149" if item['sentiment'] == 'bearish' else "#58a6ff"
                with n_cols[i % 2]: st.markdown(f'<div class="news-item" style="border-left-color:{c};"><div class="news-title">{item["title"][:90]}...</div><div class="news-meta">{item["source"]} · {" · ".join(item["categories"][:2])}</div></div>', unsafe_allow_html=True)
    
    with tabs[1]:
        st.markdown("### 🌍 Futures & Commodities")
        st.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Click any instrument for detailed technical analysis</p>", unsafe_allow_html=True)
        
        # Quick access buttons for common futures
        st.markdown("#### ⚡ Quick Access")
        quick_cols = st.columns(6)
        quick_futures = ["S&P 500", "Nasdaq 100", "VIX", "Crude Oil", "Gold", "10Y Treasury"]
        for i, name in enumerate(quick_futures):
            with quick_cols[i]:
                symbol = FUTURES_SYMBOLS[name]
                h, info = fetch_stock_data(symbol, "1d", "5m")
                m = calculate_metrics(h, info)
                if m:
                    ch = m['overnight_change_pct']
                    ch_class = "positive" if ch >= 0 else "negative"
                    if st.button(f"{name}\n${m['current_price']:,.2f}\n{ch:+.2f}%", key=f"quick_fut_{symbol}", use_container_width=True):
                        st.session_state.selected_stock = symbol
                        st.session_state.show_stock_report = True
                        st.rerun()
        
        st.markdown("---")
        sel = st.multiselect("Select instruments to display:", list(FUTURES_SYMBOLS.keys()), default=["S&P 500", "Nasdaq 100", "Crude Oil", "Gold", "VIX", "10Y Treasury"])
        
        if sel:
            st.markdown("#### 📊 Selected Instruments")
            cols = st.columns(min(4, len(sel)))
            for i, n in enumerate(sel):
                symbol = FUTURES_SYMBOLS[n]
                h, info = fetch_stock_data(symbol, "5d", "15m")
                m = calculate_metrics(h, info)
                if m:
                    with cols[i % 4]:
                        ch = m['overnight_change_pct']
                        ch_class = "positive" if ch >= 0 else "negative"
                        st.markdown(f'<div class="metric-card" style="text-align:center;"><div class="metric-label">{n}</div><div class="metric-value">${m["current_price"]:,.2f}</div><div class="{ch_class}">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
                        if st.button(f"📊 Analyze", key=f"fut_{symbol}_{i}", use_container_width=True):
                            st.session_state.selected_stock = symbol
                            st.session_state.show_stock_report = True
                            st.rerun()
            
            st.markdown("---")
            st.markdown("### 📈 Charts")
            ch_cols = st.columns(2)
            for i, n in enumerate(sel[:4]):
                symbol = FUTURES_SYMBOLS[n]
                h, _ = fetch_stock_data(symbol, "5d", "15m")
                if h is not None and not h.empty:
                    with ch_cols[i % 2]: 
                        # Get S/R levels for chart
                        price = h['Close'].iloc[-1] if not h.empty else 0
                        support, resistance = calculate_support_resistance(h, price)
                        chart_fig = create_chart(h, n, "5D", False, support, resistance)
                        if chart_fig is not None:
                            st.plotly_chart(chart_fig, use_container_width=True)
                        else:
                            st.info(f"Chart unavailable for {n}")
    
    with tabs[2]:
        st.markdown("### 📊 Stocks")
        sym = st.text_input("🔍 Search:", "", key="stk_search").upper()
        if sym:
            h, info = fetch_stock_data(sym, "5d", "15m")
            m = calculate_metrics(h, info)
            if m:
                if st.button(f"📊 View {sym} Report", key=f"view_{sym}"): st.session_state.selected_stock = sym; st.session_state.show_stock_report = True; st.rerun()
                st.write(f"**{sym}** · ${m['current_price']:.2f} · {m['overnight_change_pct']:+.2f}%")
            else: st.warning(f"Not found: {sym}")
        st.markdown("### 🔥 Popular")
        watchlist = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD", "SPY", "QQQ", "JPM", "V"]
        w_cols = st.columns(6)
        for i, s in enumerate(watchlist):
            h, info = fetch_stock_data(s, "5d", "15m")
            m = calculate_metrics(h, info)
            if m: render_clickable_stock(s, m['current_price'], m['overnight_change_pct'], w_cols[i % 6], "watch")
    
    with tabs[3]:
        st.markdown("### 🏢 Sectors")
        
        # AI-Generated Sector Summary
        with st.spinner("Analyzing sectors..."):
            sector_performance = []
            for sec_name, sec_data in SECTOR_ETFS.items():
                h, inf = fetch_stock_data(sec_data['symbol'], "5d", "15m")
                m = calculate_metrics(h, inf)
                if m:
                    sector_performance.append({
                        'name': sec_name,
                        'symbol': sec_data['symbol'],
                        'change': m['overnight_change_pct'],
                        'price': m['current_price']
                    })
            
            if sector_performance:
                sector_performance.sort(key=lambda x: x['change'], reverse=True)
                sectors_up = [s for s in sector_performance if s['change'] > 0]
                sectors_down = [s for s in sector_performance if s['change'] < 0]
                sectors_flat = [s for s in sector_performance if s['change'] == 0]
                
                # Calculate market breadth
                breadth_pct = (len(sectors_up) / len(sector_performance)) * 100 if sector_performance else 0
                avg_change = sum(s['change'] for s in sector_performance) / len(sector_performance)
                
                # Generate summary text
                if len(sectors_up) > len(sectors_down) * 2:
                    market_tone = "strongly bullish"
                    tone_color = "#3fb950"
                elif len(sectors_up) > len(sectors_down):
                    market_tone = "bullish"
                    tone_color = "#3fb950"
                elif len(sectors_down) > len(sectors_up) * 2:
                    market_tone = "strongly bearish"
                    tone_color = "#f85149"
                elif len(sectors_down) > len(sectors_up):
                    market_tone = "bearish"
                    tone_color = "#f85149"
                else:
                    market_tone = "mixed"
                    tone_color = "#d29922"
                
                # Top performers
                top_3 = sector_performance[:3]
                bottom_3 = sector_performance[-3:]
                
                # Build the summary
                up_list = ", ".join([f"**{s['name']}** ({s['change']:+.2f}%)" for s in sectors_up[:4]]) if sectors_up else "None"
                down_list = ", ".join([f"**{s['name']}** ({s['change']:+.2f}%)" for s in sectors_down[:4]]) if sectors_down else "None"
                
                # Rotation analysis
                rotation_signal = ""
                top_sectors = [s['name'] for s in top_3]
                if any(s in ['Technology', 'Communication', 'Consumer Disc.'] for s in top_sectors):
                    rotation_signal = "Growth/risk-on rotation evident with cyclical sectors leading."
                elif any(s in ['Utilities', 'Consumer Staples', 'Healthcare'] for s in top_sectors):
                    rotation_signal = "Defensive rotation in play - investors seeking safety in non-cyclicals."
                elif any(s in ['Energy', 'Materials', 'Industrials'] for s in top_sectors):
                    rotation_signal = "Value/cyclical rotation - economically sensitive sectors outperforming."
                elif any(s in ['Financial'] for s in top_sectors):
                    rotation_signal = "Financials leading - potentially rate-sensitive or economic optimism."
                
                st.markdown(f"""
                <div class="expert-analysis">
                    <div class="expert-header">🤖 AI Sector Analysis</div>
                    <p class="expert-text">
                        <b>Market Tone:</b> <span style="color: {tone_color}; font-weight: 600;">{market_tone.upper()}</span> · 
                        <b>Breadth:</b> {len(sectors_up)}/{len(sector_performance)} sectors positive ({breadth_pct:.0f}%) · 
                        <b>Avg Change:</b> <span style="color: {'#3fb950' if avg_change >= 0 else '#f85149'};">{avg_change:+.2f}%</span>
                    </p>
                    <p class="expert-text" style="margin-top: 0.75rem;">
                        <span style="color: #3fb950;">📈 <b>Sectors Up:</b></span> {up_list}
                    </p>
                    <p class="expert-text">
                        <span style="color: #f85149;">📉 <b>Sectors Down:</b></span> {down_list}
                    </p>
                    <p class="expert-text" style="margin-top: 0.75rem;">
                        <b>🔄 Rotation:</b> {rotation_signal if rotation_signal else "No clear rotation pattern - sector performance relatively balanced."}
                    </p>
                    <p class="expert-text" style="margin-top: 0.75rem;">
                        <b>💡 Insight:</b> Top performer <b>{top_3[0]['name']}</b> ({top_3[0]['change']:+.2f}%) vs laggard <b>{bottom_3[-1]['name']}</b> ({bottom_3[-1]['change']:+.2f}%). 
                        Spread of {abs(top_3[0]['change'] - bottom_3[-1]['change']):.2f}% suggests {'high dispersion - stock picking matters' if abs(top_3[0]['change'] - bottom_3[-1]['change']) > 1.5 else 'moderate dispersion - trend following favored' if abs(top_3[0]['change'] - bottom_3[-1]['change']) > 0.75 else 'low dispersion - broad market moves dominating'}.
                    </p>
                </div>
                """, unsafe_allow_html=True)
                
                # Quick sector grid - clickable
                st.markdown("#### 📊 Sector Performance Grid")
                st.markdown("<p style='color: #8b949e; font-size: 0.8rem;'>Click any sector ETF for detailed analysis</p>", unsafe_allow_html=True)
                grid_cols = st.columns(4)
                for i, s in enumerate(sector_performance):
                    with grid_cols[i % 4]:
                        ch_class = "positive" if s['change'] >= 0 else "negative"
                        st.markdown(f"""
                        <div class="metric-card" style="padding: 0.75rem; margin: 0.25rem 0;">
                            <div class="metric-label" style="font-size: 0.6rem;">{s['name']}</div>
                            <div style="font-size: 0.85rem; color: #fff;">{s['symbol']}</div>
                            <div class="{ch_class}" style="font-size: 1rem; font-weight: 600;">{s['change']:+.2f}%</div>
                        </div>
                        """, unsafe_allow_html=True)
                        if st.button(f"📊 {s['symbol']}", key=f"sec_grid_{s['symbol']}_{i}", use_container_width=True):
                            st.session_state.selected_stock = s['symbol']
                            st.session_state.show_stock_report = True
                            st.rerun()
        
        st.markdown("---")
        st.markdown("### 🔍 Sector Deep Dive")
        sector = st.selectbox("Select sector:", list(SECTOR_ETFS.keys()))
        sec_info = SECTOR_ETFS[sector]
        fin_filter = st.selectbox("Financial sub-category:", ["All"] + list(FINANCE_CATEGORIES.keys())) if sector == "Financial" else "All"
        
        h, inf = fetch_stock_data(sec_info['symbol'], "5d", "15m")
        m = calculate_metrics(h, inf)
        
        if m:
            # ETF overview row
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                ch = m['overnight_change_pct']
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{sec_info["symbol"]} · {sector} ETF</div>
                    <div class="metric-value">${m["current_price"]:.2f}</div>
                    <div class="{'positive' if ch >= 0 else 'negative'}">{ch:+.2f}%</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Day Range</div>
                    <div style="font-size: 1rem; color: #fff;">${m.get('low', 0):.2f} - ${m.get('high', 0):.2f}</div>
                    <div style="color: #8b949e; font-size: 0.8rem;">RSI: {m.get('rsi', 50):.0f}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                if st.button(f"📊 Full Analysis: {sec_info['symbol']}", key=f"v_etf_{sec_info['symbol']}", use_container_width=True):
                    st.session_state.selected_stock = sec_info['symbol']
                    st.session_state.show_stock_report = True
                    st.rerun()
            
            # Mini chart for the ETF
            if h is not None and not h.empty:
                with st.expander(f"📈 {sec_info['symbol']} Chart (5D)", expanded=False):
                    price = h['Close'].iloc[-1]
                    support, resistance = calculate_support_resistance(h, price)
                    fig = create_chart(h, sec_info['symbol'], "5D", False, support, resistance)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Chart data unavailable")
        
        st.markdown("### 📋 Top Holdings")
        st.markdown(f"<p style='color: #8b949e; font-size: 0.8rem;'>Click any stock for detailed analysis</p>", unsafe_allow_html=True)
        stocks = FINANCE_CATEGORIES[fin_filter] if sector == "Financial" and fin_filter != "All" else sec_info['stocks']
        s_cols = st.columns(4)
        for i, s in enumerate(stocks):
            h, inf = fetch_stock_data(s, "5d", "15m")
            m = calculate_metrics(h, inf)
            if m: render_clickable_stock(s, m['current_price'], m['overnight_change_pct'], s_cols[i % 4], "sec")
    
    with tabs[4]:
        st.markdown("## 📈 Options Screener")
        
        # Time context indicator
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        market_hour = now.hour
        
        if market_hour < 9 or (market_hour == 9 and now.minute < 30):
            time_badge = "🌅 Pre-Market"
            time_desc = "Focus on overnight gaps and momentum setups"
        elif market_hour < 10:
            time_badge = "🔔 Market Open"
            time_desc = "High volatility - momentum plays favored"
        elif market_hour < 12:
            time_badge = "☀️ Morning Session"
            time_desc = "Trend following opportunities"
        elif market_hour < 14:
            time_badge = "🕐 Midday"
            time_desc = "Lower conviction - wait for setups"
        elif market_hour < 15:
            time_badge = "📊 Afternoon"
            time_desc = "Institutional positioning underway"
        else:
            time_badge = "⚡ Power Hour"
            time_desc = "Momentum acceleration - fast moves"
        
        st.markdown(f"""
        <div style="background: rgba(88,166,255,0.1); border: 1px solid rgba(88,166,255,0.3); border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem; display: flex; justify-content: space-between; align-items: center;">
            <div>
                <span style="font-size: 1rem; font-weight: 600; color: #58a6ff;">{time_badge}</span>
                <span style="margin-left: 1rem; color: #8b949e; font-size: 0.85rem;">{time_desc}</span>
            </div>
            <span style="color: #6e7681; font-size: 0.75rem;">{now.strftime('%I:%M %p ET')}</span>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔄 Run Screener", type="primary", key="opt_run"): 
            st.cache_data.clear()
        
        with st.spinner("Analyzing options opportunities..."):
            calls, puts = get_top_options()
        
        # Scoring legend
        with st.expander("📊 Scoring Methodology", expanded=False):
            st.markdown("""
            **The options screener evaluates candidates on:**
            
            | Factor | Weight | Description |
            |--------|--------|-------------|
            | RSI | 0-15 pts | Oversold for calls, overbought for puts |
            | Momentum | 0-20 pts | Multi-timeframe alignment (1D, 5D, 1M) |
            | Volume | 0-15 pts | Relative volume vs average |
            | Range | 0-10 pts | Volatility expansion detection |
            | Moving Averages | 0-10 pts | Price vs 20/50 MA |
            | Options Flow | 0-10 pts | Put/call ratio analysis |
            | VIX Context | 0-5 pts | Market fear/greed adjustment |
            | Time of Day | ×0.9-1.2 | Session-based weighting |
            
            **Grades:** A+ (75+) | A (65+) | B+ (55+) | B (45+) | C+ (35+) | C (25+) | D (<25)
            """)
        
        c_col, p_col = st.columns(2)
        
        with c_col:
            st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(63,185,80,0.1) 0%, rgba(63,185,80,0.05) 100%); border: 1px solid rgba(63,185,80,0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1rem;">
                <h3 style="color:#3fb950; margin:0; display: flex; align-items: center;">
                    <span style="font-size: 1.5rem; margin-right: 0.5rem;">📈</span> TOP CALLS
                </h3>
                <p style="color: #8b949e; font-size: 0.75rem; margin: 0.25rem 0 0 0;">Bullish setups ranked by score</p>
            </div>
            """, unsafe_allow_html=True)
            
            if calls:
                for i, p in enumerate(calls, 1):
                    # Build signals display
                    signals_html = ""
                    if p.get('signals'):
                        for emoji, text in p['signals'][:3]:
                            signals_html += f'<span style="background: rgba(63,185,80,0.15); padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.65rem; margin-right: 0.25rem;">{emoji} {text}</span>'
                    
                    rel_vol = p.get('relative_volume', 1)
                    vol_indicator = "🔥" if rel_vol > 2 else "📊" if rel_vol > 1 else "📉"
                    
                    st.markdown(f"""
                    <div class="options-pick-card" style="border-left: 4px solid #3fb950;">
                        <div class="pick-header" style="display: flex; justify-content: space-between; align-items: center;">
                            <span class="pick-symbol" style="font-size: 1.1rem;">#{i} {p["symbol"]}</span>
                            <span class="pick-score {p["grade_class"]}" style="font-size: 1rem; font-weight: 700;">{p["grade"]} ({p["total_score"]:.0f})</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                            <span style="color: #fff; font-weight: 500;">${p["current_price"]:.2f}</span>
                            <span style="color: {'#3fb950' if p['overnight_change_pct'] >= 0 else '#f85149'};">{p["overnight_change_pct"]:+.2f}% today</span>
                        </div>
                        <div style="display: flex; gap: 1rem; font-size: 0.75rem; color: #8b949e; margin-bottom: 0.5rem;">
                            <span>RSI: {p["rsi"]:.0f}</span>
                            <span>5D: {p["momentum_5d"]:+.1f}%</span>
                            <span>{vol_indicator} Vol: {rel_vol:.1f}x</span>
                        </div>
                        <div style="margin-top: 0.5rem;">{signals_html}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button(f"📊 Analyze {p['symbol']}", key=f"c_{p['symbol']}_{i}", use_container_width=True):
                        st.session_state.selected_stock = p['symbol']
                        st.session_state.show_stock_report = True
                        st.rerun()
            else:
                st.info("No strong call setups found. Market conditions may favor caution.")
        
        with p_col:
            st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(248,81,73,0.1) 0%, rgba(248,81,73,0.05) 100%); border: 1px solid rgba(248,81,73,0.3); border-radius: 10px; padding: 1rem; margin-bottom: 1rem;">
                <h3 style="color:#f85149; margin:0; display: flex; align-items: center;">
                    <span style="font-size: 1.5rem; margin-right: 0.5rem;">📉</span> TOP PUTS
                </h3>
                <p style="color: #8b949e; font-size: 0.75rem; margin: 0.25rem 0 0 0;">Bearish setups ranked by score</p>
            </div>
            """, unsafe_allow_html=True)
            
            if puts:
                for i, p in enumerate(puts, 1):
                    # Build signals display
                    signals_html = ""
                    if p.get('signals'):
                        for emoji, text in p['signals'][:3]:
                            signals_html += f'<span style="background: rgba(248,81,73,0.15); padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.65rem; margin-right: 0.25rem;">{emoji} {text}</span>'
                    
                    rel_vol = p.get('relative_volume', 1)
                    vol_indicator = "🔥" if rel_vol > 2 else "📊" if rel_vol > 1 else "📉"
                    
                    st.markdown(f"""
                    <div class="options-pick-card" style="border-left: 4px solid #f85149;">
                        <div class="pick-header" style="display: flex; justify-content: space-between; align-items: center;">
                            <span class="pick-symbol" style="font-size: 1.1rem;">#{i} {p["symbol"]}</span>
                            <span class="pick-score {p["grade_class"]}" style="font-size: 1rem; font-weight: 700;">{p["grade"]} ({p["total_score"]:.0f})</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                            <span style="color: #fff; font-weight: 500;">${p["current_price"]:.2f}</span>
                            <span style="color: {'#3fb950' if p['overnight_change_pct'] >= 0 else '#f85149'};">{p["overnight_change_pct"]:+.2f}% today</span>
                        </div>
                        <div style="display: flex; gap: 1rem; font-size: 0.75rem; color: #8b949e; margin-bottom: 0.5rem;">
                            <span>RSI: {p["rsi"]:.0f}</span>
                            <span>5D: {p["momentum_5d"]:+.1f}%</span>
                            <span>{vol_indicator} Vol: {rel_vol:.1f}x</span>
                        </div>
                        <div style="margin-top: 0.5rem;">{signals_html}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button(f"📊 Analyze {p['symbol']}", key=f"p_{p['symbol']}_{i}", use_container_width=True):
                        st.session_state.selected_stock = p['symbol']
                        st.session_state.show_stock_report = True
                        st.rerun()
            else:
                st.info("No strong put setups found. Market conditions may favor bullish plays.")
        
        # Market context summary
        st.markdown("---")
        st.markdown("### 📊 Market Context")
        
        try:
            vix_data = yf.Ticker('^VIX').history(period='1d')
            spy_data = yf.Ticker('SPY').history(period='1d')
            
            ctx_col1, ctx_col2, ctx_col3 = st.columns(3)
            
            with ctx_col1:
                if not vix_data.empty:
                    vix_level = vix_data['Close'].iloc[-1]
                    vix_color = '#f85149' if vix_level > 25 else '#d29922' if vix_level > 18 else '#3fb950'
                    vix_label = 'High Fear' if vix_level > 25 else 'Elevated' if vix_level > 18 else 'Low Fear'
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center;">
                        <div style="font-size: 0.75rem; color: #8b949e;">VIX (Fear Index)</div>
                        <div style="font-size: 1.5rem; font-weight: 700; color: {vix_color};">{vix_level:.1f}</div>
                        <div style="font-size: 0.7rem; color: {vix_color};">{vix_label}</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            with ctx_col2:
                if not spy_data.empty:
                    spy_change = (spy_data['Close'].iloc[-1] - spy_data['Open'].iloc[-1]) / spy_data['Open'].iloc[-1] * 100
                    spy_color = '#3fb950' if spy_change > 0 else '#f85149'
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center;">
                        <div style="font-size: 0.75rem; color: #8b949e;">SPY Today</div>
                        <div style="font-size: 1.5rem; font-weight: 700; color: {spy_color};">{spy_change:+.2f}%</div>
                        <div style="font-size: 0.7rem; color: #8b949e;">{'Bullish' if spy_change > 0.5 else 'Bearish' if spy_change < -0.5 else 'Neutral'}</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            with ctx_col3:
                # Options recommendation based on context
                if not vix_data.empty and not spy_data.empty:
                    if vix_level > 25 and spy_change < -1:
                        rec = "⚠️ High Risk"
                        rec_desc = "Elevated premiums, favor put spreads"
                    elif vix_level < 15 and spy_change > 0:
                        rec = "🟢 Calls Favored"
                        rec_desc = "Low IV, bullish momentum"
                    elif vix_level > 20:
                        rec = "🟡 Neutral/Puts"
                        rec_desc = "Elevated fear, hedge positions"
                    else:
                        rec = "📊 Balanced"
                        rec_desc = "Normal conditions"
                    
                    st.markdown(f"""
                    <div class="metric-card" style="text-align: center;">
                        <div style="font-size: 0.75rem; color: #8b949e;">Session Bias</div>
                        <div style="font-size: 1.1rem; font-weight: 700; color: #fff;">{rec}</div>
                        <div style="font-size: 0.7rem; color: #8b949e;">{rec_desc}</div>
                    </div>
                    """, unsafe_allow_html=True)
        except:
            st.info("Market context data loading...")
    
    with tabs[5]:
        st.markdown("### 📅 Earnings Center")
        st.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Upcoming earnings, earnings analysis, and earnings call summaries</p>", unsafe_allow_html=True)
        
        earnings_tabs = st.tabs(["📆 Upcoming Earnings", "📊 Earnings Analyzer", "📰 Earnings News"])
        
        with earnings_tabs[0]:
            st.markdown("#### 📆 Earnings Calendar")
            with st.spinner("Loading earnings calendar..."):
                upcoming_earnings = get_upcoming_earnings()
            
            if upcoming_earnings:
                # Group by date
                today_earnings = [e for e in upcoming_earnings if e.get('is_today')]
                this_week_earnings = [e for e in upcoming_earnings if not e.get('is_today')]
                
                if today_earnings:
                    st.markdown("##### 🔴 Reporting Today")
                    today_cols = st.columns(min(4, len(today_earnings)))
                    for i, e in enumerate(today_earnings[:8]):
                        with today_cols[i % 4]:
                            timing_badge = "🌅 BMO" if 'before' in e.get('timing', '').lower() else "🌙 AMC" if 'after' in e.get('timing', '').lower() else "📊"
                            st.markdown(f"""
                            <div class="metric-card" style="text-align: center; padding: 1rem; border-color: #f85149;">
                                <div style="font-size: 1.1rem; font-weight: 700; color: #fff;">{e['symbol']}</div>
                                <div style="font-size: 0.75rem; color: #8b949e;">{e['name'][:20]}</div>
                                <div style="font-size: 0.7rem; color: #f85149; margin-top: 0.5rem;">{timing_badge}</div>
                                <div style="font-size: 0.65rem; color: #6e7681;">Est EPS: {e.get('est_eps', 'N/A')}</div>
                            </div>
                            """, unsafe_allow_html=True)
                            if st.button(f"Analyze {e['symbol']}", key=f"today_earn_{e['symbol']}", use_container_width=True):
                                st.session_state.selected_stock = e['symbol']
                                st.session_state.show_stock_report = True
                                st.rerun()
                
                if this_week_earnings:
                    st.markdown("##### 📅 This Week")
                    week_cols = st.columns(4)
                    for i, e in enumerate(this_week_earnings[:12]):
                        with week_cols[i % 4]:
                            st.markdown(f"""
                            <div class="metric-card" style="padding: 0.75rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <span style="font-weight: 600; color: #fff;">{e['symbol']}</span>
                                    <span style="font-size: 0.65rem; color: #58a6ff;">{e.get('date', '')}</span>
                                </div>
                                <div style="font-size: 0.7rem; color: #8b949e;">{e['name'][:25]}</div>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info("No upcoming earnings data available. Major earnings are typically reported before market open (BMO) or after market close (AMC).")
        
        with earnings_tabs[1]:
            st.markdown("#### 📊 Earnings Analyzer")
            st.markdown("<p style='color: #8b949e; font-size: 0.8rem;'>Enter a stock symbol to analyze recent earnings performance and get AI-generated insights</p>", unsafe_allow_html=True)
            
            earn_symbol = st.text_input("Stock Symbol:", placeholder="e.g., AAPL, MSFT, NVDA", key="earn_sym_input").upper().strip()
            
            if earn_symbol:
                with st.spinner(f"Analyzing {earn_symbol} earnings history..."):
                    earnings_analysis = analyze_earnings_history(earn_symbol)
                
                if earnings_analysis:
                    # Header card
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(88,166,255,0.1) 0%, rgba(163,113,247,0.1) 100%); border: 1px solid rgba(88,166,255,0.3); border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <span style="font-size: 1.5rem; font-weight: 700; color: #fff;">{earn_symbol}</span>
                                <span style="margin-left: 1rem; font-size: 0.9rem; color: #8b949e;">{earnings_analysis.get('company_name', '')}</span>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-size: 0.8rem; color: #8b949e;">Next Earnings</div>
                                <div style="font-size: 1rem; font-weight: 600; color: #58a6ff;">{earnings_analysis.get('next_earnings', 'TBD')}</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Earnings track record
                    track_record = earnings_analysis.get('track_record', {})
                    beats = track_record.get('beats', 0)
                    misses = track_record.get('misses', 0)
                    meets = track_record.get('meets', 0)
                    total = beats + misses + meets
                    beat_rate = (beats / total * 100) if total > 0 else 0
                    
                    tr_col1, tr_col2, tr_col3, tr_col4 = st.columns(4)
                    with tr_col1:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #3fb950;">{beats}</div>
                            <div style="font-size: 0.75rem; color: #8b949e;">Beats</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with tr_col2:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #f85149;">{misses}</div>
                            <div style="font-size: 0.75rem; color: #8b949e;">Misses</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with tr_col3:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #d29922;">{meets}</div>
                            <div style="font-size: 0.75rem; color: #8b949e;">In-Line</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with tr_col4:
                        beat_color = '#3fb950' if beat_rate >= 75 else '#d29922' if beat_rate >= 50 else '#f85149'
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: {beat_color};">{beat_rate:.0f}%</div>
                            <div style="font-size: 0.75rem; color: #8b949e;">Beat Rate</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Recent quarters
                    st.markdown("##### 📈 Recent Quarters")
                    quarters = earnings_analysis.get('recent_quarters', [])
                    if quarters:
                        for q in quarters[:4]:
                            result_color = '#3fb950' if q.get('result') == 'beat' else '#f85149' if q.get('result') == 'miss' else '#d29922'
                            result_icon = '✅' if q.get('result') == 'beat' else '❌' if q.get('result') == 'miss' else '➖'
                            surprise = q.get('surprise_pct', 0)
                            st.markdown(f"""
                            <div style="background: rgba(33,38,45,0.5); border-left: 3px solid {result_color}; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: 0 8px 8px 0;">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <div>
                                        <span style="font-weight: 600; color: #fff;">{q.get('quarter', '')}</span>
                                        <span style="margin-left: 1rem; color: {result_color};">{result_icon} {q.get('result', '').upper()}</span>
                                    </div>
                                    <div style="text-align: right;">
                                        <div style="font-size: 0.8rem;"><span style="color: #8b949e;">EPS:</span> <span style="color: #fff;">${q.get('actual_eps', 0):.2f}</span> vs <span style="color: #8b949e;">${q.get('est_eps', 0):.2f}</span></div>
                                        <div style="font-size: 0.75rem; color: {result_color};">Surprise: {surprise:+.1f}%</div>
                                    </div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # AI Analysis
                    st.markdown("##### 🤖 AI Earnings Analysis")
                    ai_analysis = earnings_analysis.get('ai_analysis', '')
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(163,113,247,0.1) 0%, rgba(88,166,255,0.05) 100%); border: 1px solid rgba(163,113,247,0.3); border-radius: 12px; padding: 1.25rem;">
                        <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{ai_analysis}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Post-earnings price reaction
                    st.markdown("##### 📊 Post-Earnings Price Reaction")
                    reactions = earnings_analysis.get('price_reactions', [])
                    if reactions:
                        reaction_cols = st.columns(len(reactions[:4]))
                        for i, r in enumerate(reactions[:4]):
                            with reaction_cols[i]:
                                move_color = '#3fb950' if r.get('move', 0) > 0 else '#f85149'
                                st.markdown(f"""
                                <div class="metric-card" style="text-align: center; padding: 0.75rem;">
                                    <div style="font-size: 0.7rem; color: #8b949e;">{r.get('quarter', '')}</div>
                                    <div style="font-size: 1.2rem; font-weight: 700; color: {move_color};">{r.get('move', 0):+.1f}%</div>
                                    <div style="font-size: 0.65rem; color: #6e7681;">Next Day</div>
                                </div>
                                """, unsafe_allow_html=True)
                else:
                    st.warning(f"Could not fetch earnings data for {earn_symbol}. Please verify the symbol is correct.")
        
        with earnings_tabs[2]:
            st.markdown("#### 📰 Earnings News Analyzer")
            st.markdown("<p style='color: #8b949e; font-size: 0.8rem;'>Paste an earnings call transcript or news article URL for AI analysis</p>", unsafe_allow_html=True)
            
            earnings_url = st.text_input("Earnings Article/Transcript URL:", placeholder="https://seekingalpha.com/... or earnings news URL", key="earn_url_input")
            
            if earnings_url:
                with st.spinner("Analyzing earnings content..."):
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            'Accept': 'text/html,application/xhtml+xml',
                        }
                        resp = requests.get(earnings_url, headers=headers, timeout=20)
                        soup = BeautifulSoup(resp.content, 'html.parser')
                        
                        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                            tag.decompose()
                        
                        title = soup.title.string if soup.title else "Earnings Analysis"
                        title = title.strip()[:150]
                        
                        # Extract content
                        article_text = ""
                        article_tags = soup.find_all(['article', 'main', 'div'], class_=lambda x: x and any(c in str(x).lower() for c in ['article', 'content', 'transcript', 'earnings']))
                        if article_tags:
                            article_text = article_tags[0].get_text(separator='\n', strip=True)
                        
                        if not article_text or len(article_text) < 300:
                            paragraphs = soup.find_all('p')
                            article_text = '\n'.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30])
                        
                        article_text = article_text[:20000]
                        
                        # Analyze earnings content
                        earnings_summary = analyze_earnings_content(article_text, title)
                        
                        # Display header
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, rgba(63,185,80,0.1) 0%, rgba(88,166,255,0.05) 100%); border: 1px solid rgba(63,185,80,0.3); border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem;">
                            <div style="font-size: 1.1rem; font-weight: 600; color: #fff; margin-bottom: 0.5rem;">📄 {title}</div>
                            <div style="font-size: 0.75rem; color: #8b949e;">{urlparse(earnings_url).netloc}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Key metrics extracted
                        if earnings_summary.get('key_metrics'):
                            st.markdown("##### 📊 Key Metrics Mentioned")
                            metrics_cols = st.columns(3)
                            for i, metric in enumerate(earnings_summary['key_metrics'][:6]):
                                with metrics_cols[i % 3]:
                                    st.markdown(f"""
                                    <div class="metric-card" style="padding: 0.5rem;">
                                        <div style="font-size: 0.7rem; color: #58a6ff;">{metric.get('label', '')}</div>
                                        <div style="font-size: 0.9rem; color: #fff;">{metric.get('value', '')}</div>
                                    </div>
                                    """, unsafe_allow_html=True)
                        
                        # Sentiment
                        sentiment = earnings_summary.get('sentiment', 'neutral')
                        sent_color = '#3fb950' if sentiment == 'bullish' else '#f85149' if sentiment == 'bearish' else '#d29922'
                        
                        # Key takeaways
                        st.markdown("##### 🎯 Key Takeaways")
                        for takeaway in earnings_summary.get('takeaways', [])[:5]:
                            st.markdown(f"""
                            <div style="background: rgba(33,38,45,0.5); padding: 0.5rem 1rem; margin: 0.25rem 0; border-radius: 6px; border-left: 2px solid #58a6ff;">
                                <span style="color: #c9d1d9; font-size: 0.85rem;">• {takeaway}</span>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        # Management tone
                        st.markdown("##### 🎤 Management Tone & Guidance")
                        st.markdown(f"""
                        <div style="display: flex; gap: 1rem; margin-bottom: 1rem;">
                            <div class="metric-card" style="flex: 1; text-align: center; padding: 0.75rem;">
                                <div style="font-size: 0.7rem; color: #8b949e;">Overall Sentiment</div>
                                <div style="font-size: 1rem; font-weight: 600; color: {sent_color}; text-transform: uppercase;">{sentiment}</div>
                            </div>
                            <div class="metric-card" style="flex: 1; text-align: center; padding: 0.75rem;">
                                <div style="font-size: 0.7rem; color: #8b949e;">Guidance Tone</div>
                                <div style="font-size: 1rem; font-weight: 600; color: #fff;">{earnings_summary.get('guidance_tone', 'Neutral')}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # AI Summary
                        st.markdown("##### 🤖 AI Earnings Summary")
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, rgba(163,113,247,0.1) 0%, rgba(88,166,255,0.05) 100%); border: 1px solid rgba(163,113,247,0.3); border-radius: 12px; padding: 1.25rem;">
                            <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{earnings_summary.get('summary', 'Analysis in progress...')}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Trading implications
                        st.markdown("##### 💡 Trading Implications")
                        implications = earnings_summary.get('trading_implications', [])
                        for imp in implications[:3]:
                            imp_color = '#3fb950' if 'bullish' in imp.lower() or 'positive' in imp.lower() else '#f85149' if 'bearish' in imp.lower() or 'negative' in imp.lower() else '#d29922'
                            st.markdown(f"""
                            <div style="background: rgba(33,38,45,0.3); padding: 0.5rem 1rem; margin: 0.25rem 0; border-radius: 6px;">
                                <span style="color: {imp_color}; font-size: 0.85rem;">→ {imp}</span>
                            </div>
                            """, unsafe_allow_html=True)
                        
                    except Exception as e:
                        st.error(f"Could not analyze the URL. Please check the link is valid and accessible. Error: {str(e)[:100]}")
    
    with tabs[6]:
        st.markdown("### 🔍 Research & URL Analysis")
        st.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Paste any financial news article URL for institutional-grade macro analysis</p>", unsafe_allow_html=True)
        
        url = st.text_input("Article URL:", placeholder="https://www.reuters.com/... or https://www.wsj.com/...", key="url_in")
        
        if url:
            with st.spinner("Extracting and analyzing article content..."):
                try:
                    # Fetch article
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                    }
                    resp = requests.get(url, headers=headers, timeout=20)
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    # Remove non-content elements
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'advertisement', 'iframe', 'noscript']):
                        tag.decompose()
                    
                    # Extract title
                    title = soup.title.string if soup.title else urlparse(url).netloc
                    title = title.strip()[:150] if title else "Article Analysis"
                    
                    # Extract main content
                    article_text = ""
                    
                    # Try to find article body
                    article_tags = soup.find_all(['article', 'main', 'div'], class_=lambda x: x and any(c in str(x).lower() for c in ['article', 'content', 'story', 'post', 'entry']))
                    if article_tags:
                        article_text = article_tags[0].get_text(separator='\n', strip=True)
                    
                    if not article_text or len(article_text) < 500:
                        # Fallback to paragraphs
                        paragraphs = soup.find_all('p')
                        article_text = '\n'.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50])
                    
                    if not article_text:
                        article_text = soup.get_text(separator='\n', strip=True)
                    
                    # Limit text length for processing
                    article_text = article_text[:15000]
                    text_lower = article_text.lower()
                    
                    # === COMPREHENSIVE ANALYSIS ===
                    
                    # 1. Extract key financial data points
                    numbers_with_context = []
                    sentences = article_text.replace('\n', ' ').split('.')
                    
                    for sent in sentences:
                        sent = sent.strip()
                        if len(sent) < 20 or len(sent) > 400:
                            continue
                        
                        # Look for sentences with meaningful financial data
                        has_number = bool(re.search(r'\d+\.?\d*\s*(%|percent|billion|million|trillion|bps|basis points)', sent, re.IGNORECASE))
                        has_financial_term = any(term in sent.lower() for term in ['revenue', 'earnings', 'profit', 'loss', 'gdp', 'inflation', 'rate', 'growth', 'decline', 'increase', 'decrease', 'forecast', 'estimate', 'target', 'outlook'])
                        has_market_term = any(term in sent.lower() for term in ['stock', 'bond', 'yield', 'index', 'market', 'trade', 'investor', 'fed', 'treasury', 'dollar', 'oil', 'gold'])
                        
                        if has_number and (has_financial_term or has_market_term):
                            numbers_with_context.append(sent)
                    
                    # 2. Identify macro themes
                    macro_themes = []
                    theme_keywords = {
                        'Monetary Policy': ['fed', 'federal reserve', 'interest rate', 'rate cut', 'rate hike', 'powell', 'fomc', 'monetary policy', 'hawkish', 'dovish', 'quantitative'],
                        'Inflation': ['inflation', 'cpi', 'pce', 'consumer price', 'price pressure', 'disinflation', 'deflation', 'core inflation'],
                        'Economic Growth': ['gdp', 'economic growth', 'recession', 'expansion', 'soft landing', 'hard landing', 'employment', 'jobs', 'unemployment', 'labor market'],
                        'Geopolitics': ['tariff', 'trade war', 'china', 'russia', 'ukraine', 'sanctions', 'geopolitical', 'conflict', 'tensions', 'nato'],
                        'Corporate Earnings': ['earnings', 'revenue', 'profit', 'guidance', 'beat', 'miss', 'eps', 'quarter', 'fiscal'],
                        'Technology/AI': ['artificial intelligence', ' ai ', 'nvidia', 'semiconductor', 'chip', 'tech sector', 'mega-cap', 'magnificent'],
                        'Energy': ['oil', 'crude', 'opec', 'natural gas', 'energy', 'petroleum', 'brent', 'wti'],
                        'Banking/Financial': ['bank', 'credit', 'lending', 'loan', 'financial sector', 'yield curve', 'treasury', 'bond'],
                        'Housing/Real Estate': ['housing', 'real estate', 'mortgage', 'home sales', 'construction', 'property'],
                        'Consumer': ['consumer', 'retail', 'spending', 'sentiment', 'confidence', 'discretionary'],
                    }
                    
                    for theme, keywords in theme_keywords.items():
                        count = sum(1 for kw in keywords if kw in text_lower)
                        if count >= 2:
                            macro_themes.append((theme, count))
                    
                    macro_themes.sort(key=lambda x: x[1], reverse=True)
                    primary_themes = [t[0] for t in macro_themes[:4]]
                    
                    # 3. Sentiment analysis with more nuance
                    bullish_words = ['surge', 'rally', 'beat', 'upgrade', 'record', 'strong', 'growth', 'buy', 'positive', 'optimism', 'bullish', 'outperform', 'accelerate', 'exceed', 'boom', 'soar', 'gain', 'advance', 'recovery', 'upside']
                    bearish_words = ['drop', 'fall', 'miss', 'downgrade', 'weak', 'cut', 'sell', 'warning', 'decline', 'pessimism', 'bearish', 'underperform', 'slow', 'concern', 'risk', 'crash', 'plunge', 'fear', 'recession', 'downside', 'slump']
                    
                    bull_count = sum(1 for w in bullish_words if w in text_lower)
                    bear_count = sum(1 for w in bearish_words if w in text_lower)
                    
                    if bull_count > bear_count * 1.5:
                        sentiment = "Bullish"
                        sentiment_color = "#3fb950"
                    elif bear_count > bull_count * 1.5:
                        sentiment = "Bearish"
                        sentiment_color = "#f85149"
                    elif bull_count > bear_count:
                        sentiment = "Moderately Bullish"
                        sentiment_color = "#7ee787"
                    elif bear_count > bull_count:
                        sentiment = "Moderately Bearish"
                        sentiment_color = "#ffa198"
                    else:
                        sentiment = "Neutral"
                        sentiment_color = "#d29922"
                    
                    # 4. Extract mentioned tickers
                    potential_tickers = set(re.findall(r'\b([A-Z]{2,5})\b', article_text))
                    # Filter to known tickers
                    all_known_tickers = set(OPTIONS_UNIVERSE) | set(['SPY', 'QQQ', 'IWM', 'DIA', 'VIX', 'TLT', 'GLD', 'USO', 'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLP', 'XLY', 'XLB', 'XLU', 'XLRE'])
                    mentioned_tickers = list(potential_tickers.intersection(all_known_tickers))[:8]
                    
                    # 5. Identify key entities (companies, people, organizations)
                    key_entities = []
                    entity_patterns = [
                        (r'(?:CEO|CFO|Chairman|President|Secretary|Chair)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'Executive'),
                        (r'(?:Federal Reserve|Fed|ECB|BOJ|PBOC|Treasury|IMF|World Bank)', 'Institution'),
                    ]
                    
                    # 6. Determine market implications
                    implications = []
                    
                    if 'rate cut' in text_lower or 'dovish' in text_lower:
                        implications.append("Potential tailwind for risk assets and growth stocks if rate cuts materialize")
                    if 'rate hike' in text_lower or 'hawkish' in text_lower:
                        implications.append("Rising rate expectations may pressure equity valuations, particularly in growth sectors")
                    if 'inflation' in text_lower and ('higher' in text_lower or 'rise' in text_lower or 'increase' in text_lower):
                        implications.append("Persistent inflation concerns could delay monetary easing and impact fixed income")
                    if 'recession' in text_lower or 'slowdown' in text_lower:
                        implications.append("Growth concerns warrant defensive positioning and quality factor exposure")
                    if 'tariff' in text_lower or 'trade war' in text_lower:
                        implications.append("Trade policy uncertainty may increase volatility and impact global supply chains")
                    if 'earnings beat' in text_lower or 'strong results' in text_lower:
                        implications.append("Positive earnings momentum could support continued equity market strength")
                    if 'guidance' in text_lower and ('lower' in text_lower or 'cut' in text_lower or 'reduce' in text_lower):
                        implications.append("Weakening corporate guidance signals potential earnings revisions ahead")
                    if 'ai' in text_lower or 'artificial intelligence' in text_lower:
                        implications.append("AI theme continues to drive sector rotation toward technology and semiconductors")
                    
                    if not implications:
                        implications.append("Monitor for follow-through price action to confirm directional bias")
                    
                    # === GENERATE INSTITUTIONAL ANALYSIS ===
                    
                    # Build the expert analysis paragraphs
                    eastern = pytz.timezone('US/Eastern')
                    
                    # Paragraph 1: Article summary and key findings
                    themes_str = ", ".join(primary_themes[:3]) if primary_themes else "general market dynamics"
                    
                    key_data_points = numbers_with_context[:3]
                    data_summary = ""
                    if key_data_points:
                        data_summary = f" The article cites several notable data points: {key_data_points[0]}."
                        if len(key_data_points) > 1:
                            data_summary += f" Additionally, {key_data_points[1].lower() if key_data_points[1][0].isupper() else key_data_points[1]}."
                    
                    para1 = f"This article centers on {themes_str}, presenting a {sentiment.lower()} tone based on our textual analysis ({bull_count} constructive signals vs. {bear_count} cautionary flags).{data_summary}"
                    
                    # Paragraph 2: Market implications and positioning
                    implications_text = implications[0] if implications else "The immediate market impact appears limited, with price action likely to be driven by broader macro factors."
                    if len(implications) > 1:
                        implications_text += f" Furthermore, {implications[1].lower() if implications[1][0].isupper() else implications[1]}."
                    
                    if sentiment in ['Bullish', 'Moderately Bullish']:
                        positioning = "From a positioning standpoint, the narrative supports a constructive bias, though we would advise scaling into exposure rather than aggressive accumulation given the inherent uncertainty in any single-source analysis."
                    elif sentiment in ['Bearish', 'Moderately Bearish']:
                        positioning = "The risk-reward calculus suggests caution is warranted. Institutional investors may consider hedging exposure or reducing beta until the thesis is either confirmed or invalidated by subsequent price action."
                    else:
                        positioning = "The balanced tone suggests maintaining current positioning while monitoring for catalysts that could shift the narrative in either direction."
                    
                    para2 = f"From a macro perspective, {implications_text} {positioning}"
                    
                    # === DISPLAY RESULTS ===
                    
                    # Article header
                    source_domain = urlparse(url).netloc.replace('www.', '')
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(88,166,255,0.1) 0%, rgba(163,113,247,0.05) 100%); border: 1px solid rgba(88,166,255,0.3); border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem;">
                        <div style="font-size: 1.1rem; font-weight: 600; color: #ffffff; margin-bottom: 0.5rem;">{title}</div>
                        <div style="display: flex; align-items: center; gap: 1rem;">
                            <span style="font-size: 0.75rem; color: #8b949e;">Source: {source_domain}</span>
                            <span style="background: {'rgba(63,185,80,0.2)' if 'Bullish' in sentiment else 'rgba(248,81,73,0.2)' if 'Bearish' in sentiment else 'rgba(210,153,34,0.2)'}; color: {sentiment_color}; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600;">{sentiment}</span>
                            <span style="font-size: 0.7rem; color: #6e7681;">Analyzed: {datetime.now(eastern).strftime('%I:%M %p ET')}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Metrics row
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div class="metric-value" style="color: #3fb950;">{bull_count}</div>
                            <div class="metric-label">Bullish Signals</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div class="metric-value" style="color: #f85149;">{bear_count}</div>
                            <div class="metric-label">Bearish Signals</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col3:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div class="metric-value" style="color: #a371f7;">{len(primary_themes)}</div>
                            <div class="metric-label">Macro Themes</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col4:
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: center;">
                            <div class="metric-value" style="color: #58a6ff;">{len(mentioned_tickers)}</div>
                            <div class="metric-label">Tickers Found</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Macro themes
                    if primary_themes:
                        st.markdown("#### 🏷️ Primary Macro Themes")
                        theme_cols = st.columns(len(primary_themes[:4]))
                        theme_colors = ['#58a6ff', '#a371f7', '#3fb950', '#d29922']
                        for i, theme in enumerate(primary_themes[:4]):
                            with theme_cols[i]:
                                st.markdown(f"""
                                <div style="background: rgba({48 + i*20}, {54 + i*10}, {61 + i*15}, 0.5); border-left: 3px solid {theme_colors[i % 4]}; padding: 0.5rem 0.75rem; border-radius: 0 6px 6px 0;">
                                    <span style="color: {theme_colors[i % 4]}; font-weight: 600; font-size: 0.85rem;">{theme}</span>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Expert Analysis Section
                    st.markdown("#### 🎩 Institutional Macro Analysis")
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(163,113,247,0.1) 0%, rgba(88,166,255,0.05) 100%); border: 1px solid rgba(163,113,247,0.3); border-radius: 12px; padding: 1.5rem; margin: 1rem 0;">
                        <div style="display: flex; align-items: center; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid rgba(163,113,247,0.2);">
                            <span style="font-size: 0.7rem; color: #a371f7; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em;">Expert Assessment</span>
                            <span style="margin-left: auto; font-size: 0.65rem; color: #6e7681;">Confidence: {'High' if abs(bull_count - bear_count) > 5 else 'Medium' if abs(bull_count - bear_count) > 2 else 'Low'}</span>
                        </div>
                        <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin-bottom: 1rem; font-family: 'Georgia', serif;">{para1}</p>
                        <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{para2}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Key Data Points
                    if numbers_with_context:
                        st.markdown("#### 📊 Key Data Points Extracted")
                        for i, point in enumerate(numbers_with_context[:5]):
                            st.markdown(f"""
                            <div style="background: rgba(33,38,45,0.8); border-left: 3px solid #58a6ff; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: 0 8px 8px 0;">
                                <span style="color: #c9d1d9; font-size: 0.85rem;">{point}.</span>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Market Implications
                    if implications:
                        st.markdown("#### 💡 Market Implications")
                        for imp in implications[:4]:
                            st.markdown(f"""
                            <div class="opportunity-item" style="margin: 0.4rem 0;">
                                <span style="color: #7ee787;">→</span> {imp}
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Mentioned Tickers
                    if mentioned_tickers:
                        st.markdown("#### 📈 Mentioned Securities")
                        st.markdown("<p style='color: #8b949e; font-size: 0.75rem;'>Click any ticker for detailed analysis</p>", unsafe_allow_html=True)
                        ticker_cols = st.columns(min(6, len(mentioned_tickers)))
                        for i, ticker in enumerate(mentioned_tickers[:6]):
                            with ticker_cols[i]:
                                if st.button(f"📊 {ticker}", key=f"url_ticker_{ticker}_{i}", use_container_width=True):
                                    st.session_state.selected_stock = ticker
                                    st.session_state.show_stock_report = True
                                    st.rerun()
                    
                    # Full text expander
                    with st.expander("📄 View Extracted Article Text", expanded=False):
                        st.text_area("Article Content", article_text[:5000], height=300, disabled=True)
                    
                except Exception as e:
                    st.error(f"Error analyzing URL: {str(e)}")
                    st.info("Tips: Ensure the URL is accessible and points to a public article. Some paywalled content may not be extractable.")
        else:
            st.markdown("""
            <div style="background: rgba(33,38,45,0.5); border: 1px dashed rgba(88,166,255,0.3); border-radius: 12px; padding: 2rem; text-align: center;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📰</div>
                <div style="color: #8b949e; font-size: 0.9rem;">Paste a financial news article URL above to receive institutional-grade macro analysis</div>
                <div style="color: #6e7681; font-size: 0.75rem; margin-top: 0.5rem;">Supported sources: Reuters, Bloomberg, WSJ, FT, CNBC, Yahoo Finance, and more</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    eastern = pytz.timezone('US/Eastern')
    st.markdown(f'<div class="timestamp" style="text-align:center;">{datetime.now(eastern).strftime("%I:%M:%S %p ET · %B %d, %Y")} · Institutional Analysis</div>', unsafe_allow_html=True)

if __name__ == "__main__": main()
