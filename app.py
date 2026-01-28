"""
Pre-Market Command Center v7
Institutional-Grade Market Prep Dashboard
AI Expert Analysis · Support/Resistance · Deep Technical Signals
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
warnings.filterwarnings('ignore')

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
    .earnings-card { background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 1rem; margin: 0.5rem 0; }
    .earnings-beat { border-left: 3px solid #3fb950; }
    .earnings-miss { border-left: 3px solid #f85149; }
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
</style>
""", unsafe_allow_html=True)

FUTURES_SYMBOLS = {"S&P 500": "ES=F", "Nasdaq 100": "NQ=F", "Dow Jones": "YM=F", "Russell 2000": "RTY=F", "Crude Oil": "CL=F", "Gold": "GC=F", "Silver": "SI=F", "Natural Gas": "NG=F", "VIX": "^VIX", "Dollar Index": "DX=F", "10Y Treasury": "^TNX", "Bitcoin": "BTC-USD"}
SECTOR_ETFS = {"Technology": {"symbol": "XLK", "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "AMD", "CRM", "ORCL", "ADBE"]}, "Financial": {"symbol": "XLF", "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW"]}, "Energy": {"symbol": "XLE", "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO"]}, "Healthcare": {"symbol": "XLV", "stocks": ["UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT"]}, "Consumer Disc.": {"symbol": "XLY", "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX"]}, "Consumer Staples": {"symbol": "XLP", "stocks": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL"]}, "Industrials": {"symbol": "XLI", "stocks": ["CAT", "GE", "RTX", "UNP", "BA", "HON", "DE", "LMT"]}, "Materials": {"symbol": "XLB", "stocks": ["LIN", "APD", "SHW", "FCX", "NEM", "NUE", "DOW", "ECL"]}, "Utilities": {"symbol": "XLU", "stocks": ["NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL"]}, "Real Estate": {"symbol": "XLRE", "stocks": ["AMT", "PLD", "CCI", "EQIX", "SPG", "PSA", "O", "WELL"]}, "Communication": {"symbol": "XLC", "stocks": ["META", "GOOGL", "NFLX", "DIS", "CMCSA", "VZ", "T", "TMUS"]}}
FINANCE_CATEGORIES = {"Major Banks": ["JPM", "BAC", "WFC", "C", "USB", "PNC"], "Investment Banks": ["GS", "MS", "SCHW", "RJF"], "Insurance": ["BRK-B", "AIG", "MET", "PRU", "AFL", "TRV"], "Payments": ["V", "MA", "AXP", "PYPL", "SQ"], "Asset Managers": ["BLK", "BX", "KKR", "APO", "TROW"], "Fintech": ["PYPL", "SQ", "SOFI", "HOOD", "COIN"]}
OPTIONS_UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "GLD", "SLV", "TLT", "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO", "JPM", "BAC", "GS", "MS", "C", "WFC", "XOM", "CVX", "COP", "SLB", "UNH", "JNJ", "LLY", "PFE", "ABBV", "HD", "MCD", "NKE", "SBUX", "COST", "NFLX", "CRM", "ORCL", "V", "MA", "DIS"]
GLOBAL_INDICES = {"FTSE 100": "^FTSE", "DAX": "^GDAXI", "CAC 40": "^FCHI", "Nikkei 225": "^N225", "Hang Seng": "^HSI", "Shanghai": "000001.SS"}
NEWS_FEEDS = {"Reuters": "https://feeds.reuters.com/reuters/businessNews", "CNBC": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "MarketWatch": "http://feeds.marketwatch.com/marketwatch/topstories"}
TIMEFRAMES = {"1D": ("1d", "5m"), "5D": ("5d", "15m"), "1M": ("1mo", "1h"), "3M": ("3mo", "1d"), "6M": ("6mo", "1d"), "1Y": ("1y", "1d"), "YTD": ("ytd", "1d")}

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

@st.cache_data(ttl=120)
def fetch_stock_data(symbol, period="5d", interval="15m"):
    try:
        ticker = yf.Ticker(symbol)
        # Disable pre/post market data to avoid large wicks from overnight gaps
        return ticker.history(period=period, interval=interval, prepost=False), ticker.info
    except: return None, {}

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

@st.cache_data(ttl=300)
def fetch_comprehensive_data(symbol):
    try:
        ticker = yf.Ticker(symbol)
        # Disable pre/post market data to avoid large wicks
        data = {
            'info': ticker.info, 
            'hist_1d': ticker.history(period="1d", interval="5m", prepost=False), 
            'hist_5d': ticker.history(period="5d", interval="15m", prepost=False), 
            'hist_1mo': ticker.history(period="1mo", interval="1h", prepost=False), 
            'hist_3mo': ticker.history(period="3mo", interval="1d", prepost=False), 
            'hist_6mo': ticker.history(period="6mo", interval="1d", prepost=False), 
            'hist_1y': ticker.history(period="1y", interval="1d", prepost=False), 
            'news': [], 
            'earnings': None, 
            'recommendations': None, 
            'calendar': None, 
            'holders': None
        }
        
        # Fetch news using dedicated function with multiple fallbacks
        data['news'] = fetch_stock_news_direct(symbol)
        
        # Fetch earnings with multiple fallback methods
        try:
            # Method 1: earnings_history
            eh = ticker.earnings_history
            if eh is not None and not eh.empty: 
                data['earnings'] = eh
        except: pass
        
        if data['earnings'] is None:
            try:
                # Method 2: quarterly_earnings
                qe = ticker.quarterly_earnings
                if qe is not None and not qe.empty:
                    data['earnings'] = qe
            except: pass
        
        if data['earnings'] is None:
            try:
                # Method 3: earnings_dates
                ed = ticker.earnings_dates
                if ed is not None and not ed.empty:
                    # Rename columns to match expected format
                    if 'Reported EPS' in ed.columns and 'EPS Estimate' in ed.columns:
                        ed = ed.rename(columns={'Reported EPS': 'epsActual', 'EPS Estimate': 'epsEstimate'})
                    data['earnings'] = ed.dropna(subset=['epsActual'] if 'epsActual' in ed.columns else ed.columns[:1])
            except: pass
        
        # Fetch recommendations with multiple attempts
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty: data['recommendations'] = recs.tail(30)
        except: pass
        
        if data['recommendations'] is None:
            try:
                # Alternative: recommendations_summary
                recs_sum = ticker.recommendations_summary
                if recs_sum is not None and not recs_sum.empty:
                    data['recommendations'] = recs_sum
            except: pass
        
        try: data['calendar'] = ticker.calendar
        except: pass
        try:
            h = ticker.institutional_holders
            if h is not None and not h.empty: data['holders'] = h
        except: pass
        return data
    except: return None

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

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0, "neutral"
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50.0
    cond = "overbought" if val > 70 else "oversold" if val < 30 else "bullish" if val > 60 else "bearish" if val < 40 else "neutral"
    return val, cond

def calculate_macd(prices):
    if len(prices) < 35: return 0, 0, 0, "neutral"
    ema12, ema26 = prices.ewm(span=12).mean(), prices.ewm(span=26).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9).mean()
    hist = macd_line - signal_line
    sig = "bullish" if hist.iloc[-1] > 0 and hist.iloc[-1] > hist.iloc[-2] else "bearish" if hist.iloc[-1] < 0 and hist.iloc[-1] < hist.iloc[-2] else "neutral"
    return macd_line.iloc[-1], signal_line.iloc[-1], hist.iloc[-1], sig

def calculate_bollinger(prices, period=20):
    if len(prices) < period: return None, None, None, "neutral"
    sma = prices.rolling(period).mean()
    std = prices.rolling(period).std()
    upper, lower = sma + (std * 2), sma - (std * 2)
    curr = prices.iloc[-1]
    pos = "above_upper" if curr > upper.iloc[-1] else "below_lower" if curr < lower.iloc[-1] else "upper_half" if curr > sma.iloc[-1] else "lower_half"
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

def calculate_metrics(hist, info):
    if hist is None or hist.empty: return None
    latest = hist.iloc[-1]
    price, prev = latest['Close'], info.get('regularMarketPreviousClose', latest['Close'])
    change_pct = ((price - prev) / prev * 100) if prev else 0
    vol = latest['Volume']
    avg_vol = hist['Volume'].rolling(20).mean().iloc[-1] if len(hist) > 20 else vol
    vol_vs_avg = (vol / avg_vol * 100) if avg_vol > 0 else 100
    momentum = ((price - hist['Close'].iloc[0]) / hist['Close'].iloc[0] * 100) if len(hist) > 1 else 0
    rsi, rsi_cond = calculate_rsi(hist['Close'])
    _, _, _, macd_sig = calculate_macd(hist['Close'])
    return {'current_price': price, 'prev_close': prev, 'overnight_change': price - prev, 'overnight_change_pct': change_pct, 'volume': vol, 'volume_vs_avg': vol_vs_avg, 'high': latest['High'], 'low': latest['Low'], 'momentum_5d': momentum, 'rsi': rsi, 'rsi_condition': rsi_cond, 'macd_signal': macd_sig}

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
        roc_5 = ((prices.iloc[-1] - prices.iloc[-5]) / prices.iloc[-5]) * 100
        roc_10 = ((prices.iloc[-1] - prices.iloc[-10]) / prices.iloc[-10]) * 100
        
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

def generate_expert_analysis(symbol, data, signals, support_levels, resistance_levels, news_sentiment):
    """Generate AI expert analysis synthesizing all available data."""
    info = data.get('info', {})
    hist = data.get('hist_5d')
    
    if hist is None or hist.empty:
        return None
    
    price = hist['Close'].iloc[-1]
    prev = info.get('regularMarketPreviousClose', price)
    change_pct = ((price - prev) / prev * 100) if prev else 0
    
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
    if nearest_support and nearest_resistance:
        support_dist = ((price - nearest_support[1]) / price) * 100
        resist_dist = ((nearest_resistance[1] - price) / price) * 100
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
    
    # === BUILD THE SUMMARY ===
    
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
    
    # Combine into final summary
    summary_parts = [opening, vol_analysis, rotation_insight, global_insight]
    if commodity_insight:
        summary_parts.append(commodity_insight)
    summary_parts.extend([primary_driver, outlook])
    
    summary = " ".join(summary_parts)
    
    return summary

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
        max_range_pct = 8
    elif tf in ['5D']:
        max_range_pct = 10
    else:
        max_range_pct = 15
    
    # Clean the data using helper function
    hist = clean_chart_data(hist, max_range_pct=max_range_pct)
    
    if hist is None or len(hist) < 5:
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
    """Clean OHLC data to remove bad wicks and outliers."""
    if hist is None or hist.empty:
        return None
    
    # Make a copy
    hist = hist.copy()
    
    # Remove NaN
    hist = hist.dropna(subset=['Open', 'High', 'Low', 'Close'])
    
    if len(hist) < 5:
        return None
    
    # Remove invalid candles
    hist = hist[hist['High'] >= hist['Low']]
    hist = hist[(hist['High'] >= hist['Open']) & (hist['High'] >= hist['Close'])]
    hist = hist[(hist['Low'] <= hist['Open']) & (hist['Low'] <= hist['Close'])]
    
    if len(hist) < 5:
        return None
    
    # Filter excessive range candles
    candle_range_pct = (hist['High'] - hist['Low']) / hist['Close'] * 100
    valid_range = candle_range_pct <= max_range_pct
    if valid_range.sum() >= len(hist) * 0.7:
        hist = hist[valid_range]
    
    # IQR outlier removal
    q1, q3 = hist['Close'].quantile(0.05), hist['Close'].quantile(0.95)
    iqr = q3 - q1
    price_valid = (hist['Close'] >= q1 - 2*iqr) & (hist['Close'] <= q3 + 2*iqr)
    if price_valid.sum() >= len(hist) * 0.8:
        hist = hist[price_valid]
    
    # Cap extreme wicks
    for idx in hist.index:
        body_high = max(hist.loc[idx, 'Open'], hist.loc[idx, 'Close'])
        body_low = min(hist.loc[idx, 'Open'], hist.loc[idx, 'Close'])
        body_size = body_high - body_low
        max_wick = max(body_size * 3, hist.loc[idx, 'Close'] * 0.02)
        
        if hist.loc[idx, 'High'] > body_high + max_wick:
            hist.loc[idx, 'High'] = body_high + max_wick
        if hist.loc[idx, 'Low'] < body_low - max_wick:
            hist.loc[idx, 'Low'] = body_low - max_wick
    
    return hist if len(hist) >= 5 else None

def create_mini_chart(hist, symbol, show_volume=True):
    """Create a simplified mini chart for dashboard views."""
    if hist is None or hist.empty:
        return None
    
    # Clean data using helper
    hist = clean_chart_data(hist, max_range_pct=10)
    if hist is None or len(hist) < 5:
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
    prev = info.get('regularMarketPreviousClose', price)
    ch_pct = ((price - prev) / prev * 100) if prev else 0
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
        st.plotly_chart(fig, use_container_width=True)
    
    # Support/Resistance Display
    sr_col1, sr_col2 = st.columns(2)
    with sr_col1:
        st.markdown("#### 🟢 Support Levels")
        if support_levels:
            for name, level, strength in support_levels:
                dist = ((price - level) / price) * 100
                st.markdown(f'<div class="sr-level support-level"><span>{name}</span><span>${level:.2f} ({dist:.1f}% below)</span></div>', unsafe_allow_html=True)
        else:
            st.info("No clear support levels identified")
    with sr_col2:
        st.markdown("#### 🔴 Resistance Levels")
        if resistance_levels:
            for name, level, strength in resistance_levels:
                dist = ((level - price) / price) * 100
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
                up = ((tm - price) / price) * 100
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
    data = fetch_comprehensive_data(sym)
    if not data: return None
    h = data.get('hist_5d')
    info = data.get('info', {})
    if h is None or h.empty: return None
    price = h['Close'].iloc[-1]
    prev = info.get('regularMarketPreviousClose', price)
    overnight = ((price - prev) / prev * 100) if prev else 0
    mom = ((price - h['Close'].iloc[0]) / h['Close'].iloc[0] * 100) if len(h) > 1 else 0
    avg_vol = h['Volume'].rolling(20).mean().iloc[-1] if len(h) > 20 else h['Volume'].iloc[-1]
    vol_ratio = (h['Volume'].iloc[-1] / avg_vol * 100) if avg_vol > 0 else 100
    rng = ((h['High'].iloc[-1] - h['Low'].iloc[-1]) / price * 100) if price else 0
    h1m = data.get('hist_1mo')
    rsi, _ = calculate_rsi(h1m['Close']) if h1m is not None and len(h1m) > 14 else (50, "neutral")
    score = min(25, vol_ratio / 4) + min(20, rng * 5)
    if direction == 'calls': score += min(15, max(0, mom * 2)) + min(15, max(0, overnight * 5))
    else: score += min(15, max(0, -mom * 2)) + min(15, max(0, -overnight * 5))
    score += 10 if (direction == 'calls' and rsi < 70) or (direction == 'puts' and rsi > 30) else 3
    score += 7.5
    grade = "A" if score >= 70 else "B" if score >= 55 else "C" if score >= 40 else "D"
    gr_cls = "score-excellent" if score >= 70 else "score-good" if score >= 55 else "score-fair" if score >= 40 else "score-weak"
    return {'symbol': sym, 'total_score': round(score, 1), 'grade': grade, 'grade_class': gr_cls, 'current_price': price, 'overnight_change_pct': overnight, 'momentum_5d': mom, 'rsi': rsi}

@st.cache_data(ttl=300)
def get_top_options():
    calls, puts = [], []
    for s in OPTIONS_UNIVERSE[:30]:
        c = calc_opt_score(s, 'calls')
        if c: calls.append(c)
        p = calc_opt_score(s, 'puts')
        if p: puts.append(p)
    return sorted(calls, key=lambda x: x['total_score'], reverse=True)[:3], sorted(puts, key=lambda x: x['total_score'], reverse=True)[:3]

def main():
    if st.session_state.show_stock_report and st.session_state.selected_stock: render_stock_report(st.session_state.selected_stock); return
    col_t, col_s = st.columns([3, 1])
    with col_t: st.markdown('<h1 class="main-title">📈 Pre-Market Command Center</h1>', unsafe_allow_html=True); st.markdown('<p class="subtitle">Institutional Analysis · AI Insights · Click Any Stock</p>', unsafe_allow_html=True)
    with col_s:
        sk, st_txt, cd = get_market_status()
        eastern = pytz.timezone('US/Eastern')
        st.markdown(f'<div style="text-align:right;"><span class="market-status status-{sk}">{st_txt}</span><p class="timestamp">{cd}</p><p class="timestamp">{datetime.now(eastern).strftime("%I:%M %p ET")}</p></div>', unsafe_allow_html=True)
    st.markdown("---")
    tabs = st.tabs(["🎯 Market Brief", "🌍 Futures", "📊 Stocks", "🏢 Sectors", "📈 Options", "🔍 Research"])
    
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
            expert_summary = generate_expert_macro_summary(md, ns, econ, assess)
            
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
                <p style="color: #c9d1d9; font-size: 0.9rem; line-height: 1.8; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{expert_summary}</p>
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
                        st.plotly_chart(create_chart(h, n, "5D", False, support, resistance), use_container_width=True)
    
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
                    st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 📋 Top Holdings")
        st.markdown(f"<p style='color: #8b949e; font-size: 0.8rem;'>Click any stock for detailed analysis</p>", unsafe_allow_html=True)
        stocks = FINANCE_CATEGORIES[fin_filter] if sector == "Financial" and fin_filter != "All" else sec_info['stocks']
        s_cols = st.columns(4)
        for i, s in enumerate(stocks):
            h, inf = fetch_stock_data(s, "5d", "15m")
            m = calculate_metrics(h, inf)
            if m: render_clickable_stock(s, m['current_price'], m['overnight_change_pct'], s_cols[i % 4], "sec")
    
    with tabs[4]:
        st.markdown("## 📈 Options")
        if st.button("🔄 Screener", type="primary", key="opt_run"): st.cache_data.clear()
        with st.spinner("..."): calls, puts = get_top_options()
        c_col, p_col = st.columns(2)
        with c_col:
            st.markdown('<div class="calls-card"><h3 style="color:#3fb950;margin:0;">📈 CALLS</h3></div>', unsafe_allow_html=True)
            for i, p in enumerate(calls, 1):
                st.markdown(f'<div class="options-pick-card"><div class="pick-header"><span class="pick-symbol">#{i} {p["symbol"]}</span><span class="pick-score {p["grade_class"]}">{p["grade"]} ({p["total_score"]:.0f})</span></div><div style="color:#8b949e;font-size:0.85rem;">${p["current_price"]:.2f} · {p["overnight_change_pct"]:+.2f}% · RSI:{p["rsi"]:.0f}</div></div>', unsafe_allow_html=True)
                if st.button(f"View {p['symbol']}", key=f"c_{p['symbol']}", use_container_width=True): st.session_state.selected_stock = p['symbol']; st.session_state.show_stock_report = True; st.rerun()
        with p_col:
            st.markdown('<div class="puts-card"><h3 style="color:#f85149;margin:0;">📉 PUTS</h3></div>', unsafe_allow_html=True)
            for i, p in enumerate(puts, 1):
                st.markdown(f'<div class="options-pick-card"><div class="pick-header"><span class="pick-symbol">#{i} {p["symbol"]}</span><span class="pick-score {p["grade_class"]}">{p["grade"]} ({p["total_score"]:.0f})</span></div><div style="color:#8b949e;font-size:0.85rem;">${p["current_price"]:.2f} · {p["overnight_change_pct"]:+.2f}% · RSI:{p["rsi"]:.0f}</div></div>', unsafe_allow_html=True)
                if st.button(f"View {p['symbol']}", key=f"p_{p['symbol']}", use_container_width=True): st.session_state.selected_stock = p['symbol']; st.session_state.show_stock_report = True; st.rerun()
    
    with tabs[5]:
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
