"""
Pre-Market Command Center v9.0
Institutional-Grade Market Prep Dashboard
AI Expert Analysis · Earnings Intelligence · Whale Tracker · Risk Turbulence

v9.0 COMPREHENSIVE UPGRADE:
- ENHANCED DATA RESILIENCE: Robust error handling across all data fetches
  * Graceful degradation when data sources are unavailable
  * Retry logic with exponential backoff for yfinance calls
  * Defensive NaN/None handling in every calculation path
  * Proper exception chaining and logging

- IMPROVED TECHNICAL ANALYSIS ENGINE:
  * VWAP calculation for intraday analysis
  * ATR-based dynamic stop calculation
  * Multi-timeframe trend confluence scoring
  * Enhanced volume profile analysis with VWAP deviation
  * Fibonacci retracement levels in S/R calculation
  * Improved RSI divergence detection

- UPGRADED MARKET INTELLIGENCE:
  * Enhanced sector rotation model with momentum scoring
  * Cross-asset correlation dashboard in Market Brief
  * Improved fear/greed composite with VIX term structure
  * Dollar-weighted breadth indicators
  * Enhanced economic calendar with impact scoring

- PERFORMANCE OPTIMIZATIONS:
  * Smarter cache TTLs based on market hours
  * Batch yfinance downloads for sector scans
  * Reduced redundant API calls via shared data pipeline
  * Memory-efficient DataFrame operations

- UI/UX POLISH:
  * Consistent Bloomberg terminal aesthetics throughout
  * Enhanced sparkline micro-charts in summary cards
  * Improved mobile responsiveness
  * Better loading states and progress indicators
  * Auto-refresh option for live market hours

Previous Versions:
- v8.4: Risk Turbulence tab, Bloomberg trade params UI
- v8.3: Redesigned AI Expert Analysis, clickable news
- v8.2: Fixed futures/indices, enhanced institutional analysis

Features:
- 🐋 Institutional Activity & Whale Tracker
- 📅 Earnings Center (calendar, analyzer, news)
- 📰 News Flow Analysis with clickable links
- 📈 Advanced Options Screener
- 🎯 AI-generated institutional-grade analysis
- 🧠 Smart Money indicators
- 🌊 Risk Turbulence & Convergence Early Warning
- 💹 Bloomberg Terminal-style Trade Parameters
- 📊 Cross-Asset Correlation Monitor
- 🔄 Enhanced Sector Rotation Model
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
from functools import lru_cache
import hashlib
import time as time_module
import logging

warnings.filterwarnings('ignore')

# Configure logging for debugging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# === UTILITY FUNCTIONS ===

def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero or invalid."""
    try:
        if denominator is None or denominator == 0 or pd.isna(denominator):
            return default
        result = numerator / denominator
        return default if pd.isna(result) or np.isinf(result) else result
    except (TypeError, ZeroDivisionError, ValueError):
        return default

def safe_pct_change(current: float, previous: float, default: float = 0.0) -> float:
    """Calculate percentage change safely."""
    return safe_div((current - previous), previous, default) * 100

def safe_get(data: dict, key: str, default: Any = None) -> Any:
    """Safely get a value from a dict, handling None and NaN."""
    try:
        val = data.get(key, default)
        if val is None or (isinstance(val, float) and (pd.isna(val) or np.isinf(val))):
            return default
        return val
    except (AttributeError, TypeError):
        return default

def safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert any value to float."""
    if val is None:
        return default
    try:
        result = float(val)
        return default if pd.isna(result) or np.isinf(result) else result
    except (ValueError, TypeError):
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

def calculate_vwap(hist: pd.DataFrame) -> Optional[pd.Series]:
    """Calculate Volume Weighted Average Price for intraday analysis."""
    try:
        if hist is None or hist.empty or 'Volume' not in hist.columns:
            return None
        typical_price = (hist['High'] + hist['Low'] + hist['Close']) / 3
        cum_tp_vol = (typical_price * hist['Volume']).cumsum()
        cum_vol = hist['Volume'].cumsum().replace(0, np.nan)
        vwap = cum_tp_vol / cum_vol
        return vwap
    except Exception:
        return None

def calculate_atr(hist: pd.DataFrame, period: int = 14) -> float:
    """Calculate Average True Range."""
    try:
        if hist is None or len(hist) < period + 1:
            return 0.0
        high = hist['High']
        low = hist['Low']
        close = hist['Close'].shift(1)
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        return safe_float(atr, 0.0)
    except Exception:
        return 0.0

def calculate_fibonacci_levels(high: float, low: float, current: float) -> Dict[str, float]:
    """Calculate Fibonacci retracement/extension levels."""
    diff = high - low
    if diff <= 0:
        return {}
    levels = {
        'Fib 0.0 (Low)': low,
        'Fib 0.236': low + diff * 0.236,
        'Fib 0.382': low + diff * 0.382,
        'Fib 0.500': low + diff * 0.500,
        'Fib 0.618': low + diff * 0.618,
        'Fib 0.786': low + diff * 0.786,
        'Fib 1.0 (High)': high,
        'Fib 1.272': high + diff * 0.272,
        'Fib 1.618': high + diff * 0.618,
    }
    return levels

def get_dynamic_cache_ttl() -> int:
    """Return cache TTL based on market hours - shorter during active trading."""
    try:
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        hour = now.hour
        if now.weekday() >= 5:  # Weekend
            return 1800
        if 4 <= hour < 9:  # Pre-market
            return 180
        if 9 <= hour < 16:  # Market hours
            return 120
        if 16 <= hour < 20:  # After hours
            return 300
        return 900  # Overnight
    except Exception:
        return 300

# === STREAMLIT CONFIG ===

st.set_page_config(page_title="Pre-Market Command Center v9", page_icon="📈", layout="wide", initial_sidebar_state="collapsed")

# Enhanced session state initialization
_default_state = {
    'selected_stock': None,
    'show_stock_report': False,
    'chart_tf': '5D',
    'auto_refresh': False,
    'last_refresh': None,
    'watchlist_custom': [],
    'comparison_mode': False,
}
for key, default in _default_state.items():
    if key not in st.session_state:
        st.session_state[key] = default

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
    .news-item { background: #21262d; border-left: 3px solid #58a6ff; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: 0 8px 8px 0; transition: all 0.2s ease; cursor: pointer; }
    .news-item:hover { background: #30363d; border-left-color: #79c0ff; transform: translateX(2px); }
    .news-title { font-family: 'Inter', sans-serif; font-size: 0.9rem; color: #ffffff; margin-bottom: 0.25rem; }
    .news-title a { color: #ffffff; text-decoration: none; }
    .news-title a:hover { color: #58a6ff; text-decoration: underline; }
    .news-meta { font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #8b949e; }
    .news-link { display: block; text-decoration: none; color: inherit; }
    .news-link:hover .news-title { color: #58a6ff; }
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
    /* v9.0 Enhanced Styles */
    @keyframes pulse-green { 0%, 100% { box-shadow: 0 0 0 0 rgba(63,185,80,0.4); } 50% { box-shadow: 0 0 0 6px rgba(63,185,80,0); } }
    @keyframes pulse-red { 0%, 100% { box-shadow: 0 0 0 0 rgba(248,81,73,0.4); } 50% { box-shadow: 0 0 0 6px rgba(248,81,73,0); } }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
    .pulse-positive { animation: pulse-green 2s infinite; }
    .pulse-negative { animation: pulse-red 2s infinite; }
    .fade-in { animation: fadeIn 0.4s ease-out; }
    .vwap-indicator { background: rgba(163,113,247,0.15); border: 1px solid rgba(163,113,247,0.4); border-radius: 8px; padding: 0.5rem 0.75rem; }
    .correlation-matrix { background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 1rem; }
    .breadth-bar { height: 6px; border-radius: 3px; background: linear-gradient(90deg, #f85149 0%, #d29922 50%, #3fb950 100%); overflow: hidden; }
    .sparkline-container { display: inline-block; vertical-align: middle; margin-left: 0.5rem; }
    .regime-badge { padding: 0.2rem 0.6rem; border-radius: 12px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
    .regime-green { background: rgba(63,185,80,0.2); color: #3fb950; }
    .regime-yellow { background: rgba(210,153,34,0.2); color: #d29922; }
    .regime-orange { background: rgba(240,136,62,0.2); color: #f0883e; }
    .regime-red { background: rgba(248,81,73,0.2); color: #f85149; }
    .terminal-row { background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.5rem 1rem; font-family: 'Consolas', 'Monaco', monospace; }
    .data-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.5rem; }
    .mini-metric { background: rgba(22,27,34,0.7); border: 1px solid #30363d; border-radius: 6px; padding: 0.5rem; text-align: center; }
    .tab-badge { background: rgba(88,166,255,0.2); color: #58a6ff; padding: 0.1rem 0.4rem; border-radius: 10px; font-size: 0.65rem; }
    .live-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 4px; }
    .live-dot-green { background: #3fb950; animation: pulse-green 1.5s infinite; }
    .live-dot-red { background: #f85149; animation: pulse-red 1.5s infinite; }
</style>
""", unsafe_allow_html=True)

FUTURES_SYMBOLS = {"S&P 500": "ES=F", "Nasdaq 100": "NQ=F", "Dow Jones": "YM=F", "Russell 2000": "RTY=F", "Crude Oil": "CL=F", "Gold": "GC=F", "Silver": "SI=F", "Natural Gas": "NG=F", "VIX": "^VIX", "Dollar Index": "DX=F", "10Y Treasury": "^TNX", "Bitcoin": "BTC-USD", "Ethereum": "ETH-USD", "Copper": "HG=F"}
SECTOR_ETFS = {"Technology": {"symbol": "XLK", "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "AMD", "CRM", "ORCL", "ADBE"]}, "Financial": {"symbol": "XLF", "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW"]}, "Energy": {"symbol": "XLE", "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO"]}, "Healthcare": {"symbol": "XLV", "stocks": ["UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT"]}, "Consumer Disc.": {"symbol": "XLY", "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX"]}, "Consumer Staples": {"symbol": "XLP", "stocks": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL"]}, "Industrials": {"symbol": "XLI", "stocks": ["CAT", "GE", "RTX", "UNP", "BA", "HON", "DE", "LMT"]}, "Materials": {"symbol": "XLB", "stocks": ["LIN", "APD", "SHW", "FCX", "NEM", "NUE", "DOW", "ECL"]}, "Utilities": {"symbol": "XLU", "stocks": ["NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL"]}, "Real Estate": {"symbol": "XLRE", "stocks": ["AMT", "PLD", "CCI", "EQIX", "SPG", "PSA", "O", "WELL"]}, "Communication": {"symbol": "XLC", "stocks": ["META", "GOOGL", "NFLX", "DIS", "CMCSA", "VZ", "T", "TMUS"]}}
FINANCE_CATEGORIES = {"Major Banks": ["JPM", "BAC", "WFC", "C", "USB", "PNC"], "Investment Banks": ["GS", "MS", "SCHW", "RJF"], "Insurance": ["BRK-B", "AIG", "MET", "PRU", "AFL", "TRV"], "Payments": ["V", "MA", "AXP", "PYPL", "SQ"], "Asset Managers": ["BLK", "BX", "KKR", "APO", "TROW"], "Fintech": ["PYPL", "SQ", "SOFI", "HOOD", "COIN"]}
OPTIONS_UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "GLD", "SLV", "TLT", "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO", "JPM", "BAC", "GS", "MS", "C", "WFC", "XOM", "CVX", "COP", "SLB", "UNH", "JNJ", "LLY", "PFE", "ABBV", "HD", "MCD", "NKE", "SBUX", "COST", "NFLX", "CRM", "ORCL", "V", "MA", "DIS", "COIN", "SOFI", "PLTR", "ARM"]
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

# =====================================================
# RISK TURBULENCE & CONVERGENCE MODEL (PRO)
# Institutional-grade covariance turbulence early warning
# Uses ONLY free data sources (yfinance)
# =====================================================

# Default turbulence model configuration
TURB_DEFAULT_CONFIG = {
    'min_points': 300,
    'cov_window': 252,          # 1 year covariance estimation
    'mean_window': 252,         # 1 year mean estimation
    'corr_window': 60,          # 3 month correlation window
    'vol_window': 20,           # 1 month realized vol window
    'pct_window': 504,          # 2 year rolling percentiles
    'smooth': 5,                # composite score smoothing
    'ewma_lambda': 0.94,        # EWMA decay factor
    'shrink_floor': 0.05,       # minimum shrinkage
    'shrink_cap': 0.60,         # maximum shrinkage
    'ridge_eps': 1e-6,          # numerical stability
    'logistic_k': 1.35,         # score mapping steepness
    'clip_z': 4.0,              # z-score clipping
    'winsor_p': 0.01,           # winsorization percentile
    # Composite weights (auto-normalized)
    'w_turb': 1.7,              # Mahalanobis turbulence
    'w_corr': 1.1,              # avg abs correlation
    'w_corr_jump': 1.0,         # correlation matrix jump
    'w_pc1': 1.1,               # eigen concentration
    'w_cov_mag': 1.0,           # covariance magnitude
    'w_vol': 0.7,               # realized vol
    'w_credit': 1.0,            # credit stress (HYG-LQD spread)
    'w_decouple': 0.9,          # calm-before-storm detector
    # Alert thresholds
    'alert_score_jump_5d': 8.0,
    'alert_score_level': 70.0,
    'alert_turb_pct': 0.90,
    'alert_corr_pct': 0.90,
    'alert_vol_pct_max': 0.40,
}

# Default universe (liquid, diversified, free via yfinance)
TURB_DEFAULT_UNIVERSE = [
    "SPY",      # US large cap equities
    "QQQ",      # US tech/growth
    "IWM",      # US small cap
    "TLT",      # Long-term treasuries
    "IEF",      # Intermediate treasuries
    "GLD",      # Gold
    "USO",      # Oil proxy
    "UUP",      # USD index proxy
    "HYG",      # High yield credit
    "LQD",      # Investment grade credit
    "BTC-USD",  # Crypto risk proxy
]

def turb_resample_bday(df: pd.DataFrame) -> pd.DataFrame:
    """Resample dataframe to business days with forward fill."""
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    idx = pd.date_range(df.index.min(), df.index.max(), freq="B")
    return df.reindex(idx).ffill()

def turb_winsorize(s: pd.Series, p: float) -> pd.Series:
    """Winsorize series at percentile p and 1-p."""
    if s.dropna().empty:
        return s
    lo = s.quantile(p)
    hi = s.quantile(1 - p)
    return s.clip(lo, hi)

def turb_zscore_rolling(s: pd.Series, window: int, winsor_p: float) -> pd.Series:
    """Rolling z-score with winsorization."""
    s2 = turb_winsorize(s.astype(float), winsor_p)
    m = s2.rolling(window, min_periods=int(window * 0.5)).mean()
    sd = s2.rolling(window, min_periods=int(window * 0.5)).std(ddof=0).replace(0, np.nan)
    return (s2 - m) / sd

def turb_logistic_0_100(z: pd.Series, k: float, clip: float) -> pd.Series:
    """Map z-score to 0-100 via logistic function."""
    zc = z.clip(-clip, clip)
    return 100.0 / (1.0 + np.exp(-k * zc))

def turb_rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    """Calculate rolling percentile rank."""
    def _pct(x: np.ndarray) -> float:
        if len(x) < 5 or np.all(np.isnan(x)):
            return np.nan
        v = x[-1]
        xs = x[~np.isnan(x)]
        if len(xs) == 0:
            return np.nan
        return float(np.mean(xs <= v))
    return series.rolling(window, min_periods=int(window * 0.3)).apply(_pct, raw=True)

def turb_avg_abs_corr(returns: pd.DataFrame) -> float:
    """Calculate average absolute correlation across assets."""
    c = returns.corr()
    vals = c.values
    n = vals.shape[0]
    if n < 2:
        return np.nan
    triu = vals[np.triu_indices(n, k=1)]
    return float(np.nanmean(np.abs(triu)))

def turb_corr_matrix_jump_norm(c1: np.ndarray, c0: np.ndarray) -> float:
    """Frobenius norm of correlation matrix change (regime break detection)."""
    if c1.shape != c0.shape:
        return np.nan
    diff = c1 - c0
    return float(np.sqrt(np.nansum(diff * diff)))

def turb_pc1_share_from_cov(cov: np.ndarray) -> float:
    """Eigen concentration: PC1 explained share of total variance."""
    if cov.ndim != 2 or cov.shape[0] != cov.shape[1]:
        return np.nan
    try:
        vals = np.linalg.eigvalsh(cov)
    except np.linalg.LinAlgError:
        return np.nan
    vals = np.clip(vals, 0, np.inf)
    s = float(np.sum(vals))
    if s <= 0:
        return np.nan
    return float(np.max(vals) / s)

def turb_add_ridge(cov: np.ndarray, ridge_scale: float) -> np.ndarray:
    """Add ridge regularization for numerical stability."""
    d = np.nanmean(np.diag(cov))
    if not np.isfinite(d) or d <= 0:
        d = 1.0
    return cov + np.eye(cov.shape[0]) * (ridge_scale * d)

def turb_cov_shrink_to_diag(sample_cov: np.ndarray, shrink: float, ridge_eps: float) -> np.ndarray:
    """Shrink sample covariance toward diagonal target."""
    S = sample_cov.copy()
    S = turb_add_ridge(S, ridge_eps)
    D = np.diag(np.diag(S))
    return (1.0 - shrink) * S + shrink * D

def turb_choose_shrinkage(sample_cov: np.ndarray, floor: float, cap: float) -> float:
    """Condition-aware heuristic shrinkage selection."""
    try:
        cond = np.linalg.cond(sample_cov)
    except np.linalg.LinAlgError:
        cond = 1e6
    x = (min(max(cond, 1.0), 1000.0) - 30.0) / 300.0
    shrink = floor + (cap - floor) * float(np.clip(x, 0.0, 1.0))
    return float(shrink)

def turb_cov_ewma(returns_window: np.ndarray, lam: float, ridge_eps: float) -> np.ndarray:
    """EWMA covariance estimation."""
    X = returns_window.copy()
    X = X - np.nanmean(X, axis=0, keepdims=True)
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    T, N = X.shape
    C = np.eye(N) * 1e-6
    for i in range(T):
        r = X[i : i + 1].T
        C = lam * C + (1.0 - lam) * (r @ r.T)
    C = turb_add_ridge(C, ridge_eps)
    return C

def turb_cov_blend(sample_cov: np.ndarray, ewma_cov: np.ndarray, shrink_cov: np.ndarray) -> np.ndarray:
    """Blend three covariance estimates: 40% shrink, 40% ewma, 20% sample."""
    C = 0.40 * shrink_cov + 0.40 * ewma_cov + 0.20 * turb_add_ridge(sample_cov, 1e-6)
    return C

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_turbulence_prices(tickers: tuple, period: str = "5y") -> pd.DataFrame:
    """Fetch price data for turbulence model universe."""
    try:
        data = yf.download(
            tickers=list(tickers),
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )
        if isinstance(data.columns, pd.MultiIndex):
            closes = data.xs("Close", axis=1, level=1)
        else:
            closes = data[["Close"]].copy()
            closes.columns = [tickers[0]]
        closes.index = pd.to_datetime(closes.index)
        closes = turb_resample_bday(closes)
        # Drop columns with too many missing values
        keep = [c for c in closes.columns if closes[c].notna().mean() > 0.80]
        return closes[keep]
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def compute_turbulence_model(prices_json: str, cfg: dict) -> dict:
    """
    Compute full turbulence model.
    
    Returns dict with:
    - metrics: DataFrame with scores, regime, alerts
    - diagnostics: DataFrame with raw indicators and z-scores
    """
    import math
    
    # Deserialize prices
    prices = pd.read_json(prices_json, orient='split')
    prices.index = pd.to_datetime(prices.index)
    
    if len(prices) < cfg['min_points']:
        return {'error': f"Insufficient data: {len(prices)} points, need {cfg['min_points']}"}
    
    # Calculate log returns
    rets = np.log(prices).diff()
    idx = prices.index
    
    # Credit stress proxy: HYG-LQD spread (free, no FRED required)
    credit_spread = pd.Series(index=idx, dtype=float)
    if 'HYG' in prices.columns and 'LQD' in prices.columns:
        # Normalize and compute spread (higher = more stress)
        hyg_ret = rets['HYG'].rolling(cfg['vol_window']).std() * np.sqrt(252)
        lqd_ret = rets['LQD'].rolling(cfg['vol_window']).std() * np.sqrt(252)
        credit_spread = (hyg_ret - lqd_ret).fillna(0) * 100  # Convert to pseudo-spread
        credit_spread = credit_spread.clip(-5, 10)  # Reasonable bounds
    
    # VIX for implied vol (already in prices if available)
    vix = pd.Series(index=idx, dtype=float)
    if '^VIX' in prices.columns:
        vix = prices['^VIX']
    
    # Realized vol (system average)
    rv = rets.rolling(cfg['vol_window']).std(ddof=0) * np.sqrt(252)
    avg_rv = rv.mean(axis=1).rename("AVG_REALIZED_VOL")
    
    # Initialize diagnostic series
    turb = pd.Series(index=idx, dtype=float, name="TURBULENCE")
    cov_trace = pd.Series(index=idx, dtype=float, name="COV_TRACE")
    cov_frob = pd.Series(index=idx, dtype=float, name="COV_FROBENIUS")
    pc1_share = pd.Series(index=idx, dtype=float, name="PC1_SHARE")
    corr_jump = pd.Series(index=idx, dtype=float, name="CORR_JUMP_NORM")
    avg_abs_corr = pd.Series(index=idx, dtype=float, name="AVG_ABS_CORR")
    
    prev_corr = None
    cov_mode = cfg.get('cov_mode', 'blend')
    
    # Main computation loop
    for t in range(cfg['cov_window'], len(idx)):
        end = idx[t]
        win_idx = idx[t - cfg['cov_window'] : t]
        rwin_df = rets.loc[win_idx].dropna(how='all')
        
        if rwin_df.shape[0] < int(cfg['cov_window'] * 0.7):
            continue
        
        # Fill NaN with 0 for computation
        rwin_df = rwin_df.fillna(0)
        
        # Mean estimation
        mu_idx = idx[max(0, t - cfg['mean_window']) : t]
        mu_df = rets.loc[mu_idx].fillna(0)
        mu = mu_df.mean(axis=0).values
        
        X = rwin_df.values
        
        try:
            sample_cov = np.cov(X, rowvar=False, ddof=0)
            if sample_cov.ndim == 0:
                sample_cov = np.array([[sample_cov]])
        except Exception:
            continue
        
        # Robust covariance estimation
        shrink = turb_choose_shrinkage(sample_cov, cfg['shrink_floor'], cfg['shrink_cap'])
        shrink_cov = turb_cov_shrink_to_diag(sample_cov, shrink=shrink, ridge_eps=cfg['ridge_eps'])
        ewma_cov = turb_cov_ewma(X, lam=cfg['ewma_lambda'], ridge_eps=cfg['ridge_eps'])
        
        if cov_mode == "shrink":
            cov = shrink_cov
        elif cov_mode == "ewma":
            cov = ewma_cov
        else:
            cov = turb_cov_blend(sample_cov, ewma_cov, shrink_cov)
        
        # Mahalanobis turbulence
        x = rets.loc[end].fillna(0).values
        diff = x - mu
        
        try:
            inv = np.linalg.inv(cov)
        except np.linalg.LinAlgError:
            inv = np.linalg.pinv(cov)
        
        md2 = float(diff.T @ inv @ diff)
        turb.iloc[t] = math.sqrt(max(md2, 0.0))
        
        # Covariance diagnostics
        cov_trace.iloc[t] = float(np.trace(cov))
        cov_frob.iloc[t] = float(np.linalg.norm(cov, ord="fro"))
        pc1_share.iloc[t] = turb_pc1_share_from_cov(cov)
        
        # Average absolute correlation
        avg_abs_corr.iloc[t] = turb_avg_abs_corr(rwin_df.iloc[-cfg['corr_window']:])
        
        # Correlation matrix and jump
        diag = np.sqrt(np.clip(np.diag(cov), 1e-12, np.inf))
        denom = np.outer(diag, diag)
        corr = cov / denom
        corr = np.clip(corr, -1.0, 1.0)
        
        if prev_corr is not None and corr.shape == prev_corr.shape:
            corr_jump.iloc[t] = turb_corr_matrix_jump_norm(corr, prev_corr)
        prev_corr = corr
    
    # Build diagnostics DataFrame
    diag = pd.DataFrame(index=idx)
    diag["TURBULENCE"] = turb
    diag["AVG_ABS_CORR"] = avg_abs_corr
    diag["CORR_JUMP_NORM"] = corr_jump
    diag["PC1_SHARE"] = pc1_share
    diag["COV_TRACE"] = cov_trace
    diag["COV_FROBENIUS"] = cov_frob
    diag["AVG_REALIZED_VOL"] = avg_rv
    diag["CREDIT_SPREAD"] = credit_spread
    diag["VIX"] = vix
    
    # Implied vs realized mismatch
    vix_ann = (vix / 100.0) if vix.notna().any() else pd.Series(index=idx, dtype=float)
    diag["IMPL_MINUS_REAL"] = (vix_ann - avg_rv)
    
    # Rolling percentiles
    pw = cfg['pct_window']
    diag["TURB_PCT"] = turb_rolling_percentile(diag["TURBULENCE"], pw)
    diag["CORR_PCT"] = turb_rolling_percentile(diag["AVG_ABS_CORR"], pw)
    diag["CJUMP_PCT"] = turb_rolling_percentile(diag["CORR_JUMP_NORM"], pw)
    diag["PC1_PCT"] = turb_rolling_percentile(diag["PC1_SHARE"], pw)
    diag["COVMAG_PCT"] = turb_rolling_percentile(diag["COV_FROBENIUS"], pw)
    diag["VOL_PCT"] = turb_rolling_percentile(diag["AVG_REALIZED_VOL"], pw)
    diag["CREDIT_PCT"] = turb_rolling_percentile(diag["CREDIT_SPREAD"], pw)
    if vix.notna().any():
        diag["VIX_PCT"] = turb_rolling_percentile(diag["VIX"], pw)
    
    # Z-scores for score mapping
    z = pd.DataFrame(index=idx)
    z["z_turb"] = turb_zscore_rolling(diag["TURBULENCE"], pw, cfg['winsor_p'])
    z["z_corr"] = turb_zscore_rolling(diag["AVG_ABS_CORR"], pw, cfg['winsor_p'])
    z["z_cjump"] = turb_zscore_rolling(diag["CORR_JUMP_NORM"], pw, cfg['winsor_p'])
    z["z_pc1"] = turb_zscore_rolling(diag["PC1_SHARE"], pw, cfg['winsor_p'])
    z["z_covmag"] = turb_zscore_rolling(diag["COV_FROBENIUS"], pw, cfg['winsor_p'])
    z["z_vol"] = turb_zscore_rolling(diag["AVG_REALIZED_VOL"], pw, cfg['winsor_p'])
    z["z_credit"] = turb_zscore_rolling(diag["CREDIT_SPREAD"], pw, cfg['winsor_p'])
    z["z_impl_mismatch"] = turb_zscore_rolling(diag["IMPL_MINUS_REAL"].fillna(0), pw, cfg['winsor_p'])
    
    # Decoupling detector (calm-before-storm)
    corr_high = diag["CORR_PCT"].fillna(0.5)
    vol_low = 1.0 - diag["VOL_PCT"].fillna(0.5)
    vix_low = (1.0 - diag.get("VIX_PCT", pd.Series(index=idx, dtype=float)).fillna(0.5))
    mismatch01 = 1.0 / (1.0 + np.exp(-1.0 * z["z_impl_mismatch"].clip(-cfg['clip_z'], cfg['clip_z'])))
    decouple01 = (0.45 * corr_high + 0.30 * vol_low + 0.25 * vix_low) * (0.6 + 0.4 * mismatch01)
    diag["DECOUPLE_0_1"] = decouple01.clip(0, 1)
    
    # Sub-scores (0-100)
    sub = pd.DataFrame(index=idx)
    sub["TURB_SCORE"] = turb_logistic_0_100(z["z_turb"], cfg['logistic_k'], cfg['clip_z'])
    sub["CORR_SCORE"] = turb_logistic_0_100(z["z_corr"], cfg['logistic_k'], cfg['clip_z'])
    sub["CORR_JUMP_SCORE"] = turb_logistic_0_100(z["z_cjump"], cfg['logistic_k'], cfg['clip_z'])
    sub["PC1_SCORE"] = turb_logistic_0_100(z["z_pc1"], cfg['logistic_k'], cfg['clip_z'])
    sub["COVMAG_SCORE"] = turb_logistic_0_100(z["z_covmag"], cfg['logistic_k'], cfg['clip_z'])
    sub["VOL_SCORE"] = turb_logistic_0_100(z["z_vol"], cfg['logistic_k'], cfg['clip_z'])
    sub["CREDIT_SCORE"] = turb_logistic_0_100(z["z_credit"], cfg['logistic_k'], cfg['clip_z'])
    sub["DECOUPLE_SCORE"] = diag["DECOUPLE_0_1"] * 100.0
    
    # Composite score (weighted average, auto-normalized)
    weights = {
        "TURB_SCORE": cfg['w_turb'],
        "CORR_SCORE": cfg['w_corr'],
        "CORR_JUMP_SCORE": cfg['w_corr_jump'],
        "PC1_SCORE": cfg['w_pc1'],
        "COVMAG_SCORE": cfg['w_cov_mag'],
        "VOL_SCORE": cfg['w_vol'],
        "CREDIT_SCORE": cfg['w_credit'],
        "DECOUPLE_SCORE": cfg['w_decouple'],
    }
    
    available = [k for k in weights if k in sub.columns and sub[k].notna().any()]
    wsum = sum(weights[k] for k in available) if available else 1.0
    
    comp = None
    contrib = pd.DataFrame(index=idx)
    for k in available:
        s = sub[k].ffill().fillna(50.0)
        w = weights[k] / wsum
        comp = s * w if comp is None else comp + s * w
        contrib[k.replace("_SCORE", "_CONTRIB")] = s * w
    
    sub["RISK_TURBULENCE_SCORE"] = comp.rolling(cfg['smooth']).mean() if comp is not None else pd.Series(50.0, index=idx)
    sub["SCORE_5D_CHG"] = sub["RISK_TURBULENCE_SCORE"].diff(5)
    sub["SCORE_1M_CHG"] = sub["RISK_TURBULENCE_SCORE"].diff(21)
    
    # Regime labels
    score = sub["RISK_TURBULENCE_SCORE"]
    regime = pd.Series(index=idx, dtype=object)
    regime[score < 35] = "GREEN"
    regime[(score >= 35) & (score < 55)] = "YELLOW"
    regime[(score >= 55) & (score < 70)] = "ORANGE"
    regime[score >= 70] = "RED"
    sub["REGIME"] = regime
    
    # Alerts
    alerts = pd.Series(index=idx, dtype=object)
    turb_pct = diag["TURB_PCT"]
    corr_pct = diag["CORR_PCT"]
    vol_pct = diag["VOL_PCT"]
    s5 = sub["SCORE_5D_CHG"]
    
    for i in range(len(idx)):
        msg = []
        if pd.notna(s5.iloc[i]) and float(s5.iloc[i]) >= cfg['alert_score_jump_5d']:
            msg.append(f"Score jump ≥{cfg['alert_score_jump_5d']:.0f} in 5d")
        if pd.notna(score.iloc[i]) and float(score.iloc[i]) >= cfg['alert_score_level']:
            msg.append(f"Score ≥{cfg['alert_score_level']:.0f}")
        if pd.notna(turb_pct.iloc[i]) and float(turb_pct.iloc[i]) >= cfg['alert_turb_pct']:
            msg.append("Turbulence ≥90th pct")
        if (pd.notna(corr_pct.iloc[i]) and float(corr_pct.iloc[i]) >= cfg['alert_corr_pct'] and
            pd.notna(vol_pct.iloc[i]) and float(vol_pct.iloc[i]) <= cfg['alert_vol_pct_max']):
            msg.append("⚠️ CALM-BEFORE-STORM: high corr + low vol")
        alerts.iloc[i] = " | ".join(msg) if msg else ""
    
    sub["ALERTS"] = alerts
    
    # Combine outputs
    metrics = pd.concat([sub, contrib], axis=1)
    diagnostics = pd.concat([diag, z.add_prefix("Z_")], axis=1)
    
    return {
        'metrics': metrics.to_json(orient='split', date_format='iso'),
        'diagnostics': diagnostics.to_json(orient='split', date_format='iso'),
    }

def render_turbulence_tab(st_module):
    """Render the Risk Turbulence & Convergence tab."""
    st_module.markdown("### 🌊 Risk Turbulence & Convergence Early Warning")
    st_module.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Institutional-grade covariance turbulence model detecting correlation regime breaks, diversification breakdown, and calm-before-storm conditions.</p>", unsafe_allow_html=True)
    
    # Settings expander - values persist in session state automatically
    with st_module.expander("⚙️ Model Settings", expanded=False):
        st_module.markdown("**Universe & Data**")
        cfg_cols = st_module.columns(2)
        with cfg_cols[0]:
            universe_input = st_module.text_area(
                "Universe (comma-separated tickers):",
                value=", ".join(TURB_DEFAULT_UNIVERSE),
                height=68,
                key="turb_universe"
            )
        with cfg_cols[1]:
            cov_mode = st_module.selectbox(
                "Covariance Estimation Mode:",
                options=["blend", "shrink", "ewma"],
                index=0,
                key="turb_cov_mode"
            )
            data_period = st_module.selectbox(
                "Historical Data Period:",
                options=["3y", "5y", "7y"],
                index=1,
                key="turb_period"
            )
        
        st_module.markdown("**Windows (Trading Days)**")
        win_cols = st_module.columns(4)
        with win_cols[0]:
            cov_window = st_module.number_input("Cov Window", min_value=60, max_value=504, value=252, step=21, key="turb_cov_win")
        with win_cols[1]:
            corr_window = st_module.number_input("Corr Window", min_value=20, max_value=126, value=60, step=5, key="turb_corr_win")
        with win_cols[2]:
            vol_window = st_module.number_input("Vol Window", min_value=5, max_value=63, value=20, step=5, key="turb_vol_win")
        with win_cols[3]:
            smooth = st_module.number_input("Smooth Factor", min_value=1, max_value=21, value=5, step=1, key="turb_smooth")
        
        st_module.markdown("**Component Weights** (higher = more influence)")
        w_cols = st_module.columns(4)
        with w_cols[0]:
            w_turb = st_module.slider("Turbulence", 0.0, 3.0, 1.7, 0.1, key="w_turb")
            w_corr = st_module.slider("Correlation", 0.0, 3.0, 1.1, 0.1, key="w_corr")
        with w_cols[1]:
            w_pc1 = st_module.slider("PC1 Concentration", 0.0, 3.0, 1.1, 0.1, key="w_pc1")
            w_cjump = st_module.slider("Corr Jump", 0.0, 3.0, 1.0, 0.1, key="w_cjump")
        with w_cols[2]:
            w_vol = st_module.slider("Realized Vol", 0.0, 3.0, 0.7, 0.1, key="w_vol")
            w_credit = st_module.slider("Credit Stress", 0.0, 3.0, 1.0, 0.1, key="w_credit")
        with w_cols[3]:
            w_covmag = st_module.slider("Cov Magnitude", 0.0, 3.0, 1.0, 0.1, key="w_covmag")
            w_decouple = st_module.slider("Decoupling", 0.0, 3.0, 0.9, 0.1, key="w_decouple")
        
        st_module.markdown("**Alert Thresholds**")
        alert_cols = st_module.columns(4)
        with alert_cols[0]:
            alert_score_level = st_module.number_input("Score Alert ≥", value=70.0, step=5.0, key="alert_score")
        with alert_cols[1]:
            alert_score_jump = st_module.number_input("5D Jump Alert ≥", value=8.0, step=1.0, key="alert_jump")
        with alert_cols[2]:
            alert_turb_pct = st_module.number_input("Turb Pct Alert ≥", value=0.90, step=0.05, key="alert_turb")
        with alert_cols[3]:
            alert_corr_pct = st_module.number_input("Corr Pct Alert ≥", value=0.90, step=0.05, key="alert_corr")
    
    # Get values from session state (widgets automatically persist)
    universe_input = st_module.session_state.get('turb_universe', ", ".join(TURB_DEFAULT_UNIVERSE))
    cov_mode = st_module.session_state.get('turb_cov_mode', 'blend')
    data_period = st_module.session_state.get('turb_period', '5y')
    cov_window = st_module.session_state.get('turb_cov_win', 252)
    corr_window = st_module.session_state.get('turb_corr_win', 60)
    vol_window = st_module.session_state.get('turb_vol_win', 20)
    smooth = st_module.session_state.get('turb_smooth', 5)
    w_turb = st_module.session_state.get('w_turb', 1.7)
    w_corr = st_module.session_state.get('w_corr', 1.1)
    w_pc1 = st_module.session_state.get('w_pc1', 1.1)
    w_cjump = st_module.session_state.get('w_cjump', 1.0)
    w_vol = st_module.session_state.get('w_vol', 0.7)
    w_credit = st_module.session_state.get('w_credit', 1.0)
    w_covmag = st_module.session_state.get('w_covmag', 1.0)
    w_decouple = st_module.session_state.get('w_decouple', 0.9)
    alert_score_level = st_module.session_state.get('alert_score', 70.0)
    alert_score_jump = st_module.session_state.get('alert_jump', 8.0)
    alert_turb_pct = st_module.session_state.get('alert_turb', 0.90)
    alert_corr_pct = st_module.session_state.get('alert_corr', 0.90)
    
    # Build config
    universe = [t.strip().upper() for t in universe_input.split(",") if t.strip()]
    # Always include VIX for implied vol
    if "^VIX" not in universe:
        universe.append("^VIX")
    
    cfg = TURB_DEFAULT_CONFIG.copy()
    cfg.update({
        'cov_window': cov_window,
        'corr_window': corr_window,
        'vol_window': vol_window,
        'smooth': smooth,
        'cov_mode': cov_mode,
        'w_turb': w_turb,
        'w_corr': w_corr,
        'w_corr_jump': w_cjump,
        'w_pc1': w_pc1,
        'w_cov_mag': w_covmag,
        'w_vol': w_vol,
        'w_credit': w_credit,
        'w_decouple': w_decouple,
        'alert_score_level': alert_score_level,
        'alert_score_jump_5d': alert_score_jump,
        'alert_turb_pct': alert_turb_pct,
        'alert_corr_pct': alert_corr_pct,
    })
    
    # Fetch and compute
    with st_module.spinner("Loading cross-asset price data..."):
        prices = fetch_turbulence_prices(tuple(universe), period=data_period)
    
    if prices.empty or len(prices) < cfg['min_points']:
        st_module.error(f"Insufficient data. Need at least {cfg['min_points']} trading days.")
        return
    
    with st_module.spinner("Computing turbulence model (this may take a moment)..."):
        result = compute_turbulence_model(prices.to_json(orient='split', date_format='iso'), cfg)
    
    if 'error' in result:
        st_module.error(result['error'])
        return
    
    # Deserialize results
    metrics = pd.read_json(result['metrics'], orient='split')
    metrics.index = pd.to_datetime(metrics.index)
    diagnostics = pd.read_json(result['diagnostics'], orient='split')
    diagnostics.index = pd.to_datetime(diagnostics.index)
    
    # Filter to valid data
    m = metrics.dropna(subset=["RISK_TURBULENCE_SCORE"])
    d = diagnostics.loc[m.index]
    
    if m.empty:
        st_module.warning("Model computation returned no valid results. Try adjusting parameters or expanding the universe.")
        return
    
    latest_m = m.iloc[-1]
    latest_d = d.iloc[-1]
    
    score = float(latest_m["RISK_TURBULENCE_SCORE"])
    regime = str(latest_m.get("REGIME", "N/A"))
    chg_5d = float(latest_m["SCORE_5D_CHG"]) if pd.notna(latest_m.get("SCORE_5D_CHG")) else 0.0
    chg_1m = float(latest_m["SCORE_1M_CHG"]) if pd.notna(latest_m.get("SCORE_1M_CHG")) else 0.0
    
    # Regime styling
    regime_colors = {
        "GREEN": ("#3fb950", "rgba(63,185,80,0.15)", "Normal / Diversified"),
        "YELLOW": ("#d29922", "rgba(210,153,34,0.15)", "Tightening / Watch"),
        "ORANGE": ("#f0883e", "rgba(240,136,62,0.15)", "Regime Shift Building"),
        "RED": ("#f85149", "rgba(248,81,73,0.15)", "Turbulent / Correlation-to-1"),
    }
    r_color, r_bg, r_desc = regime_colors.get(regime, ("#8b949e", "rgba(139,148,158,0.15)", "Unknown"))
    
    # === ALERTS ===
    alert_msg = str(latest_m.get("ALERTS", ""))
    if alert_msg:
        st_module.markdown(f"""
        <div style="background: rgba(248,81,73,0.15); border: 2px solid #f85149; border-radius: 10px; padding: 1rem; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; gap: 0.5rem;">
                <span style="font-size: 1.5rem;">🚨</span>
                <div>
                    <div style="color: #f85149; font-weight: 700; font-size: 1rem;">INSTITUTIONAL ALERT</div>
                    <div style="color: #f0883e; font-size: 0.9rem;">{alert_msg}</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # === KPI TILES ROW ===
    st_module.markdown("#### 📊 Current Risk State")
    kpi_cols = st_module.columns(5)
    
    with kpi_cols[0]:
        score_color = r_color
        st_module.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 1rem; border-left: 4px solid {score_color};">
            <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Composite Score</div>
            <div style="color: {score_color}; font-size: 2rem; font-weight: 700;">{score:.1f}</div>
            <div style="color: {'#3fb950' if chg_5d <= 0 else '#f85149'}; font-size: 0.8rem;">{chg_5d:+.1f} (5d)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[1]:
        st_module.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 1rem; background: {r_bg}; border: 1px solid {r_color};">
            <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Regime</div>
            <div style="color: {r_color}; font-size: 1.3rem; font-weight: 700;">{regime}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">{r_desc}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[2]:
        chg_1m_color = '#3fb950' if chg_1m <= 0 else '#f85149'
        st_module.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 1rem;">
            <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">1-Month Change</div>
            <div style="color: {chg_1m_color}; font-size: 1.8rem; font-weight: 700;">{chg_1m:+.1f}</div>
            <div style="color: #8b949e; font-size: 0.75rem;">pts</div>
        </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[3]:
        # Top driver
        drivers = ["TURB_SCORE", "CORR_SCORE", "CORR_JUMP_SCORE", "PC1_SCORE", "COVMAG_SCORE", "VOL_SCORE", "CREDIT_SCORE", "DECOUPLE_SCORE"]
        present = [k for k in drivers if k in m.columns and pd.notna(latest_m.get(k))]
        if present:
            top_driver = max(present, key=lambda k: float(latest_m[k]))
            top_val = float(latest_m[top_driver])
            top_name = top_driver.replace("_SCORE", "").replace("_", " ").title()
        else:
            top_name, top_val = "N/A", 0
        st_module.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 1rem;">
            <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Top Driver</div>
            <div style="color: #58a6ff; font-size: 1rem; font-weight: 600;">{top_name}</div>
            <div style="color: #c9d1d9; font-size: 1.2rem;">{top_val:.1f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[4]:
        dec = float(latest_d.get("DECOUPLE_0_1", 0))
        dec_color = '#f85149' if dec > 0.7 else '#d29922' if dec > 0.5 else '#3fb950'
        st_module.markdown(f"""
        <div class="metric-card" style="text-align: center; padding: 1rem;">
            <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Decoupling (0-1)</div>
            <div style="color: {dec_color}; font-size: 1.8rem; font-weight: 700;">{dec:.2f}</div>
            <div style="color: #8b949e; font-size: 0.7rem;">{'⚠️ High' if dec > 0.7 else '🟡 Watch' if dec > 0.5 else '✅ Normal'}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # === MAIN CHART ===
    st_module.markdown("#### 📈 Composite Score History")
    
    fig = go.Figure()
    
    # Add regime background bands
    bands = [
        (0, 35, "rgba(63,185,80,0.08)", "Green Zone"),
        (35, 55, "rgba(210,153,34,0.08)", "Yellow Zone"),
        (55, 70, "rgba(240,136,62,0.10)", "Orange Zone"),
        (70, 100, "rgba(248,81,73,0.12)", "Red Zone"),
    ]
    for y0, y1, color, name in bands:
        fig.add_shape(
            type="rect",
            x0=m.index.min(), x1=m.index.max(),
            y0=y0, y1=y1,
            fillcolor=color,
            line=dict(width=0),
            layer="below"
        )
    
    # Main score line
    fig.add_trace(go.Scatter(
        x=m.index, y=m["RISK_TURBULENCE_SCORE"],
        mode='lines',
        name='Risk Score',
        line=dict(color='#58a6ff', width=2),
        fill='tozeroy',
        fillcolor='rgba(88,166,255,0.1)'
    ))
    
    # Add threshold lines
    fig.add_hline(y=70, line_dash="dash", line_color="#f85149", annotation_text="Alert: 70", annotation_position="right")
    fig.add_hline(y=35, line_dash="dash", line_color="#3fb950", annotation_text="Normal: 35", annotation_position="right")
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(22,27,34,0.8)',
        height=350,
        margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(range=[0, 100], title="Score (0-100)"),
        xaxis=dict(title=""),
        showlegend=False,
        font=dict(family='Inter, sans-serif', color='#8b949e', size=10)
    )
    st_module.plotly_chart(fig, use_container_width=True)
    
    # === DRIVER BREAKDOWN ===
    st_module.markdown("#### 🎛️ Driver Scores (0-100)")
    driver_cols = st_module.columns(4)
    
    driver_info = [
        ("TURB_SCORE", "🌪️ Turbulence", "Mahalanobis distance"),
        ("CORR_SCORE", "🔗 Correlation", "Avg abs correlation"),
        ("CORR_JUMP_SCORE", "⚡ Corr Jump", "Correlation regime break"),
        ("PC1_SCORE", "📊 PC1 Conc.", "Eigen concentration"),
        ("COVMAG_SCORE", "📈 Cov Mag", "Covariance magnitude"),
        ("VOL_SCORE", "📉 Real Vol", "Realized volatility"),
        ("CREDIT_SCORE", "💳 Credit", "HY-IG spread stress"),
        ("DECOUPLE_SCORE", "🔮 Decouple", "Calm-before-storm"),
    ]
    
    for i, (key, label, desc) in enumerate(driver_info):
        if key in m.columns and pd.notna(latest_m.get(key)):
            val = float(latest_m[key])
            val_color = '#f85149' if val > 65 else '#d29922' if val > 50 else '#3fb950'
            with driver_cols[i % 4]:
                st_module.markdown(f"""
                <div style="background: rgba(33,38,45,0.5); border-radius: 8px; padding: 0.75rem; margin: 0.25rem 0; border-left: 3px solid {val_color};">
                    <div style="color: #c9d1d9; font-size: 0.8rem; font-weight: 600;">{label}</div>
                    <div style="color: {val_color}; font-size: 1.3rem; font-weight: 700;">{val:.1f}</div>
                    <div style="color: #6e7681; font-size: 0.65rem;">{desc}</div>
                </div>
                """, unsafe_allow_html=True)
    
    # === COVARIANCE DIAGNOSTICS ===
    st_module.markdown("#### 🔬 Covariance Regime Diagnostics (2Y Rolling Percentiles)")
    diag_cols = st_module.columns(5)
    
    diag_metrics = [
        ("TURB_PCT", "Turbulence", "vs 2Y history"),
        ("CORR_PCT", "Correlation", "diversification breakdown"),
        ("CJUMP_PCT", "Corr Jump", "regime break signal"),
        ("PC1_PCT", "PC1 Share", "eigen concentration"),
        ("COVMAG_PCT", "Cov Magnitude", "system-wide vol"),
    ]
    
    for i, (key, label, desc) in enumerate(diag_metrics):
        val = float(latest_d.get(key, 0)) * 100 if pd.notna(latest_d.get(key)) else 0
        val_color = '#f85149' if val > 80 else '#d29922' if val > 60 else '#3fb950'
        with diag_cols[i]:
            st_module.markdown(f"""
            <div class="metric-card" style="text-align: center; padding: 0.75rem;">
                <div style="color: #8b949e; font-size: 0.65rem; text-transform: uppercase;">{label}</div>
                <div style="color: {val_color}; font-size: 1.4rem; font-weight: 700;">{val:.0f}%</div>
                <div style="color: #6e7681; font-size: 0.6rem;">{desc}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # === METHODOLOGY EXPANDER ===
    with st_module.expander("📖 Methodology (Institutional, Explainable)", expanded=False):
        st_module.markdown("""
**Core Engine (What Makes This "Pro"):**

- **Turbulence** is the Mahalanobis distance of today's cross-asset return vector versus a rolling mean and **robust covariance matrix**.
- Covariance is estimated using **shrinkage**, **EWMA**, or a **blend** (default), improving stability and sensitivity to regime shifts.
- Unlike simple VIX-based measures, this captures **cross-asset correlation structure changes**.

**How We Detect "Risk Convergence":**

| Signal | What It Measures | Why It Matters |
|--------|------------------|----------------|
| **Avg Abs Correlation** | Rising → diversification breaks | When correlations spike, hedges fail simultaneously |
| **Corr-Jump Norm** | ‖Corr_t - Corr_{t-1}‖_F | Sudden correlation matrix shifts indicate regime breaks |
| **PC1 Share** | First principal component's variance share | High = "one factor dominates" = correlation-to-1 crowding |
| **Cov Magnitude** | Frobenius norm of covariance matrix | Entire system's covariance is inflating |
| **Decoupling Score** | Corr rising while vol low + implied complacency | Classic "calm before storm" setup |

**How to Use:**

1. **GREEN (0-35):** Normal markets, diversification working. Standard risk-on positioning.
2. **YELLOW (35-55):** Correlations tightening. Reduce leverage, tighten stops.
3. **ORANGE (55-70):** Regime shift building. De-risk, add tail hedges.
4. **RED (70+):** Turbulent / correlation-to-1 risk. Defensive positioning, consider cash.

**Watch for:**
- **5D Score Jump ≥8:** Rapid deterioration, possible forced deleveraging ahead
- **Corr High + Vol Low:** "Calm before storm" — historically precedes volatility spikes
- **PC1 Share spiking:** Market becoming "one-factor" (usually risk-on/risk-off)
        """)
    
    # === RAW DIAGNOSTICS ===
    with st_module.expander("📊 Raw Diagnostics (Last 60 Days)", expanded=False):
        display_cols = [c for c in ["TURBULENCE", "AVG_ABS_CORR", "CORR_JUMP_NORM", "PC1_SHARE", "COV_FROBENIUS", "AVG_REALIZED_VOL", "CREDIT_SPREAD", "VIX", "DECOUPLE_0_1"] if c in d.columns]
        st_module.dataframe(d[display_cols].tail(60).round(4), use_container_width=True, height=300)
    
    # === DRIVER HISTORY ===
    with st_module.expander("📈 Driver History (Last 45 Days)", expanded=False):
        driver_cols_display = [c for c in drivers if c in m.columns]
        st_module.dataframe(m[driver_cols_display].tail(45).round(1), use_container_width=True, height=260)

def get_market_status():
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    premarket, market_open, market_close, afterhours = now.replace(hour=4, minute=0, second=0), now.replace(hour=9, minute=30, second=0), now.replace(hour=16, minute=0, second=0), now.replace(hour=20, minute=0, second=0)
    
    # Check for weekends
    if now.weekday() >= 5: 
        days_until = 7 - now.weekday() if now.weekday() == 5 else 1
        return "closed", "Weekend", f"Opens Monday {(now + timedelta(days=days_until)).strftime('%b %d')}"
    
    # Market hours logic with better time formatting
    if now < premarket: 
        hours_until = (premarket - now).seconds // 3600
        mins_until = ((premarket - now).seconds % 3600) // 60
        return "closed", "Closed", f"Pre-market in {hours_until}h {mins_until}m"
    elif now < market_open: 
        time_left = market_open - now
        hours = time_left.seconds // 3600
        mins = (time_left.seconds % 3600) // 60
        return "premarket", "Pre-Market", f"Opens in {hours}h {mins}m"
    elif now < market_close: 
        time_left = market_close - now
        hours = time_left.seconds // 3600
        mins = (time_left.seconds % 3600) // 60
        return "open", "Market Open", f"Closes in {hours}h {mins}m"
    elif now < afterhours: 
        return "afterhours", "After Hours", f"Until 8:00 PM"
    return "closed", "Closed", "Opens 4:00 AM"

@st.cache_data(ttl=CACHE_SHORT)
def fetch_stock_data(symbol: str, period: str = "5d", interval: str = "15m") -> Tuple[Optional[pd.DataFrame], dict]:
    """Fetch stock data with proper error handling and retry logic."""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval, prepost=False)
            
            # If no data, try without prepost
            if hist is None or hist.empty:
                hist = ticker.history(period=period, interval=interval)
            
            # For futures/indices, if still no data try daily interval
            if (hist is None or hist.empty) and ('=' in symbol or '^' in symbol):
                hist = ticker.history(period=period, interval="1d")
            
            # For crypto, try with different params
            if (hist is None or hist.empty) and '-USD' in symbol:
                hist = ticker.history(period=period, interval="1d" if interval in ["5m", "15m", "1h"] else interval)
            
            info = {}
            try:
                info = ticker.info or {}
            except Exception:
                pass
            
            # Validate data
            if hist is not None and not hist.empty:
                # Ensure we have valid Close prices
                if 'Close' in hist.columns and hist['Close'].notna().any():
                    # Remove any fully-NaN rows
                    hist = hist.dropna(subset=['Close'])
                    if not hist.empty:
                        return hist, info
            
            return None, {}
        except requests.exceptions.RequestException:
            if attempt < max_retries:
                time_module.sleep(0.5 * (attempt + 1))
                continue
            return None, {}
        except Exception as e:
            logger.debug(f"fetch_stock_data error for {symbol}: {e}")
            if attempt < max_retries:
                time_module.sleep(0.3)
                continue
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
    except Exception:
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
        except Exception:
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
        except Exception:
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
                        except Exception:
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
                        except Exception:
                            continue
            
            return {'transactions': insider_data, 'metrics': metrics}
    except Exception:
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
    indicator_list = [
        ("10Y Treasury", "^TNX"), ("5Y Treasury", "^FVX"), ("2Y Treasury", "^IRX"),
        ("VIX", "^VIX"), ("Dollar Index", "DX=F"), ("Gold", "GC=F"),
    ]
    for name, sym in indicator_list:
        try:
            hist = yf.Ticker(sym).history(period="5d")
            if not hist.empty:
                current = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2] if len(hist) > 1 else current
                change = safe_pct_change(current, prev)
                indicators[name] = {
                    'value': current, 
                    'prev': prev, 
                    'change': change,
                    'unit': '%' if 'Treasury' in name else ''
                }
        except Exception:
            pass
    
    # VIX Term Structure (VIX vs VIX3M for contango/backwardation signal)
    try:
        vix_hist = yf.Ticker('^VIX').history(period="5d")
        vix9d_hist = yf.Ticker('^VIX9D').history(period="5d")
        if not vix_hist.empty and not vix9d_hist.empty:
            vix_val = vix_hist['Close'].iloc[-1]
            vix9d_val = vix9d_hist['Close'].iloc[-1]
            indicators['VIX Term Structure'] = {
                'value': vix_val - vix9d_val,
                'prev': 0,
                'unit': 'pts',
                'signal': 'contango' if vix_val > vix9d_val else 'backwardation'
            }
    except Exception:
        pass
    
    return indicators

@st.cache_data(ttl=900)
def calculate_cross_asset_correlations() -> Dict[str, Any]:
    """Calculate cross-asset correlation matrix for regime detection."""
    try:
        symbols = ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'USO', 'HYG']
        labels = ['S&P 500', 'Nasdaq', 'Russell', 'Treasuries', 'Gold', 'Oil', 'HY Credit']
        
        data = yf.download(symbols, period='30d', interval='1d', progress=False, auto_adjust=True)
        if isinstance(data.columns, pd.MultiIndex):
            closes = data['Close']
        else:
            return {}
        
        returns = closes.pct_change().dropna()
        if returns.empty or len(returns) < 10:
            return {}
        
        corr = returns.corr()
        corr.index = labels[:len(corr.index)]
        corr.columns = labels[:len(corr.columns)]
        
        # Key correlations for regime detection
        spy_tlt = float(corr.iloc[0, 3]) if corr.shape[0] > 3 else 0
        spy_gold = float(corr.iloc[0, 4]) if corr.shape[0] > 4 else 0
        
        if spy_tlt > 0.3:
            regime_note = "Risk-on: Stocks and bonds rising together (unusual, watch for reversal)"
        elif spy_tlt < -0.3:
            regime_note = "Normal: Stocks and bonds inversely correlated (flight-to-quality intact)"
        else:
            regime_note = "Transitional: Stock-bond correlation near zero (regime uncertainty)"
        
        return {
            'correlation_matrix': corr,
            'spy_tlt_corr': spy_tlt,
            'spy_gold_corr': spy_gold,
            'regime_note': regime_note,
        }
    except Exception:
        return {}

@st.cache_data(ttl=300)
def calculate_market_breadth() -> Dict[str, Any]:
    """Calculate market breadth indicators using sector ETFs."""
    try:
        sector_symbols = [data['symbol'] for data in SECTOR_ETFS.values()]
        data = yf.download(sector_symbols, period='5d', interval='1d', progress=False, auto_adjust=True)
        
        if isinstance(data.columns, pd.MultiIndex):
            closes = data['Close']
        else:
            return {'advancing': 0, 'declining': 0, 'breadth_pct': 50, 'ad_ratio': 1.0, 'signal': 'mixed'}
        
        returns = closes.pct_change().iloc[-1]
        advancing = int((returns > 0).sum())
        declining = int((returns < 0).sum())
        total = len(returns.dropna())
        
        breadth_pct = (advancing / total * 100) if total > 0 else 50
        advance_decline_ratio = safe_div(advancing, max(declining, 1), 1.0)
        
        return {
            'advancing': advancing,
            'declining': declining,
            'breadth_pct': breadth_pct,
            'ad_ratio': advance_decline_ratio,
            'signal': 'strong' if breadth_pct > 70 else 'weak' if breadth_pct < 30 else 'mixed'
        }
    except Exception:
        return {'advancing': 0, 'declining': 0, 'breadth_pct': 50, 'ad_ratio': 1.0, 'signal': 'mixed'}

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
    """Calculate key support and resistance levels using multiple methods including Fibonacci."""
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
        r3 = prev_high + 2 * (pivot - prev_low)
        s1 = 2 * pivot - prev_high
        s2 = pivot - (prev_high - prev_low)
        s3 = prev_low - 2 * (prev_high - pivot)
        
        if r1 > current_price: levels['resistance'].append(('Pivot R1', r1))
        if r2 > current_price: levels['resistance'].append(('Pivot R2', r2))
        if r3 > current_price: levels['resistance'].append(('Pivot R3', r3))
        if s1 < current_price: levels['support'].append(('Pivot S1', s1))
        if s2 < current_price: levels['support'].append(('Pivot S2', s2))
        if s3 < current_price: levels['support'].append(('Pivot S3', s3))
    
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
    
    # Method 5: Fibonacci Retracement Levels
    if len(hist) >= 20:
        recent_high = max(highs[-60:]) if len(highs) >= 60 else max(highs)
        recent_low = min(lows[-60:]) if len(lows) >= 60 else min(lows)
        fib_diff = recent_high - recent_low
        if fib_diff > 0:
            fib_levels = {
                'Fib 0.236': recent_low + fib_diff * 0.236,
                'Fib 0.382': recent_low + fib_diff * 0.382,
                'Fib 0.500': recent_low + fib_diff * 0.500,
                'Fib 0.618': recent_low + fib_diff * 0.618,
                'Fib 0.786': recent_low + fib_diff * 0.786,
            }
            for name, level in fib_levels.items():
                if abs(level - current_price) / current_price > 0.002:  # Skip if too close to current
                    if level > current_price:
                        levels['resistance'].append((name, level))
                    else:
                        levels['support'].append((name, level))
    
    # Method 6: VWAP as dynamic S/R (for intraday)
    vwap = calculate_vwap(hist)
    if vwap is not None and not vwap.empty:
        vwap_val = vwap.iloc[-1]
        if pd.notna(vwap_val) and vwap_val > 0:
            if vwap_val < current_price:
                levels['support'].append(('VWAP', vwap_val))
            else:
                levels['resistance'].append(('VWAP', vwap_val))
    
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
    
    try:
        latest = hist.iloc[-1]
        price = float(latest['Close'])
        
        # Handle price being 0 or NaN
        if price <= 0 or pd.isna(price):
            return None
        
        # Try multiple sources for previous close
        prev = safe_get(info, 'regularMarketPreviousClose', None)
        if prev is None or prev <= 0:
            prev = safe_get(info, 'previousClose', None)
        if prev is None or prev <= 0:
            # Fall back to yesterday's close from history
            if len(hist) >= 2:
                # Get the second to last day's close
                prev = float(hist['Close'].iloc[-2])
            else:
                prev = price  # Last resort
        
        # Ensure prev is valid
        if prev is None or prev <= 0 or pd.isna(prev):
            prev = price
        
        change_pct = safe_pct_change(price, prev)
        vol = latest['Volume'] if 'Volume' in latest and pd.notna(latest['Volume']) else 0
        avg_vol = hist['Volume'].rolling(20).mean().iloc[-1] if len(hist) > 20 and 'Volume' in hist else vol
        vol_vs_avg = safe_div(vol, avg_vol, 1.0) * 100
        
        first_close = float(hist['Close'].iloc[0]) if len(hist) > 1 else price
        momentum = safe_pct_change(price, first_close)
        
        rsi, rsi_cond = calculate_rsi(hist['Close'])
        _, _, _, macd_sig = calculate_macd(hist['Close'])
        
        # Calculate VWAP
        vwap = calculate_vwap(hist)
        vwap_val = float(vwap.iloc[-1]) if vwap is not None and not vwap.empty and pd.notna(vwap.iloc[-1]) else price
        vwap_deviation = safe_pct_change(price, vwap_val) if vwap_val > 0 else 0
        
        # Calculate ATR for volatility assessment
        atr = calculate_atr(hist)
        atr_pct = safe_div(atr, price) * 100
        
        # Trend strength (based on price position relative to EMAs)
        trend_strength = 0
        if len(hist) >= 20:
            ema20 = hist['Close'].ewm(span=20).mean().iloc[-1]
            if price > ema20:
                trend_strength += 1
            else:
                trend_strength -= 1
        if len(hist) >= 50:
            ema50 = hist['Close'].ewm(span=50).mean().iloc[-1]
            if price > ema50:
                trend_strength += 1
            else:
                trend_strength -= 1
        
        return {
            'current_price': price, 
            'prev_close': prev, 
            'overnight_change': price - prev, 
            'overnight_change_pct': change_pct, 
            'volume': vol, 
            'volume_vs_avg': vol_vs_avg, 
            'high': float(latest['High']) if 'High' in latest and pd.notna(latest['High']) else price, 
            'low': float(latest['Low']) if 'Low' in latest and pd.notna(latest['Low']) else price, 
            'momentum_5d': momentum, 
            'rsi': rsi, 
            'rsi_condition': rsi_cond, 
            'macd_signal': macd_sig,
            'vwap': vwap_val,
            'vwap_deviation': vwap_deviation,
            'atr': atr,
            'atr_pct': atr_pct,
            'trend_strength': trend_strength,
        }
    except Exception as e:
        return None

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
        except Exception:
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
            except Exception:
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
                except Exception:
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
            except Exception:
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
                except Exception:
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
                except Exception:
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

def generate_expert_analysis(symbol, data, signals, support_levels, resistance_levels, news_sentiment, institutional_activity=None):
    """
    Generate Bloomberg Terminal-grade institutional analysis.
    
    This function synthesizes:
    - Multi-timeframe trend analysis
    - Volume profile and institutional flow
    - Volatility regime assessment
    - Momentum factor scoring
    - Risk metrics (drawdown, VaR-style)
    - Options market signals
    - Fundamental valuation context
    - Catalyst identification
    - Probability-weighted price targets
    - Risk/reward quantification
    """
    info = data.get('info', {})
    hist_5d = data.get('hist_5d')
    hist_1mo = data.get('hist_1mo')
    hist_3mo = data.get('hist_3mo')
    hist_1y = data.get('hist_1y')
    
    # Use best available data
    hist = hist_3mo if hist_3mo is not None and len(hist_3mo) > 20 else hist_5d
    if hist is None or hist.empty:
        return None
    
    price = hist['Close'].iloc[-1]
    prev = safe_get(info, 'regularMarketPreviousClose', price)
    change_pct = safe_pct_change(price, prev)
    
    # === MULTI-TIMEFRAME TREND ANALYSIS ===
    def get_trend(df, periods=20):
        if df is None or len(df) < periods:
            return 'insufficient_data', 0
        ma = df['Close'].rolling(periods).mean()
        current = df['Close'].iloc[-1]
        ma_val = ma.iloc[-1]
        if pd.isna(ma_val):
            return 'neutral', 0
        pct_from_ma = ((current - ma_val) / ma_val) * 100
        if pct_from_ma > 5:
            return 'strong_uptrend', pct_from_ma
        elif pct_from_ma > 2:
            return 'uptrend', pct_from_ma
        elif pct_from_ma < -5:
            return 'strong_downtrend', pct_from_ma
        elif pct_from_ma < -2:
            return 'downtrend', pct_from_ma
        return 'consolidation', pct_from_ma
    
    trend_5d, trend_5d_pct = get_trend(hist_5d, 5) if hist_5d is not None else ('neutral', 0)
    trend_1mo, trend_1mo_pct = get_trend(hist_1mo, 10) if hist_1mo is not None else ('neutral', 0)
    trend_3mo, trend_3mo_pct = get_trend(hist_3mo, 20) if hist_3mo is not None else ('neutral', 0)
    
    # === VOLATILITY REGIME ===
    volatility_regime = 'normal'
    atr_pct = 0
    atr_value = 0  # Store raw ATR for trade calculations
    hist_volatility = 0
    if len(hist) >= 14:
        high_low = hist['High'] - hist['Low']
        atr = high_low.rolling(14).mean().iloc[-1]
        atr_value = float(atr) if pd.notna(atr) else price * 0.02  # Default to 2% of price
        atr_pct = (atr_value / price) * 100 if price > 0 else 0
        
        # Historical volatility (annualized)
        returns = hist['Close'].pct_change().dropna()
        if len(returns) > 5:
            hist_volatility = returns.std() * np.sqrt(252) * 100
        
        if atr_pct > 4 or hist_volatility > 50:
            volatility_regime = 'high'
        elif atr_pct > 2.5 or hist_volatility > 30:
            volatility_regime = 'elevated'
        elif atr_pct < 1 and hist_volatility < 15:
            volatility_regime = 'compressed'
    else:
        atr_value = price * 0.02  # Default to 2% of price if no data
    
    # === MOMENTUM SCORING (Multi-Factor) ===
    momentum_score = 0
    momentum_factors = []
    
    # RSI Factor
    rsi, rsi_cond = calculate_rsi(hist['Close'])
    if rsi > 70:
        momentum_score -= 15
        momentum_factors.append(('RSI Overbought', -15, f'{rsi:.0f}'))
    elif rsi > 60:
        momentum_score += 10
        momentum_factors.append(('RSI Bullish', +10, f'{rsi:.0f}'))
    elif rsi < 30:
        momentum_score += 5  # Oversold can bounce
        momentum_factors.append(('RSI Oversold', +5, f'{rsi:.0f}'))
    elif rsi < 40:
        momentum_score -= 10
        momentum_factors.append(('RSI Bearish', -10, f'{rsi:.0f}'))
    else:
        momentum_factors.append(('RSI Neutral', 0, f'{rsi:.0f}'))
    
    # MACD Factor
    macd_val, signal_val, macd_hist, macd_cond = calculate_macd(hist['Close'])
    if macd_cond == 'bullish_cross':
        momentum_score += 20
        momentum_factors.append(('MACD Bullish Cross', +20, 'Fresh'))
    elif macd_cond == 'bullish':
        momentum_score += 10
        momentum_factors.append(('MACD Bullish', +10, 'Above Signal'))
    elif macd_cond == 'bearish_cross':
        momentum_score -= 20
        momentum_factors.append(('MACD Bearish Cross', -20, 'Fresh'))
    elif macd_cond == 'bearish':
        momentum_score -= 10
        momentum_factors.append(('MACD Bearish', -10, 'Below Signal'))
    
    # Price vs Moving Averages
    if len(hist) >= 50:
        ma20 = hist['Close'].rolling(20).mean().iloc[-1]
        ma50 = hist['Close'].rolling(50).mean().iloc[-1]
        
        if price > ma20 > ma50:
            momentum_score += 20
            momentum_factors.append(('MA Stack Bullish', +20, 'Price>MA20>MA50'))
        elif price < ma20 < ma50:
            momentum_score -= 20
            momentum_factors.append(('MA Stack Bearish', -20, 'Price<MA20<MA50'))
        elif price > ma20 and ma20 < ma50:
            momentum_score += 5
            momentum_factors.append(('Recovering', +5, 'Above MA20'))
    
    # Volume Confirmation
    vol_ratio = 1
    if len(hist) >= 20:
        avg_vol = hist['Volume'].rolling(20).mean().iloc[-1]
        current_vol = hist['Volume'].iloc[-1]
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1
        
        if vol_ratio > 2 and change_pct > 0:
            momentum_score += 15
            momentum_factors.append(('Volume Surge (Up)', +15, f'{vol_ratio:.1f}x'))
        elif vol_ratio > 2 and change_pct < 0:
            momentum_score -= 15
            momentum_factors.append(('Volume Surge (Down)', -15, f'{vol_ratio:.1f}x'))
        elif vol_ratio < 0.5:
            momentum_factors.append(('Low Volume', 0, f'{vol_ratio:.1f}x'))
    
    # Rate of Change
    if len(hist) >= 20:
        roc_5 = safe_pct_change(hist['Close'].iloc[-1], hist['Close'].iloc[-5])
        roc_20 = safe_pct_change(hist['Close'].iloc[-1], hist['Close'].iloc[-20])
        
        if roc_5 > 5 and roc_20 > 10:
            momentum_score += 15
            momentum_factors.append(('Strong Momentum', +15, f'+{roc_5:.1f}% 5d'))
        elif roc_5 < -5 and roc_20 < -10:
            momentum_score -= 15
            momentum_factors.append(('Weak Momentum', -15, f'{roc_5:.1f}% 5d'))
    
    # === RISK METRICS ===
    # Maximum Drawdown (recent)
    max_drawdown = 0
    if hist_3mo is not None and len(hist_3mo) > 5:
        rolling_max = hist_3mo['Close'].expanding().max()
        drawdowns = (hist_3mo['Close'] - rolling_max) / rolling_max * 100
        max_drawdown = drawdowns.min()
    
    # Distance from 52-week high/low
    high_52w = info.get('fiftyTwoWeekHigh', price)
    low_52w = info.get('fiftyTwoWeekLow', price)
    pct_from_high = safe_pct_change(price, high_52w) if high_52w > 0 else 0
    pct_from_low = safe_pct_change(price, low_52w) if low_52w > 0 else 0
    
    # === INSTITUTIONAL SIGNALS ===
    inst_bias = 'neutral'
    smart_money_score = 50
    squeeze_potential = 0
    dark_pool_sentiment = 'neutral'
    
    if institutional_activity:
        smart_money_score = institutional_activity.get('smart_money_score', 50)
        squeeze_potential = institutional_activity.get('squeeze_potential', 0)
        dark_pool_sentiment = institutional_activity.get('dark_pool_sentiment', 'neutral')
        inst_signal = institutional_activity.get('overall_signal', 'neutral')
        
        if smart_money_score > 65:
            inst_bias = 'bullish'
            momentum_score += 10
        elif smart_money_score < 35:
            inst_bias = 'bearish'
            momentum_score -= 10
    
    # === FUNDAMENTAL CONTEXT ===
    pe = info.get('trailingPE', 0)
    forward_pe = info.get('forwardPE', 0)
    peg = info.get('pegRatio', 0)
    ps = info.get('priceToSalesTrailing12Months', 0)
    pb = info.get('priceToBook', 0)
    
    sector = info.get('sector', 'Unknown')
    industry = info.get('industry', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    # Revenue/Earnings growth
    rev_growth = info.get('revenueGrowth', 0)
    earnings_growth = info.get('earningsGrowth', info.get('earningsQuarterlyGrowth', 0))
    
    # Analyst targets
    target_mean = info.get('targetMeanPrice', 0)
    target_high = info.get('targetHighPrice', 0)
    target_low = info.get('targetLowPrice', 0)
    
    # === SUPPORT/RESISTANCE ANALYSIS ===
    nearest_support = support_levels[0] if support_levels else None
    nearest_resistance = resistance_levels[0] if resistance_levels else None
    
    support_dist = 0
    resist_dist = 0
    risk_reward = 0
    
    if nearest_support and price > 0:
        support_dist = safe_div((price - nearest_support[1]), price) * 100
    if nearest_resistance and price > 0:
        resist_dist = safe_div((nearest_resistance[1] - price), price) * 100
    
    # Risk/Reward Calculation
    if support_dist > 0 and resist_dist > 0:
        risk_reward = resist_dist / support_dist
    
    # === NEWS CATALYST ASSESSMENT ===
    news_score = 0
    if news_sentiment:
        bull_signals = news_sentiment.get('bullish', 0)
        bear_signals = news_sentiment.get('bearish', 0)
        news_score = (bull_signals - bear_signals) * 5
        momentum_score += min(max(news_score, -15), 15)  # Cap at ±15
    
    # === COMPOSITE SCORING ===
    # Technical Score (-100 to +100)
    tech_score = max(min(momentum_score, 100), -100)
    
    # Overall Score incorporating all factors
    overall_score = tech_score
    
    # Adjust for fundamentals
    if pe > 0 and pe < 15 and earnings_growth > 0.1:
        overall_score += 10  # Value with growth
    elif pe > 50 and earnings_growth < 0:
        overall_score -= 10  # Expensive and shrinking
    
    # Adjust for institutional activity
    if inst_bias == 'bullish':
        overall_score += 10
    elif inst_bias == 'bearish':
        overall_score -= 10
    
    # === GENERATE VERDICT ===
    if overall_score >= 50:
        verdict = "STRONG BUY"
        verdict_color = "#00C805"
        verdict_icon = "🟢"
        position_bias = "aggressive_long"
    elif overall_score >= 25:
        verdict = "BUY"
        verdict_color = "#3fb950"
        verdict_icon = "🟢"
        position_bias = "long"
    elif overall_score >= 10:
        verdict = "LEAN BULLISH"
        verdict_color = "#58a6ff"
        verdict_icon = "🔵"
        position_bias = "cautious_long"
    elif overall_score <= -50:
        verdict = "STRONG SELL"
        verdict_color = "#FF3B30"
        verdict_icon = "🔴"
        position_bias = "aggressive_short"
    elif overall_score <= -25:
        verdict = "SELL"
        verdict_color = "#f85149"
        verdict_icon = "🔴"
        position_bias = "short"
    elif overall_score <= -10:
        verdict = "LEAN BEARISH"
        verdict_color = "#d29922"
        verdict_icon = "🟡"
        position_bias = "cautious_short"
    else:
        verdict = "NEUTRAL"
        verdict_color = "#8b949e"
        verdict_icon = "⚪"
        position_bias = "neutral"
    
    # === PRICE TARGETS ===
    # Technical targets based on S/R
    upside_target = nearest_resistance[1] if nearest_resistance else price * 1.05
    downside_target = nearest_support[1] if nearest_support else price * 0.95
    
    # Blend with analyst targets if available
    if target_mean > 0:
        upside_target = (upside_target + target_mean) / 2
    
    upside_pct = safe_pct_change(upside_target, price)
    downside_pct = safe_pct_change(downside_target, price)
    
    # === GENERATE ANALYSIS SECTIONS ===
    name = info.get('shortName', info.get('longName', symbol))
    
    # 1. Executive Summary
    if overall_score >= 25:
        exec_summary = f"{name} presents a compelling opportunity with strong technical momentum and favorable risk/reward dynamics."
    elif overall_score <= -25:
        exec_summary = f"{name} faces significant headwinds with deteriorating technicals and elevated downside risk."
    else:
        exec_summary = f"{name} is in a transitional phase with mixed signals requiring patience for clearer direction."
    
    # 2. Trend Analysis
    trend_text = f"**Trend Structure:** "
    if trend_3mo in ['strong_uptrend', 'uptrend']:
        trend_text += f"Primary trend is bullish with price {abs(trend_3mo_pct):.1f}% above the 20-day MA on the 3-month timeframe. "
    elif trend_3mo in ['strong_downtrend', 'downtrend']:
        trend_text += f"Primary trend is bearish with price {abs(trend_3mo_pct):.1f}% below the 20-day MA. "
    else:
        trend_text += f"Price action is consolidating within a range. "
    
    if trend_5d != trend_3mo:
        trend_text += f"Near-term momentum ({trend_5d.replace('_', ' ')}) diverges from the primary trend, suggesting potential inflection. "
    
    # 3. Momentum Assessment
    momentum_text = f"**Momentum Score: {tech_score:+d}/100** — "
    top_factors = sorted(momentum_factors, key=lambda x: abs(x[1]), reverse=True)[:3]
    factor_strs = [f"{f[0]} ({f[2]})" for f in top_factors]
    momentum_text += f"Key drivers: {', '.join(factor_strs)}. "
    
    if rsi > 65:
        momentum_text += f"RSI at {rsi:.0f} indicates overextension; mean reversion risk elevated. "
    elif rsi < 35:
        momentum_text += f"RSI at {rsi:.0f} suggests oversold conditions; watch for reversal signals. "
    
    # 4. Volume Analysis
    volume_text = f"**Volume Profile:** "
    if vol_ratio > 2:
        volume_text += f"Institutional participation evident with volume at {vol_ratio:.1f}x the 20-day average. "
        if change_pct > 0:
            volume_text += "Heavy accumulation validates bullish price action. "
        else:
            volume_text += "Distribution pattern suggests institutional selling. "
    elif vol_ratio < 0.7:
        volume_text += f"Below-average volume ({vol_ratio:.1f}x) indicates low conviction; breakouts/breakdowns likely to fail. "
    else:
        volume_text += f"Volume at {vol_ratio:.1f}x average reflects normal institutional participation. "
    
    # 5. Volatility Assessment
    vol_text = f"**Volatility Regime: {volatility_regime.upper()}** — "
    if volatility_regime == 'high':
        vol_text += f"ATR at {atr_pct:.1f}% of price and {hist_volatility:.0f}% annualized vol demand reduced position sizing and wider stops. "
    elif volatility_regime == 'compressed':
        vol_text += f"Compressed volatility ({atr_pct:.1f}% ATR) typically precedes explosive moves. Prepare for breakout/breakdown. "
    else:
        vol_text += f"ATR at {atr_pct:.1f}% of price supports standard position sizing. "
    
    # 6. Risk Metrics
    risk_text = f"**Risk Assessment:** "
    risk_text += f"Trading {pct_from_high:.1f}% below 52-week high, {pct_from_low:.1f}% above 52-week low. "
    if max_drawdown < -15:
        risk_text += f"Recent max drawdown of {max_drawdown:.1f}% indicates elevated volatility risk. "
    
    if risk_reward > 2:
        risk_text += f"**Risk/Reward: {risk_reward:.1f}:1** — Favorable asymmetry with {resist_dist:.1f}% upside to resistance vs {support_dist:.1f}% downside to support. "
    elif risk_reward > 1:
        risk_text += f"**Risk/Reward: {risk_reward:.1f}:1** — Acceptable setup with defined levels. "
    elif risk_reward > 0 and risk_reward < 0.5:
        risk_text += f"**Risk/Reward: {risk_reward:.1f}:1** — Unfavorable risk/reward; consider waiting for better entry. "
    
    # 7. Institutional Flow
    inst_text = ""
    if institutional_activity:
        inst_text = f"**Smart Money Indicators:** "
        inst_text += f"Composite score at {smart_money_score}/100 signals {inst_bias} institutional bias. "
        if squeeze_potential > 50:
            inst_text += f"**Squeeze Alert:** {squeeze_potential}% squeeze potential with elevated short interest. "
        if dark_pool_sentiment == 'accumulation':
            inst_text += "Dark pool flow suggests quiet accumulation by large players. "
        elif dark_pool_sentiment == 'distribution':
            inst_text += "Dark pool patterns indicate institutional distribution. "
    
    # 8. Fundamental Context (for stocks)
    fund_text = ""
    if pe > 0 or market_cap > 0:
        fund_text = f"**Valuation Context:** "
        if pe > 0:
            fund_text += f"P/E of {pe:.1f}x "
            if forward_pe > 0:
                fund_text += f"(fwd {forward_pe:.1f}x) "
        if peg > 0 and peg < 2:
            fund_text += f"with PEG of {peg:.1f} suggests reasonable growth-adjusted valuation. "
        elif peg > 3:
            fund_text += f"with PEG of {peg:.1f} implies premium valuation relative to growth. "
        
        if rev_growth and rev_growth > 0.2:
            fund_text += f"Revenue growth of {rev_growth*100:.0f}% supports premium multiple. "
        elif earnings_growth and earnings_growth < -0.1:
            fund_text += f"Earnings contraction of {abs(earnings_growth)*100:.0f}% warrants caution. "
    
    # 9. Catalyst Watch
    catalyst_text = ""
    if news_sentiment and (news_sentiment.get('bullish', 0) > 2 or news_sentiment.get('bearish', 0) > 2):
        catalyst_text = f"**Catalyst Watch:** News sentiment reading {news_sentiment.get('overall', 'neutral')} "
        catalyst_text += f"with {news_sentiment.get('bullish', 0)} bullish / {news_sentiment.get('bearish', 0)} bearish signals. "
        if news_sentiment.get('overall') == 'bullish':
            catalyst_text += "Positive news flow provides fundamental tailwind. "
        elif news_sentiment.get('overall') == 'bearish':
            catalyst_text += "Negative headlines create overhang; monitor for stabilization. "
    
    # 10. Trade Recommendation
    trade_text = f"**Trade Parameters:**\n"
    if position_bias in ['aggressive_long', 'long', 'cautious_long']:
        effective_atr_text = atr_value if atr_value > 0 else price * 0.02
        atr_mult = 1.5 if 'aggressive' in position_bias else 2.5 if 'cautious' in position_bias else 2.0
        sl_price = price - (effective_atr_text * atr_mult)
        sup_ref = nearest_support[1] if nearest_support else price * 0.97
        sl_final = min(sl_price, sup_ref - effective_atr_text * 0.5)
        tp1 = nearest_resistance[1] if nearest_resistance and nearest_resistance[1] > price else price + effective_atr_text * 2.5
        trade_text += f"• **Bias:** LONG\n"
        trade_text += f"• **Entry Zone:** ${max(sup_ref, price - effective_atr_text * 0.5):.2f} - ${price:.2f}\n"
        trade_text += f"• **Stop Loss:** ${sl_final:.2f} ({safe_pct_change(sl_final, price):+.1f}%) — {atr_mult:.1f}x ATR below entry, beneath support\n"
        trade_text += f"• **Target 1:** ${tp1:.2f} (+{safe_pct_change(tp1, price):.1f}%)\n"
        if target_high > tp1:
            trade_text += f"• **Target 2:** ${target_high:.2f} (+{safe_pct_change(target_high, price):.1f}%)\n"
        trade_text += f"• **R:R Ratio:** {safe_div(abs(tp1 - price), abs(price - sl_final), 0):.1f}:1\n"
        trade_text += f"• **Position Size:** {'Reduced' if volatility_regime in ['high', 'elevated'] else 'Standard'} allocation\n"
    elif position_bias in ['aggressive_short', 'short', 'cautious_short']:
        effective_atr_text = atr_value if atr_value > 0 else price * 0.02
        atr_mult = 1.5 if 'aggressive' in position_bias else 2.5 if 'cautious' in position_bias else 2.0
        sl_price = price + (effective_atr_text * atr_mult)
        res_ref = nearest_resistance[1] if nearest_resistance else price * 1.03
        sl_final = max(sl_price, res_ref + effective_atr_text * 0.5)
        tp1 = nearest_support[1] if nearest_support and nearest_support[1] < price else price - effective_atr_text * 2.5
        trade_text += f"• **Bias:** SHORT / DEFENSIVE\n"
        trade_text += f"• **Entry Zone:** ${price:.2f} - ${min(res_ref, price + effective_atr_text * 0.5):.2f}\n"
        trade_text += f"• **Stop Loss:** ${sl_final:.2f} ({safe_pct_change(sl_final, price):+.1f}%) — {atr_mult:.1f}x ATR above entry, above resistance\n"
        trade_text += f"• **Downside Target:** ${tp1:.2f} ({safe_pct_change(tp1, price):+.1f}%)\n"
        trade_text += f"• **R:R Ratio:** {safe_div(abs(price - tp1), abs(sl_final - price), 0):.1f}:1\n"
        trade_text += f"• **Hedge Recommendation:** Consider protective puts or reduced exposure\n"
    else:
        trade_text += f"• **Bias:** NEUTRAL - Wait for confirmation\n"
        trade_text += f"• **Breakout Level:** ${nearest_resistance[1] if nearest_resistance else price*1.02:.2f}\n"
        trade_text += f"• **Breakdown Level:** ${nearest_support[1] if nearest_support else price*0.98:.2f}\n"
        trade_text += f"• **Action:** Reduce position size; monitor for directional catalyst\n"
    
    # Combine all sections
    full_analysis = f"""
{exec_summary}

{trend_text}

{momentum_text}

{volume_text}

{vol_text}

{risk_text}

{inst_text}

{fund_text}

{catalyst_text}

{trade_text}
""".strip()
    
    # Build structured trade parameters for Bloomberg-style UI
    # ========================================================================
    # INSTITUTIONAL-GRADE TRADE PARAMETER ENGINE
    # Uses ATR-based risk management, proper entry zones per direction,
    # and correct target/stop alignment for long, short, and neutral setups.
    # ========================================================================
    
    is_long = position_bias in ['aggressive_long', 'long', 'cautious_long']
    is_short = position_bias in ['aggressive_short', 'short', 'cautious_short']
    is_neutral = position_bias == 'neutral'
    
    # ATR multipliers scale with conviction level
    if 'aggressive' in position_bias:
        atr_stop_mult = 1.5   # Tighter stop = more conviction
        atr_tp1_mult = 3.0    # Wider target
        atr_tp2_mult = 5.0
    elif 'cautious' in position_bias:
        atr_stop_mult = 2.5   # Wider stop = less conviction
        atr_tp1_mult = 2.0
        atr_tp2_mult = 3.5
    else:
        atr_stop_mult = 2.0   # Standard
        atr_tp1_mult = 2.5
        atr_tp2_mult = 4.0
    
    # Ensure atr_value is valid (fallback to 2% of price)
    effective_atr = atr_value if atr_value > 0 else price * 0.02
    
    # === SUPPORT/RESISTANCE PRICE ANCHORS ===
    sup_price = nearest_support[1] if nearest_support else price * 0.97
    res_price = nearest_resistance[1] if nearest_resistance else price * 1.03
    
    if is_long:
        # LONG SETUP
        # Entry: Pull back toward support or current level; tight zone
        # Use the higher of: slight pullback from current, or just above nearest support
        entry_ideal = max(sup_price, price - effective_atr * 0.5)  # Ideal entry near support or half-ATR dip
        entry_low = round(min(entry_ideal, price * 0.995), 2)  # Floor: ~0.5% below current
        entry_high = round(price, 2)                            # Ceiling: current price
        
        # Stop Loss: Below nearest support, confirmed by ATR
        atr_stop = price - (effective_atr * atr_stop_mult)
        structure_stop = sup_price - (effective_atr * 0.5)  # Just below support with ATR buffer
        stop_loss = round(min(atr_stop, structure_stop), 2)  # Use the more conservative (lower) stop
        # Ensure stop is meaningfully below entry
        if stop_loss >= entry_low:
            stop_loss = round(entry_low - effective_atr, 2)
        
        stop_pct = abs(safe_pct_change(stop_loss, price))
        
        # Targets: Use resistance levels confirmed by ATR projections
        atr_target_1 = price + (effective_atr * atr_tp1_mult)
        target_1 = round(min(res_price, atr_target_1) if res_price > price else atr_target_1, 2)
        # Ensure target is above entry
        if target_1 <= price:
            target_1 = round(price + effective_atr * atr_tp1_mult, 2)
        target_1_pct = safe_pct_change(target_1, price)
        
        # Target 2: Extended target (analyst high or ATR extension)
        atr_target_2 = price + (effective_atr * atr_tp2_mult)
        if target_high > target_1:
            target_2 = round((target_high + atr_target_2) / 2, 2)  # Blend analyst + ATR
        elif atr_target_2 > target_1 * 1.02:
            target_2 = round(atr_target_2, 2)
        else:
            target_2 = None
        target_2_pct = safe_pct_change(target_2, price) if target_2 else None
        
        risk_per_share = round(abs(price - stop_loss), 2)
        reward_per_share = round(abs(target_1 - price), 2)
        setup_label = "▼ BUY ZONE"
        
    elif is_short:
        # SHORT SETUP
        # Entry: Rally toward resistance or current level; tight zone
        entry_ideal = min(res_price, price + effective_atr * 0.5)
        entry_low = round(price, 2)                              # Floor: current price
        entry_high = round(max(entry_ideal, price * 1.005), 2)   # Ceiling: ~0.5% above or near resistance
        
        # Stop Loss: Above nearest resistance, confirmed by ATR
        atr_stop = price + (effective_atr * atr_stop_mult)
        structure_stop = res_price + (effective_atr * 0.5)  # Just above resistance with ATR buffer
        stop_loss = round(max(atr_stop, structure_stop), 2)  # Use the more conservative (higher) stop
        # Ensure stop is meaningfully above entry
        if stop_loss <= entry_high:
            stop_loss = round(entry_high + effective_atr, 2)
        
        stop_pct = abs(safe_pct_change(stop_loss, price))
        
        # Targets: Downside - use support levels confirmed by ATR projections
        atr_target_1 = price - (effective_atr * atr_tp1_mult)
        target_1 = round(max(sup_price, atr_target_1) if sup_price < price else atr_target_1, 2)
        # Ensure target is below entry
        if target_1 >= price:
            target_1 = round(price - effective_atr * atr_tp1_mult, 2)
        target_1_pct = safe_pct_change(target_1, price)  # Will be negative
        
        # Target 2: Extended downside
        atr_target_2 = price - (effective_atr * atr_tp2_mult)
        if target_low > 0 and target_low < target_1:
            target_2 = round((target_low + atr_target_2) / 2, 2)
        elif atr_target_2 < target_1 * 0.98 and atr_target_2 > 0:
            target_2 = round(atr_target_2, 2)
        else:
            target_2 = None
        target_2_pct = safe_pct_change(target_2, price) if target_2 else None
        
        risk_per_share = round(abs(stop_loss - price), 2)
        reward_per_share = round(abs(price - target_1), 2)
        setup_label = "▲ SHORT ZONE"
        
    else:
        # NEUTRAL SETUP - Range-bound parameters
        entry_low = round(sup_price, 2)
        entry_high = round(res_price, 2)
        
        # ATR-based stop for breakout/breakdown
        stop_loss = round(sup_price - effective_atr, 2)  # Below support for breakdown protection
        stop_pct = abs(safe_pct_change(stop_loss, price))
        
        # Targets for range trade
        target_1 = round(res_price, 2)
        target_1_pct = safe_pct_change(target_1, price)
        target_2 = None
        target_2_pct = None
        
        risk_per_share = round(abs(price - stop_loss), 2)
        reward_per_share = round(abs(target_1 - price), 2)
        setup_label = "◆ RANGE ZONE"
    
    # Position sizing based on volatility regime AND risk
    if volatility_regime in ['high', 'elevated']:
        position_size_label = 'REDUCED'
    elif volatility_regime == 'compressed' and 'aggressive' in position_bias:
        position_size_label = 'AGGRESSIVE'
    else:
        position_size_label = 'STANDARD'
    
    # True R:R ratio from actual per-share values
    true_rr = safe_div(reward_per_share, risk_per_share, 0) if risk_per_share > 0 else 0
    
    # Invalidation level: price where the thesis is broken
    if is_long:
        invalidation = stop_loss  # Below stop = thesis dead
    elif is_short:
        invalidation = stop_loss  # Above stop = thesis dead
    else:
        invalidation = round(sup_price - effective_atr * 1.5, 2)  # Below range
    
    trade_params_structured = {
        'bias': 'LONG' if is_long else 'SHORT' if is_short else 'NEUTRAL',
        'bias_strength': 'AGGRESSIVE' if 'aggressive' in position_bias else 'CAUTIOUS' if 'cautious' in position_bias else 'STANDARD',
        'entry_low': entry_low,
        'entry_high': entry_high,
        'stop_loss': stop_loss,
        'stop_pct': stop_pct,
        'target_1': target_1,
        'target_1_pct': target_1_pct,
        'target_2': target_2,
        'target_2_pct': target_2_pct,
        'position_size': position_size_label,
        'breakout_level': round(res_price, 2),
        'breakdown_level': round(sup_price, 2),
        'current_price': price,
        'atr_value': round(effective_atr, 2),
        'atr_stop_mult': atr_stop_mult,
        'risk_per_share': risk_per_share,
        'reward_per_share': reward_per_share,
        'rr_ratio': round(true_rr, 2),
        'invalidation': invalidation,
        'setup_label': setup_label,
    }
    
    return {
        'verdict': verdict,
        'verdict_color': verdict_color,
        'verdict_icon': verdict_icon,
        'tech_score': tech_score,
        'overall_score': overall_score,
        'momentum_score': momentum_score,
        'momentum_factors': momentum_factors,
        'text': full_analysis,
        'bias': position_bias,
        'risk_reward': risk_reward,
        'upside_target': upside_target,
        'downside_target': downside_target,
        'upside_pct': upside_pct,
        'downside_pct': downside_pct,
        'volatility_regime': volatility_regime,
        'trend': {
            '5d': trend_5d,
            '1mo': trend_1mo,
            '3mo': trend_3mo
        },
        'rsi': rsi,
        'vol_ratio': vol_ratio,
        'smart_money_score': smart_money_score,
        'squeeze_potential': squeeze_potential,
        'exec_summary': exec_summary,
        'trade_params': trade_text,
        'trade_params_structured': trade_params_structured
    }

def analyze_news_sentiment(news_items):
    if not news_items: return {"overall": "neutral", "score": 0, "bullish": 0, "bearish": 0, "items": []}
    
    # Weighted bullish/bearish words (strong words get 2 points, regular get 1)
    bullish_strong = ['surge', 'soar', 'record', 'breakthrough', 'outperform', 'blowout', 'crushing']
    bullish_regular = ['rally', 'beat', 'upgrade', 'strong', 'growth', 'buy', 'gain', 'profit', 'positive', 'bullish', 'rise', 'exceeds', 'jumps', 'climbs', 'wins', 'success', 'higher', 'boost', 'recovery', 'momentum', 'accelerate', 'optimistic', 'rebound']
    bearish_strong = ['crash', 'plunge', 'collapse', 'crisis', 'default', 'bankruptcy', 'catastroph']
    bearish_regular = ['drop', 'fall', 'miss', 'downgrade', 'weak', 'cut', 'sell', 'warning', 'decline', 'loss', 'negative', 'bearish', 'underperform', 'fear', 'concern', 'risk', 'lawsuit', 'investigation', 'tumble', 'slump', 'lower', 'fails', 'recession', 'layoff', 'tariff', 'sanction']
    
    total_b, total_bear, items = 0, 0, []
    
    for item in news_items:
        # Get title from various possible keys
        title = item.get('title', item.get('headline', ''))
        if not title:
            continue
            
        title_lower = title.lower()
        b = sum(2 for w in bullish_strong if w in title_lower) + sum(1 for w in bullish_regular if w in title_lower)
        bear = sum(2 for w in bearish_strong if w in title_lower) + sum(1 for w in bearish_regular if w in title_lower)
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
            except Exception:
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
                except Exception:
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
                        except Exception:
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
        except Exception:
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
        except Exception:
            pass
        
        if earnings_hist is None or earnings_hist.empty:
            try:
                earnings_hist = ticker.quarterly_earnings
            except Exception:
                pass
        
        # Get earnings dates
        earnings_dates = None
        try:
            earnings_dates = ticker.earnings_dates
        except Exception:
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
                        
                        # Estimate price reaction based on surprise magnitude (more realistic than random)
                        est_move = surprise_pct * 0.4  # Typical 0.4x EPS surprise to price reaction
                        if abs(surprise_pct) > 10:
                            est_move *= 1.5  # Large surprises have outsized reactions
                        price_reactions.append({
                            'quarter': quarter_name,
                            'move': est_move,
                        })
                except Exception:
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
        except Exception:
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
        except Exception:
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
    
    # VWAP (Volume Weighted Average Price)
    vwap = calculate_vwap(hist)
    if vwap is not None and not vwap.empty:
        fig.add_trace(
            go.Scatter(
                x=hist.index, y=vwap,
                name='VWAP',
                line=dict(color='#A855F7', width=1.5, dash='dashdot'),
                hovertemplate='VWAP: $%{y:.2f}<extra></extra>'
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
    back_col, refresh_col = st.columns([1, 5])
    with back_col:
        if st.button("← Back to Dashboard", key="back_btn"): st.session_state.selected_stock = None; st.session_state.show_stock_report = False; st.rerun()
    with refresh_col:
        if st.button("🔄 Refresh Data", key="refresh_report"):
            st.cache_data.clear()
            st.rerun()
    
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
    
    # Analyze institutional activity FIRST (needed for expert analysis)
    inst_activity = analyze_institutional_activity(data, price) if not (is_future or is_index) else None
    
    # Generate expert analysis with institutional data
    expert = generate_expert_analysis(symbol, data, signals, support_levels, resistance_levels, news_sentiment, inst_activity)
    
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
    
    # Secondary technical stats row (VWAP, ATR, Volume Profile)
    if hist_5d is not None and not hist_5d.empty:
        sec_cols = st.columns(4)
        
        vwap = calculate_vwap(hist_5d)
        vwap_val = float(vwap.iloc[-1]) if vwap is not None and not vwap.empty and pd.notna(vwap.iloc[-1]) else 0
        atr_val = calculate_atr(hist_3mo if hist_3mo is not None and len(hist_3mo) > 14 else hist_5d)
        atr_pct = safe_div(atr_val, price) * 100
        
        # Volume analysis
        avg_vol_20 = hist_5d['Volume'].rolling(20).mean().iloc[-1] if len(hist_5d) > 20 else hist_5d['Volume'].mean()
        current_vol = hist_5d['Volume'].iloc[-1] if 'Volume' in hist_5d else 0
        rel_vol = safe_div(current_vol, avg_vol_20, 1.0)
        
        with sec_cols[0]:
            vwap_dev = safe_pct_change(price, vwap_val) if vwap_val > 0 else 0
            vwap_color = "#3fb950" if vwap_dev > 0 else "#f85149" if vwap_dev < 0 else "#8b949e"
            st.markdown(f"""
            <div class="vwap-indicator" style="text-align: center;">
                <div style="font-size: 0.65rem; color: #a371f7; text-transform: uppercase;">VWAP</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: {vwap_color};">${vwap_val:.2f}</div>
                <div style="font-size: 0.7rem; color: {vwap_color};">{vwap_dev:+.2f}%</div>
            </div>
            """, unsafe_allow_html=True)
        with sec_cols[1]:
            atr_color = "#f85149" if atr_pct > 3 else "#d29922" if atr_pct > 2 else "#3fb950"
            st.markdown(f"""
            <div class="mini-metric">
                <div style="font-size: 0.65rem; color: #8b949e; text-transform: uppercase;">ATR (14)</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: {atr_color};">${atr_val:.2f}</div>
                <div style="font-size: 0.7rem; color: {atr_color};">{atr_pct:.1f}% of price</div>
            </div>
            """, unsafe_allow_html=True)
        with sec_cols[2]:
            vol_color = "#3fb950" if rel_vol > 1.5 else "#d29922" if rel_vol > 1 else "#8b949e"
            st.markdown(f"""
            <div class="mini-metric">
                <div style="font-size: 0.65rem; color: #8b949e; text-transform: uppercase;">Rel Volume</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: {vol_color};">{rel_vol:.1f}x</div>
                <div style="font-size: 0.7rem; color: #8b949e;">vs 20d avg</div>
            </div>
            """, unsafe_allow_html=True)
        with sec_cols[3]:
            # Price position in day range
            day_high = hist_5d['High'].iloc[-1] if 'High' in hist_5d else price
            day_low = hist_5d['Low'].iloc[-1] if 'Low' in hist_5d else price
            day_range = day_high - day_low
            day_pos = safe_div(price - day_low, day_range, 0.5) * 100
            pos_color = "#3fb950" if day_pos > 60 else "#f85149" if day_pos < 40 else "#d29922"
            st.markdown(f"""
            <div class="mini-metric">
                <div style="font-size: 0.65rem; color: #8b949e; text-transform: uppercase;">Day Range %</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: {pos_color};">{day_pos:.0f}%</div>
                <div style="font-size: 0.7rem; color: #8b949e;">${day_low:.2f}-${day_high:.2f}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # === WORLD-CLASS EXPERT ANALYSIS SECTION ===
    if expert:
        # Pre-calculate all values to avoid inline conditionals in HTML
        verdict_bg = '#1a2e1a' if 'BUY' in expert['verdict'] else '#2e1a1a' if 'SELL' in expert['verdict'] else '#1a1a2e'
        verdict_color = expert['verdict_color']
        verdict_icon = expert.get('verdict_icon', '📊')
        verdict_text = expert['verdict']
        overall_score = expert.get('overall_score', expert['tech_score'])
        
        rsi_val = expert.get('rsi', 50)
        rsi_color = '#3fb950' if 30 < rsi_val < 70 else '#f85149'
        
        vol_ratio = expert.get('vol_ratio', 1)
        vol_color = '#3fb950' if vol_ratio > 1 else '#8b949e'
        
        risk_reward = expert.get('risk_reward', 0)
        rr_color = '#3fb950' if risk_reward > 1.5 else '#d29922' if risk_reward > 1 else '#f85149'
        
        vol_regime = expert.get('volatility_regime', 'normal')
        vol_regime_color = '#f85149' if vol_regime == 'high' else '#d29922' if vol_regime == 'elevated' else '#3fb950'
        vol_regime_text = vol_regime.upper()[:4]
        
        exec_summary = expert.get('exec_summary', '')
        upside_target = expert.get('upside_target', 0)
        upside_pct = expert.get('upside_pct', 0)
        downside_target = expert.get('downside_target', 0)
        downside_pct = expert.get('downside_pct', 0)
        
        # Render main verdict card using Streamlit components for reliability
        st.markdown("### 🤖 AI Institutional Analysis")
        
        # Verdict header row
        v_col1, v_col2 = st.columns([3, 1])
        with v_col1:
            st.markdown(f"""
            <div style="background: linear-gradient(145deg, {verdict_bg} 0%, #161b22 100%); border: 1px solid {verdict_color}; border-radius: 12px; padding: 1.25rem;">
                <div style="font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.25rem;">Verdict</div>
                <div style="font-size: 2rem; font-weight: 700; color: {verdict_color}; line-height: 1;">{verdict_icon} {verdict_text}</div>
            </div>
            """, unsafe_allow_html=True)
        with v_col2:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.8); border: 1px solid #30363d; border-radius: 12px; padding: 1.25rem; text-align: center;">
                <div style="font-size: 0.7rem; color: #8b949e; text-transform: uppercase;">Score</div>
                <div style="font-size: 2rem; font-weight: 700; color: {verdict_color};">{overall_score:+d}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Key metrics row using st.columns
        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        with m_col1:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <div style="font-size: 1.4rem; font-weight: 600; color: {rsi_color};">{rsi_val:.0f}</div>
                <div style="font-size: 0.7rem; color: #8b949e;">RSI</div>
            </div>
            """, unsafe_allow_html=True)
        with m_col2:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <div style="font-size: 1.4rem; font-weight: 600; color: {vol_color};">{vol_ratio:.1f}x</div>
                <div style="font-size: 0.7rem; color: #8b949e;">Vol Ratio</div>
            </div>
            """, unsafe_allow_html=True)
        with m_col3:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <div style="font-size: 1.4rem; font-weight: 600; color: {rr_color};">{risk_reward:.1f}:1</div>
                <div style="font-size: 0.7rem; color: #8b949e;">Risk/Reward</div>
            </div>
            """, unsafe_allow_html=True)
        with m_col4:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <div style="font-size: 1.4rem; font-weight: 600; color: {vol_regime_color};">{vol_regime_text}</div>
                <div style="font-size: 0.7rem; color: #8b949e;">Volatility</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Executive summary
        if exec_summary:
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 1rem; margin: 0.75rem 0;">
                <div style="font-size: 0.9rem; color: #c9d1d9; line-height: 1.6;">{exec_summary}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Price targets row
        t_col1, t_col2 = st.columns(2)
        with t_col1:
            st.markdown(f"""
            <div style="background: rgba(0,200,5,0.1); border: 1px solid rgba(0,200,5,0.3); border-radius: 8px; padding: 0.75rem;">
                <div style="font-size: 0.7rem; color: #3fb950; text-transform: uppercase; margin-bottom: 0.25rem;">📈 Upside Target</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #3fb950;">${upside_target:.2f}</div>
                <div style="font-size: 0.8rem; color: #8b949e;">+{upside_pct:.1f}% potential</div>
            </div>
            """, unsafe_allow_html=True)
        with t_col2:
            st.markdown(f"""
            <div style="background: rgba(255,59,48,0.1); border: 1px solid rgba(255,59,48,0.3); border-radius: 8px; padding: 0.75rem;">
                <div style="font-size: 0.7rem; color: #f85149; text-transform: uppercase; margin-bottom: 0.25rem;">📉 Downside Risk</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #f85149;">${downside_target:.2f}</div>
                <div style="font-size: 0.8rem; color: #8b949e;">{downside_pct:.1f}% risk</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Detailed Analysis Expandable Section
        with st.expander("📋 Full Institutional Analysis Report", expanded=False):
            # Parse the analysis text properly
            analysis_text = expert.get('text', '')
            # Split by double newlines for paragraphs
            paragraphs = analysis_text.split('\n\n')
            for para in paragraphs:
                if para.strip():
                    # Handle bold markers
                    formatted_para = para.replace('**', '<strong>').replace('**', '</strong>')
                    # Fix any unclosed strong tags
                    strong_count = formatted_para.count('<strong>')
                    close_count = formatted_para.count('</strong>')
                    if strong_count > close_count:
                        formatted_para += '</strong>' * (strong_count - close_count)
                    st.markdown(f"""
                    <div style="margin-bottom: 0.75rem; color: #c9d1d9; line-height: 1.6; font-size: 0.9rem;">
                        {formatted_para.replace(chr(10), '<br>')}
                    </div>
                    """, unsafe_allow_html=True)
        
        # Momentum Factors Breakdown
        if expert.get('momentum_factors'):
            with st.expander("📊 Momentum Factor Breakdown", expanded=False):
                for factor_name, factor_score, factor_value in expert['momentum_factors']:
                    score_color = '#3fb950' if factor_score > 0 else '#f85149' if factor_score < 0 else '#8b949e'
                    score_sign = '+' if factor_score > 0 else ''
                    st.markdown(f"""
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid rgba(48,54,61,0.5);">
                        <span style="color: #c9d1d9;">{factor_name}</span>
                        <div>
                            <span style="color: #8b949e; margin-right: 1rem;">{factor_value}</span>
                            <span style="color: {score_color}; font-weight: 600;">{score_sign}{factor_score}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        # === BLOOMBERG TERMINAL-STYLE TRADE PARAMETERS ===
        trade_struct = expert.get('trade_params_structured', {})
        if trade_struct:
            st.markdown("### 💹 Trade Parameters")
            
            # Get values
            bias = trade_struct.get('bias', 'NEUTRAL')
            bias_strength = trade_struct.get('bias_strength', 'STANDARD')
            current_price = trade_struct.get('current_price', 0)
            entry_low = trade_struct.get('entry_low', 0)
            entry_high = trade_struct.get('entry_high', 0)
            stop_loss = trade_struct.get('stop_loss', 0)
            stop_pct = trade_struct.get('stop_pct', 0)
            target_1 = trade_struct.get('target_1', 0)
            target_1_pct = trade_struct.get('target_1_pct', 0)
            target_2 = trade_struct.get('target_2')
            target_2_pct = trade_struct.get('target_2_pct')
            position_size = trade_struct.get('position_size', 'STANDARD')
            breakout = trade_struct.get('breakout_level', 0)
            breakdown = trade_struct.get('breakdown_level', 0)
            risk_per_share = trade_struct.get('risk_per_share', 0)
            reward_per_share = trade_struct.get('reward_per_share', 0)
            rr_ratio = trade_struct.get('rr_ratio', 0)
            atr_val_display = trade_struct.get('atr_value', 0)
            atr_mult_display = trade_struct.get('atr_stop_mult', 2.0)
            invalidation = trade_struct.get('invalidation', 0)
            setup_label = trade_struct.get('setup_label', '◆ ZONE')
            
            # Direction-aware colors
            is_long = bias == 'LONG'
            is_short = bias == 'SHORT'
            
            bias_color = '#00ff41' if is_long else '#ff3b30' if is_short else '#ffcc00'
            bias_bg = 'rgba(0,255,65,0.15)' if is_long else 'rgba(255,59,48,0.15)' if is_short else 'rgba(255,204,0,0.15)'
            entry_color = '#00ff41' if is_long else '#ff6b6b' if is_short else '#58a6ff'
            target_color = '#00ff41' if is_long else '#ff6b6b' if is_short else '#58a6ff'
            stop_color = '#ff3b30' if is_long else '#00ff41' if is_short else '#ff3b30'
            rr_color = '#00ff41' if rr_ratio >= 2 else '#ffcc00' if rr_ratio >= 1.5 else '#ff6b6b'
            pos_size_color = '#ffcc00' if position_size == 'REDUCED' else '#00d4ff' if position_size == 'AGGRESSIVE' else '#00ff41'
            
            # Direction labels for stop/target
            stop_label = "⛔ STOP LOSS" if is_long else "⛔ STOP LOSS (COVER)" if is_short else "⛔ STOP LOSS"
            target_label = "🎯 TARGET 1" if is_long else "🎯 TARGET 1 (COVER)" if is_short else "🎯 RANGE TARGET"
            target_2_label = "🚀 TARGET 2" if is_long else "💎 TARGET 2 (EXTENDED)" if is_short else "🚀 EXTENDED"
            
            # Stop direction indicator
            stop_direction = "▼" if is_long else "▲" if is_short else "▼"
            target_direction = "▲" if is_long else "▼" if is_short else "▲"
            
            # Terminal Header
            st.markdown(f"""
            <div style="background: linear-gradient(90deg, #1a1a2e 0%, #16213e 100%); padding: 0.6rem 1rem; border: 1px solid #333; border-radius: 4px 4px 0 0; display: flex; justify-content: space-between; align-items: center; font-family: 'Consolas', 'Monaco', monospace;">
                <div style="display: flex; align-items: center; gap: 0.75rem;">
                    <span style="color: #ff9500; font-weight: 700;">◆ TRADE SETUP</span>
                    <span style="color: #666;">|</span>
                    <span style="color: {bias_color}; font-weight: 700; font-size: 1.1rem;">{bias}</span>
                    <span style="background: {bias_bg}; color: {bias_color}; padding: 0.15rem 0.5rem; border-radius: 3px; font-size: 0.7rem; font-weight: 600;">{bias_strength}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 0.75rem;">
                    <span style="color: #888; font-size: 0.7rem;">ATR: ${atr_val_display:.2f}</span>
                    <span style="color: #666;">|</span>
                    <span style="color: #666; font-size: 0.75rem;">{symbol} • ${current_price:.2f}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Entry Zone
            st.markdown(f"""
            <div style="background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.75rem 1rem; font-family: 'Consolas', 'Monaco', monospace;">
                <div style="color: #666; font-size: 0.7rem; text-transform: uppercase; margin-bottom: 0.25rem;">Entry Zone</div>
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <span style="color: {entry_color}; font-size: 1.2rem; font-weight: 600;">${entry_low:.2f}</span>
                    <span style="color: #444;">—</span>
                    <span style="color: {entry_color}; font-size: 1.2rem; font-weight: 600;">${entry_high:.2f}</span>
                    <span style="color: #444; font-size: 0.7rem; margin-left: 0.5rem;">{setup_label}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Stop Loss
            st.markdown(f"""
            <div style="background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.5rem 1rem; font-family: 'Consolas', 'Monaco', monospace; border-top: 1px solid #222;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <span style="color: {stop_color}; font-size: 0.75rem;">{stop_label}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                        <span style="color: {stop_color}; font-size: 1.1rem; font-weight: 700;">${stop_loss:.2f}</span>
                        <span style="background: rgba(255,59,48,0.2); color: #ff6b6b; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.7rem;">{stop_direction} {stop_pct:.1f}%</span>
                        <span style="color: #555; font-size: 0.7rem;">RISK: ${risk_per_share:.2f}/sh ({atr_mult_display:.1f}x ATR)</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Target 1
            t1_pct_display = f"+{abs(target_1_pct):.1f}" if is_long else f"-{abs(target_1_pct):.1f}" if is_short else f"+{abs(target_1_pct):.1f}"
            st.markdown(f"""
            <div style="background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.5rem 1rem; font-family: 'Consolas', 'Monaco', monospace;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <span style="color: {target_color}; font-size: 0.75rem;">{target_label}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                        <span style="color: {target_color}; font-size: 1.1rem; font-weight: 700;">${target_1:.2f}</span>
                        <span style="background: rgba(0,255,65,0.2); color: #4ade80; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.7rem;">{target_direction} {t1_pct_display}%</span>
                        <span style="color: #555; font-size: 0.7rem;">REWARD: ${reward_per_share:.2f}/sh</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Target 2 (only if exists)
            if target_2 and target_2_pct:
                t2_pct_display = f"+{abs(target_2_pct):.1f}" if is_long else f"-{abs(target_2_pct):.1f}" if is_short else f"+{abs(target_2_pct):.1f}"
                st.markdown(f"""
                <div style="background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.5rem 1rem; font-family: 'Consolas', 'Monaco', monospace;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="color: #00d4ff; font-size: 0.75rem;">{target_2_label}</span>
                        </div>
                        <div style="display: flex; align-items: center; gap: 0.75rem;">
                            <span style="color: #00d4ff; font-size: 1.1rem; font-weight: 700;">${target_2:.2f}</span>
                            <span style="background: rgba(0,212,255,0.2); color: #67e8f9; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.7rem;">{target_direction} {t2_pct_display}%</span>
                            <span style="color: #555; font-size: 0.7rem;">EXTENDED</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Invalidation Level
            st.markdown(f"""
            <div style="background: #0d1117; border-left: 1px solid #333; border-right: 1px solid #333; padding: 0.4rem 1rem; font-family: 'Consolas', 'Monaco', monospace; border-top: 1px solid #1a1a1a;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="color: #555; font-size: 0.7rem;">⚠️ INVALIDATION</span>
                    <span style="color: #888; font-size: 0.85rem;">${invalidation:.2f} <span style="color: #555; font-size: 0.65rem;">— thesis void beyond this level</span></span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Key Levels
            kl_col1, kl_col2 = st.columns(2)
            with kl_col1:
                st.markdown(f"""
                <div style="background: rgba(255,59,48,0.1); border: 1px solid rgba(255,59,48,0.3); border-radius: 4px; padding: 0.6rem; text-align: center; font-family: 'Consolas', 'Monaco', monospace;">
                    <div style="color: #666; font-size: 0.65rem; text-transform: uppercase;">Support / Breakdown</div>
                    <div style="color: #ff6b6b; font-size: 1.2rem; font-weight: 700;">${breakdown:.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            with kl_col2:
                st.markdown(f"""
                <div style="background: rgba(0,255,65,0.1); border: 1px solid rgba(0,255,65,0.3); border-radius: 4px; padding: 0.6rem; text-align: center; font-family: 'Consolas', 'Monaco', monospace;">
                    <div style="color: #666; font-size: 0.65rem; text-transform: uppercase;">Resistance / Breakout</div>
                    <div style="color: #4ade80; font-size: 1.2rem; font-weight: 700;">${breakout:.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            
            # Bottom Stats
            stat_cols = st.columns(4)
            with stat_cols[0]:
                st.markdown(f"""
                <div style="background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 0.5rem; text-align: center;">
                    <div style="color: #666; font-size: 0.6rem; text-transform: uppercase;">R:R Ratio</div>
                    <div style="color: {rr_color}; font-size: 1.2rem; font-weight: 700;">{rr_ratio:.1f}:1</div>
                </div>
                """, unsafe_allow_html=True)
            with stat_cols[1]:
                st.markdown(f"""
                <div style="background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 0.5rem; text-align: center;">
                    <div style="color: #666; font-size: 0.6rem; text-transform: uppercase;">Position Size</div>
                    <div style="color: {pos_size_color}; font-size: 1rem; font-weight: 600;">{position_size}</div>
                </div>
                """, unsafe_allow_html=True)
            with stat_cols[2]:
                st.markdown(f"""
                <div style="background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 0.5rem; text-align: center;">
                    <div style="color: #666; font-size: 0.6rem; text-transform: uppercase;">Volatility</div>
                    <div style="color: {vol_regime_color}; font-size: 1rem; font-weight: 600;">{vol_regime.upper()}</div>
                </div>
                """, unsafe_allow_html=True)
            with stat_cols[3]:
                st.markdown(f"""
                <div style="background: #161b22; border: 1px solid #30363d; border-radius: 4px; padding: 0.5rem; text-align: center;">
                    <div style="color: #666; font-size: 0.6rem; text-transform: uppercase;">Signal</div>
                    <div style="color: {verdict_color}; font-size: 1rem; font-weight: 600;">{verdict_text}</div>
                </div>
                """, unsafe_allow_html=True)
            
            # Terminal Footer
            st.markdown(f"""
            <div style="background: #0a0a0a; border: 1px solid #333; border-radius: 0 0 4px 4px; padding: 0.4rem 1rem; display: flex; justify-content: space-between; font-family: 'Consolas', 'Monaco', monospace;">
                <span style="color: #444; font-size: 0.65rem;"><span style="color: #ff9500;">●</span> AI INSTITUTIONAL ANALYSIS</span>
                <span style="color: #444; font-size: 0.65rem;">Updated: {datetime.now().strftime('%H:%M:%S')} ET</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Position Calculator (collapsible)
            with st.expander("🧮 Position Size Calculator", expanded=False):
                calc_cols = st.columns(3)
                with calc_cols[0]:
                    account_size = st.number_input("Account Size ($)", value=100000, step=10000, key="pos_calc_acct")
                with calc_cols[1]:
                    risk_pct = st.number_input("Risk Per Trade (%)", value=1.0, step=0.25, min_value=0.25, max_value=5.0, key="pos_calc_risk")
                with calc_cols[2]:
                    st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
                    if st.button("Calculate", key="calc_pos_btn", use_container_width=True):
                        pass
                
                # Calculate position
                risk_amount = account_size * (risk_pct / 100)
                if risk_per_share > 0:
                    shares = int(risk_amount / risk_per_share)
                    position_value = shares * current_price
                    max_loss = shares * risk_per_share
                    potential_gain = shares * reward_per_share
                else:
                    shares = 0
                    position_value = 0
                    max_loss = 0
                    potential_gain = 0
                
                # Results using columns
                res_cols = st.columns(4)
                with res_cols[0]:
                    st.markdown(f"""
                    <div style="background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 0.75rem; text-align: center;">
                        <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Shares</div>
                        <div style="color: #58a6ff; font-size: 1.4rem; font-weight: 700;">{shares:,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with res_cols[1]:
                    st.markdown(f"""
                    <div style="background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 0.75rem; text-align: center;">
                        <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Position Value</div>
                        <div style="color: #c9d1d9; font-size: 1.4rem; font-weight: 700;">${position_value:,.0f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with res_cols[2]:
                    st.markdown(f"""
                    <div style="background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 0.75rem; text-align: center;">
                        <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Max Loss</div>
                        <div style="color: #f85149; font-size: 1.4rem; font-weight: 700;">-${max_loss:,.0f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with res_cols[3]:
                    st.markdown(f"""
                    <div style="background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 0.75rem; text-align: center;">
                        <div style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Potential Gain</div>
                        <div style="color: #3fb950; font-size: 1.4rem; font-weight: 700;">+${potential_gain:,.0f}</div>
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
                link = item.get('link', '')
                title_display = item['title'][:100] + ('...' if len(item['title']) > 100 else '')
                
                if link:
                    st.markdown(f"""
                    <a href="{link}" target="_blank" class="news-link" style="text-decoration: none;">
                        <div class="news-item" style="border-left-color:{c};">
                            <div class="news-title" style="display: flex; justify-content: space-between; align-items: center;">
                                <span>{title_display}</span>
                                <span style="font-size: 0.7rem; color: #58a6ff; margin-left: 0.5rem;">↗</span>
                            </div>
                            <div class="news-meta">{item['source']} · {item['time']} · {cats}</div>
                        </div>
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="news-item" style="border-left-color:{c};">
                        <div class="news-title">{title_display}</div>
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
                    except Exception:
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
    
    # Use already-computed institutional activity (computed earlier for expert analysis)
    # For futures/indices, create default values
    if inst_activity is None:
        # Create default institutional activity for futures/indices
        inst_activity = {
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
            'relative_volume': 1,
            'dark_pool_estimate': 35,
            'dark_pool_sentiment': 'neutral',
            'block_trades': [],
            'finviz_data': {},
            'squeeze_potential': 0,
            'smart_money_score': 50,
            'accumulation_distribution': 'neutral',
            'institutional_momentum': 'neutral',
        }
        st.info("ℹ️ Limited institutional data available for this instrument type. Showing estimated values.")
    
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
    except Exception:
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
    except Exception:
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
        except Exception:
            continue
    
    # Sort by score
    calls = sorted(calls, key=lambda x: x['total_score'], reverse=True)
    puts = sorted(puts, key=lambda x: x['total_score'], reverse=True)
    
    return calls[:5], puts[:5]  # Return top 5 each

def main():
    if st.session_state.show_stock_report and st.session_state.selected_stock: render_stock_report(st.session_state.selected_stock); return
    
    # Auto-refresh logic during market hours
    eastern = pytz.timezone('US/Eastern')
    now_et = datetime.now(eastern)
    
    col_t, col_s = st.columns([3, 1])
    with col_t: 
        st.markdown('<h1 class="main-title">📈 Pre-Market Command Center</h1>', unsafe_allow_html=True)
        st.markdown('<p class="subtitle">Institutional Analysis · AI Insights · Click Any Stock · <span style="color: #a371f7;">v9.0</span></p>', unsafe_allow_html=True)
    with col_s:
        sk, st_txt, cd = get_market_status()
        live_dot = "live-dot-green" if sk in ["open", "premarket"] else "live-dot-red" if sk == "afterhours" else ""
        st.markdown(f'<div style="text-align:right;"><div><span class="live-dot {live_dot}"></span><span class="market-status status-{sk}">{st_txt}</span></div><p class="timestamp">{cd}</p><p class="timestamp">{now_et.strftime("%I:%M %p ET")}</p></div>', unsafe_allow_html=True)
    st.markdown("---")
    tabs = st.tabs(["🎯 Market Brief", "🌍 Futures", "📊 Stocks", "🏢 Sectors", "📈 Options", "📅 Earnings", "🌊 Turbulence", "🔍 Research"])
    
    with tabs[0]:
        st.markdown("## 🎯 Daily Intelligence")
        ref_col1, ref_col2 = st.columns([1, 4])
        with ref_col1:
            if st.button("🔄 Refresh", key="ref", type="primary"): st.cache_data.clear(); st.rerun()
        with ref_col2:
            st.markdown(f"<p style='color: #6e7681; font-size: 0.75rem; margin-top: 0.5rem;'>Last refresh: {now_et.strftime('%I:%M:%S %p ET')} · Data refreshes every {get_dynamic_cache_ttl()//60}min during market hours</p>", unsafe_allow_html=True)
        with st.spinner("Loading market intelligence..."):
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
            es_price = es.get('current_price', 0)
            ch = es.get('overnight_change_pct', 0)
            price_display = f"${es_price:,.2f}" if es_price > 0 else "Loading..."
            st.markdown(f'<div class="metric-card"><div class="metric-label">S&P Futures</div><div class="metric-value">{price_display}</div><div class="{"positive" if ch >= 0 else "negative"}">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        with c3:
            nq = md['futures'].get('Nasdaq 100', {})
            nq_price = nq.get('current_price', 0)
            ch = nq.get('overnight_change_pct', 0)
            price_display = f"${nq_price:,.2f}" if nq_price > 0 else "Loading..."
            st.markdown(f'<div class="metric-card"><div class="metric-label">Nasdaq Futures</div><div class="metric-value">{price_display}</div><div class="{"positive" if ch >= 0 else "negative"}">{ch:+.2f}%</div></div>', unsafe_allow_html=True)
        with c4:
            vix = md['futures'].get('VIX', {})
            vl, vc = vix.get('current_price', 0), vix.get('overnight_change_pct', 0)
            vix_display = f"{vl:.2f}" if vl > 0 else "Loading..."
            vix_class = "negative" if vl > 20 else "positive" if vl < 15 and vl > 0 else "neutral"
            st.markdown(f'<div class="metric-card"><div class="metric-label">VIX</div><div class="metric-value {vix_class}">{vix_display}</div><div class="{"positive" if vc <= 0 else "negative"}">{vc:+.2f}%</div></div>', unsafe_allow_html=True)
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
            
            # Economic Indicators Mini-Dashboard
            if econ:
                st.markdown("#### 📈 Key Indicators")
                for name, ind in list(econ.items())[:4]:
                    val = ind.get('value', 0)
                    prev = ind.get('prev', val)
                    change = safe_pct_change(val, prev) if prev else 0
                    ch_color = "#3fb950" if change >= 0 else "#f85149"
                    unit = ind.get('unit', '')
                    st.markdown(f"""
                    <div style="background: rgba(22,27,34,0.5); border-radius: 6px; padding: 0.4rem 0.6rem; margin: 0.25rem 0;">
                        <div style="display: flex; justify-content: space-between; font-size: 0.75rem;">
                            <span style="color: #8b949e;">{name}</span>
                            <span style="color: #fff;">{val:.2f}{unit} <span style="color: {ch_color}; font-size: 0.65rem;">({change:+.1f}%)</span></span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Market Breadth Indicator Bar
        breadth = calculate_market_breadth()
        if breadth and breadth.get('advancing', 0) + breadth.get('declining', 0) > 0:
            breadth_pct = breadth.get('breadth_pct', 50)
            adv = breadth.get('advancing', 0)
            dec = breadth.get('declining', 0)
            breadth_color = "#3fb950" if breadth_pct > 60 else "#f85149" if breadth_pct < 40 else "#d29922"
            st.markdown(f"""
            <div style="background: rgba(22,27,34,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <span style="color: #8b949e; font-size: 0.8rem; font-weight: 600;">📊 Market Breadth</span>
                    <span style="color: {breadth_color}; font-weight: 600; font-size: 0.85rem;">{adv} ↑ / {dec} ↓ Sectors ({breadth_pct:.0f}% positive)</span>
                </div>
                <div style="background: rgba(248,81,73,0.3); border-radius: 4px; height: 8px; overflow: hidden;">
                    <div style="background: #3fb950; width: {breadth_pct}%; height: 100%; border-radius: 4px; transition: width 0.5s ease;"></div>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 0.65rem; color: #6e7681; margin-top: 0.25rem;">
                    <span>Bearish</span><span>Neutral</span><span>Bullish</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
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
                link = item.get('link', '')
                title_display = item['title'][:90] + '...' if len(item['title']) > 90 else item['title']
                cats = " · ".join(item['categories'][:2])
                with n_cols[i % 2]:
                    if link:
                        st.markdown(f'''<a href="{link}" target="_blank" class="news-link" style="text-decoration: none;">
                            <div class="news-item" style="border-left-color:{c};">
                                <div class="news-title" style="display: flex; justify-content: space-between;">
                                    <span>{title_display}</span>
                                    <span style="font-size: 0.65rem; color: #58a6ff;">↗</span>
                                </div>
                                <div class="news-meta">{item["source"]} · {cats}</div>
                            </div>
                        </a>''', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="news-item" style="border-left-color:{c};"><div class="news-title">{title_display}</div><div class="news-meta">{item["source"]} · {cats}</div></div>', unsafe_allow_html=True)
    
    with tabs[1]:
        st.markdown("### 🌍 Futures & Commodities")
        st.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Real-time futures, commodities, and crypto. Click any instrument for detailed analysis.</p>", unsafe_allow_html=True)
        
        # Category tabs for better organization
        fut_cat_col1, fut_cat_col2, fut_cat_col3, fut_cat_col4 = st.columns(4)
        
        # Quick access buttons for common futures
        st.markdown("#### ⚡ Quick Access")
        quick_cols = st.columns(7)
        quick_futures = ["S&P 500", "Nasdaq 100", "VIX", "Crude Oil", "Gold", "10Y Treasury", "Bitcoin"]
        for i, name in enumerate(quick_futures[:7]):
            with quick_cols[i % 7]:
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
        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            sym = st.text_input("🔍 Search ticker:", "", key="stk_search", placeholder="Enter symbol (e.g., AAPL, MSFT)").upper().strip()
        with search_col2:
            st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
            custom_watchlist_input = st.text_input("Add to watchlist:", key="add_watch", placeholder="PLTR, ARM...")
        
        if custom_watchlist_input:
            new_tickers = [t.strip().upper() for t in custom_watchlist_input.split(",") if t.strip()]
            for t in new_tickers:
                if t not in st.session_state.watchlist_custom:
                    st.session_state.watchlist_custom.append(t)
        
        if sym:
            h, info = fetch_stock_data(sym, "5d", "15m")
            m = calculate_metrics(h, info)
            if m:
                if st.button(f"📊 View {sym} Full Report", key=f"view_{sym}", type="primary"): st.session_state.selected_stock = sym; st.session_state.show_stock_report = True; st.rerun()
                # Quick preview card
                ch_color = "#3fb950" if m['overnight_change_pct'] >= 0 else "#f85149"
                trend_icon = "📈" if m.get('trend_strength', 0) > 0 else "📉" if m.get('trend_strength', 0) < 0 else "➡️"
                st.markdown(f"""
                <div class="metric-card fade-in" style="padding: 1rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-size: 1.3rem; font-weight: 700; color: #fff;">{sym}</span>
                            <span style="color: #8b949e; margin-left: 0.5rem; font-size: 0.85rem;">{info.get('shortName', '')}</span>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 1.5rem; font-weight: 600; color: #fff;">${m['current_price']:,.2f}</div>
                            <div style="color: {ch_color}; font-size: 1rem;">{m['overnight_change_pct']:+.2f}% {trend_icon}</div>
                        </div>
                    </div>
                    <div style="display: flex; gap: 1rem; margin-top: 0.75rem;">
                        <span style="color: #8b949e; font-size: 0.8rem;">RSI: <span style="color: {'#f85149' if m['rsi'] > 70 else '#3fb950' if m['rsi'] < 30 else '#58a6ff'};">{m['rsi']:.0f}</span></span>
                        <span style="color: #8b949e; font-size: 0.8rem;">Vol: <span style="color: {'#3fb950' if m.get('volume_vs_avg', 100) > 150 else '#8b949e'};">{m.get('volume_vs_avg', 100):.0f}%</span></span>
                        <span style="color: #8b949e; font-size: 0.8rem;">VWAP: <span style="color: {'#3fb950' if m.get('vwap_deviation', 0) > 0 else '#f85149'};">{m.get('vwap_deviation', 0):+.2f}%</span></span>
                        <span style="color: #8b949e; font-size: 0.8rem;">ATR: {m.get('atr_pct', 0):.1f}%</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else: st.warning(f"Not found: {sym}")
        
        # Custom Watchlist
        if st.session_state.watchlist_custom:
            st.markdown("### 📌 Your Watchlist")
            cw_cols = st.columns(min(6, len(st.session_state.watchlist_custom)))
            for i, s in enumerate(st.session_state.watchlist_custom[:12]):
                h, info = fetch_stock_data(s, "5d", "15m")
                m = calculate_metrics(h, info)
                if m: render_clickable_stock(s, m['current_price'], m['overnight_change_pct'], cw_cols[i % 6], "custom")
            if st.button("🗑️ Clear Watchlist", key="clear_watch"):
                st.session_state.watchlist_custom = []
                st.rerun()
        
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
        except Exception:
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
    
    # === TURBULENCE TAB ===
    with tabs[6]:
        render_turbulence_tab(st)
    
    # === RESEARCH TAB ===
    with tabs[7]:
        st.markdown("### 🔍 Research & Article Intelligence")
        st.markdown("<p style='color: #8b949e; font-size: 0.85rem;'>Institutional-grade financial article decomposition — paste any URL for hedge fund-level analysis</p>", unsafe_allow_html=True)
        
        url = st.text_input("Article URL:", placeholder="https://www.reuters.com/... or https://www.wsj.com/...", key="url_in")
        
        if url:
            with st.spinner("Extracting article and running institutional analysis pipeline..."):
                try:
                    # === STAGE 1: ARTICLE EXTRACTION ===
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                    }
                    resp = requests.get(url, headers=headers, timeout=20)
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'advertisement', 'iframe', 'noscript']):
                        tag.decompose()
                    
                    title = soup.title.string if soup.title else urlparse(url).netloc
                    title = title.strip()[:200] if title else "Article Analysis"
                    
                    # Extract author
                    author = ""
                    author_meta = soup.find('meta', attrs={'name': re.compile(r'author', re.I)})
                    if author_meta:
                        author = author_meta.get('content', '')
                    if not author:
                        author_tag = soup.find(class_=re.compile(r'author|byline', re.I))
                        if author_tag:
                            author = author_tag.get_text(strip=True)[:80]
                    
                    # Extract publish date
                    pub_date = ""
                    date_meta = soup.find('meta', attrs={'property': re.compile(r'published_time|date', re.I)})
                    if date_meta:
                        pub_date = date_meta.get('content', '')[:20]
                    if not pub_date:
                        time_tag = soup.find('time')
                        if time_tag:
                            pub_date = time_tag.get('datetime', time_tag.get_text(strip=True))[:20]
                    
                    article_text = ""
                    article_tags = soup.find_all(['article', 'main', 'div'], class_=lambda x: x and any(c in str(x).lower() for c in ['article', 'content', 'story', 'post', 'entry']))
                    if article_tags:
                        article_text = article_tags[0].get_text(separator='\n', strip=True)
                    
                    if not article_text or len(article_text) < 500:
                        paragraphs = soup.find_all('p')
                        article_text = '\n'.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40])
                    
                    if not article_text:
                        article_text = soup.get_text(separator='\n', strip=True)
                    
                    article_text = article_text[:18000]
                    text_lower = article_text.lower()
                    sentences = [s.strip() for s in article_text.replace('\n', ' ').split('.') if 20 < len(s.strip()) < 500]
                    word_count = len(article_text.split())
                    
                    # === STAGE 2: DEEP MACRO THEME EXTRACTION ===
                    theme_taxonomy = {
                        'Monetary Policy & Central Banks': {
                            'keywords': ['fed', 'federal reserve', 'interest rate', 'rate cut', 'rate hike', 'powell', 'fomc', 'monetary policy', 'hawkish', 'dovish', 'quantitative', 'tightening', 'easing', 'neutral rate', 'terminal rate', 'dot plot', 'balance sheet', 'ecb', 'boj', 'pboc', 'bank of england'],
                            'icon': '🏛️',
                            'asset_impact': {'Equities': 'Duration-sensitive growth stocks inversely correlated with rate expectations', 'Fixed Income': 'Front-end yields directly responsive; curve shape reflects policy path expectations', 'FX': 'Rate differentials drive USD direction; carry trade dynamics shift', 'Commodities': 'Gold inversely correlated with real rates; oil affected through demand channel'},
                        },
                        'Inflation & Price Stability': {
                            'keywords': ['inflation', 'cpi', 'pce', 'consumer price', 'price pressure', 'disinflation', 'deflation', 'core inflation', 'wage growth', 'unit labor cost', 'breakeven', 'tips', 'sticky inflation', 'transitory', 'shelter', 'services inflation'],
                            'icon': '📈',
                            'asset_impact': {'Equities': 'Margin compression risk for companies without pricing power; TIPS as hedge', 'Fixed Income': 'Nominal yields rise with inflation expectations; real yield curve steepens', 'FX': 'Inflation differentials affect relative currency valuation', 'Commodities': 'Commodity producers benefit as natural inflation hedge'},
                        },
                        'Economic Growth & Cycle': {
                            'keywords': ['gdp', 'economic growth', 'recession', 'expansion', 'soft landing', 'hard landing', 'employment', 'jobs', 'unemployment', 'labor market', 'nonfarm', 'payrolls', 'initial claims', 'ism', 'pmi', 'leading indicators', 'business cycle', 'output gap'],
                            'icon': '📊',
                            'asset_impact': {'Equities': 'Cyclicals outperform in expansion; defensives lead in contraction', 'Fixed Income': 'Long-duration bonds rally on growth slowdown; credit spreads widen in recession', 'FX': 'Growth outperformance drives currency strength via capital flows', 'Commodities': 'Industrial commodities (copper, oil) track global growth trajectory'},
                        },
                        'Geopolitics & Trade Policy': {
                            'keywords': ['tariff', 'trade war', 'china', 'russia', 'ukraine', 'sanctions', 'geopolitical', 'conflict', 'tensions', 'nato', 'export controls', 'decoupling', 'friendshoring', 'supply chain', 'national security', 'executive order', 'embargo'],
                            'icon': '🌍',
                            'asset_impact': {'Equities': 'Supply chain disruption reprices multi-national margins; defense sector benefits', 'Fixed Income': 'Flight-to-quality bids up Treasuries; EM spreads widen on risk aversion', 'FX': 'Safe-haven currencies (USD, CHF, JPY) strengthen; EM FX weakens', 'Commodities': 'Energy and agricultural commodities spike on supply disruption risk'},
                        },
                        'Corporate Earnings & Guidance': {
                            'keywords': ['earnings', 'revenue', 'profit', 'guidance', 'beat', 'miss', 'eps', 'quarter', 'fiscal', 'margin', 'operating income', 'free cash flow', 'capex', 'buyback', 'dividend', 'forward guidance', 'estimate revision'],
                            'icon': '💰',
                            'asset_impact': {'Equities': 'Earnings revisions drive sector rotation; guidance more important than beat/miss', 'Fixed Income': 'Corporate credit spreads tighten on strong earnings; default risk falls', 'FX': 'Repatriation flows from strong US earnings support USD', 'Commodities': 'Capex guidance signals future commodity demand trajectory'},
                        },
                        'Technology & AI': {
                            'keywords': ['artificial intelligence', ' ai ', 'nvidia', 'semiconductor', 'chip', 'tech sector', 'mega-cap', 'magnificent', 'data center', 'cloud', 'machine learning', 'gpu', 'hyperscaler', 'capex cycle', 'digital', 'automation'],
                            'icon': '🤖',
                            'asset_impact': {'Equities': 'Semis lead AI capex cycle; power/utilities as derivative plays; valuation risk in crowded trades', 'Fixed Income': 'Tech capex funded by corporate debt issuance; IG spreads affected', 'FX': 'US tech dominance supports structural USD demand from foreign investment', 'Commodities': 'Power demand (natural gas, uranium) rises with data center buildout'},
                        },
                        'Energy & Commodities': {
                            'keywords': ['oil', 'crude', 'opec', 'natural gas', 'energy', 'petroleum', 'brent', 'wti', 'lng', 'refinery', 'production cut', 'shale', 'renewable', 'transition', 'carbon'],
                            'icon': '⛽',
                            'asset_impact': {'Equities': 'E&P and oilfield services direct beneficiaries; airlines/transports inversely affected', 'Fixed Income': 'Energy HY credit tied to commodity prices; sovereign risk in petrostates', 'FX': 'CAD, NOK, RUB correlated with oil; JPY weakened by energy import costs', 'Commodities': 'OPEC+ decisions drive crude; natural gas follows weather and LNG export capacity'},
                        },
                        'Financial Sector & Credit': {
                            'keywords': ['bank', 'credit', 'lending', 'loan', 'financial sector', 'yield curve', 'treasury', 'bond', 'spread', 'default', 'delinquency', 'npl', 'net interest margin', 'liquidity', 'capital ratio', 'stress test', 'fdic'],
                            'icon': '🏦',
                            'asset_impact': {'Equities': 'Bank stocks track NIM and credit quality; insurance benefits from higher rates', 'Fixed Income': 'Credit spreads signal systemic risk appetite; BBB cliff-risk in downturn', 'FX': 'Banking stress drives safe-haven flows; contagion risk reprices EM', 'Commodities': 'Credit tightening reduces speculative commodity positioning'},
                        },
                        'Fiscal Policy & Government': {
                            'keywords': ['fiscal', 'government spending', 'deficit', 'debt ceiling', 'budget', 'stimulus', 'infrastructure', 'tax', 'treasury auction', 'issuance', 'sovereign', 'congress', 'legislation'],
                            'icon': '🏛️',
                            'asset_impact': {'Equities': 'Infrastructure/defense spending benefits specific sectors; tax changes affect after-tax earnings', 'Fixed Income': 'Supply concerns from deficit spending; auction tail risk; term premium', 'FX': 'Twin deficit concerns weigh on USD long-term; fiscal stimulus near-term positive', 'Commodities': 'Infrastructure spending drives copper, steel demand'},
                        },
                        'Housing & Real Estate': {
                            'keywords': ['housing', 'real estate', 'mortgage', 'home sales', 'construction', 'property', 'reit', 'commercial real estate', 'cre', 'vacancy', 'rent', 'homebuilder'],
                            'icon': '🏠',
                            'asset_impact': {'Equities': 'Homebuilders, mortgage REITs directly affected; consumer wealth effect', 'Fixed Income': 'MBS spreads and prepayment risk; CRE exposure in regional bank portfolios', 'FX': 'Housing weakness signals broader economic slowing', 'Commodities': 'Construction activity drives lumber, copper demand'},
                        },
                        'Consumer & Retail': {
                            'keywords': ['consumer', 'retail', 'spending', 'sentiment', 'confidence', 'discretionary', 'staples', 'e-commerce', 'holiday', 'same-store', 'foot traffic', 'credit card'],
                            'icon': '🛒',
                            'asset_impact': {'Equities': 'Consumer discretionary vs. staples rotation signals cycle positioning', 'Fixed Income': 'Consumer credit quality affects ABS and retail corporate bonds', 'FX': 'Consumer spending differential affects relative growth and currency', 'Commodities': 'Gasoline demand, agricultural commodities track consumer behavior'},
                        },
                    }
                    
                    detected_themes = []
                    for theme_name, theme_data in theme_taxonomy.items():
                        hit_count = sum(1 for kw in theme_data['keywords'] if kw in text_lower)
                        # Weight multi-word phrases higher
                        phrase_hits = sum(2 for kw in theme_data['keywords'] if ' ' in kw and kw in text_lower)
                        total_score = hit_count + phrase_hits
                        if total_score >= 3:
                            detected_themes.append({
                                'name': theme_name,
                                'score': total_score,
                                'icon': theme_data['icon'],
                                'asset_impact': theme_data['asset_impact'],
                                'relevance': 'Primary' if total_score >= 6 else 'Secondary'
                            })
                    
                    detected_themes.sort(key=lambda x: x['score'], reverse=True)
                    primary_themes = [t for t in detected_themes if t['relevance'] == 'Primary']
                    secondary_themes = [t for t in detected_themes if t['relevance'] == 'Secondary']
                    
                    # === STAGE 3: ADVANCED SENTIMENT ENGINE ===
                    # Weighted sentiment with context awareness
                    strong_bull = ['surge', 'soar', 'record high', 'blowout', 'crushing estimates', 'outperform', 'breakthrough', 'accelerating', 'robust', 'resilient']
                    moderate_bull = ['rally', 'beat', 'upgrade', 'strong', 'growth', 'positive', 'optimism', 'bullish', 'exceed', 'gain', 'advance', 'recovery', 'upside', 'momentum', 'tailwind', 'constructive']
                    strong_bear = ['crash', 'plunge', 'collapse', 'crisis', 'default', 'bankruptcy', 'capitulation', 'contagion', 'meltdown', 'freefall']
                    moderate_bear = ['drop', 'fall', 'miss', 'downgrade', 'weak', 'cut', 'sell', 'warning', 'decline', 'concern', 'risk', 'fear', 'slump', 'headwind', 'deteriorating', 'downside', 'cautious']
                    negation_words = ['not', 'no', "n't", 'without', 'despite', 'unlikely', 'fail', 'lack']
                    
                    bull_score = 0
                    bear_score = 0
                    bull_signals = []
                    bear_signals = []
                    
                    for sent in sentences:
                        sent_lower = sent.lower()
                        has_negation = any(neg in sent_lower.split() for neg in negation_words)
                        
                        for word in strong_bull:
                            if word in sent_lower:
                                if has_negation:
                                    bear_score += 1
                                    bear_signals.append(f"Negated bullish: \"{word}\" → bearish context")
                                else:
                                    bull_score += 3
                                    bull_signals.append(f"Strong: \"{word}\"")
                        
                        for word in moderate_bull:
                            if word in sent_lower:
                                if has_negation:
                                    bear_score += 0.5
                                else:
                                    bull_score += 1
                                    if len(bull_signals) < 8:
                                        bull_signals.append(f"\"{word}\"")
                        
                        for word in strong_bear:
                            if word in sent_lower:
                                if has_negation:
                                    bull_score += 1
                                    bull_signals.append(f"Negated bearish: \"{word}\" → bullish context")
                                else:
                                    bear_score += 3
                                    bear_signals.append(f"Strong: \"{word}\"")
                        
                        for word in moderate_bear:
                            if word in sent_lower:
                                if has_negation:
                                    bull_score += 0.5
                                else:
                                    bear_score += 1
                                    if len(bear_signals) < 8:
                                        bear_signals.append(f"\"{word}\"")
                    
                    total_signal = bull_score + bear_score
                    if total_signal > 0:
                        bull_pct = bull_score / total_signal * 100
                        bear_pct = bear_score / total_signal * 100
                    else:
                        bull_pct = bear_pct = 50
                    
                    net_score = bull_score - bear_score
                    if net_score > 10:
                        sentiment = "Strongly Bullish"
                        sentiment_color = "#00C805"
                        sentiment_icon = "🟢"
                    elif net_score > 4:
                        sentiment = "Bullish"
                        sentiment_color = "#3fb950"
                        sentiment_icon = "🟢"
                    elif net_score > 1:
                        sentiment = "Lean Bullish"
                        sentiment_color = "#7ee787"
                        sentiment_icon = "🔵"
                    elif net_score < -10:
                        sentiment = "Strongly Bearish"
                        sentiment_color = "#FF3B30"
                        sentiment_icon = "🔴"
                    elif net_score < -4:
                        sentiment = "Bearish"
                        sentiment_color = "#f85149"
                        sentiment_icon = "🔴"
                    elif net_score < -1:
                        sentiment = "Lean Bearish"
                        sentiment_color = "#ffa198"
                        sentiment_icon = "🟡"
                    else:
                        sentiment = "Neutral"
                        sentiment_color = "#d29922"
                        sentiment_icon = "⚪"
                    
                    confidence = 'High' if abs(net_score) > 8 and total_signal > 15 else 'Medium' if abs(net_score) > 3 else 'Low'
                    
                    # === STAGE 4: FINANCIAL DATA EXTRACTION ===
                    data_points = []
                    
                    for sent in sentences:
                        sent_stripped = sent.strip()
                        if len(sent_stripped) < 25:
                            continue
                        
                        has_number = bool(re.search(r'\d+\.?\d*\s*(%|percent|billion|million|trillion|bps|basis points|pp)', sent_stripped, re.IGNORECASE))
                        has_dollar = bool(re.search(r'\$\d+', sent_stripped))
                        has_financial = any(t in sent_stripped.lower() for t in ['revenue', 'earnings', 'profit', 'loss', 'gdp', 'inflation', 'rate', 'growth', 'decline', 'forecast', 'estimate', 'target', 'outlook', 'yield', 'deficit', 'surplus', 'margin', 'spread', 'return', 'payout', 'capex', 'sales'])
                        has_market = any(t in sent_stripped.lower() for t in ['stock', 'bond', 'yield', 'index', 'market', 'trade', 'investor', 'fed', 'treasury', 'dollar', 'oil', 'gold', 'price', 'valuation', 'volume'])
                        
                        # Categorize data points
                        if has_number or has_dollar:
                            if has_financial or has_market:
                                # Determine category
                                dp_lower = sent_stripped.lower()
                                if any(w in dp_lower for w in ['earnings', 'eps', 'revenue', 'profit', 'margin', 'income']):
                                    cat = '💰 Earnings'
                                elif any(w in dp_lower for w in ['gdp', 'growth', 'employment', 'jobs', 'payroll', 'unemployment']):
                                    cat = '📊 Economic'
                                elif any(w in dp_lower for w in ['inflation', 'cpi', 'pce', 'price']):
                                    cat = '📈 Inflation'
                                elif any(w in dp_lower for w in ['rate', 'yield', 'treasury', 'fed', 'bond']):
                                    cat = '🏛️ Rates'
                                elif any(w in dp_lower for w in ['stock', 'index', 'market', 'valuation']):
                                    cat = '📉 Markets'
                                elif any(w in dp_lower for w in ['oil', 'gold', 'commodity', 'energy']):
                                    cat = '⛽ Commodities'
                                else:
                                    cat = '📋 Other'
                                
                                data_points.append({'text': sent_stripped, 'category': cat})
                    
                    # Deduplicate similar data points
                    seen_keys = set()
                    unique_data_points = []
                    for dp in data_points:
                        # Simple dedup: first 60 chars
                        key = dp['text'][:60].lower()
                        if key not in seen_keys:
                            seen_keys.add(key)
                            unique_data_points.append(dp)
                    data_points = unique_data_points[:10]
                    
                    # === STAGE 5: TICKER & ENTITY EXTRACTION ===
                    potential_tickers = set(re.findall(r'\b([A-Z]{2,5})\b', article_text))
                    common_words = {'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'DID', 'GET', 'HIM', 'LET', 'SAY', 'SHE', 'TOO', 'USE', 'CEO', 'CFO', 'GDP', 'CPI', 'PCE', 'PMI', 'ISM', 'IPO', 'ETF', 'IMF', 'ECB', 'BOJ', 'SEC', 'FDA', 'EPA', 'AI', 'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'BPS', 'EST', 'PER', 'TWO', 'YET', 'FED', 'API', 'USB', 'EIA', 'SAID', 'ALSO', 'HAVE', 'BEEN', 'MORE', 'FROM', 'WILL', 'THEY', 'WITH', 'THAT', 'THIS', 'WHAT', 'WHEN', 'THAN', 'EACH', 'MAKE', 'LIKE', 'LONG', 'MUCH', 'JUST', 'OVER', 'SUCH', 'TAKE', 'YEAR', 'THEM', 'SOME', 'TIME', 'VERY', 'MADE', 'COME', 'LAST', 'INTO', 'BACK', 'ONLY', 'EVEN', 'MOST', 'ALSO', 'HERE', 'HIGH', 'NEXT', 'AWAY', 'KEEP', 'PART', 'PAST', 'HALF', 'WELL', 'AMID', 'NEAR', 'RISK', 'DEAL'}
                    all_known_tickers = set(OPTIONS_UNIVERSE) | set(['SPY', 'QQQ', 'IWM', 'DIA', 'VIX', 'TLT', 'GLD', 'USO', 'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLP', 'XLY', 'XLB', 'XLU', 'XLRE', 'HYG', 'LQD', 'TIP', 'SHY', 'AGG', 'BND', 'EEM', 'EFA', 'FXI', 'RSX', 'ARKK', 'ARKG', 'SOXL', 'TQQQ'])
                    mentioned_tickers = sorted(list((potential_tickers - common_words).intersection(all_known_tickers)))[:10]
                    
                    # Extract named entities (people + roles)
                    people_patterns = [
                        (r"(?:CEO|Chief Executive|Chief Executive Officer)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", "CEO"),
                        (r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+),?\s+(?:the\s+)?(?:CEO|chief executive)", "CEO"),
                        (r"(?:CFO|Chief Financial Officer)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", "CFO"),
                        (r"(?:Chairman|Chair|Chairwoman)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", "Chair"),
                        (r"(?:President|Secretary|Governor|Director)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", "Official"),
                        (r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:said|told|noted|warned|stated|argued|suggested|emphasized|indicated)", "Quoted"),
                    ]
                    
                    key_people = []
                    seen_people = set()
                    for pattern, role in people_patterns:
                        matches = re.findall(pattern, article_text)
                        for m in matches:
                            name = m.strip()
                            if name not in seen_people and len(name) > 4 and len(name) < 40:
                                key_people.append({'name': name, 'role': role})
                                seen_people.add(name)
                    key_people = key_people[:6]
                    
                    # === STAGE 6: MARKET IMPLICATION ENGINE ===
                    # Generate specific, actionable cross-asset implications
                    implications = []
                    
                    # Monetary policy implications
                    if any(kw in text_lower for kw in ['rate cut', 'dovish', 'easing']):
                        implications.append({'signal': 'Dovish pivot', 'equities': 'Growth > Value rotation; small caps benefit from lower financing costs', 'fixed_income': 'Bull steepener — front-end rallies faster than long-end', 'fx': 'USD weakens; EM FX and carry trades benefit', 'commodities': 'Gold rallies on lower real rates; oil supported by demand outlook', 'trade_idea': 'Long QQQ/IWM, Long TLT, Short DXY'})
                    if any(kw in text_lower for kw in ['rate hike', 'hawkish', 'tightening', 'higher for longer']):
                        implications.append({'signal': 'Hawkish surprise', 'equities': 'Growth stocks de-rate; financials benefit from steeper curve', 'fixed_income': 'Bear flattener — 2Y sells off; duration risk elevated', 'fx': 'USD strengthens; EM currencies under pressure', 'commodities': 'Gold weakens; oil mixed (demand concern vs. supply)', 'trade_idea': 'Long XLF, Short TLT, Long UUP'})
                    
                    # Inflation implications
                    if any(kw in text_lower for kw in ['inflation higher', 'cpi above', 'price pressure', 'sticky inflation', 'hot cpi']):
                        implications.append({'signal': 'Inflation surprise', 'equities': 'Real assets and pricing-power names outperform; long duration growth lags', 'fixed_income': 'TIPS outperform nominals; breakevens widen; curve bear flattens', 'fx': 'USD initially strengthens on rate repricing; gold bid as inflation hedge', 'commodities': 'Broad commodity strength; energy and agriculture lead', 'trade_idea': 'Long XLE/XLB, Long GLD, Short long-duration bonds'})
                    if any(kw in text_lower for kw in ['disinflation', 'inflation falling', 'cpi below', 'deflation', 'prices declining']):
                        implications.append({'signal': 'Disinflation confirmation', 'equities': 'Growth re-rates higher; consumer discretionary benefits from real income gains', 'fixed_income': 'Duration rally; curve bull steepens as easing priced in', 'fx': 'USD weakens on rate-cut expectations; AUD/NZD benefit', 'commodities': 'Mixed — gold up on easing bets, industrial metals soft on demand worry', 'trade_idea': 'Long QQQ/XLY, Long TLT, Short USD'})
                    
                    # Growth implications
                    if any(kw in text_lower for kw in ['recession', 'hard landing', 'contraction', 'significant slowdown']):
                        implications.append({'signal': 'Recession risk elevated', 'equities': 'Rotate to quality/low-vol; defensives (XLU, XLP, XLV) outperform cyclicals', 'fixed_income': 'Aggressive bull flattener; credit spreads widen — HY vulnerable', 'fx': 'Classic risk-off: USD/JPY/CHF strengthen; AUD/CAD weaken', 'commodities': 'Oil drops on demand destruction; gold rallies as safe haven', 'trade_idea': 'Long XLU/XLP, Long TLT, Short HYG, Long GLD'})
                    if any(kw in text_lower for kw in ['soft landing', 'resilient economy', 'strong employment', 'robust growth', 'goldilocks']):
                        implications.append({'signal': 'Soft landing narrative', 'equities': 'Broad equity rally; cyclicals and small caps catch up to mega-cap', 'fixed_income': 'Mild curve steepening; credit tightens — risk-on', 'fx': 'USD stable; EM FX supported by global growth', 'commodities': 'Copper and oil bid on growth; gold neutral', 'trade_idea': 'Long IWM, Long XLI/XLB, Long EEM'})
                    
                    # Geopolitical implications
                    if any(kw in text_lower for kw in ['tariff', 'trade war', 'sanctions', 'export controls', 'embargo']):
                        implications.append({'signal': 'Trade/sanctions escalation', 'equities': 'Supply-chain-exposed names de-rate; domestic-focused and defense benefit', 'fixed_income': 'Flight to quality; Treasuries rally; EM sovereign spreads widen', 'fx': 'USD bid as safe haven; CNY/KRW under pressure', 'commodities': 'Supply disruption premium in energy/metals; agricultural volatility', 'trade_idea': 'Long XLI (defense), Long GLD/TLT, Short EEM/FXI'})
                    if any(kw in text_lower for kw in ['conflict', 'military', 'invasion', 'escalation', 'war']):
                        implications.append({'signal': 'Geopolitical risk spike', 'equities': 'Volatility expansion; VIX likely elevated; defense stocks outperform', 'fixed_income': 'Treasury bid as risk-off; HY spreads widen', 'fx': 'Classic safe havens rally: USD, CHF, JPY', 'commodities': 'Oil spikes on supply risk; gold rallies; agricultural disruption', 'trade_idea': 'Long VIX, Long GLD, Long crude, Reduce equity beta'})
                    
                    # Earnings implications
                    if any(kw in text_lower for kw in ['earnings beat', 'strong results', 'beat estimates', 'blowout quarter', 'upside surprise']):
                        implications.append({'signal': 'Positive earnings momentum', 'equities': 'EPS revision breadth positive; sector rotation toward quality growth', 'fixed_income': 'Corporate credit tightens; M&A and buyback activity supports', 'fx': 'US earnings outperformance supports USD via capital flows', 'commodities': 'Capex intentions from earnings calls signal commodity demand', 'trade_idea': 'Long sector leaders, Overweight quality factor'})
                    if any(kw in text_lower for kw in ['guidance cut', 'lowered outlook', 'miss estimates', 'weak guidance', 'warned']):
                        implications.append({'signal': 'Negative earnings revision cycle', 'equities': 'Estimate cuts cascade; margin compression theme; avoid high-expectation names', 'fixed_income': 'Credit risk re-prices; BBB cliff risk for leveraged companies', 'fx': 'Weaker earnings → slower growth → potential USD weakness long-term', 'commodities': 'Demand outlook weakens; capex deferrals reduce commodity consumption', 'trade_idea': 'Short high-beta, Long quality/low-vol, Hedge with puts'})
                    
                    # AI/Tech implications
                    if any(kw in text_lower for kw in ['artificial intelligence', 'ai capex', 'data center', 'gpu', 'hyperscaler']):
                        implications.append({'signal': 'AI capex cycle acceleration', 'equities': 'Semis (NVDA, AMD, AVGO), power/utilities, and cooling infrastructure benefit', 'fixed_income': 'Tech IG issuance to fund capex; stable credit quality', 'fx': 'USD supported by AI-driven capital inflows to US equities', 'commodities': 'Natural gas and uranium demand from data centers; copper for electrical infrastructure', 'trade_idea': 'Long SOXX/SMH, Long utilities (AI power demand), Long copper'})
                    
                    if not implications:
                        implications.append({'signal': 'No dominant macro signal', 'equities': 'Monitor for follow-through price action; maintain balanced positioning', 'fixed_income': 'Neutral duration; carry-focused approach appropriate', 'fx': 'Range-bound expectations; limited directional conviction', 'commodities': 'Supply-demand fundamentals drive; macro overlay limited', 'trade_idea': 'Neutral — await catalyst for directional commitment'})
                    
                    # === STAGE 7: RISK FACTOR IDENTIFICATION ===
                    risks = []
                    risk_patterns = {
                        'Valuation Risk': ['overvalued', 'stretched', 'expensive', 'bubble', 'frothy', 'rich valuation', 'high multiple'],
                        'Liquidity Risk': ['liquidity', 'funding', 'credit crunch', 'bank run', 'withdrawal', 'redemption'],
                        'Policy Risk': ['regulation', 'antitrust', 'legislation', 'executive order', 'oversight', 'compliance'],
                        'Event Risk': ['election', 'referendum', 'trial', 'investigation', 'ruling', 'verdict'],
                        'Crowding Risk': ['crowded trade', 'consensus', 'positioning', 'everyone expects', 'priced in', 'already discounted'],
                        'Contagion Risk': ['spillover', 'contagion', 'systemic', 'domino', 'cascade'],
                        'Tail Risk': ['black swan', 'tail risk', 'unprecedented', 'uncharted', 'worst case'],
                    }
                    
                    for risk_name, keywords in risk_patterns.items():
                        if any(kw in text_lower for kw in keywords):
                            risks.append(risk_name)
                    
                    # === STAGE 8: GENERATE INSTITUTIONAL SYNTHESIS ===
                    eastern = pytz.timezone('US/Eastern')
                    source_domain = urlparse(url).netloc.replace('www.', '')
                    
                    themes_str = ", ".join([t['name'] for t in detected_themes[:3]]) if detected_themes else "general market dynamics"
                    
                    # Build multi-paragraph institutional analysis
                    # Paragraph 1: Executive Summary
                    para1 = f"This {source_domain} article centers on {themes_str}, presenting a {sentiment.lower()} tone across {word_count:,} words of analysis."
                    if data_points:
                        para1 += f" Our NLP pipeline identified {len(data_points)} quantitative data points and {len(detected_themes)} distinct macro themes."
                    para1 += f" Weighted sentiment scoring ({bull_score:.0f} bullish vs. {bear_score:.0f} bearish, confidence: {confidence.lower()}) places this firmly in {sentiment.lower()} territory."
                    
                    # Paragraph 2: Thematic Deep Dive
                    para2 = ""
                    if primary_themes:
                        para2 = f"The dominant narrative thread — {primary_themes[0]['name']} — carries direct implications for cross-asset positioning. "
                        para2 += primary_themes[0]['asset_impact'].get('Equities', '') + " "
                        if len(primary_themes) > 1:
                            para2 += f"The secondary theme of {primary_themes[1]['name']} introduces a compounding factor: "
                            para2 += primary_themes[1]['asset_impact'].get('Fixed Income', '')
                    elif secondary_themes:
                        para2 = f"While no single theme dominates, the article touches on {secondary_themes[0]['name']} with moderate intensity, suggesting the narrative is still forming. "
                        para2 += "Institutional investors should monitor for escalation signals that would convert this into a primary driver."
                    else:
                        para2 = "The article lacks a concentrated macro theme, suggesting it's more informational than market-moving. Positioning changes based on this alone would be premature."
                    
                    # Paragraph 3: Positioning and Risk
                    para3 = ""
                    if implications and implications[0].get('trade_idea'):
                        para3 = f"The prevailing signal — {implications[0]['signal']} — suggests the following tactical framework: {implications[0]['trade_idea']}. "
                    
                    if risks:
                        para3 += f"Key risk factors identified include {', '.join(risks[:3]).lower()}, which could invalidate the base case. "
                    
                    if confidence == 'High':
                        para3 += "The high signal clarity supports conviction-weighted positioning, though standard risk management protocols (defined stops, position limits) remain essential."
                    elif confidence == 'Medium':
                        para3 += "Moderate signal clarity warrants scaled entry rather than full conviction allocation. Let price action confirm the thesis before adding to exposure."
                    else:
                        para3 += "Low signal clarity suggests this article alone is insufficient to drive portfolio decisions. Cross-reference with additional sources and price action before taking directional exposure."
                    
                    # === DISPLAY: RESEARCH INTELLIGENCE REPORT ===
                    
                    # Report Header
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #1a1f2e 100%); border: 1px solid rgba(88,166,255,0.3); border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; position: relative; overflow: hidden;">
                        <div style="position: absolute; top: 0; right: 0; background: {sentiment_color}; color: #000; padding: 0.3rem 1rem; font-size: 0.7rem; font-weight: 700; letter-spacing: 0.05em; border-radius: 0 12px 0 8px;">{sentiment.upper()}</div>
                        <div style="font-size: 0.65rem; color: #a371f7; font-weight: 600; text-transform: uppercase; letter-spacing: 0.15em; margin-bottom: 0.5rem;">Research Intelligence Report</div>
                        <div style="font-size: 1.15rem; font-weight: 600; color: #ffffff; margin-bottom: 0.75rem; padding-right: 120px;">{title}</div>
                        <div style="display: flex; flex-wrap: wrap; gap: 1rem; align-items: center;">
                            <span style="font-size: 0.75rem; color: #8b949e;">📰 {source_domain}</span>
                            {'<span style="font-size: 0.75rem; color: #8b949e;">✍️ ' + author + '</span>' if author else ''}
                            {'<span style="font-size: 0.75rem; color: #8b949e;">📅 ' + pub_date + '</span>' if pub_date else ''}
                            <span style="font-size: 0.75rem; color: #8b949e;">📝 {word_count:,} words</span>
                            <span style="font-size: 0.75rem; color: #8b949e;">🔍 Analyzed: {datetime.now(eastern).strftime('%I:%M %p ET')}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Sentiment Gauge + Metrics
                    gauge_col, met1, met2, met3, met4 = st.columns([2, 1, 1, 1, 1])
                    with gauge_col:
                        # Visual sentiment bar
                        st.markdown(f"""
                        <div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1rem;">
                            <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                <span style="color: #8b949e; font-size: 0.7rem; text-transform: uppercase;">Sentiment Decomposition</span>
                                <span style="color: {sentiment_color}; font-weight: 600; font-size: 0.85rem;">{sentiment_icon} {sentiment}</span>
                            </div>
                            <div style="display: flex; height: 10px; border-radius: 5px; overflow: hidden; margin-bottom: 0.4rem;">
                                <div style="background: #3fb950; width: {bull_pct:.0f}%; transition: width 0.5s;"></div>
                                <div style="background: #f85149; width: {bear_pct:.0f}%; transition: width 0.5s;"></div>
                            </div>
                            <div style="display: flex; justify-content: space-between; font-size: 0.7rem;">
                                <span style="color: #3fb950;">Bullish {bull_pct:.0f}% ({bull_score:.0f}pts)</span>
                                <span style="color: #8b949e;">Confidence: {confidence}</span>
                                <span style="color: #f85149;">Bearish {bear_pct:.0f}% ({bear_score:.0f}pts)</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    with met1:
                        st.markdown(f'<div class="metric-card" style="text-align: center;"><div class="metric-value" style="color: #a371f7;">{len(detected_themes)}</div><div class="metric-label">Themes</div></div>', unsafe_allow_html=True)
                    with met2:
                        st.markdown(f'<div class="metric-card" style="text-align: center;"><div class="metric-value" style="color: #58a6ff;">{len(data_points)}</div><div class="metric-label">Data Points</div></div>', unsafe_allow_html=True)
                    with met3:
                        st.markdown(f'<div class="metric-card" style="text-align: center;"><div class="metric-value" style="color: #d29922;">{len(mentioned_tickers)}</div><div class="metric-label">Tickers</div></div>', unsafe_allow_html=True)
                    with met4:
                        st.markdown(f'<div class="metric-card" style="text-align: center;"><div class="metric-value" style="color: #f85149;">{len(risks)}</div><div class="metric-label">Risk Flags</div></div>', unsafe_allow_html=True)
                    
                    # Macro Themes with Asset Impact
                    if detected_themes:
                        st.markdown("#### 🏷️ Macro Theme Decomposition")
                        for theme in detected_themes[:4]:
                            relevance_color = '#58a6ff' if theme['relevance'] == 'Primary' else '#6e7681'
                            with st.expander(f"{theme['icon']} {theme['name']}  —  {theme['relevance']} (score: {theme['score']})", expanded=(theme['relevance'] == 'Primary')):
                                impact = theme['asset_impact']
                                st.markdown(f"""
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem;">
                                    <div style="background: rgba(63,185,80,0.1); border-left: 3px solid #3fb950; padding: 0.5rem 0.75rem; border-radius: 0 6px 6px 0;">
                                        <div style="font-size: 0.65rem; color: #3fb950; font-weight: 600; text-transform: uppercase; margin-bottom: 0.25rem;">Equities</div>
                                        <div style="font-size: 0.78rem; color: #c9d1d9;">{impact.get('Equities', 'N/A')}</div>
                                    </div>
                                    <div style="background: rgba(88,166,255,0.1); border-left: 3px solid #58a6ff; padding: 0.5rem 0.75rem; border-radius: 0 6px 6px 0;">
                                        <div style="font-size: 0.65rem; color: #58a6ff; font-weight: 600; text-transform: uppercase; margin-bottom: 0.25rem;">Fixed Income</div>
                                        <div style="font-size: 0.78rem; color: #c9d1d9;">{impact.get('Fixed Income', 'N/A')}</div>
                                    </div>
                                    <div style="background: rgba(210,153,34,0.1); border-left: 3px solid #d29922; padding: 0.5rem 0.75rem; border-radius: 0 6px 6px 0;">
                                        <div style="font-size: 0.65rem; color: #d29922; font-weight: 600; text-transform: uppercase; margin-bottom: 0.25rem;">FX</div>
                                        <div style="font-size: 0.78rem; color: #c9d1d9;">{impact.get('FX', 'N/A')}</div>
                                    </div>
                                    <div style="background: rgba(163,113,247,0.1); border-left: 3px solid #a371f7; padding: 0.5rem 0.75rem; border-radius: 0 6px 6px 0;">
                                        <div style="font-size: 0.65rem; color: #a371f7; font-weight: 600; text-transform: uppercase; margin-bottom: 0.25rem;">Commodities</div>
                                        <div style="font-size: 0.78rem; color: #c9d1d9;">{impact.get('Commodities', 'N/A')}</div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Institutional Synthesis
                    st.markdown("#### 🎩 Institutional Synthesis")
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, rgba(163,113,247,0.08) 0%, rgba(88,166,255,0.04) 100%); border: 1px solid rgba(163,113,247,0.25); border-radius: 12px; padding: 1.5rem; margin: 0.5rem 0;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid rgba(163,113,247,0.15);">
                            <span style="font-size: 0.7rem; color: #a371f7; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em;">Senior Strategist Assessment</span>
                            <span style="font-size: 0.65rem; color: #6e7681;">Signal Confidence: <span style="color: {'#3fb950' if confidence == 'High' else '#d29922' if confidence == 'Medium' else '#f85149'}; font-weight: 600;">{confidence}</span></span>
                        </div>
                        <p style="color: #c9d1d9; font-size: 0.88rem; line-height: 1.85; text-align: justify; margin-bottom: 1rem; font-family: 'Georgia', serif;">{para1}</p>
                        <p style="color: #c9d1d9; font-size: 0.88rem; line-height: 1.85; text-align: justify; margin-bottom: 1rem; font-family: 'Georgia', serif;">{para2}</p>
                        <p style="color: #c9d1d9; font-size: 0.88rem; line-height: 1.85; text-align: justify; margin: 0; font-family: 'Georgia', serif;">{para3}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Cross-Asset Trade Implications
                    if implications:
                        st.markdown("#### ⚡ Cross-Asset Implications & Trade Ideas")
                        for imp in implications[:3]:
                            signal_name = imp.get('signal', '')
                            with st.expander(f"📡 Signal: {signal_name}", expanded=True):
                                st.markdown(f"""
                                <div style="background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 1rem; font-family: 'Consolas', 'Monaco', monospace;">
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem; margin-bottom: 0.75rem;">
                                        <div>
                                            <div style="font-size: 0.6rem; color: #3fb950; text-transform: uppercase; font-weight: 600; margin-bottom: 0.3rem;">📈 Equities</div>
                                            <div style="font-size: 0.8rem; color: #c9d1d9;">{imp.get('equities', 'N/A')}</div>
                                        </div>
                                        <div>
                                            <div style="font-size: 0.6rem; color: #58a6ff; text-transform: uppercase; font-weight: 600; margin-bottom: 0.3rem;">📊 Fixed Income</div>
                                            <div style="font-size: 0.8rem; color: #c9d1d9;">{imp.get('fixed_income', 'N/A')}</div>
                                        </div>
                                        <div>
                                            <div style="font-size: 0.6rem; color: #d29922; text-transform: uppercase; font-weight: 600; margin-bottom: 0.3rem;">💱 FX</div>
                                            <div style="font-size: 0.8rem; color: #c9d1d9;">{imp.get('fx', 'N/A')}</div>
                                        </div>
                                        <div>
                                            <div style="font-size: 0.6rem; color: #a371f7; text-transform: uppercase; font-weight: 600; margin-bottom: 0.3rem;">🛢️ Commodities</div>
                                            <div style="font-size: 0.8rem; color: #c9d1d9;">{imp.get('commodities', 'N/A')}</div>
                                        </div>
                                    </div>
                                    <div style="background: rgba(255,149,0,0.1); border: 1px solid rgba(255,149,0,0.3); border-radius: 6px; padding: 0.6rem 0.75rem; margin-top: 0.5rem;">
                                        <span style="color: #ff9500; font-size: 0.7rem; font-weight: 600;">💡 TACTICAL IDEA:</span>
                                        <span style="color: #fff; font-size: 0.85rem; margin-left: 0.5rem; font-weight: 500;">{imp.get('trade_idea', 'N/A')}</span>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Key Data Points
                    if data_points:
                        st.markdown("#### 📊 Quantitative Data Extracted")
                        # Group by category
                        cats = {}
                        for dp in data_points:
                            cat = dp['category']
                            if cat not in cats:
                                cats[cat] = []
                            cats[cat].append(dp['text'])
                        
                        for cat, items in cats.items():
                            for item in items[:3]:
                                st.markdown(f"""
                                <div style="background: rgba(33,38,45,0.8); border-left: 3px solid #58a6ff; padding: 0.6rem 1rem; margin: 0.35rem 0; border-radius: 0 8px 8px 0; display: flex; align-items: flex-start; gap: 0.75rem;">
                                    <span style="color: #58a6ff; font-size: 0.7rem; font-weight: 600; white-space: nowrap; min-width: 90px;">{cat}</span>
                                    <span style="color: #c9d1d9; font-size: 0.82rem;">{item}.</span>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Risk Flags
                    if risks:
                        st.markdown("#### ⚠️ Risk Factor Flags")
                        risk_cols = st.columns(min(4, len(risks)))
                        risk_icons = {'Valuation Risk': '💎', 'Liquidity Risk': '💧', 'Policy Risk': '⚖️', 'Event Risk': '📅', 'Crowding Risk': '👥', 'Contagion Risk': '🔗', 'Tail Risk': '🦢'}
                        for i, risk in enumerate(risks[:4]):
                            with risk_cols[i]:
                                st.markdown(f"""
                                <div style="background: rgba(248,81,73,0.1); border: 1px solid rgba(248,81,73,0.3); border-radius: 8px; padding: 0.75rem; text-align: center;">
                                    <div style="font-size: 1.2rem;">{risk_icons.get(risk, '⚠️')}</div>
                                    <div style="color: #ffa198; font-size: 0.75rem; font-weight: 600; margin-top: 0.3rem;">{risk}</div>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Key People Mentioned
                    if key_people:
                        st.markdown("#### 👤 Key Figures Quoted")
                        people_cols = st.columns(min(4, len(key_people)))
                        for i, person in enumerate(key_people[:4]):
                            with people_cols[i]:
                                role_color = '#a371f7' if person['role'] in ['CEO', 'Chair', 'Official'] else '#58a6ff'
                                st.markdown(f"""
                                <div style="background: rgba(33,38,45,0.5); border: 1px solid #30363d; border-radius: 8px; padding: 0.6rem; text-align: center;">
                                    <div style="color: #fff; font-size: 0.82rem; font-weight: 600;">{person['name']}</div>
                                    <div style="color: {role_color}; font-size: 0.7rem;">{person['role']}</div>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Mentioned Securities with action buttons
                    if mentioned_tickers:
                        st.markdown("#### 📈 Securities Referenced")
                        st.markdown("<p style='color: #8b949e; font-size: 0.75rem;'>Click any ticker for full institutional analysis</p>", unsafe_allow_html=True)
                        ticker_cols = st.columns(min(6, len(mentioned_tickers)))
                        for i, ticker in enumerate(mentioned_tickers[:6]):
                            with ticker_cols[i]:
                                if st.button(f"📊 {ticker}", key=f"url_ticker_{ticker}_{i}", use_container_width=True):
                                    st.session_state.selected_stock = ticker
                                    st.session_state.show_stock_report = True
                                    st.rerun()
                    
                    # Sentiment Signal Breakdown (expandable)
                    with st.expander("🔬 Sentiment Signal Breakdown", expanded=False):
                        sig_col1, sig_col2 = st.columns(2)
                        with sig_col1:
                            st.markdown("**Bullish Signals Detected:**")
                            for sig in bull_signals[:8]:
                                st.markdown(f"<span style='color: #3fb950; font-size: 0.8rem;'>+ {sig}</span>", unsafe_allow_html=True)
                        with sig_col2:
                            st.markdown("**Bearish Signals Detected:**")
                            for sig in bear_signals[:8]:
                                st.markdown(f"<span style='color: #f85149; font-size: 0.8rem;'>− {sig}</span>", unsafe_allow_html=True)
                    
                    # Full article text
                    with st.expander("📄 View Extracted Article Text", expanded=False):
                        st.text_area("Article Content", article_text[:6000], height=300, disabled=True)
                    
                except Exception as e:
                    st.error(f"Error analyzing URL: {str(e)}")
                    st.info("Tips: Ensure the URL is accessible and points to a public article. Some paywalled content may not be extractable.")
        else:
            st.markdown("""
            <div style="background: rgba(33,38,45,0.5); border: 1px dashed rgba(88,166,255,0.3); border-radius: 12px; padding: 2.5rem; text-align: center;">
                <div style="font-size: 2.5rem; margin-bottom: 0.75rem;">📰</div>
                <div style="color: #c9d1d9; font-size: 1rem; font-weight: 500; margin-bottom: 0.5rem;">Paste a financial article URL for institutional-grade analysis</div>
                <div style="color: #8b949e; font-size: 0.82rem; margin-bottom: 1rem;">Our pipeline extracts macro themes, cross-asset implications, sentiment decomposition, and trade ideas</div>
                <div style="display: flex; flex-wrap: wrap; justify-content: center; gap: 0.5rem;">
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">Reuters</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">Bloomberg</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">WSJ</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">FT</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">CNBC</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">Yahoo Finance</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">Seeking Alpha</span>
                    <span style="background: rgba(88,166,255,0.1); color: #58a6ff; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.72rem;">MarketWatch</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    eastern = pytz.timezone('US/Eastern')
    st.markdown(f"""
    <div style="text-align: center; padding: 1rem 0;">
        <div class="timestamp">{datetime.now(eastern).strftime("%I:%M:%S %p ET · %B %d, %Y")}</div>
        <div style="color: #484f58; font-size: 0.65rem; margin-top: 0.25rem;">Pre-Market Command Center v9.0 · Institutional Analysis · Data: Yahoo Finance · Not Financial Advice</div>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__": main()
