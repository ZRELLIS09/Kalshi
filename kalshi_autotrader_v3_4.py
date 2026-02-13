#!/usr/bin/env python3
"""
Kalshi AutoTrader v3.0 — Multi-Exchange EMA + IDX Proxy

STRATEGIES:
  1. NO_HARVEST    — Safe harvester (91-99¢) + Upset hunter (1-9¢)
  2. STRUCTURAL    — STALE + FRAGILE book detection
  3. CROSS_MARKET  — Event arbitrage (mutually exclusive markets)
  4. CRYPTO_SCALP  — Orderbook imbalance + Binance spot confirmation
  5. INDEX_SCALP   — S&P 500 / Nasdaq-100 at HALF fees (0.035)
  6. MARKET_MAKER  — Post both sides, capture spread

UPGRADES:
  P1: Take-profit exits (sell when bid >= 88¢, prevents catastrophic reversals)
  P2: Kalshi WebSocket streaming (replaces 34s REST polling with <1s push)
  P3: Binance spot confirmation (filters ~70% false crypto signals)
  P4: Kelly Criterion sizing (edge-proportional bets, quarter-Kelly default)
  P5: Batch orders (multiple orders in single API call, 3x faster)
  P7: Index market expansion (S&P/Nasdaq, half fees)
  P8: Auto-hedging (25% opposing position on low-confidence trades)
  P9: Market making (spread capture with inventory management)

SAFETY:
  - Demo mode by default
  - Position size limits (max $ and % of bankroll)
  - Daily loss circuit breaker
  - Requires --live flag for real trading
  - Full audit log
  - Stale order cancellation (30s TTL)

Requires: pip install cryptography requests websockets
"""

import requests
import re
import json
import math
import time
import uuid
import hashlib
import base64
import argparse
import logging
import os
import asyncio
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend

try:
    import websockets
    import websockets.client
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    logging.warning("websockets not installed — WebSocket features disabled. "
                    "Install with: pip install websockets")


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradingConfig:
    """All tunable parameters in one place."""

    # ── Risk Management ──────────────────────────────────────────────────
    MAX_POSITION_CENTS: int = 5500  # BTC $50 at 55¢ floor = 90 contracts = 4950¢, need headroom
    MAX_POSITION_PCT: float = 1.0
    MAX_DAILY_LOSS_CENTS: int = 5000
    MAX_OPEN_POSITIONS: int = 100
    MAX_OPEN_ORDERS: int = 50

    # ── P4: Kelly Criterion ──────────────────────────────────────────────
    USE_KELLY: bool = True
    KELLY_FRACTION: float = 0.25
    KELLY_MIN_EDGE: float = 0.02
    KELLY_MIN_BET_CENTS: int = 50
    KELLY_MAX_BET_CENTS: int = 1000

    # ── Strategy 1: NO Harvester ─────────────────────────────────────────
    NO_HARVEST_MIN_PRICE: int = 91
    NO_HARVEST_MAX_PRICE: int = 99
    NO_HARVEST_MIN_VOLUME: int = 500
    NO_HARVEST_MIN_MINUTES: int = 60
    NO_HARVEST_MAX_CONTRACTS: int = 10

    # ── Strategy 2: Structural Arb ──────────────────────────────────────
    STRUCT_MIN_ROI: float = 0.20
    STRUCT_MIN_SPREAD: int = 10
    STRUCT_MIN_VOLUME: int = 100
    STRUCT_REQUIRE_STALE: bool = True
    STRUCT_MAX_CONTRACTS: int = 5

    # ── Strategy 3: Cross-Market ────────────────────────────────────────
    CROSS_MIN_EDGE_CENTS: int = 5
    CROSS_MIN_VOLUME: int = 200
    CROSS_MAX_CONTRACTS: int = 5

    # ── Strategy 4: Crypto Scalper ──────────────────────────────────────
    CRYPTO_ENABLED: bool = True
    CRYPTO_MIN_PRICE: int = 55  # Hard floor — do not buy contracts below 55¢
    CRYPTO_MAX_PRICE: int = 85
    CRYPTO_SIZE_DOLLARS: float = 10.0  # PATCHED: was 20, reduced to limit concentration risk
    CRYPTO_SIZE_BTC: float = 20.0   # BTC: ~20 contracts at mid-range entries
    CRYPTO_SIZE_XRP: float = 10.0   # XRP: higher vol (0.50%), wider losses → smaller position
    CRYPTO_TICKERS: List[str] = field(default_factory=lambda: [
        "BTC"])
    CRYPTO_KEYWORDS: List[str] = field(default_factory=lambda: [
        "up", "down", "above", "below", "higher", "lower"])
    CRYPTO_MIN_VOLUME: int = 1
    CRYPTO_MAX_SPREAD: int = 20
    CRYPTO_MIN_IMBALANCE: float = 0.05

    # P3: Spot price feed (REST-based, primary signal)
    CRYPTO_REQUIRE_SPOT_CONFIRM: bool = True
    CRYPTO_SPOT_MOMENTUM_THRESHOLD: float = 0.0005
    CRYPTO_SPOT_LOOKBACK_SECONDS: int = 60
    CRYPTO_SPOT_MIN_READINGS: int = 6  # need N readings (~60s) before trading — prevents noise entries
    CRYPTO_SPOT_DRIVEN: bool = True  # use spot as PRIMARY signal (not just confirmation)

    # EMA noise filter (tick-level, replaces 5-min MA — faster for latency arb)
    CRYPTO_EMA_SPAN: int = 3            # EMA span — ~1.5s effective lag at 1 tick/sec
    CRYPTO_EMA_MIN_TICKS: int = 3       # minimum ticks before EMA is valid
    CRYPTO_EMA_NOISE_CUSHION: float = 0.0010  # 0.10% — if cushion >= this, skip EMA check entirely

    # EMA 50/100/150 structure filter (1-minute candle based)
    CRYPTO_EMA_PERIODS: List[int] = field(default_factory=lambda: [50, 100, 150])
    CRYPTO_EMA_MIN_CANDLES: int = 150        # minimum 1-min candles before filter activates
    CRYPTO_EMA_STRUCTURE_ENABLED: bool = True # master toggle for EMA structure filter

    # RSI indicator (1-minute candle based, Wilder's smoothed)
    CRYPTO_RSI_PERIOD: int = 14              # standard 14-period RSI
    CRYPTO_RSI_OVERBOUGHT: float = 70.0      # RSI above this = overbought
    CRYPTO_RSI_OVERSOLD: float = 30.0        # RSI below this = oversold
    CRYPTO_RSI_ENABLED: bool = True          # master toggle for RSI filter

    # MACD indicator (1-minute candle based, 12/26/9)
    CRYPTO_MACD_FAST: int = 12               # fast EMA period
    CRYPTO_MACD_SLOW: int = 26               # slow EMA period
    CRYPTO_MACD_SIGNAL: int = 9              # signal line EMA period
    CRYPTO_MACD_ENABLED: bool = True         # master toggle for MACD filter

    # Stop-loss (per-position, monitors bid vs entry)
    CRYPTO_STOP_LOSS_ENABLED: bool = True
    CRYPTO_STOP_LOSS_CENTS: int = 10    # sell if bid drops 10¢ below entry
    CRYPTO_STOP_CUSHION_THRESHOLD: float = 0.0  # thesis broken when cushion below this (0 = spot crossed strike)

    # ── Strategy 5: Index Scalper (P7) ──────────────────────────────────
    INDEX_ENABLED: bool = False  # DISABLED: no spot feed, no cushion calc, bought 22hr daily contracts at 48¢
    INDEX_SERIES: List[str] = field(default_factory=lambda: [
        # S&P 500 above/below + range (covers daily, hourly, intraday)
        'KXINXU', 'KXINX',
        # Nasdaq-100 above/below + range
        'KXNASDAQ100U', 'KXNASDAQ100',
    ])
    INDEX_FEE_RATE: float = 0.035
    INDEX_MIN_PRICE: int = 15
    INDEX_MAX_PRICE: int = 85
    INDEX_MIN_VOLUME: int = 10
    INDEX_MAX_SPREAD: int = 15
    INDEX_MIN_IMBALANCE: float = 0.08

    # ── Strategy 6: Market Maker (P9) ───────────────────────────────────
    MM_ENABLED: bool = False
    MM_SPREAD_TARGET: int = 8
    MM_MIN_SPREAD: int = 6
    MM_SIZE_CONTRACTS: int = 5
    MM_MAX_INVENTORY: int = 20
    MM_INVENTORY_LEAN: int = 2

    # ── P8: Hedging ─────────────────────────────────────────────────────
    HEDGE_ENABLED: bool = True
    HEDGE_CONFIDENCE_THRESHOLD: float = 0.55
    HEDGE_RATIO: float = 0.25

    # ── P1: Take Profit ─────────────────────────────────────────────────
    TAKE_PROFIT_ENABLED: bool = True
    TAKE_PROFIT_THRESHOLD: int = 92

    # ── P2: WebSocket ───────────────────────────────────────────────────
    USE_WEBSOCKET: bool = True

    # ── P5: Batch Orders ────────────────────────────────────────────────
    USE_BATCH_ORDERS: bool = True

    # ── Calibration Upgrades ─────────────────────────────────────────────
    # Signal persistence: require N consecutive scans before trading
    REQUIRE_SIGNAL_PERSISTENCE: bool = True
    SIGNAL_PERSISTENCE_COUNT: int = 1  # act on first scan — cushion IS the confirmation

    # Minimum book depth (total contracts both sides)
    MIN_BOOK_DEPTH: int = 50        # skip books with < 50 total contracts
    MIN_DEPTH_FILTER_ENABLED: bool = True

    # ── Execution ────────────────────────────────────────────────────────
    USE_LIMIT_ORDERS: bool = True
    LIMIT_OFFSET_CENTS: int = 1
    ORDER_TTL_SECONDS: int = 30
    SCAN_INTERVAL_SECONDS: int = 20
    API_RATE_DELAY: float = 0.10

    # ── Fee Model ────────────────────────────────────────────────────────
    FEE_RATE: float = 0.07


# ═══════════════════════════════════════════════════════════════════════════════
# KALSHI API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class KalshiAPIClient:
    """Authenticated Kalshi API client with RSA-PSS signing."""

    PROD_BASE = "https://api.elections.kalshi.com/trade-api/v2"
    DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"

    def __init__(self, api_key_id: str, private_key_path: str, live: bool = False):
        self.api_key_id = api_key_id
        self.base_url = self.PROD_BASE if live else self.DEMO_BASE
        self.ws_url = ("wss://api.elections.kalshi.com" if live
                       else "wss://demo-api.kalshi.co")
        self.live = live
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        self.private_key = self._load_key(private_key_path)
        self.request_count = 0
        self.last_request_time = 0.0

    def _load_key(self, path: str):
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend())

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        message = (timestamp_ms + method + path).encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256())
        return base64.b64encode(signature).decode('utf-8')

    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        sign_path = path.split('?')[0]
        sig = self._sign(ts, method, sign_path)
        return {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': ts,
        }

    def ws_auth_headers(self) -> dict:
        """Generate auth headers for WebSocket handshake."""
        ts = str(int(time.time() * 1000))
        sig = self._sign(ts, "GET", "/trade-api/ws/v2")
        return {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': ts,
        }

    def _rate_limit(self):
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1

    def _request(self, method: str, path: str, data: dict = None,
                 params: dict = None) -> dict:
        self._rate_limit()
        url = self.base_url + path
        headers = self._headers(method.upper(), "/trade-api/v2" + path)
        try:
            if method.upper() == 'GET':
                resp = self.session.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST':
                resp = self.session.post(url, headers=headers, json=data, timeout=10)
            elif method.upper() == 'DELETE':
                resp = self.session.delete(url, headers=headers, timeout=10)
            else:
                raise ValueError(f"Unknown method: {method}")
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if method.upper() == 'DELETE' and e.response and e.response.status_code == 404:
                raise  # Silently raise — caller handles cleanup
            logging.error(f"API error {method} {path}: {e} — "
                          f"{e.response.text if e.response else ''}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error {method} {path}: {e}")
            raise

    # ── Public Data ──────────────────────────────────────────────────────
    def get_exchange_status(self) -> dict:
        return self._request('GET', '/exchange/status')

    def get_markets(self, status='open', limit=1000, cursor=None,
                    mve_filter='exclude', series_ticker=None) -> dict:
        params = {'status': status, 'limit': str(limit), 'mve_filter': mve_filter}
        if cursor:
            params['cursor'] = cursor
        if series_ticker:
            params['series_ticker'] = series_ticker
        return self._request('GET', '/markets', params=params)

    def get_market(self, ticker: str) -> dict:
        return self._request('GET', f'/markets/{ticker}')

    def get_orderbook(self, ticker: str) -> dict:
        return self._request('GET', f'/markets/{ticker}/orderbook')

    def get_trades(self, ticker: str, limit: int = 100) -> dict:
        return self._request('GET', f'/markets/{ticker}/trades',
                             params={'limit': str(limit)})

    def get_events(self, status='open', limit=200, cursor=None) -> dict:
        params = {'status': status, 'limit': str(limit)}
        if cursor:
            params['cursor'] = cursor
        return self._request('GET', '/events', params=params)

    def get_event(self, event_ticker: str) -> dict:
        return self._request('GET', f'/events/{event_ticker}')

    # ── Portfolio ────────────────────────────────────────────────────────
    def get_balance(self) -> dict:
        return self._request('GET', '/portfolio/balance')

    def get_positions(self, limit=200) -> dict:
        return self._request('GET', '/portfolio/positions',
                             params={'limit': str(limit)})

    def get_orders(self, status=None, limit=200) -> dict:
        params = {'limit': str(limit)}
        if status:
            params['status'] = status
        return self._request('GET', '/portfolio/orders', params=params)

    def get_fills(self, limit=100) -> dict:
        return self._request('GET', '/portfolio/fills',
                             params={'limit': str(limit)})

    # ── Order Management ─────────────────────────────────────────────────
    def create_order(self, ticker: str, side: str, action: str,
                     count: int, order_type: str = 'limit',
                     yes_price: int = None, no_price: int = None) -> dict:
        data = {
            'ticker': ticker,
            'side': side.lower(),
            'action': action.lower(),
            'count': count,
            'type': order_type,
            'client_order_id': str(uuid.uuid4()),
        }
        if yes_price is not None:
            data['yes_price'] = yes_price
        if no_price is not None:
            data['no_price'] = no_price
        return self._request('POST', '/portfolio/orders', data=data)

    def cancel_order(self, order_id: str) -> dict:
        return self._request('DELETE', f'/portfolio/orders/{order_id}')

    def batch_create_orders(self, orders: List[dict]) -> dict:
        """P5: Submit up to 20 orders in a single batch call."""
        return self._request('POST', '/portfolio/orders/batched',
                             data={'orders': orders})


# ═══════════════════════════════════════════════════════════════════════════════
# P2: KALSHI WEBSOCKET MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class KalshiWebSocket:
    """
    Real-time WebSocket connection to Kalshi exchange.
    Maintains local orderbook + ticker state from push updates.
    Replaces 34-second REST polling with <1s push latency.
    """

    def __init__(self, api: KalshiAPIClient, config: TradingConfig):
        self.api = api
        self.config = config
        self.ws = None
        self.connected = False
        self.running = False
        self.orderbooks: Dict[str, dict] = {}
        self.tickers: Dict[str, dict] = {}
        self.fills: List[dict] = []
        self._sub_orderbook: set = set()
        self._sub_ticker: set = set()
        self._sid_orderbook: Optional[int] = None
        self._sid_ticker: Optional[int] = None
        self._cmd_id = 0
        self._lock = threading.Lock()
        self.on_fill: Optional[callable] = None

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    async def _connect(self):
        headers = self.api.ws_auth_headers()
        ws_url = self.api.ws_url + "/trade-api/ws/v2"
        try:
            self.ws = await websockets.connect(
                ws_url, additional_headers=headers,
                ping_interval=10, ping_timeout=20, close_timeout=5)
            self.connected = True
            logging.info(f"🔌 WebSocket connected to {ws_url}")
        except Exception as e:
            logging.error(f"WebSocket connect failed: {e}")
            self.connected = False
            raise

    async def subscribe_orderbook(self, tickers: List[str]):
        if not self.ws or not self.connected:
            return
        new = [t for t in tickers if t not in self._sub_orderbook]
        if not new:
            return
        for i in range(0, len(new), 50):
            chunk = new[i:i+50]
            cmd = {"id": self._next_id(), "cmd": "subscribe",
                   "params": {"channels": ["orderbook_delta"],
                              "market_tickers": chunk}}
            await self.ws.send(json.dumps(cmd))
            self._sub_orderbook.update(chunk)
        logging.info(f"📡 Subscribed orderbook_delta for {len(new)} tickers")

    async def subscribe_ticker(self, tickers: List[str]):
        if not self.ws or not self.connected:
            return
        new = [t for t in tickers if t not in self._sub_ticker]
        if not new:
            return
        for i in range(0, len(new), 50):
            chunk = new[i:i+50]
            cmd = {"id": self._next_id(), "cmd": "subscribe",
                   "params": {"channels": ["ticker"],
                              "market_tickers": chunk}}
            await self.ws.send(json.dumps(cmd))
            self._sub_ticker.update(chunk)
        logging.info(f"📡 Subscribed ticker for {len(new)} tickers")

    async def subscribe_fills(self):
        if not self.ws or not self.connected:
            return
        cmd = {"id": self._next_id(), "cmd": "subscribe",
               "params": {"channels": ["fill"]}}
        await self.ws.send(json.dumps(cmd))
        logging.info("📡 Subscribed to fill notifications")

    async def add_tickers(self, channel: str, tickers: List[str]):
        """Add tickers to existing subscription using sid."""
        if not self.ws or not self.connected or not tickers:
            return
        # Determine which sid to use
        if channel == "orderbook_delta":
            sid = self._sid_orderbook
        elif channel == "ticker":
            sid = self._sid_ticker
        else:
            return
        if sid is None:
            # No existing subscription — do a full subscribe
            if channel == "orderbook_delta":
                await self.subscribe_orderbook(tickers)
            elif channel == "ticker":
                await self.subscribe_ticker(tickers)
            return
        for i in range(0, len(tickers), 50):
            chunk = tickers[i:i+50]
            cmd = {"id": self._next_id(), "cmd": "update_subscription",
                   "params": {"sids": [sid], "market_tickers": chunk,
                              "action": "add_markets"}}
            await self.ws.send(json.dumps(cmd))
        if channel == "orderbook_delta":
            self._sub_orderbook.update(tickers)
        elif channel == "ticker":
            self._sub_ticker.update(tickers)

    def _handle_snapshot(self, msg: dict):
        ticker = msg.get('market_ticker', '')
        if not ticker:
            return
        with self._lock:
            self.orderbooks[ticker] = {
                'yes': [list(lv) for lv in msg.get('yes', [])],
                'no': [list(lv) for lv in msg.get('no', [])],
            }

    def _handle_delta(self, msg: dict):
        ticker = msg.get('market_ticker', '')
        price = msg.get('price', 0)
        delta = msg.get('delta', 0)
        side = msg.get('side', '')
        if not ticker or not side or not price:
            return
        with self._lock:
            if ticker not in self.orderbooks:
                self.orderbooks[ticker] = {'yes': [], 'no': []}
            levels = self.orderbooks[ticker].get(side, [])
            new_levels = []
            found = False
            for lv in levels:
                if lv[0] == price:
                    new_qty = lv[1] + delta
                    if new_qty > 0:
                        new_levels.append([price, new_qty])
                    found = True
                else:
                    new_levels.append(lv)
            if not found and delta > 0:
                new_levels.append([price, delta])
            self.orderbooks[ticker][side] = new_levels

    def _handle_ticker(self, msg: dict):
        ticker = msg.get('market_ticker', '')
        if not ticker:
            return
        with self._lock:
            self.tickers[ticker] = {
                'yes_bid': msg.get('yes_bid', 0),
                'yes_ask': msg.get('yes_ask', 0),
                'price': msg.get('price', 0),
                'volume': msg.get('volume', 0),
                'open_interest': msg.get('open_interest', 0),
                'ts': msg.get('ts', 0),
            }

    def _handle_fill(self, msg: dict):
        fill = {
            'trade_id': msg.get('trade_id', ''),
            'order_id': msg.get('order_id', ''),
            'market_ticker': msg.get('market_ticker', ''),
            'side': msg.get('side', ''),
            'yes_price': msg.get('yes_price', 0),
            'count': msg.get('count', 0),
            'action': msg.get('action', ''),
            'is_taker': msg.get('is_taker', False),
            'ts': msg.get('ts', 0),
        }
        with self._lock:
            self.fills.append(fill)
            if len(self.fills) > 500:
                self.fills = self.fills[-500:]
        logging.info(f"💰 FILL: {fill['action'].upper()} {fill['side'].upper()} "
                     f"{fill['market_ticker'][:30]} "
                     f"@{fill['yes_price'] if fill['side'] == 'yes' else 100 - fill['yes_price']}¢ "
                     f"(yes_px={fill['yes_price']}¢) x{fill['count']}")
        if self.on_fill:
            try:
                self.on_fill(fill)
            except Exception as e:
                logging.debug(f"Fill callback error: {e}")

    async def _recv_loop(self):
        while self.running and self.ws:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=30)
                data = json.loads(raw)
                msg_type = data.get('type', '')
                msg = data.get('msg', {})
                if msg_type == 'orderbook_snapshot':
                    self._handle_snapshot(msg)
                elif msg_type == 'orderbook_delta':
                    self._handle_delta(msg)
                elif msg_type == 'ticker':
                    self._handle_ticker(msg)
                elif msg_type == 'fill':
                    self._handle_fill(msg)
                elif msg_type == 'error':
                    logging.warning(f"WS error: {data}")
                elif msg_type == 'subscribed':
                    sid = data.get('sid')
                    channel = msg.get('channel', '')
                    if 'orderbook' in channel and sid:
                        self._sid_orderbook = sid
                        logging.debug(f"WS orderbook sid={sid}")
                    elif 'ticker' in channel and sid:
                        self._sid_ticker = sid
                        logging.debug(f"WS ticker sid={sid}")
            except asyncio.TimeoutError:
                try:
                    await self.ws.ping()
                except Exception:
                    logging.warning("WS ping failed, reconnecting")
                    break
            except websockets.exceptions.ConnectionClosed:
                logging.warning("🔌 WebSocket closed")
                self.connected = False
                break
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logging.error(f"WS recv error: {e}")
                continue

    async def run(self, initial_tickers: List[str] = None):
        self.running = True
        while self.running:
            try:
                await self._connect()
                if not self.connected:
                    await asyncio.sleep(5)
                    continue
                await self.subscribe_fills()
                if initial_tickers:
                    await self.subscribe_orderbook(initial_tickers)
                    await self.subscribe_ticker(initial_tickers)
                await self._recv_loop()
            except Exception as e:
                logging.error(f"WS run error: {e}")
            if self.running:
                logging.info("🔄 WS reconnecting in 3s...")
                self.connected = False
                await asyncio.sleep(3)

    def stop(self):
        self.running = False

    def get_orderbook(self, ticker: str) -> Optional[dict]:
        with self._lock:
            book = self.orderbooks.get(ticker)
            if book:
                return {'yes': list(book['yes']), 'no': list(book['no'])}
            return None

    def get_all_orderbooks(self) -> dict:
        with self._lock:
            return {t: {'yes': list(b['yes']), 'no': list(b['no'])}
                    for t, b in self.orderbooks.items()}

    def get_ticker(self, ticker: str) -> Optional[dict]:
        with self._lock:
            return dict(self.tickers[ticker]) if ticker in self.tickers else None

    def get_recent_fills(self) -> List[dict]:
        with self._lock:
            return list(self.fills)


# ═══════════════════════════════════════════════════════════════════════════════
# P3: BINANCE SPOT PRICE FEED
# ═══════════════════════════════════════════════════════════════════════════════

class SpotPriceFeed:
    """
    Spot price feed via Coinbase WebSocket (US-legal).
    Binance blocks US IPs (HTTP 451), so we use Coinbase instead.
    Same interface: get_momentum(), confirms_direction(), etc.
    """

    COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
    SYMBOLS = {'BTC': 'BTC-USD'}

    def __init__(self):
        self.prices: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        self.latest: Dict[str, float] = {}
        self.running = False
        self._lock = threading.Lock()

    async def run(self):
        if not HAS_WEBSOCKETS:
            logging.warning("Spot feed disabled: pip install websockets")
            return
        self.running = True
        url = self.COINBASE_WS
        subscribe_msg = {
            "type": "subscribe",
            "channels": [{"name": "ticker",
                          "product_ids": list(self.SYMBOLS.values())}]
        }
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logging.info("📈 Coinbase spot feed connected (BTC)")
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(raw)
                            if data.get('type') != 'ticker':
                                continue
                            product = data.get('product_id', '')
                            price = float(data.get('price', 0))
                            ts = time.time()
                            for name, prod_id in self.SYMBOLS.items():
                                if product == prod_id:
                                    with self._lock:
                                        self.latest[name] = price
                                        self.prices[name].append((ts, price))
                                        cutoff = ts - 600
                                        self.prices[name] = [
                                            x for x in self.prices[name]
                                            if x[0] > cutoff]
                                    break
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logging.debug(f"Coinbase msg error: {e}")
            except Exception as e:
                logging.warning(f"Coinbase connect error: {e}")
            if self.running:
                await asyncio.sleep(3)

    def stop(self):
        self.running = False

    def get_momentum(self, symbol: str, lookback_sec: int = 60) -> Optional[float]:
        with self._lock:
            ticks = self.prices.get(symbol, [])
        if len(ticks) < 2:
            return None
        now = time.time()
        recent = [(t, p) for t, p in ticks if t > now - lookback_sec]
        if len(recent) < 2:
            return None
        first, last = recent[0][1], recent[-1][1]
        return (last - first) / first if first > 0 else None

    def get_volatility(self, symbol: str, lookback_sec: int = 3600) -> Optional[float]:
        with self._lock:
            ticks = self.prices.get(symbol, [])
        if len(ticks) < 10:
            return None
        now = time.time()
        recent = [(t, p) for t, p in ticks if t > now - lookback_sec]
        if len(recent) < 10:
            return None
        sampled, last_t = [], 0
        for t, p in recent:
            if t - last_t >= 60:
                sampled.append(p)
                last_t = t
        if len(sampled) < 3:
            return None
        returns = [(sampled[i] - sampled[i-1]) / sampled[i-1]
                   for i in range(1, len(sampled)) if sampled[i-1] > 0]
        if not returns:
            return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean)**2 for r in returns) / len(returns)
        return math.sqrt(var)

    def confirms_direction(self, symbol: str, direction: str,
                           threshold: float = 0.0005, lookback: int = 60) -> bool:
        mom = self.get_momentum(symbol, lookback)
        if mom is None:
            return False
        if direction == 'UP':
            return mom >= threshold
        elif direction == 'DOWN':
            return mom <= -threshold
        return False

    def get_latest_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            return self.latest.get(symbol)


# ═══════════════════════════════════════════════════════════════════════════════
# SPOT PRICE REST FEED (replaces WebSocket for reliability)
# ═══════════════════════════════════════════════════════════════════════════════

class SpotPriceREST:
    """REST-based spot price feed using Coinbase EXCHANGE API (real-time).
    The consumer API (/v2/prices) returns cached/stale prices.
    The exchange API (/products/ticker) returns actual last trade price."""

    # Primary: Exchange API (real-time last trade)
    EXCHANGE_URL = "https://api.exchange.coinbase.com/products/{}-USD/ticker"
    # Fallback: Consumer API (cached but always available)
    CONSUMER_URL = "https://api.coinbase.com/v2/prices/{}-USD/spot"
    SYMBOLS = ['BTC']

    def __init__(self):
        self.history: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        self.latest: Dict[str, float] = {}
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json',
                                       'User-Agent': 'KalshiTrader/2.0'})
        self._use_exchange = True  # start with exchange API
        # EMA system: tick-level exponential moving average (replaces minute-close MA)
        self._ema: Dict[str, float] = {}          # symbol -> current EMA value
        self._ema_tick_count: Dict[str, int] = defaultdict(int)  # symbol -> ticks seen
        self._ema_span: int = 3  # default, can be overridden
        # MA system: 1-minute close aggregation (LEGACY — kept for logging only)
        self._minute_closes: Dict[str, List[float]] = defaultdict(list)  # symbol -> [close_prices]
        self._current_minute: Dict[str, int] = {}   # symbol -> minute_int
        self._last_price_in_minute: Dict[str, float] = {}  # symbol -> last price seen this minute
        self._max_closes = 15  # keep last 15 minutes
        # Passive candle aggregation (data collection only — NOT used for trading)
        self._candles_15m: Dict[str, List[dict]] = defaultdict(list)  # symbol -> [{o,h,l,c,ts}]
        self._candles_1h: Dict[str, List[dict]] = defaultdict(list)
        self._current_candle_15m: Dict[str, dict] = {}  # symbol -> live candle
        self._current_candle_1h: Dict[str, dict] = {}
        self._max_candles = 50  # keep last 50 of each timeframe

    def fetch(self) -> Dict[str, float]:
        """Call once per scan cycle. Returns {symbol: price}."""
        fetched = {}
        for symbol in self.SYMBOLS:
            try:
                price = None
                # Try exchange API first (real-time)
                if self._use_exchange:
                    try:
                        resp = self._session.get(
                            self.EXCHANGE_URL.format(symbol), timeout=3)
                        if resp.status_code == 200:
                            data = resp.json()
                            price = float(data['price'])
                        elif resp.status_code == 429:
                            # Rate limited, fall back to consumer API
                            logging.debug("Exchange API rate limited, using consumer API")
                            self._use_exchange = False
                    except Exception:
                        pass

                # Fallback to consumer API
                if price is None:
                    resp = self._session.get(
                        self.CONSUMER_URL.format(symbol), timeout=3)
                    if resp.status_code == 200:
                        price = float(resp.json()['data']['amount'])

                if price and price > 0:
                    ts = time.time()
                    self.latest[symbol] = price
                    self.history[symbol].append((ts, price))
                    cutoff = ts - 600  # keep 10 min
                    self.history[symbol] = [
                        x for x in self.history[symbol] if x[0] > cutoff]
                    fetched[symbol] = price
                    # ── Update tick-level EMA ──
                    self._update_ema(symbol, price)
                    # ── Aggregate 1-minute closes for MA (legacy logging) ──
                    self._update_minute_close(symbol, price, ts)
                    # ── Passive candle aggregation (data collection only) ──
                    self._update_candle(symbol, price, ts)
            except Exception as e:
                logging.debug(f"Spot REST {symbol}: {e}")
        return fetched

    def _update_minute_close(self, symbol: str, price: float, ts: float):
        """Track 1-minute close prices for MA computation."""
        minute = int(ts // 60)
        prev_minute = self._current_minute.get(symbol)
        if prev_minute is None:
            # First tick ever
            self._current_minute[symbol] = minute
            self._last_price_in_minute[symbol] = price
            return
        if minute != prev_minute:
            # Minute rolled — record the CLOSE of the previous minute
            last_close = self._last_price_in_minute.get(symbol)
            if last_close:
                closes = self._minute_closes[symbol]
                closes.append(last_close)
                # Trim to max
                if len(closes) > self._max_closes:
                    self._minute_closes[symbol] = closes[-self._max_closes:]
            self._current_minute[symbol] = minute
        self._last_price_in_minute[symbol] = price

    def _update_ema(self, symbol: str, price: float):
        """Update tick-level EMA. Called every fetch cycle (~10s REST or every WS tick)."""
        self._ema_tick_count[symbol] += 1
        alpha = 2.0 / (self._ema_span + 1)
        prev = self._ema.get(symbol)
        if prev is None:
            self._ema[symbol] = price  # seed with first price
        else:
            self._ema[symbol] = prev + alpha * (price - prev)

    def get_ema(self, symbol: str) -> Optional[float]:
        """Current EMA value, or None if not enough ticks."""
        if self._ema_tick_count.get(symbol, 0) < self._ema_span:
            return None
        return self._ema.get(symbol)

    def get_ema_direction(self, symbol: str) -> Optional[str]:
        """Is current price above or below EMA? Returns 'UP', 'DOWN', or None."""
        ema = self.get_ema(symbol)
        spot = self.latest.get(symbol)
        if ema is None or spot is None:
            return None
        if spot > ema:
            return 'UP'
        elif spot < ema:
            return 'DOWN'
        return None

    def get_ema_delta_pct(self, symbol: str) -> Optional[float]:
        """How far current spot is from EMA, as a fraction.
        Positive = price above EMA, negative = below."""
        ema = self.get_ema(symbol)
        spot = self.latest.get(symbol)
        if ema is None or spot is None or ema == 0:
            return None
        return (spot - ema) / ema

    def _update_candle(self, symbol: str, price: float, ts: float):
        """Passively build 15m and 1h OHLC candles. NOT used for trading."""
        for tf_secs, candle_dict, history_list in [
            (900, self._current_candle_15m, self._candles_15m),   # 15 minutes
            (3600, self._current_candle_1h, self._candles_1h),    # 1 hour
        ]:
            bucket = int(ts // tf_secs) * tf_secs  # start of current candle
            live = candle_dict.get(symbol)
            if live is None or live['ts'] != bucket:
                # New candle — archive old one if it exists
                if live is not None:
                    history_list[symbol].append(live)
                    if len(history_list[symbol]) > self._max_candles:
                        history_list[symbol] = history_list[symbol][-self._max_candles:]
                candle_dict[symbol] = {
                    'ts': bucket, 'o': price, 'h': price, 'l': price, 'c': price}
            else:
                # Update existing candle
                live['h'] = max(live['h'], price)
                live['l'] = min(live['l'], price)
                live['c'] = price

    def get_candles(self, symbol: str, timeframe: str = '15m') -> List[dict]:
        """Return completed candles for a symbol. Passive data — not used in trading."""
        if timeframe == '15m':
            return list(self._candles_15m.get(symbol, []))
        elif timeframe == '1h':
            return list(self._candles_1h.get(symbol, []))
        return []

    def get_candle_count(self, symbol: str, timeframe: str = '15m') -> int:
        """How many completed candles we have."""
        if timeframe == '15m':
            return len(self._candles_15m.get(symbol, []))
        elif timeframe == '1h':
            return len(self._candles_1h.get(symbol, []))
        return 0

    def get_latest_price(self, symbol: str) -> Optional[float]:
        return self.latest.get(symbol)

    def get_momentum(self, symbol: str, lookback_sec: int = 60) -> Optional[float]:
        """Returns % change over lookback period."""
        ticks = self.history.get(symbol, [])
        if len(ticks) < 2:
            return None
        now = time.time()
        recent = [(t, p) for t, p in ticks if t > now - lookback_sec]
        if len(recent) < 2:
            return None
        return (recent[-1][1] - recent[0][1]) / recent[0][1]

    def get_dollar_change(self, symbol: str, lookback_sec: int = 60) -> Optional[float]:
        """Returns absolute dollar change over lookback."""
        ticks = self.history.get(symbol, [])
        if len(ticks) < 2:
            return None
        now = time.time()
        recent = [(t, p) for t, p in ticks if t > now - lookback_sec]
        if len(recent) < 2:
            return None
        return recent[-1][1] - recent[0][1]

    def get_volatility(self, symbol: str, lookback_sec: int = 300) -> Optional[float]:
        """Average absolute $ change between readings over lookback."""
        ticks = self.history.get(symbol, [])
        now = time.time()
        recent = [p for t, p in ticks if t > now - lookback_sec]
        if len(recent) < 3:
            return None
        changes = [abs(recent[i] - recent[i-1]) for i in range(1, len(recent))]
        return sum(changes) / len(changes) if changes else None

    def get_reading_count(self, symbol: str) -> int:
        return len(self.history.get(symbol, []))

    def get_status_str(self) -> str:
        parts = []
        for sym in self.SYMBOLS:
            p = self.latest.get(sym)
            n = self.get_reading_count(sym)
            if p:
                mom = self.get_momentum(sym, 60)
                mom_str = f"{mom*100:+.3f}%" if mom else "?"
                price_fmt = f"${p:,.2f}" if p < 100 else f"${p:,.0f}"
                parts.append(f"{sym}={price_fmt}({mom_str},n={n})")
        return " | ".join(parts) if parts else "waiting for data..."

    def get_ma(self, symbol: str, period: int = 5) -> Optional[float]:
        """Simple moving average of last N 1-minute closes."""
        closes = self._minute_closes.get(symbol, [])
        if len(closes) < period:
            return None
        window = closes[-period:]
        return sum(window) / len(window)

    def get_ma_slope(self, symbol: str, period: int = 5) -> Optional[float]:
        """% change between current MA(period) and MA(period) shifted by `period` minutes.
        Uses non-overlapping windows to avoid dampened signals."""
        closes = self._minute_closes.get(symbol, [])
        need = period * 2  # e.g. 10 closes for two non-overlapping 5-period windows
        if len(closes) < need:
            return None
        ma_now = sum(closes[-period:]) / period
        ma_prev = sum(closes[-need:-period]) / period
        if ma_prev == 0:
            return None
        return (ma_now - ma_prev) / ma_prev

    def get_distance_from_ma(self, symbol: str, period: int = 5) -> Optional[float]:
        """How far current spot is from the MA, as a fraction.
        Positive = price above MA, negative = below."""
        ma = self.get_ma(symbol, period)
        spot = self.latest.get(symbol)
        if ma is None or spot is None or ma == 0:
            return None
        return (spot - ma) / ma

    def get_minute_close_count(self, symbol: str) -> int:
        """How many completed 1-minute closes we have."""
        return len(self._minute_closes.get(symbol, []))


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-EXCHANGE INDEX PROXY (v3 — settlement-aligned pricing)
# ═══════════════════════════════════════════════════════════════════════════════

class ExchangeFeed:
    """Base class for a single exchange WebSocket feed.
    Tracks mid-price (best_bid + best_ask) / 2 for BRTI proxy alignment."""

    def __init__(self, name: str):
        self.name = name
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.last_trade: Optional[float] = None
        self.last_update: float = 0.0
        self.running = False
        self._lock = threading.Lock()

    @property
    def mid_price(self) -> Optional[float]:
        with self._lock:
            if self.best_bid and self.best_ask:
                return (self.best_bid + self.best_ask) / 2
            return self.last_trade  # fallback to last trade if no book data

    @property
    def age_seconds(self) -> float:
        if self.last_update == 0:
            return float('inf')
        return time.time() - self.last_update

    def _update_book(self, bid: float, ask: float):
        with self._lock:
            self.best_bid = bid
            self.best_ask = ask
            self.last_update = time.time()

    def _update_trade(self, price: float):
        with self._lock:
            self.last_trade = price
            self.last_update = time.time()


class KrakenFeed(ExchangeFeed):
    """Kraken WebSocket v2 — BTC/USD ticker."""

    WS_URL = "wss://ws.kraken.com/v2"

    def __init__(self):
        super().__init__("Kraken")

    async def run(self):
        if not HAS_WEBSOCKETS:
            return
        self.running = True
        subscribe_msg = {
            "method": "subscribe",
            "params": {"channel": "ticker", "symbol": ["BTC/USD"]}
        }
        while self.running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logging.info(f"📡 {self.name} feed connected (BTC/USD)")
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(raw)
                            # Kraken v2 ticker: {"channel":"ticker","data":[{"symbol":"BTC/USD","bid":...,"ask":...}]}
                            if data.get('channel') == 'ticker' and 'data' in data:
                                for item in data['data']:
                                    bid = float(item.get('bid', 0))
                                    ask = float(item.get('ask', 0))
                                    if bid > 0 and ask > 0:
                                        self._update_book(bid, ask)
                                    last = item.get('last')
                                    if last:
                                        self._update_trade(float(last))
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logging.debug(f"{self.name} msg error: {e}")
            except Exception as e:
                logging.warning(f"{self.name} connect error: {e}")
            if self.running:
                await asyncio.sleep(3)

    def stop(self):
        self.running = False


class BitstampFeed(ExchangeFeed):
    """Bitstamp WebSocket — BTC/USD order book + trades."""

    WS_URL = "wss://ws.bitstamp.net"

    def __init__(self):
        super().__init__("Bitstamp")

    async def run(self):
        if not HAS_WEBSOCKETS:
            return
        self.running = True
        subscribe_book = {
            "event": "bts:subscribe",
            "data": {"channel": "order_book_btcusd"}
        }
        subscribe_trades = {
            "event": "bts:subscribe",
            "data": {"channel": "live_trades_btcusd"}
        }
        while self.running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps(subscribe_book))
                    await ws.send(json.dumps(subscribe_trades))
                    logging.info(f"📡 {self.name} feed connected (BTC/USD)")
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(raw)
                            channel = data.get('channel', '')
                            event = data.get('event', '')
                            if 'order_book' in channel and event == 'data':
                                book = data.get('data', {})
                                bids = book.get('bids', [])
                                asks = book.get('asks', [])
                                if bids and asks:
                                    best_bid = float(bids[0][0])
                                    best_ask = float(asks[0][0])
                                    if best_bid > 0 and best_ask > 0:
                                        self._update_book(best_bid, best_ask)
                            elif 'live_trades' in channel and event == 'trade':
                                trade_data = data.get('data', {})
                                price = float(trade_data.get('price', 0))
                                if price > 0:
                                    self._update_trade(price)
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logging.debug(f"{self.name} msg error: {e}")
            except Exception as e:
                logging.warning(f"{self.name} connect error: {e}")
            if self.running:
                await asyncio.sleep(3)

    def stop(self):
        self.running = False


class GeminiFeed(ExchangeFeed):
    """Gemini v1 marketdata WebSocket — BTC/USD.
    Uses top_of_book=true for lightweight top-of-book updates only."""

    WS_URL = "wss://api.gemini.com/v1/marketdata/BTCUSD?top_of_book=true"

    def __init__(self):
        super().__init__("Gemini")

    async def run(self):
        if not HAS_WEBSOCKETS:
            return
        self.running = True
        while self.running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    logging.info(f"📡 {self.name} feed connected (BTC/USD)")
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(raw)
                            # Gemini v1 sends {"type":"update","events":[{"type":"change","side":"bid","price":"66300.00","remaining":"1.5"}, ...]}
                            events = data.get('events', [])
                            for evt in events:
                                if evt.get('type') == 'change':
                                    side = evt.get('side', '')
                                    price = float(evt.get('price', 0))
                                    remaining = float(evt.get('remaining', 0))
                                    if price > 0 and remaining > 0:
                                        if side == 'bid':
                                            with self._lock:
                                                self.best_bid = price
                                                self.last_update = time.time()
                                        elif side == 'ask':
                                            with self._lock:
                                                self.best_ask = price
                                                self.last_update = time.time()
                                elif evt.get('type') == 'trade':
                                    price = float(evt.get('price', 0))
                                    if price > 0:
                                        self._update_trade(price)
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logging.debug(f"{self.name} msg error: {e}")
            except Exception as e:
                logging.warning(f"{self.name} connect error: {e}")
            if self.running:
                await asyncio.sleep(3)

    def stop(self):
        self.running = False


class MultiExchangeFeed:
    """Manages multiple exchange feeds and computes IDX_PROXY.

    IDX_PROXY = median of mid-prices from all connected exchanges.
    This approximates BRTI (which uses 8 exchanges with order-book weighting).
    Using mid-prices (not last trades) more closely matches BRTI methodology.

    Also maintains a rolling 60-second average for settlement projection.
    """

    STALENESS_THRESHOLD = 30.0  # seconds — matches BRTI's disregard threshold

    def __init__(self):
        self.coinbase_price: Optional[float] = None  # set externally from SpotPriceREST
        self.feeds: Dict[str, ExchangeFeed] = {
            'kraken': KrakenFeed(),
            'bitstamp': BitstampFeed(),
            'gemini': GeminiFeed(),
        }
        self._idx_proxy: Optional[float] = None
        self._idx_history: List[Tuple[float, float]] = []  # (timestamp, price) — all samples
        self._rolling_60s: deque = deque(maxlen=60)  # 1 sample/sec for settlement sim
        self._last_sample_time: float = 0.0
        self._opening_avg: Optional[float] = None
        self._opening_avg_time: Optional[float] = None
        self._lock = threading.Lock()

    def update_coinbase(self, price: float):
        """Called from SpotPriceREST.fetch() to inject Coinbase price."""
        self.coinbase_price = price
        self._recompute_idx()

    def _recompute_idx(self):
        """Recompute IDX_PROXY as median of all fresh mid-prices."""
        prices = []
        # Coinbase (from REST — always "fresh" if recently fetched)
        if self.coinbase_price:
            prices.append(self.coinbase_price)
        # Exchange feeds
        for name, feed in self.feeds.items():
            if feed.age_seconds < self.STALENESS_THRESHOLD:
                mid = feed.mid_price
                if mid and mid > 0:
                    prices.append(mid)

        if not prices:
            return

        with self._lock:
            prices.sort()
            n = len(prices)
            if n % 2 == 1:
                self._idx_proxy = prices[n // 2]
            else:
                self._idx_proxy = (prices[n // 2 - 1] + prices[n // 2]) / 2

            ts = time.time()
            self._idx_history.append((ts, self._idx_proxy))
            # Keep 10 minutes of history
            cutoff = ts - 600
            self._idx_history = [(t, p) for t, p in self._idx_history if t > cutoff]

            # Sample once per second for rolling 60s average
            if ts - self._last_sample_time >= 1.0:
                self._rolling_60s.append((ts, self._idx_proxy))
                self._last_sample_time = ts

    def get_idx_proxy(self) -> Optional[float]:
        """Current IDX_PROXY (median of all exchange mid-prices)."""
        with self._lock:
            return self._idx_proxy

    def get_rolling_60s_avg(self) -> Optional[float]:
        """Rolling 60-second average of IDX_PROXY — simulates BRTI settlement."""
        with self._lock:
            if len(self._rolling_60s) < 5:  # need at least 5 seconds
                return None
            return sum(p for _, p in self._rolling_60s) / len(self._rolling_60s)

    def capture_opening_avg(self):
        """Snapshot current rolling avg as the opening average for a 15-min window."""
        avg = self.get_rolling_60s_avg()
        if avg:
            self._opening_avg = avg
            self._opening_avg_time = time.time()
            logging.info(f"📌 Opening avg captured: ${avg:,.2f}")

    def get_opening_avg(self) -> Optional[float]:
        return self._opening_avg

    def get_projected_final_avg(self, settlement_time: float) -> Optional[float]:
        """Project what the final 60-second average will be if price holds.
        Use in the last 90 seconds before settlement."""
        with self._lock:
            if not self._idx_proxy or len(self._rolling_60s) < 5:
                return None
            current_samples = list(self._rolling_60s)
            seconds_remaining = max(0, settlement_time - time.time())
            if seconds_remaining > 90:
                return None  # only valid in last 90 seconds
            current_sum = sum(p for _, p in current_samples)
            samples_remaining = max(0, 60 - len(current_samples))
            projected_sum = current_sum + (self._idx_proxy * samples_remaining)
            return projected_sum / 60

    def get_settlement_delta(self, settlement_time: float) -> Optional[float]:
        """Projected final avg minus opening avg. Positive = YES likely, negative = NO likely."""
        projected = self.get_projected_final_avg(settlement_time)
        opening = self._opening_avg
        if projected is None or opening is None:
            return None
        return projected - opening

    def get_feed_count(self) -> int:
        """How many exchange feeds are currently live (non-stale)."""
        count = 1 if self.coinbase_price else 0
        for feed in self.feeds.values():
            if feed.age_seconds < self.STALENESS_THRESHOLD:
                count += 1
        return count

    def get_status_str(self) -> str:
        """Status string for logging."""
        parts = []
        if self.coinbase_price:
            parts.append(f"CB=${self.coinbase_price:,.0f}")
        for name, feed in self.feeds.items():
            mid = feed.mid_price
            age = feed.age_seconds
            if mid and age < self.STALENESS_THRESHOLD:
                parts.append(f"{name[:3].upper()}=${mid:,.0f}")
            else:
                parts.append(f"{name[:3].upper()}=stale")
        idx = self.get_idx_proxy()
        if idx:
            parts.append(f"IDX=${idx:,.2f}")
        avg = self.get_rolling_60s_avg()
        if avg:
            parts.append(f"avg60=${avg:,.2f}")
        return " | ".join(parts)

    async def _run_feed(self, feed: ExchangeFeed):
        """Run a single exchange feed with error isolation."""
        try:
            await feed.run()
        except Exception as e:
            logging.error(f"Exchange feed {feed.name} crashed: {e}")

    async def run_all(self):
        """Run all exchange feeds concurrently."""
        tasks = [self._run_feed(feed) for feed in self.feeds.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop_all(self):
        for feed in self.feeds.values():
            feed.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# FEE CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def kalshi_fee_cents(price_cents: int, contracts: int = 1,
                     fee_rate: float = 0.07) -> int:
    p = price_cents / 100.0
    fee_dollars = fee_rate * contracts * p * (1 - p)
    return math.ceil(fee_dollars * 100)

def net_profit_cents(buy_price_cents: int, contracts: int = 1,
                     fee_rate: float = 0.07) -> int:
    gross = (100 - buy_price_cents) * contracts
    fee = kalshi_fee_cents(buy_price_cents, contracts, fee_rate)
    return gross - fee

def roi(buy_price_cents: int, contracts: int = 1,
        fee_rate: float = 0.07) -> float:
    cost = buy_price_cents * contracts
    if cost <= 0:
        return 0.0
    return net_profit_cents(buy_price_cents, contracts, fee_rate) / cost


# ═══════════════════════════════════════════════════════════════════════════════
# P4: KELLY CRITERION POSITION SIZING
# ═══════════════════════════════════════════════════════════════════════════════

class KellySizer:
    """
    Fractional Kelly for Kalshi binary contracts.
    Kelly % = (p * b - q) / b, with fee-adjusted payout odds.
    Quarter-Kelly default to reduce variance.
    """

    def __init__(self, fraction=0.25, min_edge=0.02,
                 min_bet_cents=50, max_bet_cents=1000, max_pct=0.05):
        self.fraction = fraction
        self.min_edge = min_edge
        self.min_bet_cents = min_bet_cents
        self.max_bet_cents = max_bet_cents
        self.max_pct = max_pct

    def calculate_bet(self, est_prob: float, price_cents: int,
                      bankroll_cents: int, fee_rate: float = 0.07) -> int:
        if price_cents <= 0 or price_cents >= 100 or bankroll_cents <= 0:
            return 0
        p, q = est_prob, 1.0 - est_prob
        gross_b = (100 - price_cents) / price_cents
        fee_per = kalshi_fee_cents(price_cents, 1, fee_rate)
        net_b = gross_b - (fee_per / price_cents)
        if net_b <= 0:
            return 0
        kelly = (p * net_b - q) / net_b
        if kelly <= self.min_edge:
            return 0
        adjusted = kelly * self.fraction
        bet = int(adjusted * bankroll_cents)
        bet = max(self.min_bet_cents, bet)
        bet = min(self.max_bet_cents, bet)
        bet = min(int(bankroll_cents * self.max_pct), bet)
        return bet

    def bet_to_contracts(self, bet_cents: int, price_cents: int) -> int:
        if price_cents <= 0:
            return 0
        return max(1, bet_cents // price_cents)

    def estimate_probability(self, confidence: float, price_cents: int) -> float:
        market_prob = price_cents / 100.0
        blended = 0.7 * confidence + 0.3 * (1.0 - market_prob)
        return max(0.01, min(0.99, blended))

    def size_signal(self, signal, bankroll_cents: int) -> int:
        est_p = self.estimate_probability(signal.confidence, signal.price_cents)
        fee_rate = getattr(signal, 'fee_rate', 0.07)
        bet = self.calculate_bet(est_p, signal.price_cents, bankroll_cents, fee_rate)
        if bet <= 0:
            signal.contracts = 0
            return 0
        contracts = self.bet_to_contracts(bet, signal.price_cents)
        signal.contracts = contracts
        if not hasattr(signal, 'market_data') or signal.market_data is None:
            signal.market_data = {}
        signal.market_data['kelly_prob'] = est_p
        signal.market_data['kelly_bet_cents'] = bet
        return contracts


# ═══════════════════════════════════════════════════════════════════════════════
# TRADE SIGNAL
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeSignal:
    strategy: str
    ticker: str
    side: str
    action: str
    price_cents: int
    contracts: int
    edge_cents: int
    edge_roi: float
    confidence: float
    reason: str
    market_data: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


# ═══════════════════════════════════════════════════════════════════════════════
# P1: TAKE-PROFIT MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

class TakeProfitMonitor:
    """
    Monitors open positions for take-profit and stop-loss exits.
    
    Stop-loss uses cushion-dynamic confirmation:
    - Bid triggers stop → check CB spot vs strike
    - cushion < 0.05%: EXIT immediately (micro-noise zone, not real strength)
    - cushion 0.05-0.10%: HOLD 1 scan, exit if cushion shrinks, hold if it expands
    - cushion > 0.10%: HOLD up to 3 scans (strong thesis, bid noise likely)
    - Safety valve: 4 scans max regardless
    """

    STOP_MAX_SCANS = 4  # absolute safety valve

    def __init__(self, threshold: int = 88, stop_loss_cents: int = 0,
                 stop_cushion_threshold: float = 0.0):
        self.threshold = threshold
        self.stop_loss_cents = stop_loss_cents
        self.stop_cushion_threshold = stop_cushion_threshold  # kept for config compat
        self.positions: Dict[str, dict] = {}
        self._exited: set = set()
        self._pending_stops: Dict[str, dict] = {}  # ticker -> {scans, prev_cushion, max_hold}

    def track(self, ticker: str, side: str, entry_price: int, contracts: int,
              strike: float = None, symbol: str = None, direction: str = None):
        existing = self.positions.get(ticker)
        if existing and existing['side'] == side:
            # Same ticker, same side → accumulate contracts, weighted avg entry
            old_ct = existing['contracts']
            old_px = existing['entry_price']
            new_ct = old_ct + contracts
            avg_px = round((old_px * old_ct + entry_price * contracts) / new_ct) if new_ct > 0 else entry_price
            existing['contracts'] = new_ct
            existing['entry_price'] = avg_px
            logging.debug(f"TP tracking UPDATE: {ticker} {side.upper()} "
                          f"+{contracts}@{entry_price}¢ → total {new_ct}@{avg_px}¢"
                          f" strike={strike} sym={symbol}")
        else:
            self.positions[ticker] = {
                'side': side, 'entry_price': entry_price,
                'contracts': contracts, 'tracked_at': time.time(),
                'strike': strike, 'symbol': symbol, 'direction': direction}
            logging.debug(f"TP tracking NEW: {ticker} {side.upper()}@{entry_price}¢ x{contracts}"
                          f" strike={strike} sym={symbol}")

    def untrack(self, ticker: str):
        self.positions.pop(ticker, None)
        self._exited.discard(ticker)
        self._pending_stops.pop(ticker, None)

    def check(self, ticker_data: dict, orderbooks: dict,
              spot_rest=None) -> List[dict]:
        exits = []
        for ticker, pos in list(self.positions.items()):
            if ticker in self._exited:
                continue
            side = pos['side']
            current_bid = self._get_bid(ticker, side, ticker_data, orderbooks)
            if current_bid is None:
                logging.debug(f"⚠️ NO_BID_DATA: {ticker} {side} — "
                              f"tk={'Y' if ticker in ticker_data else 'N'} "
                              f"ob={'Y' if ticker in orderbooks else 'N'}")
                continue

            # ── Take-profit check (unchanged) ──
            if current_bid >= self.threshold:
                profit_per = current_bid - pos['entry_price']
                total_profit = profit_per * pos['contracts']
                logging.info(f"🎯 TAKE PROFIT: {ticker} {side.upper()} "
                             f"bid={current_bid}¢ >= {self.threshold}¢ "
                             f"(entry={pos['entry_price']}¢, "
                             f"+{profit_per}¢/contract, +{total_profit}¢ total)")
                exits.append({'ticker': ticker, 'side': side, 'action': 'sell',
                              'count': pos['contracts'], 'price': current_bid,
                              'exit_type': 'take_profit'})
                self._exited.add(ticker)
                self._pending_stops.pop(ticker, None)
                continue

            # ── Stop-loss with cushion-dynamic confirmation ──
            if self.stop_loss_cents <= 0:
                continue
            stop_price = pos['entry_price'] - self.stop_loss_cents

            # Check if bid has recovered above stop
            pending = self._pending_stops.get(ticker)
            if pending and current_bid > stop_price:
                logging.info(f"✅ STOP_RECOVERED: {ticker} {side.upper()} "
                             f"bid={current_bid}¢ recovered above stop={stop_price}¢ "
                             f"(was pending {pending['scans']} scans)")
                self._pending_stops.pop(ticker, None)
                continue

            if current_bid > stop_price or current_bid <= 0:
                continue  # bid above stop, no trigger

            # ── Bid at or below stop — compute cushion state ──
            strike = pos.get('strike')
            symbol = pos.get('symbol')
            cushion = None
            ema_dir = None

            if spot_rest and strike and symbol:
                spot = spot_rest.get_latest_price(symbol)
                ema_dir = spot_rest.get_ema_direction(symbol)
                if spot and strike > 0:
                    cushion = (spot - strike) / strike if side == 'yes' else (strike - spot) / strike

            # ── DUAL-CONFIRM GATE: require CB spot to agree with Kalshi bid ──
            # Kalshi bid says we're losing, but does Coinbase spot confirm?
            # If CB spot still shows healthy cushion (>= 0.05%), the underlying
            # hasn't actually moved against us — Kalshi book is just thin/noisy.
            DUAL_CONFIRM_CUSHION = 0.0005  # 0.05% — CB spot must also show weak thesis
            if cushion is not None and cushion >= DUAL_CONFIRM_CUSHION:
                # CB spot still supports our thesis — block the stop
                _cush_str = f"{cushion*100:.3f}%"
                _ema_str = f"EMA={ema_dir}" if ema_dir is not None else ""
                logging.info(f"🛡️ STOP_DUAL_BLOCK: {ticker} {side.upper()} "
                             f"bid={current_bid}¢ <= stop={stop_price}¢ BUT "
                             f"CB cushion={_cush_str} still healthy {_ema_str} "
                             f"— Kalshi bid alone, NOT confirmed by spot")
                # Clear any pending stop since spot disagrees
                self._pending_stops.pop(ticker, None)
                continue

            # ── Decision tree based on cushion tier ──
            # Compute EMA delta for shadow log
            _ema_delta = None
            if spot_rest and symbol:
                _ema_delta = spot_rest.get_ema_delta_pct(symbol)

            # Compute minutes to settle from ticker
            _mins_to_settle = None
            try:
                # Ticker: KXBTC15M-26FEB121500-00 → HHMM is chars 7-11 of date part
                _tparts = ticker.split('-')
                if len(_tparts) >= 2:
                    _dp = _tparts[1]  # e.g. "26FEB121500"
                    _hh = int(_dp[-4:-2])
                    _mm = int(_dp[-2:])
                    _now = datetime.now(timezone.utc)
                    _settle = _now.replace(hour=_hh, minute=_mm, second=0, microsecond=0)
                    if _settle < _now:
                        _settle += timedelta(days=1)
                    _mins_to_settle = max(0, int((_settle - _now).total_seconds() / 60))
            except Exception:
                pass

            def _exit(reason_tag):
                loss_per = pos['entry_price'] - current_bid
                total_loss = loss_per * pos['contracts']
                _cush_str = f"cushion={cushion*100:.3f}%" if cushion is not None else "cushion=?"
                _ema_str = f"EMA={ema_dir}" if ema_dir is not None else "EMA=?"
                logging.warning(f"🛑 STOP LOSS ({reason_tag}): {ticker} {side.upper()} "
                                f"bid={current_bid}¢ <= stop={stop_price}¢ "
                                f"{_cush_str} {_ema_str} "
                                f"(entry={pos['entry_price']}¢, "
                                f"-{loss_per}¢/contract, -{total_loss}¢ total)")
                # Structured shadow snapshot for offline analysis
                _spot_val = spot_rest.get_latest_price(symbol) if spot_rest and symbol else None
                shadow_data = {
                    "ticker": ticker, "side": side, "reason": reason_tag,
                    "entry_px": pos['entry_price'], "current_bid": current_bid,
                    "stop_price": stop_price,
                    "spot": round(_spot_val, 2) if _spot_val else None,
                    "strike": round(strike, 2) if strike else None,
                    "cushion_pct": round(cushion * 100, 5) if cushion is not None else None,
                    "ema_dir": ema_dir,
                    "ema_delta_pct": round(_ema_delta * 100, 5) if _ema_delta is not None else None,
                    "minutes_to_settle": _mins_to_settle
                }
                logging.info(f"📊 STOP_SHADOW {shadow_data}")
                exits.append({'ticker': ticker, 'side': side, 'action': 'sell',
                              'count': pos['contracts'], 'price': current_bid,
                              'exit_type': 'stop_loss', 'exit_reason': reason_tag})
                self._exited.add(ticker)
                self._pending_stops.pop(ticker, None)

            # Can't verify thesis → exit
            if cushion is None:
                _exit('NO_DATA')
                continue

            # Tier 1: cushion < 0.05% → exit immediately (micro-noise, not real strength)
            if cushion < 0.0005:
                _exit('THIN_CUSHION')
                continue

            # Tier 2+: cushion >= 0.05% → thesis has some strength, check EMA
            ema_supports = True
            if ema_dir is not None:
                ema_supports = (ema_dir == 'UP') if side == 'yes' else (ema_dir == 'DOWN')

            if not ema_supports and cushion < 0.0010:
                # Weak cushion + EMA against = exit
                _exit('EMA_AGAINST')
                continue

            # Determine max hold scans based on cushion strength
            if cushion >= 0.0010:   # > 0.10%: strong thesis
                max_hold = 3
            else:                    # 0.05-0.10%: moderate
                max_hold = 1

            # Start or update pending stop
            if ticker not in self._pending_stops:
                self._pending_stops[ticker] = {
                    'scans': 1, 'prev_cushion': cushion, 'max_hold': max_hold}
                _cush_str = f"{cushion*100:.3f}%"
                _ema_str = f"EMA={ema_dir}" if ema_dir is not None else "EMA=?"
                logging.info(f"⏳ STOP_PENDING: {ticker} {side.upper()} "
                             f"bid={current_bid}¢ <= stop={stop_price}¢ | "
                             f"cushion={_cush_str} {_ema_str} — "
                             f"holding scan 1/{max_hold}")
            else:
                p = self._pending_stops[ticker]
                p['scans'] += 1
                prev_cushion = p['prev_cushion']

                # Cushion direction check
                cushion_expanding = cushion > prev_cushion
                cushion_shrinking = cushion < prev_cushion - 0.0001  # small noise tolerance
                p['prev_cushion'] = cushion  # update for next scan

                # Safety valve
                if p['scans'] >= self.STOP_MAX_SCANS:
                    _exit('TIMEOUT')
                    continue

                # Cushion shrinking → exit (thesis deteriorating)
                if cushion_shrinking and cushion < 0.0010:
                    _exit('CUSHION_SHRINKING')
                    continue

                # Past max hold scans for this tier
                if p['scans'] >= p['max_hold'] and not cushion_expanding:
                    _exit('MAX_HOLD')
                    continue

                # Still holding
                _cush_str = f"{cushion*100:.3f}%"
                _ema_str = f"EMA={ema_dir}" if ema_dir is not None else "EMA=?"
                _dir = "↑" if cushion_expanding else ("↓" if cushion_shrinking else "→")
                logging.info(f"⏳ STOP_PENDING: {ticker} {side.upper()} "
                             f"bid={current_bid}¢ | cushion={_cush_str}{_dir} "
                             f"{_ema_str} — scan {p['scans']}/{p['max_hold']}")
        return exits

    def _get_bid(self, ticker, side, ticker_data, orderbooks):
        tk = ticker_data.get(ticker)
        if tk:
            if side == 'yes':
                bid = tk.get('yes_bid', 0)
                if bid > 0:
                    return bid
            else:
                yes_ask = tk.get('yes_ask', 0)
                if yes_ask > 0:
                    return 100 - yes_ask
        book = orderbooks.get(ticker)
        if book:
            levels = book.get(side, [])
            if levels:
                return max(lv[0] for lv in levels)
        return None

    def get_tracked_count(self) -> int:
        return len(self.positions)


# ═══════════════════════════════════════════════════════════════════════════════
# STOP-LOSS SHADOW TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

class StopShadowTracker:
    """Tracks what would have happened if we held through a stop-loss exit.
    Records spot at +3min and at settlement to measure stop quality.
    
    Lifecycle per event:
    1. record_stop() — called when stop fires, stores all context
    2. check_pending() — called every scan, checks for 3-min and settlement marks
    3. Writes to CSV progressively (one row per stop, updated in place via JSONL)
    """

    def __init__(self, log_dir: str = 'logs'):
        self.pending: List[dict] = []  # active shadow events
        self.completed: List[dict] = []  # for end-of-session summary
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._csv_path = self._log_dir / f'stop_shadows_{ts}.csv'
        self._jsonl_path = self._log_dir / f'stop_shadows_{ts}.jsonl'
        # Init CSV with headers
        with open(self._csv_path, 'w') as f:
            f.write("exit_time,ticker,side,entry_price,exit_bid,exit_reason,"
                    "spot_at_exit,strike,cushion_at_exit,ma_slope_at_exit,"
                    "spot_3min,cushion_3min,bid_3min,"
                    "spot_at_settle,settled_side_won,would_have_won,"
                    "foregone_pnl_cents,contracts,close_time\n")

    def record_stop(self, ticker: str, side: str, entry_price: int,
                    exit_bid: int, exit_reason: str, spot: float,
                    strike: float, cushion: float, ma_slope: float,
                    contracts: int, close_time: str = ''):
        """Called when a stop-loss fires. Starts the shadow tracking."""
        event = {
            'exit_time': time.time(),
            'exit_time_str': datetime.now(timezone.utc).isoformat(),
            'ticker': ticker,
            'side': side,
            'entry_price': entry_price,
            'exit_bid': exit_bid,
            'exit_reason': exit_reason,
            'spot_at_exit': spot,
            'strike': strike,
            'cushion_at_exit': cushion,
            'ma_slope_at_exit': ma_slope,
            'contracts': contracts,
            'close_time': close_time,
            # Deferred fields
            'spot_3min': None,
            'cushion_3min': None,
            'bid_3min': None,
            'checked_3min': False,
            'spot_at_settle': None,
            'settled_side_won': None,
            'would_have_won': None,
            'foregone_pnl_cents': None,
            'checked_settle': False,
        }
        self.pending.append(event)
        logging.info(f"👁️ STOP_SHADOW: tracking {ticker} {side} "
                     f"(exited @{exit_bid}¢, cushion={cushion*100:.3f}%)")

    def check_pending(self, spot_rest=None, ticker_data: dict = None,
                      orderbooks: dict = None, api_client=None):
        """Called every scan. Checks for 3-min mark and settlement."""
        now = time.time()
        still_pending = []

        for ev in self.pending:
            changed = False

            # ── 3-minute check ──
            if not ev['checked_3min'] and now - ev['exit_time'] >= 180:
                symbol = 'BTC' if 'BTC' in ev['ticker'].upper() else ''
                if spot_rest and symbol:
                    spot_now = spot_rest.get_latest_price(symbol)
                    if spot_now:
                        ev['spot_3min'] = spot_now
                        strike = ev['strike']
                        if strike and strike > 0:
                            if ev['side'] == 'yes':
                                ev['cushion_3min'] = (spot_now - strike) / strike
                            else:
                                ev['cushion_3min'] = (strike - spot_now) / strike
                # Try to get current bid
                if ticker_data:
                    tk = ticker_data.get(ev['ticker'], {})
                    if ev['side'] == 'yes':
                        ev['bid_3min'] = tk.get('yes_bid', 0)
                    else:
                        ya = tk.get('yes_ask', 0)
                        ev['bid_3min'] = 100 - ya if ya > 0 else 0
                ev['checked_3min'] = True
                changed = True

                _s3 = f"${ev['spot_3min']:,.0f}" if ev['spot_3min'] else "?"
                _c3 = f"{ev['cushion_3min']*100:.3f}%" if ev['cushion_3min'] is not None else "?"
                _b3 = f"{ev['bid_3min']}¢" if ev['bid_3min'] else "?"
                logging.info(f"👁️ STOP_SHADOW_3MIN: {ev['ticker']} {ev['side']} "
                             f"spot={_s3} cushion={_c3} bid={_b3} "
                             f"(was {ev['exit_bid']}¢ at exit)")

            # ── Settlement check ──
            if not ev['checked_settle']:
                close_str = ev.get('close_time', '')
                settled = False
                if close_str:
                    try:
                        close_dt = datetime.fromisoformat(
                            close_str.replace('Z', '+00:00'))
                        if datetime.now(timezone.utc) > close_dt + timedelta(seconds=30):
                            settled = True
                    except (ValueError, AttributeError):
                        # If can't parse, check if enough time passed (~15min max)
                        if now - ev['exit_time'] > 1200:
                            settled = True
                else:
                    # No close_time, use 15 minute timeout
                    if now - ev['exit_time'] > 1200:
                        settled = True

                if settled:
                    # Check settlement: get final spot
                    symbol = 'BTC' if 'BTC' in ev['ticker'].upper() else ''
                    if spot_rest and symbol:
                        spot_final = spot_rest.get_latest_price(symbol)
                        ev['spot_at_settle'] = spot_final
                        strike = ev['strike']
                        if spot_final and strike and strike > 0:
                            if ev['side'] == 'yes':
                                # YES wins if spot > strike at settlement
                                won = spot_final > strike
                            else:
                                # NO wins if spot < strike at settlement
                                won = spot_final < strike
                            ev['settled_side_won'] = won
                            ev['would_have_won'] = won
                            if won:
                                # Would have settled at 100¢
                                ev['foregone_pnl_cents'] = (100 - ev['exit_bid']) * ev['contracts']
                            else:
                                # Would have settled at 0¢ — stop was correct
                                ev['foregone_pnl_cents'] = -(ev['exit_bid']) * ev['contracts']

                    ev['checked_settle'] = True
                    changed = True
                    self._write_csv_row(ev)
                    self.completed.append(ev)

                    won_str = "WON" if ev.get('would_have_won') else "LOST"
                    pnl = ev.get('foregone_pnl_cents', 0) or 0
                    spot_str = f"${ev['spot_at_settle']:,.0f}" if ev.get('spot_at_settle') else "?"
                    logging.info(f"👁️ STOP_SHADOW_SETTLE: {ev['ticker']} {ev['side']} "
                                 f"would have {won_str} | spot={spot_str} "
                                 f"foregone={pnl:+}¢ "
                                 f"(exit was @{ev['exit_bid']}¢, reason={ev['exit_reason']})")
                    continue  # don't add to still_pending

            if not ev['checked_settle']:
                still_pending.append(ev)
            if changed:
                self._write_jsonl(ev)

        self.pending = still_pending

    def _write_csv_row(self, ev):
        """Write completed shadow event to CSV."""
        try:
            with open(self._csv_path, 'a') as f:
                _cush_exit = f"{ev['cushion_at_exit']*100:.4f}" if ev.get('cushion_at_exit') is not None else ''
                _slope = f"{ev['ma_slope_at_exit']*100:.5f}" if ev.get('ma_slope_at_exit') is not None else ''
                _s3 = f"{ev['spot_3min']:.2f}" if ev.get('spot_3min') else ''
                _c3 = f"{ev['cushion_3min']*100:.4f}" if ev.get('cushion_3min') is not None else ''
                _b3 = str(ev.get('bid_3min', '')) if ev.get('bid_3min') else ''
                _sf = f"{ev['spot_at_settle']:.2f}" if ev.get('spot_at_settle') else ''
                _won = str(ev.get('settled_side_won', ''))
                _whw = str(ev.get('would_have_won', ''))
                _fpnl = str(ev.get('foregone_pnl_cents', ''))
                f.write(f"{ev['exit_time_str']},{ev['ticker']},{ev['side']},"
                        f"{ev['entry_price']},{ev['exit_bid']},{ev['exit_reason']},"
                        f"{ev.get('spot_at_exit', '')},{ev.get('strike', '')},"
                        f"{_cush_exit},{_slope},"
                        f"{_s3},{_c3},{_b3},"
                        f"{_sf},{_won},{_whw},{_fpnl},"
                        f"{ev['contracts']},{ev.get('close_time', '')}\n")
        except Exception:
            pass

    def _write_jsonl(self, ev):
        """Write event snapshot to JSONL for progressive tracking."""
        try:
            with open(self._jsonl_path, 'a') as f:
                f.write(json.dumps(ev, default=str) + '\n')
        except Exception:
            pass

    def get_summary(self) -> dict:
        """Summary stats across all completed shadow events."""
        if not self.completed:
            return {'total': 0}
        total = len(self.completed)
        would_won = sum(1 for e in self.completed if e.get('would_have_won'))
        would_lost = total - would_won
        foregone = sum(e.get('foregone_pnl_cents', 0) or 0 for e in self.completed)
        return {
            'total': total,
            'would_have_won': would_won,
            'would_have_lost': would_lost,
            'win_rate_if_held': f"{would_won/total*100:.1f}%" if total else "n/a",
            'net_foregone_pnl_cents': foregone
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: NO HARVESTER
# ═══════════════════════════════════════════════════════════════════════════════

class NoHarvester:
    """Buy YES at 91-99¢ (safe) or NO at 1-9¢ (upset hunter)."""

    def __init__(self, config: TradingConfig):
        self.config = config

    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        signals = []
        for market in markets:
            ticker = market.get('ticker', '')
            volume = market.get('volume', 0)
            if volume < self.config.NO_HARVEST_MIN_VOLUME:
                continue
            close_time_str = market.get('close_time')
            if close_time_str:
                try:
                    close_time = datetime.fromisoformat(
                        close_time_str.replace('Z', '+00:00'))
                    mins = (close_time - datetime.now(timezone.utc)).total_seconds() / 60
                    if mins < self.config.NO_HARVEST_MIN_MINUTES:
                        continue
                except (ValueError, AttributeError):
                    continue
            book = orderbooks.get(ticker)
            if not book:
                continue
            yes_bids = book.get('yes', [])
            no_bids = book.get('no', [])
            if not yes_bids or not no_bids:
                continue
            best_yes_bid_price = max(b[0] for b in yes_bids)
            best_no_bid_price = max(b[0] for b in no_bids)
            buy_yes_cost = 100 - best_no_bid_price if best_no_bid_price > 0 else None
            buy_no_cost = 100 - best_yes_bid_price if best_yes_bid_price > 0 else None

            if buy_yes_cost and self.config.NO_HARVEST_MIN_PRICE <= buy_yes_cost <= self.config.NO_HARVEST_MAX_PRICE:
                profit = net_profit_cents(buy_yes_cost, 1, self.config.FEE_RATE)
                trade_roi = roi(buy_yes_cost, 1, self.config.FEE_RATE)
                if profit > 0:
                    max_qty = min(self.config.NO_HARVEST_MAX_CONTRACTS, best_no_bid_price)
                    if max_qty >= 1:
                        signals.append(TradeSignal(
                            strategy='NO_HARVEST_SAFE', ticker=ticker, side='yes',
                            action='buy', price_cents=buy_yes_cost, contracts=max_qty,
                            edge_cents=profit * max_qty, edge_roi=trade_roi,
                            confidence=0.90,
                            reason=f"Buy YES@{buy_yes_cost}¢ win {profit}¢/c (vol={volume:,} {mins:.0f}min)",
                            market_data={'volume': volume, 'minutes_to_close': mins}))

            if buy_no_cost and 1 <= buy_no_cost <= (100 - self.config.NO_HARVEST_MIN_PRICE):
                profit = net_profit_cents(buy_no_cost, 1, self.config.FEE_RATE)
                trade_roi = roi(buy_no_cost, 1, self.config.FEE_RATE)
                if profit > 0 and trade_roi > 5.0:
                    max_qty = min(self.config.NO_HARVEST_MAX_CONTRACTS, 3)
                    signals.append(TradeSignal(
                        strategy='NO_HARVEST_UPSET', ticker=ticker, side='no',
                        action='buy', price_cents=buy_no_cost, contracts=max_qty,
                        edge_cents=profit * max_qty, edge_roi=trade_roi,
                        confidence=0.10,
                        reason=f"Buy NO@{buy_no_cost}¢ upset ROI={trade_roi:.0%} (vol={volume:,})",
                        market_data={'volume': volume, 'minutes_to_close': mins}))
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: STRUCTURAL ARB
# ═══════════════════════════════════════════════════════════════════════════════

class StructuralArb:
    """Detect stale/fragile orderbooks and trade before MMs update."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.previous_books: Dict[str, Tuple[str, float]] = {}

    def _book_signature(self, orderbook: dict) -> str:
        yes_top5 = sorted(orderbook.get('yes', []), key=lambda x: x[0], reverse=True)[:5]
        no_top5 = sorted(orderbook.get('no', []), key=lambda x: x[0], reverse=True)[:5]
        raw = json.dumps({'y': yes_top5, 'n': no_top5}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _is_stale(self, ticker: str, book: dict) -> bool:
        sig = self._book_signature(book)
        now = time.time()
        if ticker in self.previous_books:
            prev_sig, prev_time = self.previous_books[ticker]
            age = now - prev_time
            if age > 60 and sig == prev_sig:
                self.previous_books[ticker] = (sig, prev_time)
                return True
        self.previous_books[ticker] = (sig, now)
        return False

    def _is_fragile(self, levels: list, min_gap=30, max_qty=500) -> bool:
        if len(levels) < 2:
            return False
        sorted_levels = sorted(levels, key=lambda x: x[0], reverse=True)
        best_price, best_qty = sorted_levels[0]
        second_price, _ = sorted_levels[1]
        gap = best_price - second_price
        return gap >= min_gap and best_qty <= max_qty

    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        signals = []
        for market in markets:
            ticker = market.get('ticker', '')
            volume = market.get('volume', 0)
            if volume < self.config.STRUCT_MIN_VOLUME:
                continue
            book = orderbooks.get(ticker)
            if not book:
                continue
            yes_bids = book.get('yes', [])
            no_bids = book.get('no', [])
            if not yes_bids or not no_bids:
                continue
            is_stale = self._is_stale(ticker, book)
            yes_fragile = self._is_fragile(yes_bids)
            no_fragile = self._is_fragile(no_bids)
            best_yes_bid = max(b[0] for b in yes_bids)
            best_no_bid = max(b[0] for b in no_bids)
            buy_yes_cost = 100 - best_no_bid
            buy_no_cost = 100 - best_yes_bid
            spread = buy_yes_cost - best_yes_bid
            if spread < self.config.STRUCT_MIN_SPREAD:
                continue
            for side, cost, fragile in [('yes', buy_yes_cost, no_fragile),
                                        ('no', buy_no_cost, yes_fragile)]:
                if cost <= 0 or cost >= 100:
                    continue
                profit = net_profit_cents(cost, 1, self.config.FEE_RATE)
                trade_roi = roi(cost, 1, self.config.FEE_RATE)
                if trade_roi < self.config.STRUCT_MIN_ROI:
                    continue
                score, flags = 0, []
                if is_stale:
                    score += 3; flags.append('STALE')
                if fragile:
                    score += 2; flags.append('FRAGILE')
                if spread >= 20:
                    score += 1; flags.append(f'SPREAD={spread}¢')
                if volume >= 1000:
                    score += 1; flags.append(f'VOL={volume}')
                if self.config.STRUCT_REQUIRE_STALE and not is_stale:
                    continue
                if score < 3:
                    continue
                contracts = min(self.config.STRUCT_MAX_CONTRACTS, 5)
                signals.append(TradeSignal(
                    strategy='STRUCTURAL', ticker=ticker, side=side,
                    action='buy', price_cents=cost, contracts=contracts,
                    edge_cents=profit * contracts, edge_roi=trade_roi,
                    confidence=min(0.3 + score * 0.1, 0.8),
                    reason=f"STRUCTURAL [{','.join(flags)}] {side.upper()}@{cost}¢ spread={spread}¢",
                    market_data={'volume': volume, 'spread': spread,
                                 'is_stale': is_stale, 'score': score}))
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: CROSS-MARKET CORRELATION
# ═══════════════════════════════════════════════════════════════════════════════

class CrossMarketArb:
    """Find mispriced markets within mutually exclusive events."""

    def __init__(self, config: TradingConfig):
        self.config = config

    def scan(self, events: List[dict], orderbooks: dict) -> List[TradeSignal]:
        signals = []
        for event in events:
            if not event.get('mutually_exclusive', False):
                continue
            markets = event.get('markets', [])
            if len(markets) < 2:
                continue
            market_prices = []
            all_have_books = True
            for m in markets:
                ticker = m.get('ticker', '')
                book = orderbooks.get(ticker)
                if not book or not book.get('yes') or not book.get('no'):
                    all_have_books = False
                    break
                yes_bids = book.get('yes', [])
                no_bids = book.get('no', [])
                best_yes_bid = max(b[0] for b in yes_bids) if yes_bids else 0
                best_no_bid = max(b[0] for b in no_bids) if no_bids else 0
                buy_yes = 100 - best_no_bid if best_no_bid > 0 else None
                buy_no = 100 - best_yes_bid if best_yes_bid > 0 else None
                market_prices.append({
                    'ticker': ticker, 'title': m.get('title', '')[:40],
                    'volume': m.get('volume', 0),
                    'best_yes_bid': best_yes_bid, 'best_no_bid': best_no_bid,
                    'buy_yes': buy_yes, 'buy_no': buy_no,
                    'mid_yes': (buy_yes + best_yes_bid) / 2 if buy_yes and best_yes_bid else None})
            if not all_have_books or not market_prices:
                continue
            valid_prices = [mp for mp in market_prices if mp['mid_yes'] is not None]
            if len(valid_prices) < 2:
                continue
            sum_yes = sum(mp['mid_yes'] for mp in valid_prices)
            overpricing = sum_yes - 100
            if abs(overpricing) < self.config.CROSS_MIN_EDGE_CENTS:
                continue
            if overpricing > 0:
                valid_prices.sort(key=lambda x: x['mid_yes'], reverse=True)
                target = valid_prices[0]
                if not target['buy_no'] or target['volume'] < self.config.CROSS_MIN_VOLUME:
                    continue
                edge = max(1, int(overpricing / 2))
                profit = net_profit_cents(target['buy_no'], 1, self.config.FEE_RATE)
                if profit < self.config.CROSS_MIN_EDGE_CENTS:
                    continue
                signals.append(TradeSignal(
                    strategy='CROSS_MARKET', ticker=target['ticker'], side='no',
                    action='buy', price_cents=target['buy_no'],
                    contracts=min(self.config.CROSS_MAX_CONTRACTS, 3),
                    edge_cents=edge,
                    edge_roi=roi(target['buy_no'], 1, self.config.FEE_RATE),
                    confidence=0.6,
                    reason=f"CROSS Sum={sum_yes:.0f}¢ Buy NO@{target['buy_no']}¢ overpriced {overpricing:.0f}¢",
                    market_data={'sum_yes': sum_yes, 'overpricing': overpricing}))
            elif overpricing < 0:
                valid_prices.sort(key=lambda x: x['mid_yes'])
                target = valid_prices[0]
                if not target['buy_yes'] or target['volume'] < self.config.CROSS_MIN_VOLUME:
                    continue
                edge = max(1, int(abs(overpricing) / 2))
                profit = net_profit_cents(target['buy_yes'], 1, self.config.FEE_RATE)
                if profit < self.config.CROSS_MIN_EDGE_CENTS:
                    continue
                signals.append(TradeSignal(
                    strategy='CROSS_MARKET', ticker=target['ticker'], side='yes',
                    action='buy', price_cents=target['buy_yes'],
                    contracts=min(self.config.CROSS_MAX_CONTRACTS, 3),
                    edge_cents=edge,
                    edge_roi=roi(target['buy_yes'], 1, self.config.FEE_RATE),
                    confidence=0.6,
                    reason=f"CROSS Sum={sum_yes:.0f}¢ Buy YES@{target['buy_yes']}¢ underpriced {abs(overpricing):.0f}¢",
                    market_data={'sum_yes': sum_yes, 'overpricing': overpricing}))
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: CRYPTO SCALPER (+ P3 Binance Confirmation)
# ═══════════════════════════════════════════════════════════════════════════════

class CryptoScalper:
    """Crypto scalper with two modes:
    1. SPOT-DRIVEN (default): Spot price momentum is the PRIMARY signal.
       Parse strike from market, compare to current price + momentum.
       Orderbook provides price/spread/depth filtering only.
    2. ORDERBOOK-ONLY (--no-spot): Pure orderbook imbalance (legacy mode).
    """

    STRIKE_RE = re.compile(r'\$([0-9,]+(?:\.[0-9]+)?)')

    def __init__(self, config: TradingConfig):
        self.config = config
        self.spot_feed: Optional[SpotPriceFeed] = None  # legacy WS feed
        self.spot_rest: Optional[SpotPriceREST] = None  # new REST feed
        self.shadow_blocks: List[dict] = []  # blocked signals for shadow analysis
        # EMA 50/100/150 structure filter state (1-min candles)
        self._1min_candles: List[float] = []
        self._1min_candle_start: Optional[int] = None
        self._1min_candle_prices: List[float] = []
        self._ema50: Optional[float] = None
        self._ema100: Optional[float] = None
        self._ema150: Optional[float] = None
        self._ema_ready: bool = False
        # RSI state (Wilder's smoothed, 14-period on 1-min candles)
        self._rsi_avg_gain: float = 0.0
        self._rsi_avg_loss: float = 0.0
        self._rsi_value: Optional[float] = None
        self._rsi_ready: bool = False
        self._rsi_prev_close: Optional[float] = None
        self._rsi_count: int = 0  # candles processed for RSI
        # MACD state (12/26/9 on 1-min candles)
        self._macd_ema_fast: Optional[float] = None   # EMA-12
        self._macd_ema_slow: Optional[float] = None   # EMA-26
        self._macd_line: Optional[float] = None        # EMA12 - EMA26
        self._macd_signal_line: Optional[float] = None # EMA-9 of MACD line
        self._macd_histogram: Optional[float] = None   # MACD - Signal
        self._macd_ready: bool = False
        self._macd_line_count: int = 0  # how many MACD line values we've computed

    def _accumulate_candle(self, spot_price: float):
        """Accumulate 1-minute candles from spot price ticks for EMA 50/100/150, RSI, MACD."""
        current_min = int(time.time() // 60)
        if self._1min_candle_start is None or current_min != self._1min_candle_start:
            # Close previous candle if we have prices
            if self._1min_candle_prices:
                close_price = self._1min_candle_prices[-1]
                self._1min_candles.append(close_price)
                self._update_emas(close_price)
                self._update_rsi(close_price)
                self._update_macd(close_price)
                # Keep max 300 candles (5 hours)
                if len(self._1min_candles) > 300:
                    self._1min_candles.pop(0)
            # Start new candle
            self._1min_candle_start = current_min
            self._1min_candle_prices = []
        self._1min_candle_prices.append(spot_price)

    def _update_emas(self, close_price: float):
        """Update EMA 50/100/150 incrementally on each new 1-min candle close."""
        if len(self._1min_candles) == 1:
            self._ema50 = close_price
            self._ema100 = close_price
            self._ema150 = close_price
            return

        k50 = 2 / (50 + 1)
        k100 = 2 / (100 + 1)
        k150 = 2 / (150 + 1)

        self._ema50 = close_price * k50 + self._ema50 * (1 - k50)
        self._ema100 = close_price * k100 + self._ema100 * (1 - k100)
        self._ema150 = close_price * k150 + self._ema150 * (1 - k150)

        if len(self._1min_candles) >= self.config.CRYPTO_EMA_MIN_CANDLES:
            self._ema_ready = True

    def _update_rsi(self, close_price: float):
        """Update RSI incrementally using Wilder's smoothing on each new 1-min candle close.
        
        RSI = 100 - (100 / (1 + RS))  where RS = avg_gain / avg_loss
        First 14 changes: simple average of gains/losses
        After: smoothed: avg = (prev_avg * 13 + current) / 14
        """
        period = self.config.CRYPTO_RSI_PERIOD

        if self._rsi_prev_close is None:
            # First candle — no change to compute yet, don't count
            self._rsi_prev_close = close_price
            return

        # Compute price change
        change = close_price - self._rsi_prev_close
        self._rsi_prev_close = close_price
        gain = max(0.0, change)
        loss = max(0.0, -change)
        self._rsi_count += 1  # count actual changes, not candles

        if self._rsi_count <= period:
            # Accumulation phase: sum up gains/losses
            self._rsi_avg_gain += gain
            self._rsi_avg_loss += loss

            if self._rsi_count == period:
                # Initial averages (simple mean of first N periods)
                self._rsi_avg_gain /= period
                self._rsi_avg_loss /= period
                if self._rsi_avg_loss > 0:
                    rs = self._rsi_avg_gain / self._rsi_avg_loss
                    self._rsi_value = 100.0 - (100.0 / (1.0 + rs))
                else:
                    self._rsi_value = 100.0  # all gains, no losses
                self._rsi_ready = True
        else:
            # Wilder's smoothing: (prev * (period-1) + current) / period
            self._rsi_avg_gain = (self._rsi_avg_gain * (period - 1) + gain) / period
            self._rsi_avg_loss = (self._rsi_avg_loss * (period - 1) + loss) / period
            if self._rsi_avg_loss > 0:
                rs = self._rsi_avg_gain / self._rsi_avg_loss
                self._rsi_value = 100.0 - (100.0 / (1.0 + rs))
            else:
                self._rsi_value = 100.0

    def _update_macd(self, close_price: float):
        """Update MACD (12/26/9) incrementally on each new 1-min candle close.
        
        MACD line = EMA(fast) - EMA(slow)
        Signal line = EMA(signal_period) of MACD line
        Histogram = MACD line - Signal line
        """
        fast_p = self.config.CRYPTO_MACD_FAST    # 12
        slow_p = self.config.CRYPTO_MACD_SLOW    # 26
        sig_p = self.config.CRYPTO_MACD_SIGNAL   # 9
        n = len(self._1min_candles)

        # Update fast EMA (EMA-12)
        if n == 1:
            self._macd_ema_fast = close_price
            self._macd_ema_slow = close_price
            return

        k_fast = 2.0 / (fast_p + 1)
        k_slow = 2.0 / (slow_p + 1)

        self._macd_ema_fast = close_price * k_fast + self._macd_ema_fast * (1 - k_fast)
        self._macd_ema_slow = close_price * k_slow + self._macd_ema_slow * (1 - k_slow)

        # Need at least slow_p candles before MACD line is meaningful
        if n < slow_p:
            return

        # Compute MACD line
        self._macd_line = self._macd_ema_fast - self._macd_ema_slow
        self._macd_line_count += 1

        # Update signal line (EMA-9 of MACD line)
        if self._macd_line_count == 1:
            self._macd_signal_line = self._macd_line
        else:
            k_sig = 2.0 / (sig_p + 1)
            self._macd_signal_line = self._macd_line * k_sig + self._macd_signal_line * (1 - k_sig)

        # Histogram
        self._macd_histogram = self._macd_line - self._macd_signal_line

        # Ready once we have enough signal line samples
        if self._macd_line_count >= sig_p:
            self._macd_ready = True

    def _get_ema_structure(self, spot: float):
        """Determine EMA stack direction and price zone. Returns (stack, zone) or (None, None)."""
        if not self._ema_ready or self._ema50 is None:
            return None, None

        bull_stack = self._ema50 > self._ema100 > self._ema150
        bear_stack = self._ema50 < self._ema100 < self._ema150

        if not bull_stack and not bear_stack:
            return 'FLAT', None

        if bull_stack:
            if spot > self._ema50:
                zone = "ABOVE_ALL"
            elif spot > self._ema100:
                zone = "PULLBACK_50_100"
            elif spot > self._ema150:
                zone = "PULLBACK_100_150"
            else:
                zone = "BELOW_ALL"
            return 'BULL', zone

        # bear_stack
        if spot < self._ema50:
            zone = "BELOW_ALL"
        elif spot < self._ema100:
            zone = "BOUNCE_50_100"
        elif spot < self._ema150:
            zone = "BOUNCE_100_150"
        else:
            zone = "ABOVE_ALL"
        return 'BEAR', zone

    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        if not self.config.CRYPTO_ENABLED:
            return []
        signals = []
        crypto_kws = [k.upper() for k in self.config.CRYPTO_TICKERS]
        directional_kws = [k.lower() for k in self.config.CRYPTO_KEYWORDS]
        for market in markets:
            ticker = market.get('ticker', '')
            title = market.get('title', '').lower()
            volume = market.get('volume', 0) or 0
            if not any(kw in ticker.upper() for kw in crypto_kws):
                continue
            if not any(kw in title for kw in directional_kws):
                continue
            if volume < self.config.CRYPTO_MIN_VOLUME:
                continue
            # CRITICAL: Only trade 15-minute contracts, NOT daily/hourly
            if '15M' not in ticker.upper():
                logging.debug(f"SKIP_NON_15M: {ticker}")
                continue

            book = orderbooks.get(ticker)
            if not book:
                continue

            # Route to spot-driven or orderbook-only mode
            if self.config.CRYPTO_SPOT_DRIVEN and self.spot_rest:
                reading_count = self.spot_rest.get_reading_count(
                    self._ticker_to_symbol(ticker))
                if reading_count >= self.config.CRYPTO_SPOT_MIN_READINGS:
                    signal = self._analyze_spot_driven(book, market, ticker, title, volume)
                else:
                    # WARMUP: spot feed not ready — skip, do NOT fall back to blind orderbook
                    logging.debug(f"WARMUP: {ticker} spot n={reading_count}/{self.config.CRYPTO_SPOT_MIN_READINGS} — waiting")
                    continue
            else:
                # Legacy: try both sides with pure orderbook
                for side in ['yes', 'no']:
                    signal = self._analyze_orderbook_only(book, side, ticker, title, volume)
                    if signal:
                        signals.append(signal)
                continue

            if signal:
                signals.append(signal)

        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals

    def _ticker_to_symbol(self, ticker: str) -> str:
        t = ticker.upper()
        for sym in ['BTC']:
            if sym in t:
                return sym
        return ''

    def _parse_strike(self, market: dict) -> Optional[float]:
        """Extract strike price from market data."""
        # Try floor_strike field first (API may include it)
        strike = market.get('floor_strike')
        if strike and float(strike) > 0:
            return float(strike)
        # Fallback: parse from title
        title = market.get('title', '')
        match = self.STRIKE_RE.search(title)
        if match:
            return float(match.group(1).replace(',', ''))
        return None

    def _parse_direction(self, title: str) -> str:
        """Is this an 'above' or 'below' contract?"""
        t = title.lower()
        if any(kw in t for kw in ['above', 'up', 'higher', 'at or above']):
            return 'ABOVE'
        elif any(kw in t for kw in ['below', 'down', 'lower', 'at or below']):
            return 'BELOW'
        return 'UNKNOWN'

    def _analyze_spot_driven(self, book, market, ticker, title, volume):
        """SPOT-DRIVEN MODE: Price action is the signal, orderbook is the filter."""
        try:
            symbol = self._ticker_to_symbol(ticker)
            if not symbol or not self.spot_rest:
                logging.debug(f"💀 NO_SYMBOL {ticker}")
                return None

            spot = self.spot_rest.get_latest_price(symbol)
            if not spot:
                logging.info(f"💀 NO_SPOT {ticker} ({symbol})")
                return None

            momentum = self.spot_rest.get_momentum(
                symbol, self.config.CRYPTO_SPOT_LOOKBACK_SECONDS)
            if momentum is None:
                logging.info(f"💀 NO_MOMENTUM {ticker} ({symbol})")
                return None

            dollar_change = self.spot_rest.get_dollar_change(
                symbol, self.config.CRYPTO_SPOT_LOOKBACK_SECONDS)

            # ── Parse market structure ────────────────────────────
            direction = self._parse_direction(title)
            if direction == 'UNKNOWN':
                logging.info(f"💀 UNKNOWN_DIRECTION {ticker}: title='{title}'")
                return None
            strike = self._parse_strike(market)

            # ── Determine trade side from spot position relative to strike ──
            # "Stay" bet logic: buy the outcome that's already winning.
            # If spot > strike on "above X" → YES is winning → buy YES
            # If spot < strike on "above X" → NO is winning → buy NO
            # Momentum is used for confidence, NOT side selection.
            if not strike or not spot:
                logging.info(f"💀 NO_STRIKE_DATA {ticker}: strike={strike} spot={spot}, cannot validate cushion")
                return None

            if direction == 'ABOVE':
                side = 'yes' if spot > strike else 'no'
            else:  # BELOW
                side = 'yes' if spot < strike else 'no'

            # ── Cushion validation (spot must already be on winning side) ──
            # This is now guaranteed by the side selection above, but we keep
            # an explicit check for safety + logging clarity.
            cushion_ok = True
            if side == 'yes' and direction == 'ABOVE' and spot <= strike:
                cushion_ok = False
            elif side == 'no' and direction == 'ABOVE' and spot >= strike:
                cushion_ok = False
            elif side == 'yes' and direction == 'BELOW' and spot >= strike:
                cushion_ok = False
            elif side == 'no' and direction == 'BELOW' and spot <= strike:
                cushion_ok = False
            if not cushion_ok:
                logging.info(f"💀 NO_CUSHION {ticker}: spot=${spot:,.2f} strike=${strike:,.2f} "
                             f"side={side} dir={direction}, skipping")
                return None

            # ── Minimum cushion distance: spot must be ≥0.05% from strike ──
            cushion_pct = abs(spot - strike) / strike
            if cushion_pct < 0.0010:  # 0.10%
                logging.info(f"💀 THIN_CUSHION {ticker}: cushion={cushion_pct*100:.3f}% < 0.10% "
                             f"(spot=${spot:,.2f} strike=${strike:,.2f}), skipping")
                return None

            # ── EMA NOISE FILTER: fast directional confirmation ──────────
            # Replaces 5-minute MA gate. EMA(3) has ~1.5s effective lag vs
            # 2.5 MINUTES for the old MA. Only blocks when cushion is thin
            # AND EMA disagrees — strong cushion overrides everything.
            ema_dir = self.spot_rest.get_ema_direction(symbol) if self.spot_rest else None
            ema_delta = self.spot_rest.get_ema_delta_pct(symbol) if self.spot_rest else None
            _close_time = market.get('close_time', '')

            # Shadow block helper
            def _shadow(reason):
                self.shadow_blocks.append({
                    'ticker': ticker, 'side': side, 'buy_price': 0,
                    'contracts': 0, 'block_reason': reason,
                    'spot': spot, 'strike': strike, 'cushion_pct': cushion_pct,
                    'momentum': momentum, 'ma_slope': None,
                    'distance_from_ma': ema_delta,
                    'close_time': _close_time})

            # Strong cushion (>= 0.15%) → skip EMA check, this is a real move
            if cushion_pct < self.config.CRYPTO_EMA_NOISE_CUSHION:
                # Thin cushion — need EMA to agree to avoid single-tick noise
                if ema_dir is not None:
                    ema_agrees = (ema_dir == 'UP' and side == 'yes') or \
                                 (ema_dir == 'DOWN' and side == 'no')
                    if not ema_agrees:
                        logging.info(f"💀 EMA_NOISE_BLOCK {ticker} {side.upper()}: "
                                     f"EMA says {ema_dir} but want {side.upper()}, "
                                     f"cushion={cushion_pct*100:.3f}% < {self.config.CRYPTO_EMA_NOISE_CUSHION*100:.2f}% "
                                     f"(thin cushion + EMA disagree = likely noise)")
                        _shadow('EMA_NOISE_BLOCK')
                        return None
                elif self.spot_rest and self.spot_rest._ema_tick_count.get(symbol, 0) < self.config.CRYPTO_EMA_MIN_TICKS:
                    # EMA not ready yet AND cushion is thin — wait
                    _tc = self.spot_rest._ema_tick_count.get(symbol, 0)
                    logging.debug(f"EMA_WARMUP {ticker}: {_tc}/{self.config.CRYPTO_EMA_MIN_TICKS} ticks, "
                                  f"cushion={cushion_pct*100:.3f}% thin — waiting")
                    return None

            # ── EMA 50/100/150 STRUCTURE FILTER ──────────────────────
            # Higher-level trend filter using 1-min candle EMAs.
            # Blocks low-WR setups, logs high-WR boosts.
            if self.config.CRYPTO_EMA_STRUCTURE_ENABLED:
                _stack, _zone = self._get_ema_structure(spot)
                if _stack and _zone:  # ema_ready and clear trend
                    # === BLOCK RULES (backtested WR below 70%) ===
                    if _stack == 'BEAR' and _zone == 'BOUNCE_50_100' and side == 'no':
                        logging.info(f"💀 EMA_STRUCTURE_BLOCK {ticker} NO: bear stack bounce zone, "
                                     f"NO trade blocked (68.5% backtest WR)")
                        _shadow('EMA_STRUCTURE_BLOCK')
                        return None
                    if _stack == 'BULL' and _zone == 'BELOW_ALL' and side == 'yes':
                        logging.info(f"💀 EMA_STRUCTURE_BLOCK {ticker} YES: bull trend broken below EMA150, "
                                     f"YES trade blocked (71.4% backtest WR)")
                        _shadow('EMA_STRUCTURE_BLOCK')
                        return None
                    if _stack == 'BEAR' and _zone == 'ABOVE_ALL' and side == 'yes':
                        logging.info(f"💀 EMA_STRUCTURE_BLOCK {ticker} YES: bear trend broken above EMA150, "
                                     f"YES trade blocked (69.7% backtest WR)")
                        _shadow('EMA_STRUCTURE_BLOCK')
                        return None
                    # === HIGH CONFIDENCE SIGNALS (backtested WR above 80%) ===
                    if _stack == 'BULL' and _zone == 'PULLBACK_50_100' and side == 'no':
                        logging.info(f"⚡ EMA_STRUCTURE_BOOST {ticker} NO: bull pullback zone NO trade "
                                     f"(83.3% backtest WR)")
                    if _stack == 'BEAR' and _zone == 'BOUNCE_50_100' and side == 'yes':
                        logging.info(f"⚡ EMA_STRUCTURE_BOOST {ticker} YES: bear bounce zone YES trade "
                                     f"(81.7% backtest WR)")
                elif not self._ema_ready and self.config.CRYPTO_EMA_STRUCTURE_ENABLED:
                    logging.debug(f"EMA_STRUCTURE: warming up ({len(self._1min_candles)}/{self.config.CRYPTO_EMA_MIN_CANDLES} candles), passing")

            # ── RSI FILTER: block overbought/oversold counter-trend entries ──
            if self.config.CRYPTO_RSI_ENABLED and self._rsi_ready and self._rsi_value is not None:
                _rsi = self._rsi_value
                if _rsi >= self.config.CRYPTO_RSI_OVERBOUGHT and side == 'yes':
                    logging.info(f"💀 RSI_BLOCK {ticker} YES: RSI={_rsi:.1f} >= {self.config.CRYPTO_RSI_OVERBOUGHT} "
                                 f"(overbought, YES likely to reverse)")
                    _shadow('RSI_BLOCK')
                    return None
                if _rsi <= self.config.CRYPTO_RSI_OVERSOLD and side == 'no':
                    logging.info(f"💀 RSI_BLOCK {ticker} NO: RSI={_rsi:.1f} <= {self.config.CRYPTO_RSI_OVERSOLD} "
                                 f"(oversold, NO likely to bounce)")
                    _shadow('RSI_BLOCK')
                    return None

            # ── MACD FILTER: block trades against momentum direction ──
            if self.config.CRYPTO_MACD_ENABLED and self._macd_ready and self._macd_histogram is not None:
                _hist = self._macd_histogram
                if _hist < 0 and side == 'yes':
                    logging.info(f"💀 MACD_BLOCK {ticker} YES: histogram={_hist:.2f} < 0 "
                                 f"(bearish momentum, YES risky)")
                    _shadow('MACD_BLOCK')
                    return None
                if _hist > 0 and side == 'no':
                    logging.info(f"💀 MACD_BLOCK {ticker} NO: histogram={_hist:+.2f} > 0 "
                                 f"(bullish momentum, NO risky)")
                    _shadow('MACD_BLOCK')
                    return None

            # ── Orderbook filter (price, spread, depth) ───────────
            yes_levels = book.get("yes", []) or []
            no_levels = book.get("no", []) or []
            if not yes_levels or not no_levels:
                logging.info(f"💀 EMPTY_BOOK {ticker} {side}: yes={len(yes_levels)} no={len(no_levels)}")
                return None

            if side == "yes":
                best_bid = max(lv[0] for lv in yes_levels)
                best_no_bid = max(lv[0] for lv in no_levels)
                buy_price = 100 - best_no_bid if best_no_bid > 0 else 100
            else:
                best_bid = max(lv[0] for lv in no_levels)
                best_yes_bid = max(lv[0] for lv in yes_levels)
                buy_price = 100 - best_yes_bid if best_yes_bid > 0 else 100

            # PATCHED: Floor at 55¢ — hard minimum, do not buy speculative cheap contracts
            if buy_price < 3 or buy_price > self.config.CRYPTO_MAX_PRICE:
                logging.info(f"💀 PRICE_REJECT {ticker} {side}: buy_price={buy_price}¢ (outside 3-{self.config.CRYPTO_MAX_PRICE})")
                return None
            spread = buy_price - best_bid
            if spread > self.config.CRYPTO_MAX_SPREAD or spread <= 0:
                logging.info(f"💀 SPREAD_REJECT {ticker} {side}: spread={spread}¢ (max={self.config.CRYPTO_MAX_SPREAD})")
                return None

            yes_depth = sum(lv[1] for lv in yes_levels)
            no_depth = sum(lv[1] for lv in no_levels)
            total_depth = yes_depth + no_depth
            if total_depth == 0:
                logging.info(f"💀 ZERO_DEPTH {ticker} {side}")
                return None
            if self.config.MIN_DEPTH_FILTER_ENABLED:
                if total_depth < self.config.MIN_BOOK_DEPTH:
                    logging.info(f"💀 LOW_DEPTH {ticker} {side}: depth={total_depth} < {self.config.MIN_BOOK_DEPTH}")
                    return None

            # ── Confidence from momentum strength + strike proximity ──
            mom_strength = min(1.0, abs(momentum) / 0.005)  # max at 0.5% move
            spread_score = max(0, 1 - spread / self.config.CRYPTO_MAX_SPREAD)
            vol_score = min(1.0, volume / 500.0)
            depth_score = min(1.0, total_depth / 200.0) * 0.05

            # Strike proximity bonus: contracts near current price have more edge
            strike_bonus = 0.0
            if strike and spot:
                distance_pct = abs(strike - spot) / spot
                if distance_pct < 0.005:  # within 0.5% of spot
                    strike_bonus = 0.10
                elif distance_pct < 0.01:  # within 1%
                    strike_bonus = 0.05

            # Orderbook confirmation bonus
            if side == "yes":
                imbalance = (yes_depth - no_depth) / total_depth
            else:
                imbalance = (no_depth - yes_depth) / total_depth
            ob_bonus = max(0, imbalance * 0.10)  # small bonus if book agrees

            confidence = min(0.95, 0.30 + mom_strength * 0.35 + strike_bonus
                             + spread_score * 0.10 + vol_score * 0.05
                             + depth_score + ob_bonus)

            # Two-tier sizing: cheap contracts get full size, expensive get half
            _full_size = self.config.CRYPTO_SIZE_BTC if symbol == 'BTC' else self.config.CRYPTO_SIZE_XRP
            if buy_price <= 60:
                _size = _full_size       # 3-60¢: full size
            else:
                _size = _full_size / 2   # 61-70¢: half size
            contracts = min(100, max(1, int(_size * 100 / buy_price)))
            profit = net_profit_cents(buy_price, contracts, self.config.FEE_RATE)
            trade_roi = roi(buy_price, contracts, self.config.FEE_RATE)

            spot_fmt = f"${spot:,.2f}" if spot < 100 else f"${spot:,.0f}"
            strike_fmt = f" strike=${strike:,.2f}" if strike and strike < 100 else (f" strike=${strike:,.0f}" if strike else "")
            dir_arrow = "↑" if momentum > 0 else "↓"
            ema_str = f" EMA={ema_dir}" if ema_dir is not None else ""
            ema_d_str = f" ema_Δ={ema_delta*100:.3f}%" if ema_delta is not None else ""
            _rsi_sig = f" RSI={self._rsi_value:.0f}" if self._rsi_ready and self._rsi_value is not None else ""
            _macd_sig = f" MACD={self._macd_histogram:+.1f}" if self._macd_ready and self._macd_histogram is not None else ""
            logging.info(f"🎯 SPOT_SIGNAL {ticker} {side.upper()}: "
                         f"{symbol}={spot_fmt}{dir_arrow} mom={momentum*100:+.3f}% "
                         f"Δ${dollar_change:+,.2f}{strike_fmt} "
                         f"cush={cushion_pct*100:.3f}%{ema_str}{ema_d_str}"
                         f"{_rsi_sig}{_macd_sig} "
                         f"conf={confidence:.0%} @{buy_price}¢")

            return TradeSignal(
                strategy='CRYPTO_SCALP', ticker=ticker, side=side,
                action='buy', price_cents=buy_price, contracts=contracts,
                edge_cents=profit, edge_roi=trade_roi, confidence=confidence,
                reason=f"SPOT {symbol}={spot_fmt}{dir_arrow} mom={momentum*100:+.3f}% "
                       f"Δ${dollar_change:+,.2f}{strike_fmt} "
                       f"spread={spread}¢ depth={total_depth}",
                market_data={'imbalance': imbalance, 'spread': spread,
                             'volume': volume, 'direction': direction,
                             'yes_depth': yes_depth, 'no_depth': no_depth,
                             'total_depth': total_depth, 'momentum': momentum,
                             'spot_price': spot, 'strike': strike,
                             'cushion_pct': cushion_pct,
                             'ma_slope': None,
                             'distance_from_ma': ema_delta})
        except Exception as e:
            logging.info(f"💀 SPOT_ERROR {ticker}: {e}")
            return None

    def _analyze_orderbook_only(self, book, side, ticker, title, volume):
        """LEGACY ORDERBOOK-ONLY MODE: Pure imbalance signal."""
        try:
            yes_levels = book.get("yes", []) or []
            no_levels = book.get("no", []) or []
            if not yes_levels or not no_levels:
                return None
            if side == "yes":
                best_bid = max(lv[0] for lv in yes_levels)
                best_no_bid = max(lv[0] for lv in no_levels)
                best_ask = 100 - best_no_bid if best_no_bid > 0 else 100
            else:
                best_bid = max(lv[0] for lv in no_levels)
                best_yes_bid = max(lv[0] for lv in yes_levels)
                best_ask = 100 - best_yes_bid if best_yes_bid > 0 else 100
            if best_ask < self.config.CRYPTO_MIN_PRICE or best_ask > self.config.CRYPTO_MAX_PRICE:
                return None
            spread = best_ask - best_bid
            if spread > self.config.CRYPTO_MAX_SPREAD or spread <= 0:
                return None
            yes_depth = sum(lv[1] for lv in yes_levels)
            no_depth = sum(lv[1] for lv in no_levels)
            total_depth = yes_depth + no_depth
            if total_depth == 0:
                return None
            if self.config.MIN_DEPTH_FILTER_ENABLED:
                if total_depth < self.config.MIN_BOOK_DEPTH:
                    return None
            if side == "yes":
                imbalance = (yes_depth - no_depth) / total_depth
            else:
                imbalance = (no_depth - yes_depth) / total_depth
            if imbalance < self.config.CRYPTO_MIN_IMBALANCE:
                return None

            direction = "UP" if any(kw in title for kw in ["above", "up", "higher"]) else "DOWN"

            # Legacy spot WS confirmation
            if self.spot_feed is not None:
                symbol = self._ticker_to_symbol(ticker)
                if symbol:
                    confirm_dir = direction if side == "yes" else ("DOWN" if direction == "UP" else "UP")
                    if not self.spot_feed.confirms_direction(
                            symbol, confirm_dir,
                            self.config.CRYPTO_SPOT_MOMENTUM_THRESHOLD,
                            self.config.CRYPTO_SPOT_LOOKBACK_SECONDS):
                        return None

            buy_price = best_ask
            # Two-tier sizing: cheap contracts get full size, expensive get half
            _sym = self._ticker_to_symbol(ticker)
            _full_size = self.config.CRYPTO_SIZE_BTC if _sym == 'BTC' else self.config.CRYPTO_SIZE_XRP
            if buy_price <= 60:
                _size = _full_size       # 3-60¢: full size
            else:
                _size = _full_size / 2   # 61-70¢: half size
            contracts = min(100, max(1, int(_size * 100 / buy_price)))
            profit = net_profit_cents(buy_price, contracts, self.config.FEE_RATE)
            trade_roi = roi(buy_price, contracts, self.config.FEE_RATE)
            spread_score = max(0, 1 - spread / self.config.CRYPTO_MAX_SPREAD)
            vol_score = min(1.0, volume / 500.0)
            depth_score = min(1.0, total_depth / 200.0) * 0.05
            confidence = min(1.0, 0.35 + imbalance * 0.4 + spread_score * 0.15
                             + vol_score * 0.1 + depth_score)

            return TradeSignal(
                strategy='CRYPTO_SCALP', ticker=ticker, side=side,
                action='buy', price_cents=buy_price, contracts=contracts,
                edge_cents=profit, edge_roi=trade_roi, confidence=confidence,
                reason=f"OB {direction} | imbal={imbalance:.2f} "
                       f"depth={yes_depth}/{no_depth} spread={spread}¢ vol={volume}",
                market_data={'imbalance': imbalance, 'spread': spread,
                             'volume': volume, 'direction': direction,
                             'yes_depth': yes_depth, 'no_depth': no_depth,
                             'total_depth': total_depth})
        except Exception as e:
            logging.debug(f"OB scan error {ticker}: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 5: INDEX SCALPER (P7)
# ═══════════════════════════════════════════════════════════════════════════════

class IndexScalper:
    """S&P 500 / Nasdaq-100 contracts with HALF fees (0.035 vs 0.07)."""

    def __init__(self, config: TradingConfig):
        self.config = config

    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        signals = []
        kws = [s.upper() for s in self.config.INDEX_SERIES]
        for market in markets:
            ticker = market.get('ticker', '')
            volume = market.get('volume', 0) or 0
            if not any(kw in ticker.upper() for kw in kws):
                continue
            if volume < self.config.INDEX_MIN_VOLUME:
                continue

            book = orderbooks.get(ticker)
            if not book:
                continue
            yes_levels = book.get('yes', []) or []
            no_levels = book.get('no', []) or []
            if not yes_levels or not no_levels:
                continue

            yes_depth = sum(lv[1] for lv in yes_levels)
            no_depth = sum(lv[1] for lv in no_levels)
            total_depth = yes_depth + no_depth
            if total_depth == 0:
                continue

            # ── UPGRADE: Minimum book depth filter ────────────────
            if self.config.MIN_DEPTH_FILTER_ENABLED:
                if total_depth < self.config.MIN_BOOK_DEPTH:
                    logging.debug(f"DEPTH_SKIP {ticker}: {total_depth} < {self.config.MIN_BOOK_DEPTH}")
                    continue
            # ── End depth filter ──────────────────────────────────

            best_yes_bid = max(lv[0] for lv in yes_levels)
            best_no_bid = max(lv[0] for lv in no_levels)
            for side in ['yes', 'no']:
                if side == 'yes':
                    best_ask = 100 - best_no_bid if best_no_bid > 0 else 100
                    best_bid = best_yes_bid
                else:
                    best_ask = 100 - best_yes_bid if best_yes_bid > 0 else 100
                    best_bid = best_no_bid
                if best_ask < self.config.INDEX_MIN_PRICE or best_ask > self.config.INDEX_MAX_PRICE:
                    continue
                spread = best_ask - best_bid
                if spread > self.config.INDEX_MAX_SPREAD or spread <= 0:
                    continue
                imbalance = ((yes_depth - no_depth) / total_depth if side == 'yes'
                             else (no_depth - yes_depth) / total_depth)
                if imbalance < self.config.INDEX_MIN_IMBALANCE:
                    continue
                fee_rate = self.config.INDEX_FEE_RATE
                buy_price = best_ask
                contracts = min(5, max(1, int(self.config.CRYPTO_SIZE_DOLLARS * 50 / buy_price)))
                profit = net_profit_cents(buy_price, contracts, fee_rate)
                trade_roi = roi(buy_price, contracts, fee_rate)
                spread_score = max(0, 1 - spread / self.config.INDEX_MAX_SPREAD)
                vol_score = min(1.0, volume / 1000.0)
                # ── UPGRADE: Depth confidence ─────────────────────
                depth_score = min(1.0, total_depth / 200.0) * 0.05
                confidence = min(0.70, 0.35 + imbalance * 0.4 + spread_score * 0.15
                                 + vol_score * 0.1 + depth_score)
                # ── End confidence ────────────────────────────────
                sig = TradeSignal(
                    strategy='INDEX_SCALP', ticker=ticker, side=side,
                    action='buy', price_cents=buy_price, contracts=contracts,
                    edge_cents=profit, edge_roi=trade_roi, confidence=confidence,
                    reason=f"INDEX imbal={imbalance:.2f} depth={yes_depth}/{no_depth} "
                           f"spread={spread}¢ vol={volume} HALF_FEE",
                    market_data={'imbalance': imbalance, 'spread': spread,
                                 'volume': volume, 'fee_rate': fee_rate,
                                 'total_depth': total_depth})
                sig.fee_rate = fee_rate
                signals.append(sig)
        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# P8: HEDGE GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class HedgeGenerator:
    """Auto-hedge low-confidence trades with 25% opposing position."""

    def __init__(self, confidence_threshold=0.55, hedge_ratio=0.25):
        self.threshold = confidence_threshold
        self.ratio = hedge_ratio

    def should_hedge(self, signal: TradeSignal) -> bool:
        if signal.confidence >= self.threshold:
            return False
        if 'MARKET_MAKER' in signal.strategy or 'HEDGE' in signal.strategy:
            return False
        return True

    def generate(self, signal: TradeSignal) -> Optional[TradeSignal]:
        if not self.should_hedge(signal):
            return None
        hedge_side = 'no' if signal.side == 'yes' else 'yes'
        hedge_contracts = max(1, int(signal.contracts * self.ratio))
        hedge_price = 100 - signal.price_cents
        if hedge_price <= 0 or hedge_price >= 100:
            return None
        fee_rate = getattr(signal, 'fee_rate', 0.07)
        profit = net_profit_cents(hedge_price, hedge_contracts, fee_rate)
        return TradeSignal(
            strategy=f'{signal.strategy}_HEDGE', ticker=signal.ticker,
            side=hedge_side, action='buy', price_cents=hedge_price,
            contracts=hedge_contracts, edge_cents=profit,
            edge_roi=roi(hedge_price, hedge_contracts, fee_rate),
            confidence=signal.confidence,
            reason=f"HEDGE {hedge_side.upper()}@{hedge_price}¢ x{hedge_contracts} "
                   f"(hedging {signal.ticker} conf={signal.confidence:.0%})",
            market_data={'parent_strategy': signal.strategy,
                         'parent_side': signal.side, 'hedge_ratio': self.ratio})


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 6: MARKET MAKER (P9)
# ═══════════════════════════════════════════════════════════════════════════════

class MarketMaker:
    """Post both sides to capture spread with inventory management."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.spread_target = config.MM_SPREAD_TARGET
        self.min_spread = config.MM_MIN_SPREAD
        self.size = config.MM_SIZE_CONTRACTS
        self.max_inventory = config.MM_MAX_INVENTORY
        self.lean_per_unit = config.MM_INVENTORY_LEAN
        self.fee_rate = config.FEE_RATE
        self.inventory: Dict[str, int] = {}

    def update_inventory(self, ticker, side, count, action):
        if ticker not in self.inventory:
            self.inventory[ticker] = 0
        if action == 'buy':
            self.inventory[ticker] += count if side == 'yes' else -count
        elif action == 'sell':
            self.inventory[ticker] -= count if side == 'yes' else count

    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        signals = []
        for market in markets:
            ticker = market.get('ticker', '')
            volume = market.get('volume', 0) or 0
            if volume < 50:
                continue
            book = orderbooks.get(ticker)
            if not book:
                continue
            yes_levels = book.get('yes', []) or []
            no_levels = book.get('no', []) or []
            if not yes_levels or not no_levels:
                continue
            best_yes_bid = max(lv[0] for lv in yes_levels)
            best_no_bid = max(lv[0] for lv in no_levels)
            best_yes_ask = 100 - best_no_bid
            current_spread = best_yes_ask - best_yes_bid
            if current_spread < self.min_spread:
                continue
            mid = (best_yes_bid + best_yes_ask) / 2.0
            half = self.spread_target / 2.0
            net_inv = self.inventory.get(ticker, 0)
            if abs(net_inv) >= self.max_inventory:
                continue
            lean = (net_inv / self.max_inventory) * self.lean_per_unit if self.max_inventory > 0 else 0
            our_bid = max(1, min(98, int(mid - half - lean)))
            our_ask = max(2, min(99, int(mid + half - lean)))
            if our_ask <= our_bid:
                continue
            fee_bid = kalshi_fee_cents(our_bid, 1, self.fee_rate)
            fee_ask = kalshi_fee_cents(our_ask, 1, self.fee_rate)
            net_spread = (our_ask - our_bid) - fee_bid - fee_ask
            if net_spread < 2:
                continue
            signals.append(TradeSignal(
                strategy='MARKET_MAKER', ticker=ticker, side='yes', action='buy',
                price_cents=our_bid, contracts=self.size,
                edge_cents=net_spread * self.size // 2,
                edge_roi=net_spread / our_bid if our_bid > 0 else 0,
                confidence=0.50,
                reason=f"MM BID YES@{our_bid}¢ mid={mid:.0f} spread={current_spread}¢ inv={net_inv}",
                market_data={'mid': mid, 'our_bid': our_bid, 'our_ask': our_ask,
                             'net_spread': net_spread, 'inventory': net_inv, 'mm_side': 'bid'}))
            no_price = 100 - our_ask
            signals.append(TradeSignal(
                strategy='MARKET_MAKER', ticker=ticker, side='no', action='buy',
                price_cents=no_price, contracts=self.size,
                edge_cents=net_spread * self.size // 2,
                edge_roi=net_spread / no_price if no_price > 0 else 0,
                confidence=0.50,
                reason=f"MM ASK YES@{our_ask}¢ (NO@{no_price}¢) mid={mid:.0f} inv={net_inv}",
                market_data={'mid': mid, 'our_bid': our_bid, 'our_ask': our_ask,
                             'net_spread': net_spread, 'inventory': net_inv, 'mm_side': 'ask'}))
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# RISK MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class RiskManager:
    """Enforces position limits, daily loss limits, and circuit breakers."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.daily_pnl_cents: int = 0
        self.session_start_balance: int = 0  # Set on first balance update
        self.open_position_count: int = 0
        self.open_order_count: int = 0
        self.positions: Dict[str, dict] = {}
        self.trade_log: List[dict] = []
        self.halted: bool = False
        self.halt_reason: str = ''
        self.day_start = datetime.now(timezone.utc).date()

    def reset_daily(self):
        today = datetime.now(timezone.utc).date()
        if today > self.day_start:
            logging.info(f"New day — resetting PnL (was {self.daily_pnl_cents}¢)")
            self.daily_pnl_cents = 0
            self.day_start = today
            if self.halted and 'daily_loss' in self.halt_reason:
                self.halted = False
                self.halt_reason = ''

    def update_positions(self, positions_response: dict):
        self.positions = {}
        now = datetime.now(timezone.utc)
        _skipped = 0
        for pos in positions_response.get('market_positions', []):
            ticker = pos.get('ticker', '')
            if not ticker:
                continue
            # Skip zero-quantity positions (settled/resolved)
            qty = abs(pos.get('position', 0))
            if qty == 0:
                _skipped += 1
                continue
            # Skip positions whose settle time has passed (stale from prior windows)
            try:
                _tparts = ticker.split('-')
                if len(_tparts) >= 2:
                    _dp = _tparts[1]  # e.g. "26FEB121845"
                    _hh = int(_dp[-4:-2])
                    _mm = int(_dp[-2:])
                    _settle = now.replace(hour=_hh, minute=_mm, second=0, microsecond=0)
                    # Handle day boundary: if parsed time is >12h ahead, it was yesterday
                    if (_settle - now).total_seconds() > 43200:
                        _settle -= timedelta(days=1)
                    if now > _settle + timedelta(minutes=2):  # 2 min grace after settle
                        _skipped += 1
                        continue
            except Exception:
                pass  # If parse fails, keep the position (safe default)
            self.positions[ticker] = pos
        if _skipped:
            logging.debug(f"Filtered {_skipped} settled/zero positions from API")
        self.open_position_count = len(self.positions)

    def update_orders(self, orders_response: dict):
        orders = orders_response.get('orders', [])
        self.open_order_count = len([o for o in orders if o.get('status') == 'resting'])

    def record_fill(self, ticker, side, action, price_cents, contracts, fee_cents):
        self.trade_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'ticker': ticker, 'side': side, 'action': action,
            'price_cents': price_cents, 'contracts': contracts,
            'fee_cents': fee_cents, 'daily_pnl_after': self.daily_pnl_cents})
        if self.daily_pnl_cents <= -self.config.MAX_DAILY_LOSS_CENTS:
            self.halted = True
            self.halt_reason = f'daily_loss: {self.daily_pnl_cents}¢'
            logging.critical(f"🛑 CIRCUIT BREAKER: {self.halt_reason}")

    def check_trade(self, signal: TradeSignal, balance_cents: int) -> Tuple[bool, str]:
        self.reset_daily()
        if self.halted:
            return False, f"HALTED: {self.halt_reason}"
        if self.open_position_count >= self.config.MAX_OPEN_POSITIONS:
            return False, f"Max positions ({self.open_position_count})"
        if self.open_order_count >= self.config.MAX_OPEN_ORDERS:
            return False, f"Max orders ({self.open_order_count})"
        # Per-ticker concentration limit
        existing_pos = self.positions.get(signal.ticker, {})
        existing_qty = abs(existing_pos.get('position', 0)) if existing_pos else 0
        ticker_cap = 100
        if existing_qty >= ticker_cap:
            return False, f"Per-ticker limit: already at {existing_qty} on {signal.ticker[:30]}"
        if existing_qty + signal.contracts > ticker_cap:
            # Auto-reduce to fit
            signal.contracts = ticker_cap - existing_qty
            logging.info(f"📉 SIZED_DOWN: {signal.ticker[:25]} reduced to {signal.contracts} contracts (have {existing_qty})")
            if signal.contracts < 1:
                return False, f"Per-ticker limit: no room on {signal.ticker[:30]}"
        trade_cost = signal.price_cents * signal.contracts
        if trade_cost > self.config.MAX_POSITION_CENTS:
            return False, f"Cost {trade_cost}¢ > max {self.config.MAX_POSITION_CENTS}¢"
        max_by_pct = int(balance_cents * self.config.MAX_POSITION_PCT)
        if trade_cost > max_by_pct:
            return False, f"Cost {trade_cost}¢ > {self.config.MAX_POSITION_PCT:.0%} bankroll"
        if trade_cost > balance_cents:
            return False, f"Insufficient balance: need {trade_cost}¢, have {balance_cents}¢"
        remaining = self.config.MAX_DAILY_LOSS_CENTS + self.daily_pnl_cents
        if trade_cost > remaining:
            return False, f"Exceeds daily loss budget ({remaining}¢)"
        if signal.price_cents < 1 or signal.price_cents > 99:
            return False, f"Price {signal.price_cents}¢ out of range"
        return True, "APPROVED"

    def get_status(self) -> dict:
        return {
            'halted': self.halted, 'halt_reason': self.halt_reason,
            'daily_pnl_cents': self.daily_pnl_cents,
            'open_positions': self.open_position_count,
            'open_orders': self.open_order_count,
            'trades_today': len(self.trade_log),
            'remaining_loss_budget': self.config.MAX_DAILY_LOSS_CENTS + self.daily_pnl_cents}


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT LOGGER
# ═══════════════════════════════════════════════════════════════════════════════

class AuditLogger:
    """Logs every decision and trade to console and JSONL files."""

    def __init__(self, log_dir: str = 'logs'):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_file = self.log_dir / f'session_{timestamp}.jsonl'
        self.trade_file = self.log_dir / f'trades_{timestamp}.jsonl'
        self.signal_csv = self.log_dir / f'signals_{timestamp}.csv'
        self.fills_csv = self.log_dir / f'fills_{timestamp}.csv'
        self.shadow_csv = self.log_dir / f'shadow_blocks_{timestamp}.csv'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s [%(levelname)s] %(message)s',
                            datefmt='%H:%M:%S')
        self.logger = logging.getLogger('autotrader')
        # Initialize signal CSV for calibration
        self._init_signal_csv()
        self._init_fills_csv()
        self._init_shadow_csv()

    def _append(self, fp, record):
        with open(fp, 'a') as f:
            f.write(json.dumps(record, default=str) + '\n')

    def _init_signal_csv(self):
        """Create signal CSV for post-hoc confidence calibration."""
        with open(self.signal_csv, 'w') as f:
            f.write("timestamp,strategy,ticker,side,price_cents,contracts,"
                    "confidence,imbalance,spread,volume,total_depth,"
                    "spot_price,strike,cushion_pct,momentum,"
                    "traded,approved_reason\n")

    def log_signal_csv(self, signal, traded: bool, reason: str = ''):
        """Log every signal (traded or not) for calibration analysis.
        After settlement, join on ticker to compute actual win/loss rates
        per confidence bucket → recalibrate confidence formula."""
        md = signal.market_data or {}
        spot = md.get('spot_price', '')
        strike = md.get('strike', '')
        momentum = md.get('momentum', '')
        # Cushion: how far past the strike is spot price (%)
        cushion_pct = ''
        if spot and strike and strike > 0:
            cushion_pct = f"{abs(spot - strike) / strike * 100:.4f}"
        try:
            with open(self.signal_csv, 'a') as f:
                f.write(f"{datetime.now(timezone.utc).isoformat()},"
                        f"{signal.strategy},{signal.ticker},{signal.side},"
                        f"{signal.price_cents},{signal.contracts},"
                        f"{signal.confidence:.4f},"
                        f"{md.get('imbalance', '')},{md.get('spread', '')},"
                        f"{md.get('volume', '')},{md.get('total_depth', '')},"
                        f"{spot},{strike},{cushion_pct},{momentum},"
                        f"{traded},{reason}\n")
        except Exception:
            pass  # never crash on logging

    def _init_fills_csv(self):
        """Create fills CSV for edge analysis — coworker needs this data."""
        with open(self.fills_csv, 'w') as f:
            f.write("timestamp,ticker,side,action,fill_price_cents,contracts,"
                    "spot_price,strike,cushion_pct,momentum,confidence,"
                    "imbalance,spread,total_depth\n")

    def _init_shadow_csv(self):
        """Shadow log: blocked signals with enough context to back-test against settlement."""
        with open(self.shadow_csv, 'w') as f:
            f.write("timestamp,ticker,side,would_buy_price,contracts,"
                    "block_reason,spot_price,strike,cushion_pct,momentum,"
                    "ma_slope,distance_from_ma,window_minute,close_time\n")

    def log_shadow_block(self, ticker, side, buy_price, contracts,
                         block_reason, spot, strike, cushion_pct,
                         momentum, ma_slope, distance_from_ma,
                         window_minute=None, close_time=None):
        """Log a signal that WOULD have traded but was blocked by a filter.
        After settlement, join on ticker to see if block was correct."""
        try:
            with open(self.shadow_csv, 'a') as f:
                _cush = f"{cushion_pct*100:.4f}" if cushion_pct is not None else ''
                _mom = f"{momentum*100:.5f}" if momentum is not None else ''
                _slope = f"{ma_slope*100:.5f}" if ma_slope is not None else ''
                _dist = f"{distance_from_ma*100:.5f}" if distance_from_ma is not None else ''
                _wm = str(window_minute) if window_minute is not None else ''
                _ct = str(close_time) if close_time else ''
                f.write(f"{datetime.now(timezone.utc).isoformat()},"
                        f"{ticker},{side},{buy_price},{contracts},"
                        f"{block_reason},{spot},{strike},{_cush},{_mom},"
                        f"{_slope},{_dist},{_wm},{_ct}\n")
        except Exception:
            pass

    def log_fill_csv(self, ticker, side, action, fill_price, contracts, signal_data=None):
        """Log every fill with edge-analysis fields."""
        sd = signal_data or {}
        spot = sd.get('spot_price', '')
        strike = sd.get('strike', '')
        cushion_pct = ''
        if spot and strike and strike > 0:
            cushion_pct = f"{abs(spot - strike) / strike * 100:.4f}"
        try:
            with open(self.fills_csv, 'a') as f:
                f.write(f"{datetime.now(timezone.utc).isoformat()},"
                        f"{ticker},{side},{action},{fill_price},{contracts},"
                        f"{spot},{strike},{cushion_pct},"
                        f"{sd.get('momentum', '')},{sd.get('confidence', '')},"
                        f"{sd.get('imbalance', '')},{sd.get('spread', '')},"
                        f"{sd.get('total_depth', '')}\n")
        except Exception:
            pass

    def log_scan(self, strategy, markets_scanned, signals_found, duration_ms):
        self._append(self.session_file, {
            'type': 'scan', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': strategy, 'markets_scanned': markets_scanned,
            'signals_found': signals_found, 'duration_ms': round(duration_ms, 1)})
        self.logger.info(f"📊 {strategy}: scanned {markets_scanned} → "
                         f"{signals_found} signals ({duration_ms:.0f}ms)")

    def log_signal(self, signal, approved, reason):
        self._append(self.session_file, {
            'type': 'signal', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': signal.strategy, 'ticker': signal.ticker,
            'side': signal.side, 'price_cents': signal.price_cents,
            'contracts': signal.contracts, 'confidence': signal.confidence,
            'approved': approved, 'reason': reason})
        icon = "✅" if approved else "❌"
        self.logger.info(f"{icon} {signal.strategy} {signal.ticker} "
                         f"{signal.side.upper()}@{signal.price_cents}¢ x{signal.contracts} — {reason}")

    def log_order(self, signal, order_response, status):
        self._append(self.trade_file, {
            'type': 'order', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': signal.strategy, 'ticker': signal.ticker,
            'side': signal.side, 'price_cents': signal.price_cents,
            'contracts': signal.contracts,
            'order_id': order_response.get('order', {}).get('order_id', ''),
            'status': status})

    def log_error(self, context, error):
        self._append(self.session_file, {
            'type': 'error', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'context': context, 'error': str(error)})
        self.logger.error(f"💥 {context}: {error}")

    def log_status(self, risk_status, balance_cents):
        self._append(self.session_file, {
            'type': 'status', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'balance_cents': balance_cents, **risk_status})


# ═══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class OrderManager:
    """Manages order lifecycle: place, batch, track, cancel stale."""

    def __init__(self, api: KalshiAPIClient, config: TradingConfig):
        self.api = api
        self.config = config
        self.active_orders: Dict[str, dict] = {}

    def place_order(self, signal: TradeSignal) -> Optional[dict]:
        try:
            # Crypto scalp: submit at max cap for instant fill — Kalshi fills at best available
            # The PRICE_REJECT gate already caps buy_price, so this just ensures fast execution
            is_crypto = 'CRYPTO' in signal.strategy
            if is_crypto:
                order_type = 'limit'
                yes_price = signal.price_cents if signal.side == 'yes' else None
                no_price = signal.price_cents if signal.side == 'no' else None
            elif self.config.USE_LIMIT_ORDERS:
                order_type = 'limit'
                yes_price = signal.price_cents if signal.side == 'yes' else None
                no_price = signal.price_cents if signal.side == 'no' else None
            else:
                order_type, yes_price, no_price = 'market', None, None
            response = self.api.create_order(
                ticker=signal.ticker, side=signal.side, action=signal.action,
                count=signal.contracts, order_type=order_type,
                yes_price=yes_price, no_price=no_price)
            order = response.get('order', {})
            order_id = order.get('order_id', '')
            if order_id:
                self.active_orders[order_id] = {
                    'signal': signal, 'placed_at': time.time(),
                    'status': order.get('status', 'unknown'), 'ticker': signal.ticker}
            return response
        except Exception as e:
            logging.error(f"Order failed for {signal.ticker}: {e}")
            return None

    def cancel_stale_orders(self) -> int:
        cancelled = 0
        now = time.time()
        stale_ids = []
        for oid, info in self.active_orders.items():
            age = now - info['placed_at']
            if age > self.config.ORDER_TTL_SECONDS and info['status'] == 'resting':
                try:
                    self.api.cancel_order(oid)
                    stale_ids.append(oid)
                    cancelled += 1
                    logging.info(f"🗑️  Cancelled stale {oid[:12]}... ({info['ticker']})")
                except Exception as e:
                    if "404" in str(e):
                        stale_ids.append(oid)  # Already gone from Kalshi, remove from tracking
                    else:
                        logging.warning(f"Cancel failed {oid[:12]}: {e}")
        for oid in stale_ids:
            del self.active_orders[oid]
        return cancelled

    def sync_orders(self):
        try:
            response = self.api.get_orders(status='resting')
            api_orders = {o['order_id']: o for o in response.get('orders', [])}
            filled_ids = [oid for oid in list(self.active_orders.keys())
                          if oid not in api_orders]
            for oid in filled_ids:
                info = self.active_orders.pop(oid, None)
                if info:
                    logging.info(f"📦 Order {oid[:12]}... no longer resting ({info['ticker']})")
        except Exception as e:
            logging.warning(f"Order sync failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# AUTOTRADER V2 ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class AutoTraderV2:
    """Main orchestrator with all v2 upgrades."""

    def __init__(self, api: KalshiAPIClient, config: TradingConfig,
                 strategies: List[str] = None):
        self.api = api
        self.config = config
        self.risk = RiskManager(config)
        self.orders = OrderManager(api, config)
        self.audit = AuditLogger()

        # P4: Kelly sizer
        self.kelly = KellySizer(
            fraction=config.KELLY_FRACTION, min_edge=config.KELLY_MIN_EDGE,
            min_bet_cents=config.KELLY_MIN_BET_CENTS,
            max_bet_cents=config.KELLY_MAX_BET_CENTS,
            max_pct=config.MAX_POSITION_PCT)

        # P1: Take-profit + stop-loss
        _sl = config.CRYPTO_STOP_LOSS_CENTS if config.CRYPTO_STOP_LOSS_ENABLED else 0
        self.take_profit = TakeProfitMonitor(
            threshold=config.TAKE_PROFIT_THRESHOLD,
            stop_loss_cents=_sl,
            stop_cushion_threshold=config.CRYPTO_STOP_CUSHION_THRESHOLD)

        # Stop-loss shadow tracker (deferred outcome logging)
        self.stop_shadow = StopShadowTracker()

        # P8: Hedge generator
        self.hedger = HedgeGenerator(
            confidence_threshold=config.HEDGE_CONFIDENCE_THRESHOLD,
            hedge_ratio=config.HEDGE_RATIO)

        # Initialize strategies
        self.strategies = {}
        enabled = strategies or ['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET',
                                 'CRYPTO_SCALP', 'INDEX_SCALP']
        if 'NO_HARVEST' in enabled:
            self.strategies['NO_HARVEST'] = NoHarvester(config)
        if 'STRUCTURAL' in enabled:
            self.strategies['STRUCTURAL'] = StructuralArb(config)
        if 'CROSS_MARKET' in enabled:
            self.strategies['CROSS_MARKET'] = CrossMarketArb(config)
        if 'CRYPTO_SCALP' in enabled:
            self.strategies['CRYPTO_SCALP'] = CryptoScalper(config)
        if 'INDEX_SCALP' in enabled and config.INDEX_ENABLED:
            self.strategies['INDEX_SCALP'] = IndexScalper(config)
        if 'MARKET_MAKER' in enabled and config.MM_ENABLED:
            self.market_maker = MarketMaker(config)
            self.strategies['MARKET_MAKER'] = self.market_maker
        else:
            self.market_maker = None

        # P2: WebSocket (started in run())
        self.ws_manager: Optional[KalshiWebSocket] = None
        self.ws_thread = None
        # P3: Spot price feeds
        self.spot_feed: Optional[SpotPriceFeed] = None  # legacy WS
        self.spot_rest: Optional[SpotPriceREST] = None  # new REST (primary)
        self.spot_thread = None
        # V3: Multi-exchange index proxy
        self.multi_feed: Optional[MultiExchangeFeed] = None
        self.multi_feed_thread = None

        # Inject REST spot feed into CryptoScalper
        if config.CRYPTO_SPOT_DRIVEN and 'CRYPTO_SCALP' in self.strategies:
            self.spot_rest = SpotPriceREST()
            self.strategies['CRYPTO_SCALP'].spot_rest = self.spot_rest
            # Initialize multi-exchange feed
            self.multi_feed = MultiExchangeFeed()

        self.running = False
        self.scan_count = 0
        self.crypto_positions = {}
        self._pending_signals = {}  # ticker -> signal market_data for fill logging
        self.total_signals = 0
        self.total_trades = 0
        self.balance_cents = 0

        # ── Calibration: signal persistence tracking ──────────────
        # Maps signal_key -> consecutive scan count
        self._signal_streak: Dict[str, int] = {}
        self._signal_seen_this_scan: set = set()

        # ── Per-window loss tracking ──────────────────────────────
        # If both symbols lose in the same 15-min window, skip next window
        self._window_entries: set = set()       # symbols entered this window
        self._entered_tickers: set = set()      # specific tickers entered this window (hard 1-per-ticker cap)
        self._last_window_key: str = ''         # e.g. "14:30"
        self._window_start_balance: int = 0     # balance at window start
        self._skip_until: float = 0.0           # UTC timestamp, skip execution until

    # ── Helpers ──────────────────────────────────────────────────────────

    def _fetch_balance(self) -> int:
        try:
            resp = self.api.get_balance()
            self.balance_cents = resp.get('balance', 0)
            # Track session start balance for accurate PnL
            if self.risk.session_start_balance == 0:
                self.risk.session_start_balance = self.balance_cents
                logging.info(f"📊 Session start balance: ${self.balance_cents/100:.2f}")
            # Update daily PnL from actual balance change (includes settlements)
            self.risk.daily_pnl_cents = self.balance_cents - self.risk.session_start_balance
            return self.balance_cents
        except Exception as e:
            self.audit.log_error('fetch_balance', e)
            return self.balance_cents

    def _fetch_markets(self, max_markets=2000) -> List[dict]:
        all_markets, cursor = [], None
        while len(all_markets) < max_markets:
            try:
                resp = self.api.get_markets(
                    limit=min(1000, max_markets - len(all_markets)), cursor=cursor)
                markets = resp.get('markets', [])
                if not markets:
                    break
                all_markets.extend(markets)
                next_cursor = resp.get('cursor')
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                time.sleep(self.config.API_RATE_DELAY)
            except Exception as e:
                self.audit.log_error('fetch_markets', e)
                break
        return all_markets

    def _fetch_crypto_markets(self) -> List[dict]:
        crypto_series = ['KXBTC15M']
        all_crypto = []
        for series in crypto_series:
            try:
                resp = self.api.get_markets(series_ticker=series, limit=200)
                all_crypto.extend(resp.get('markets', []))
            except Exception:
                continue
        logging.info(f"📊 Fetched {len(all_crypto)} crypto markets")
        return all_crypto

    def _fetch_index_markets(self) -> List[dict]:
        all_index = []
        for series in self.config.INDEX_SERIES:
            try:
                resp = self.api.get_markets(series_ticker=series, limit=200)
                all_index.extend(resp.get('markets', []))
                time.sleep(self.config.API_RATE_DELAY)
            except Exception:
                continue
        logging.info(f"📊 Fetched {len(all_index)} index markets")
        return all_index

    def _fetch_orderbooks(self, tickers: List[str]) -> dict:
        books = {}
        for ticker in tickers:
            try:
                resp = self.api.get_orderbook(ticker)
                book = resp.get('orderbook', {})
                if book:
                    books[ticker] = book
                time.sleep(self.config.API_RATE_DELAY)
            except Exception:
                continue
        return books

    def _fetch_events(self) -> List[dict]:
        all_events, cursor = [], None
        while True:
            try:
                resp = self.api.get_events(limit=200, cursor=cursor)
                events = resp.get('events', [])
                if not events:
                    break
                all_events.extend(events)
                next_cursor = resp.get('cursor')
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                time.sleep(self.config.API_RATE_DELAY)
            except Exception as e:
                self.audit.log_error('fetch_events', e)
                break
        return all_events

    def _get_orderbooks(self, tickers: List[str]) -> dict:
        """P2: Read from WS cache or fall back to REST.
        NEVER re-subscribes inside scan cycle — subscriptions are handled
        at startup only. Missing tickers fall back to REST (capped)."""
        if self.ws_manager and self.ws_manager.connected:
            books, missing = {}, []
            for t in tickers:
                b = self.ws_manager.get_orderbook(t)
                if b:
                    books[t] = b
                else:
                    missing.append(t)
            # REST fallback only for critical tickers (crypto), capped at 10
            if missing:
                crypto_missing = [t for t in missing
                                  if any(p in t.upper() for p in ['KXBTC', 'INX', 'NASDAQ'])]
                if crypto_missing:
                    rest = self._fetch_orderbooks(crypto_missing[:10])
                    books.update(rest)
            return books
        else:
            return self._fetch_orderbooks(tickers[:700])

    def _prioritize_signals(self, signals: List[TradeSignal]) -> List[TradeSignal]:
        for s in signals:
            roi_factor = min(s.edge_roi, 5.0) / 5.0
            s._score = s.edge_cents * s.confidence * (1 + roi_factor)
        signals.sort(key=lambda s: s._score, reverse=True)
        return signals

    # ── P2: Start WebSocket ─────────────────────────────────────────────

    def _start_websocket(self, initial_tickers):
        if not HAS_WEBSOCKETS or not self.config.USE_WEBSOCKET:
            logging.info("⚠️  WebSocket disabled — REST polling mode")
            return
        self.ws_manager = KalshiWebSocket(self.api, self.config)

        def on_fill(fill):
            ticker = fill.get('market_ticker', '')
            side = fill.get('side', '')
            action = fill.get('action', '')
            price = fill.get('yes_price', 0)
            count = fill.get('count', 0)
            is_taker = fill.get('is_taker', False)
            fee = kalshi_fee_cents(price, count, self.config.FEE_RATE if is_taker else 0)
            self.risk.record_fill(ticker, side, action, price, count, fee)
            # Log fill with signal context for edge analysis
            fill_price = price if side == 'yes' else 100 - price
            signal_data = self._pending_signals.get(ticker, {})
            self.audit.log_fill_csv(ticker, side, action, fill_price, count, signal_data)
            if action == 'buy':
                _sd = self._pending_signals.get(ticker, {})
                _sym = 'BTC' if 'BTC' in ticker.upper() else ''
                self.take_profit.track(ticker, side, price, count,
                                       strike=_sd.get('strike'),
                                       symbol=_sym,
                                       direction=_sd.get('direction'))
            if self.market_maker:
                self.market_maker.update_inventory(ticker, side, count, action)

        self.ws_manager.on_fill = on_fill

        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.ws_manager.run(initial_tickers))

        self.ws_thread = threading.Thread(target=_run, daemon=True)
        self.ws_thread.start()
        time.sleep(2)
        logging.info(f"🔌 WebSocket started ({len(initial_tickers)} tickers)")

    # ── P3: Start spot price feed ─────────────────────────────────────────

    def _start_spot_feed(self):
        if not HAS_WEBSOCKETS:
            return
        self.spot_feed = SpotPriceFeed()
        if 'CRYPTO_SCALP' in self.strategies:
            self.strategies['CRYPTO_SCALP'].spot_feed = self.spot_feed

        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.spot_feed.run())

        self.spot_thread = threading.Thread(target=_run, daemon=True)
        self.spot_thread.start()
        time.sleep(1)
        logging.info("📈 Coinbase spot feed started (BTC)")

    # ── Main scan cycle ─────────────────────────────────────────────────

    def run_scan_cycle(self) -> int:
        self.scan_count += 1
        cycle_trades = 0

        ws_tag = '[WS]' if self.ws_manager and self.ws_manager.connected else '[REST]'
        now_utc = datetime.now(timezone.utc)
        _wmin = now_utc.minute % 15
        _wstart = f":{(now_utc.minute // 15) * 15:02d}"
        _settle = 15 - _wmin
        logging.info(f"\n{'━' * 80}")
        logging.info(f"🔄 SCAN #{self.scan_count} — "
                     f"{datetime.now().strftime('%H:%M:%S')} {ws_tag} "
                     f"| Window {_wstart} min {_wmin}/14 ({_settle}m to settle)")
        logging.info(f"{'━' * 80}")

        # ── Fetch spot prices (one REST call per symbol) ──────────
        if self.spot_rest:
            spot_prices = self.spot_rest.fetch()
            if spot_prices:
                # Base spot status
                spot_str = self.spot_rest.get_status_str()
                # Append EMA data if available
                for sym in ['BTC']:
                    _ema_dir = self.spot_rest.get_ema_direction(sym)
                    _ema_delta = self.spot_rest.get_ema_delta_pct(sym)
                    _tc = self.spot_rest._ema_tick_count.get(sym, 0)
                    if _ema_dir is not None:
                        _delta_str = f" Δ={_ema_delta*100:+.3f}%" if _ema_delta is not None else ""
                        spot_str += f" | EMA3: {_ema_dir}{_delta_str}"
                    else:
                        spot_str += f" | EMA3: warming({_tc}/{self.config.CRYPTO_EMA_MIN_TICKS}ticks)"
                    # Passive candle collection status
                    _c15 = self.spot_rest.get_candle_count(sym, '15m')
                    _c1h = self.spot_rest.get_candle_count(sym, '1h')
                    if _c15 > 0 or _c1h > 0:
                        spot_str += f" | 🕯️{_c15}x15m {_c1h}x1h"
                logging.info(f"📈 SPOT: {spot_str}")
                # Feed Coinbase price into multi-exchange IDX_PROXY
                if self.multi_feed:
                    btc_price = spot_prices.get('BTC')
                    if btc_price:
                        self.multi_feed.update_coinbase(btc_price)
                # Accumulate 1-min candles for EMA 50/100/150 structure filter
                if 'CRYPTO_SCALP' in self.strategies:
                    _cs = self.strategies['CRYPTO_SCALP']
                    _btc = spot_prices.get('BTC')
                    if _btc:
                        _cs._accumulate_candle(_btc)
            # Log multi-exchange IDX_PROXY status
            if self.multi_feed:
                _feed_count = self.multi_feed.get_feed_count()
                if _feed_count > 1:  # more than just Coinbase
                    logging.info(f"🌐 IDX: {self.multi_feed.get_status_str()} ({_feed_count} feeds)")
            # Log EMA 50/100/150 structure status
            if 'CRYPTO_SCALP' in self.strategies:
                _cs = self.strategies['CRYPTO_SCALP']
                if _cs._ema_ready:
                    _stack, _zone = _cs._get_ema_structure(spot_prices.get('BTC', 0))
                    _stack_str = _stack if _stack else 'FLAT'
                    _zone_str = _zone if _zone else 'N/A'
                    logging.info(f"📐 EMA: 50=${_cs._ema50:,.0f} | 100=${_cs._ema100:,.0f} | 150=${_cs._ema150:,.0f} "
                                 f"| {_stack_str} stack | zone={_zone_str} | candles={len(_cs._1min_candles)}")
                else:
                    logging.info(f"📐 EMA: warming up ({len(_cs._1min_candles)}/{_cs.config.CRYPTO_EMA_MIN_CANDLES} candles)")
                # Log RSI and MACD status
                _rsi_str = ""
                if _cs._rsi_ready and _cs._rsi_value is not None:
                    _rsi_v = _cs._rsi_value
                    _rsi_zone = "OB" if _rsi_v >= _cs.config.CRYPTO_RSI_OVERBOUGHT else (
                        "OS" if _rsi_v <= _cs.config.CRYPTO_RSI_OVERSOLD else "OK")
                    _rsi_str = f"RSI={_rsi_v:.1f}({_rsi_zone})"
                else:
                    _rsi_str = f"RSI=warming({_cs._rsi_count}/{_cs.config.CRYPTO_RSI_PERIOD})"
                _macd_str = ""
                if _cs._macd_ready and _cs._macd_histogram is not None:
                    _h = _cs._macd_histogram
                    _m = _cs._macd_line
                    _s = _cs._macd_signal_line
                    _macd_dir = "BULL" if _h > 0 else "BEAR"
                    _macd_str = f"MACD={_m:+.1f} sig={_s:+.1f} hist={_h:+.1f}({_macd_dir})"
                elif _cs._macd_line is not None:
                    _macd_str = f"MACD=warming(sig {_cs._macd_line_count}/{_cs.config.CRYPTO_MACD_SIGNAL})"
                else:
                    _macd_str = f"MACD=warming({len(_cs._1min_candles)}/{_cs.config.CRYPTO_MACD_SLOW})"
                logging.info(f"📊 TA: {_rsi_str} | {_macd_str}")

        balance = self._fetch_balance()
        try:
            self.risk.update_positions(self.api.get_positions())
        except Exception as e:
            self.audit.log_error('sync_positions', e)

        # PATCHED: Clean up crypto_positions for settled/closed contracts
        active_tickers = set(self.risk.positions.keys())
        stale_crypto = [t for t in self.crypto_positions if t not in active_tickers]
        for t in stale_crypto:
            del self.crypto_positions[t]
        if stale_crypto:
            logging.debug(f"Cleaned {len(stale_crypto)} settled crypto position entries")
        try:
            self.risk.update_orders(self.api.get_orders(status='resting'))
        except Exception as e:
            self.audit.log_error('sync_orders', e)
        # PATCHED: Also sync OrderManager active_orders so ALREADY_IN guard clears filled orders
        try:
            self.orders.sync_orders()
        except Exception as e:
            self.audit.log_error('sync_orders_om', e)

        status = self.risk.get_status()
        self.audit.log_status(status, balance)
        logging.info(f"💰 ${balance/100:.2f} | PnL: {status['daily_pnl_cents']}¢ | "
                     f"Pos: {status['open_positions']} | Ord: {status['open_orders']}")
        if status['halted']:
            logging.warning(f"🛑 HALTED: {status['halt_reason']}")
            return 0

        cancelled = self.orders.cancel_stale_orders()
        if cancelled:
            logging.info(f"🗑️  Cancelled {cancelled} stale orders")

        # ── P1: Take-profit + Stop-loss — MOVED after orderbook fetch ──
        # (see below, after fresh orderbooks are available)

        # ── Fetch markets ────────────────────────────────────────
        active_strats = set(self.strategies.keys())
        crypto_only = active_strats.issubset({'CRYPTO_SCALP', 'INDEX_SCALP'})

        if crypto_only:
            # Skip the 2000-market REST call — only need crypto/index series
            markets = []
        else:
            markets = self._fetch_markets(max_markets=2000)

        if 'CRYPTO_SCALP' in self.strategies:
            crypto = self._fetch_crypto_markets()
            existing = {m['ticker'] for m in markets}
            markets.extend([m for m in crypto if m['ticker'] not in existing])

        if 'INDEX_SCALP' in self.strategies:
            idx = self._fetch_index_markets()
            existing = {m['ticker'] for m in markets}
            markets.extend([m for m in idx if m['ticker'] not in existing])

        if not markets:
            logging.warning("No markets fetched")
            return 0

        # Log Coinbase vs Strike gap for BTC
        if self.spot_rest and 'CRYPTO_SCALP' in self.strategies:
            crypto = [m for m in markets if 'KXBTC' in m.get('ticker', '').upper()]
            btc_spot = self.spot_rest.get_latest_price('BTC')
            scalper = self.strategies.get('CRYPTO_SCALP')
            if btc_spot and scalper and crypto:
                for m in crypto:
                    if 'KXBTC' in m.get('ticker', ''):
                        strike = scalper._parse_strike(m)
                        if strike and strike > 0:
                            delta = btc_spot - strike
                            gap_pct = delta / strike * 100
                            _tk = m.get('ticker', '')
                            _ws = self.ws_manager.tickers.get(_tk, {}) if self.ws_manager else {}
                            _yb = _ws.get('yes_bid', 0)
                            _ya = _ws.get('yes_ask', 0)
                            _nb = 100 - _ya if _ya > 0 else 0
                            _na = 100 - _yb if _yb > 0 else 0
                            _px_str = f" | YES {_yb}/{_ya}¢ NO {_nb}/{_na}¢" if _yb or _ya else ""
                            logging.info(f"📊 GAP: CB=${btc_spot:,.0f} | "
                                         f"Strike=${strike:,.0f} | "
                                         f"Δ=${delta:+,.0f} ({gap_pct:+.3f}%){_px_str}")
                            break

        logging.info(f"📊 {len(markets)} markets")

        # ── Get orderbooks (WS cache or REST) ───────────────────
        # Only fetch books for tickers relevant to active strategies
        candidate_tickers, crypto_force = [], []
        active_strats = set(self.strategies.keys())
        for m in markets:
            ticker = m.get('ticker', '')
            vol = m.get('volume', 0)
            t_upper = ticker.upper()
            if any(p in t_upper for p in ['KXBTC', 'INX', 'NASDAQ100']):
                crypto_force.append(ticker)
            elif vol >= 50 and active_strats - {'CRYPTO_SCALP', 'INDEX_SCALP'}:
                # Only fetch non-crypto books if non-crypto strategies are active
                candidate_tickers.append(ticker)

        all_tickers = list(set(crypto_force + candidate_tickers))[:700]
        # Ensure held positions are ALWAYS in the fetch list (stop-loss needs their bids)
        held_tickers = list(self.take_profit.positions.keys()) if hasattr(self, 'take_profit') else []
        for ht in held_tickers:
            if ht not in all_tickers:
                all_tickers.append(ht)
        logging.info(f"📖 Fetching {len(all_tickers)} orderbooks...")
        t0 = time.time()
        orderbooks = self._get_orderbooks(all_tickers)
        fetch_ms = (time.time() - t0) * 1000
        ws_hits = len(orderbooks) - len([t for t in all_tickers
                                          if t not in orderbooks])  # approx
        logging.info(f"📖 Got {len(orderbooks)} books ({fetch_ms:.0f}ms) "
                     f"[WS:{len(orderbooks)}/{len(all_tickers)} "
                     f"= {len(orderbooks)/max(len(all_tickers),1)*100:.0f}%]")

        # ── P1: Take-profit + Stop-loss check (uses FRESH orderbooks) ──
        if self.config.TAKE_PROFIT_ENABLED or self.config.CRYPTO_STOP_LOSS_ENABLED:
            tk_data = self.ws_manager.tickers if self.ws_manager else {}
            # CRITICAL: use fresh `orderbooks` from _get_orderbooks(), NOT stale ws_manager.orderbooks
            ob_data = orderbooks
            exits = self.take_profit.check(tk_data, ob_data,
                                              spot_rest=self.spot_rest)
            for ex in exits:
                try:
                    exit_type = ex.get('exit_type', 'take_profit')
                    # Stop-loss: sell at bid (accept the loss), don't wait for better price
                    sell_price = max(1, ex['price'] - (2 if exit_type == 'stop_loss' else 0))
                    self.api.create_order(
                        ticker=ex['ticker'], side=ex['side'], action='sell',
                        count=ex['count'], order_type='limit',
                        yes_price=sell_price if ex['side'] == 'yes' else None,
                        no_price=sell_price if ex['side'] == 'no' else None)
                    emoji = "🛑" if exit_type == 'stop_loss' else "🎯"
                    label = "STOP LOSS" if exit_type == 'stop_loss' else "TAKE PROFIT"
                    logging.info(f"{emoji} EXIT ({label}): SELL {ex['side'].upper()} "
                                 f"{ex['ticker'][:30]} @{sell_price}¢ x{ex['count']}")
                    self.crypto_positions.pop(ex['ticker'], None)
                    # ── Shadow track stop-loss exits ──
                    if exit_type == 'stop_loss' and self.spot_rest:
                        _tp_pos = self.take_profit.positions.get(ex['ticker'], {})
                        _sym = _tp_pos.get('symbol', 'BTC' if 'BTC' in ex['ticker'].upper() else '')
                        _strike = _tp_pos.get('strike', 0)
                        _spot = self.spot_rest.get_latest_price(_sym) if _sym else None
                        _ema_dir = self.spot_rest.get_ema_direction(_sym) if _sym else None
                        _cushion = 0.0
                        if _spot and _strike and _strike > 0:
                            _cushion = ((_spot - _strike) / _strike if ex['side'] == 'yes'
                                        else (_strike - _spot) / _strike)
                        _close_time = ''
                        try:
                            _parts = ex['ticker'].split('-')
                            if len(_parts) >= 2:
                                _date_part = _parts[1]
                                _close_time = _date_part
                        except Exception:
                            pass
                        self.stop_shadow.record_stop(
                            ticker=ex['ticker'], side=ex['side'],
                            entry_price=_tp_pos.get('entry_price', 0),
                            exit_bid=ex['price'], exit_reason=ex.get('exit_reason', 'unknown'),
                            spot=_spot or 0, strike=_strike or 0,
                            cushion=_cushion, ma_slope=0,
                            contracts=ex['count'],
                            close_time=_close_time)
                except Exception as e:
                    self.audit.log_error(f"exit_{ex['ticker']}", e)

        # ── Check stop shadow tracker (deferred 3min + settlement) ──
        if self.stop_shadow.pending:
            tk_data_shadow = self.ws_manager.tickers if self.ws_manager else {}
            self.stop_shadow.check_pending(
                spot_rest=self.spot_rest,
                ticker_data=tk_data_shadow, orderbooks=orderbooks)

        # ── Run all strategies ───────────────────────────────────
        all_signals = []
        for name, engine in self.strategies.items():
            t0 = time.time()
            if name == 'CROSS_MARKET':
                events = self._fetch_events()
                evt_tickers = []
                for evt in events:
                    for m in evt.get('markets', []):
                        t_str = m.get('ticker', '')
                        if t_str and t_str not in orderbooks:
                            evt_tickers.append(t_str)
                if evt_tickers:
                    orderbooks.update(self._get_orderbooks(evt_tickers[:200]))
                sigs = engine.scan(events, orderbooks)
            else:
                sigs = engine.scan(markets, orderbooks)
            ms = (time.time() - t0) * 1000
            self.audit.log_scan(name, len(markets), len(sigs), ms)
            all_signals.extend(sigs)

        # ── Drain shadow blocks from CryptoScalper for phase 2 data ──
        crypto_engine = self.strategies.get('CRYPTO_SCALP')
        if crypto_engine and hasattr(crypto_engine, 'shadow_blocks') and crypto_engine.shadow_blocks:
            for sb in crypto_engine.shadow_blocks:
                self.audit.log_shadow_block(
                    ticker=sb['ticker'], side=sb['side'],
                    buy_price=sb.get('buy_price', 0),
                    contracts=sb.get('contracts', 0),
                    block_reason=sb['block_reason'],
                    spot=sb.get('spot'), strike=sb.get('strike'),
                    cushion_pct=sb.get('cushion_pct'),
                    momentum=sb.get('momentum'),
                    ma_slope=sb.get('ma_slope'),
                    distance_from_ma=sb.get('distance_from_ma'),
                    close_time=sb.get('close_time'))
            crypto_engine.shadow_blocks.clear()

        # ── UPGRADE: Signal persistence filter ─────────────────
        # Require signals to appear on N consecutive scans before trading.
        # Eliminates ~50% of false signals from fleeting orderbook noise.
        if self.config.REQUIRE_SIGNAL_PERSISTENCE and all_signals:
            self._signal_seen_this_scan = set()
            persistent_signals = []
            for sig in all_signals:
                # Key = strategy + ticker + side (same signal across scans)
                sig_key = f"{sig.strategy}|{sig.ticker}|{sig.side}"
                self._signal_seen_this_scan.add(sig_key)
                prev_count = self._signal_streak.get(sig_key, 0)
                if prev_count + 1 >= self.config.SIGNAL_PERSISTENCE_COUNT:
                    persistent_signals.append(sig)
                else:
                    logging.info(f"🔄 PERSISTENCE: {sig.ticker[:25]} {sig.side} "
                                 f"scan {prev_count + 1}/{self.config.SIGNAL_PERSISTENCE_COUNT} "
                                 f"(waiting for confirmation)")
            # Update streaks: increment seen, reset unseen
            new_streak = {}
            for sig_key in self._signal_seen_this_scan:
                new_streak[sig_key] = self._signal_streak.get(sig_key, 0) + 1
            self._signal_streak = new_streak
            filtered_count = len(all_signals) - len(persistent_signals)
            if filtered_count > 0:
                logging.info(f"🔄 PERSISTENCE: {filtered_count} signals waiting for confirmation, "
                             f"{len(persistent_signals)} confirmed")
            all_signals = persistent_signals
        # ── End persistence filter ────────────────────────────

        # ── P4: Apply Kelly sizing ───────────────────────────────
        if self.config.USE_KELLY:
            for sig in all_signals:
                self.kelly.size_signal(sig, balance)
            all_signals = [s for s in all_signals if s.contracts > 0]

        # ── P8: Generate hedges ──────────────────────────────────
        if self.config.HEDGE_ENABLED:
            hedges = []
            for sig in all_signals:
                h = self.hedger.generate(sig)
                if h:
                    hedges.append(h)
            all_signals.extend(hedges)

        self.total_signals += len(all_signals)
        if not all_signals:
            logging.info("😴 No signals this cycle")
            return 0

        # ── Prioritize ──────────────────────────────────────────
        ranked = self._prioritize_signals(all_signals)
        logging.info(f"🎯 {len(ranked)} signals — top 5:")
        for s in ranked[:5]:
            logging.info(f"   {s.strategy} {s.ticker[:30]} "
                         f"{s.side.upper()}@{s.price_cents}¢ x{s.contracts} "
                         f"edge={s.edge_cents}¢ conf={s.confidence:.0%}")

        # ── Execute ─────────────────────────────────────────────
        # Entry window: only enter minutes 0-11 of each 15-min window
        # Blocks last 3 minutes (settlement convergence — prices collapse to 0/100)
        _wmin = datetime.now(timezone.utc).minute % 15
        if _wmin < 0 or _wmin > 11:
            logging.info(f"⏰ OUTSIDE_ENTRY_WINDOW: window min {_wmin}/14 (need 0-11). Scanning only.")
            return 0

        # Per-window loss limit: if both symbols lost last window, skip this one
        now_utc = datetime.now(timezone.utc)
        current_window_key = f"{now_utc.hour}:{(now_utc.minute // 15) * 15:02d}"
        if current_window_key != self._last_window_key:
            # Window transition — check if both symbols lost in previous window
            if (len(self._window_entries) >= 2
                    and self._window_start_balance > 0
                    and self.balance_cents < self._window_start_balance):
                loss = self._window_start_balance - self.balance_cents
                self._skip_until = time.time() + 15 * 60
                skip_until_str = datetime.fromtimestamp(
                    self._skip_until, tz=timezone.utc).strftime('%H:%M:%S')
                logging.warning(f"⛔ WINDOW_COOLDOWN: Both symbols entered in window "
                                f"{self._last_window_key} and lost {loss}¢. "
                                f"Skipping execution until {skip_until_str} UTC")
            self._window_entries = set()
            self._entered_tickers = set()
            self._last_window_key = current_window_key
            self._window_start_balance = self.balance_cents

        if time.time() < self._skip_until:
            skip_remaining = int(self._skip_until - time.time())
            logging.info(f"⛔ WINDOW_COOLDOWN: Skipping execution ({skip_remaining}s remaining)")
            return 0

        max_per_cycle = 3
        to_execute = []
        for signal in ranked[:max_per_cycle]:
            if 'CRYPTO' in signal.strategy:
                # PATCHED: Already-in check — one entry per ticker per contract window
                # HARD CAP: One entry per ticker per window — no stacking, no re-entry
                if signal.ticker in self._entered_tickers:
                    logging.info(f"⛔ ONE_PER_TICKER: Already entered {signal.ticker[:30]} this window, skipping")
                    continue
                # Checks BOTH filled positions AND resting/pending orders on same ticker.
                # Without this, bot stacks 90+ contracts as resting orders before any fill.
                existing_pos = self.risk.positions.get(signal.ticker, {})
                existing_qty = abs(existing_pos.get('position', 0))
                if existing_qty > 0:
                    logging.info(f"⛔ ALREADY_IN: {signal.ticker[:30]} has {existing_qty} filled contracts, skipping")
                    continue
                # Also check resting orders on this ticker
                has_resting = any(
                    info.get('ticker') == signal.ticker
                    for info in self.orders.active_orders.values())
                if has_resting:
                    logging.info(f"⛔ ALREADY_IN: {signal.ticker[:30]} has resting order, skipping")
                    continue
                # Anti-whipsaw: check session-level tracking
                _ex = self.crypto_positions.get(signal.ticker)
                if _ex and _ex != signal.side:
                    logging.info(f"⛔ WHIPSAW_BLOCK: Have {_ex} on {signal.ticker}, rejecting {signal.side}")
                    continue
                # Anti-whipsaw: check Kalshi API positions
                api_pos = self.risk.positions.get(signal.ticker, {})
                existing_qty = api_pos.get('position', 0)
                # position > 0 means holding YES, < 0 means holding NO
                if existing_qty > 0 and signal.side == 'no':
                    logging.info(f"⛔ WHIPSAW_BLOCK: API shows YES({existing_qty}) on {signal.ticker}, rejecting NO")
                    continue
                if existing_qty < 0 and signal.side == 'yes':
                    logging.info(f"⛔ WHIPSAW_BLOCK: API shows NO({abs(existing_qty)}) on {signal.ticker}, rejecting YES")
                    continue
                # ── HARD CONTEXT GATE: block if core crypto data missing ──
                md = signal.market_data or {}
                _spot = md.get('spot_price')
                _strike = md.get('strike')
                _mom = md.get('momentum')
                _cush = md.get('cushion_pct')
                try:
                    _context_ok = (
                        _spot is not None and isinstance(_spot, (int, float)) and _spot > 0
                        and _strike is not None and isinstance(_strike, (int, float)) and _strike > 0
                        and _mom is not None and isinstance(_mom, (int, float))
                        and _cush is not None and isinstance(_cush, (int, float))
                    )
                except (TypeError, ValueError):
                    _context_ok = False
                if not _context_ok:
                    logging.warning(f"⛔ BLOCK_MISSING_CRYPTO_CONTEXT: {signal.ticker[:30]} "
                                    f"spot={_spot} strike={_strike} mom={_mom} cush={_cush}")
                    continue
            approved, reason = self.risk.check_trade(signal, balance)
            self.audit.log_signal(signal, approved, reason)
            # ── CALIBRATION: Log every signal for post-hoc analysis ──
            self.audit.log_signal_csv(signal, traded=approved, reason=reason)
            if approved:
                to_execute.append(signal)
                # Stash signal context for fill CSV logging
                md = signal.market_data or {}
                self._pending_signals[signal.ticker] = {
                    'spot_price': md.get('spot_price', ''),
                    'strike': md.get('strike', ''),
                    'momentum': md.get('momentum', ''),
                    'confidence': f"{signal.confidence:.4f}",
                    'imbalance': md.get('imbalance', ''),
                    'spread': md.get('spread', ''),
                    'total_depth': md.get('total_depth', ''),
                    'cushion_pct': md.get('cushion_pct', ''),
                    'ma_slope': md.get('ma_slope', ''),
                    'distance_from_ma': md.get('distance_from_ma', ''),
                    'direction': md.get('direction', '')
                }

        if not to_execute:
            return 0

        # P5: Batch orders if multiple signals
        if self.config.USE_BATCH_ORDERS and len(to_execute) > 1:
            orders_payload = []
            for sig in to_execute[:20]:
                # Crypto scalp: submit at signal price for instant fill
                is_crypto = 'CRYPTO' in sig.strategy
                if is_crypto:
                    od = {'ticker': sig.ticker, 'side': sig.side.lower(),
                          'action': sig.action.lower(), 'count': sig.contracts,
                          'type': 'limit',
                          'client_order_id': str(uuid.uuid4())}
                    if sig.side == 'yes':
                        od['yes_price'] = sig.price_cents
                    else:
                        od['no_price'] = sig.price_cents
                else:
                    use_limit = self.config.USE_LIMIT_ORDERS
                    od = {'ticker': sig.ticker, 'side': sig.side.lower(),
                          'action': sig.action.lower(), 'count': sig.contracts,
                          'type': 'limit' if use_limit else 'market',
                          'client_order_id': str(uuid.uuid4())}
                    if use_limit:
                        if sig.side == 'yes':
                            od['yes_price'] = sig.price_cents
                        else:
                            od['no_price'] = sig.price_cents
                orders_payload.append(od)
            try:
                resp = self.api.batch_create_orders(orders_payload)
                for item in resp.get('orders', []):
                    order = item.get('order', item)
                    oid = order.get('order_id', '')
                    if oid:
                        self.orders.active_orders[oid] = {
                            'placed_at': time.time(),
                            'status': order.get('status', 'unknown'),
                            'ticker': order.get('ticker', '')}
                        if order.get('status') == 'executed':
                            sig_match = next(
                                (s for s in to_execute
                                 if s.ticker == order.get('ticker')), None)
                            if sig_match:
                                fee = kalshi_fee_cents(
                                    sig_match.price_cents, sig_match.contracts,
                                    getattr(sig_match, 'fee_rate', self.config.FEE_RATE))
                                self.risk.record_fill(
                                    sig_match.ticker, sig_match.side, sig_match.action,
                                    sig_match.price_cents, sig_match.contracts, fee)
                                self.take_profit.track(
                                    sig_match.ticker, sig_match.side,
                                    sig_match.price_cents, sig_match.contracts,
                                    strike=(sig_match.market_data or {}).get('strike'),
                                    symbol='BTC' if 'BTC' in sig_match.ticker.upper() else '',
                                    direction=(sig_match.market_data or {}).get('direction'))
                                if 'CRYPTO' in sig_match.strategy:
                                    self.crypto_positions[sig_match.ticker] = sig_match.side
                                    self._entered_tickers.add(sig_match.ticker)
                                    # Track entry for per-window loss limit
                                    _sym = 'BTC' if 'BTC' in sig_match.ticker else ''
                                    if _sym:
                                        self._window_entries.add(_sym)
                cycle_trades = len(orders_payload)
                self.total_trades += cycle_trades
                logging.info(f"📦 BATCH: {cycle_trades} orders in 1 API call")
            except Exception as e:
                logging.warning(f"Batch failed, falling back: {e}")
                for sig in to_execute:
                    resp = self.orders.place_order(sig)
                    if resp:
                        self.audit.log_order(sig, resp,
                                             resp.get('order', {}).get('status', '?'))
                        cycle_trades += 1
                        self.total_trades += 1
                        balance = self._fetch_balance()
        else:
            for sig in to_execute:
                resp = self.orders.place_order(sig)
                if resp:
                    st = resp.get('order', {}).get('status', 'unknown')
                    self.audit.log_order(sig, resp, st)
                    if st == 'executed':
                        fee = kalshi_fee_cents(
                            sig.price_cents, sig.contracts,
                            getattr(sig, 'fee_rate', self.config.FEE_RATE))
                        self.risk.record_fill(sig.ticker, sig.side, sig.action,
                                              sig.price_cents, sig.contracts, fee)
                        self.take_profit.track(sig.ticker, sig.side,
                                               sig.price_cents, sig.contracts,
                                               strike=(sig.market_data or {}).get('strike'),
                                               symbol='BTC' if 'BTC' in sig.ticker.upper() else '',
                                               direction=(sig.market_data or {}).get('direction'))
                    if 'CRYPTO' in sig.strategy:
                        self.crypto_positions[sig.ticker] = sig.side
                        self._entered_tickers.add(sig.ticker)
                        # Track entry for per-window loss limit
                        _sym = 'BTC' if 'BTC' in sig.ticker else ''
                        if _sym:
                            self._window_entries.add(_sym)
                    cycle_trades += 1
                    self.total_trades += 1
                    balance = self._fetch_balance()

        return cycle_trades

    # ── Main run loop ───────────────────────────────────────────────────

    def run(self, max_cycles: int = 0):
        self.running = True
        mode = "🔴 LIVE" if self.api.live else "🟢 DEMO"

        logging.info(f"\n{'#' * 80}")
        logging.info(f"# KALSHI AUTOTRADER v3.4 — {mode}")
        _ema = self.config.CRYPTO_EMA_NOISE_CUSHION * 100
        _maxp = self.config.CRYPTO_MAX_PRICE
        _ema_struct = "ON" if self.config.CRYPTO_EMA_STRUCTURE_ENABLED else "OFF"
        _rsi_str = "ON" if self.config.CRYPTO_RSI_ENABLED else "OFF"
        _macd_str = "ON" if self.config.CRYPTO_MACD_ENABLED else "OFF"
        logging.info(f"# Cushion: 0.10% | EMA noise: {_ema:.2f}% | Max price: {_maxp}¢ | 1-per-ticker")
        logging.info(f"# EMA 50/100/150 structure: {_ema_struct} (warmup: {self.config.CRYPTO_EMA_MIN_CANDLES} candles)")
        logging.info(f"# RSI({self.config.CRYPTO_RSI_PERIOD}): {_rsi_str} OB={self.config.CRYPTO_RSI_OVERBOUGHT} OS={self.config.CRYPTO_RSI_OVERSOLD} | "
                     f"MACD({self.config.CRYPTO_MACD_FAST}/{self.config.CRYPTO_MACD_SLOW}/{self.config.CRYPTO_MACD_SIGNAL}): {_macd_str}")
        logging.info(f"# Strategies: {', '.join(self.strategies.keys())}")
        logging.info(f"# WebSocket: {'ON' if self.config.USE_WEBSOCKET else 'OFF'}")
        spot_mode = "SPOT REST (primary)" if self.config.CRYPTO_SPOT_DRIVEN else (
            "WS confirm" if self.config.CRYPTO_REQUIRE_SPOT_CONFIRM else "OFF")
        logging.info(f"# Spot feed: {spot_mode}")
        if self.config.USE_KELLY:
            logging.info(f"# Kelly: ON (fraction={self.config.KELLY_FRACTION:.0%})")
        else:
            logging.info(f"# Kelly: OFF (fixed sizing)")
        logging.info(f"# Batch: {'ON' if self.config.USE_BATCH_ORDERS else 'OFF'}")
        logging.info(f"# Take-profit: {'ON' if self.config.TAKE_PROFIT_ENABLED else 'OFF'} "
                     f"@ {self.config.TAKE_PROFIT_THRESHOLD}¢")
        logging.info(f"# Hedging: {'ON' if self.config.HEDGE_ENABLED else 'OFF'}")
        logging.info(f"# Scan interval: {self.config.SCAN_INTERVAL_SECONDS}s")
        logging.info(f"# Max position: ${self.config.MAX_POSITION_CENTS/100:.2f}")
        logging.info(f"# Daily loss limit: ${self.config.MAX_DAILY_LOSS_CENTS/100:.2f}")
        logging.info(f"{'#' * 80}\n")

        try:
            status = self.api.get_exchange_status()
            if not status.get('trading_active'):
                logging.warning("⚠️  Exchange trading is NOT active")
            else:
                logging.info("✅ Exchange active")
            balance = self._fetch_balance()
            logging.info(f"💰 Starting balance: ${balance/100:.2f}")

            # PATCHED: Cancel resting orders from previous sessions
            # Found $85 in dead orders tying up capital during previous session
            try:
                resting = self.api.get_orders(status='resting')
                resting_orders = resting.get('orders', [])
                if resting_orders:
                    startup_cancelled = 0
                    for order in resting_orders:
                        try:
                            self.api.cancel_order(order['order_id'])
                            startup_cancelled += 1
                        except Exception:
                            pass
                    logging.info(f"🗑️  Startup: cancelled {startup_cancelled} resting orders from previous session")
                    # Re-fetch balance after freeing committed capital
                    balance = self._fetch_balance()
                    logging.info(f"💰 Balance after cleanup: ${balance/100:.2f}")
            except Exception as e:
                logging.warning(f"Startup order cleanup failed: {e}")
        except Exception as e:
            logging.critical(f"❌ Cannot connect: {e}")
            return

        # Bootstrap WS tickers
        initial_tickers = []
        try:
            for series in ['KXBTC15M']:
                try:
                    resp = self.api.get_markets(series_ticker=series, limit=50)
                    initial_tickers.extend(m['ticker'] for m in resp.get('markets', []))
                except Exception:
                    pass
            if self.config.INDEX_ENABLED:
                for series in self.config.INDEX_SERIES[:4]:
                    try:
                        resp = self.api.get_markets(series_ticker=series, limit=50)
                        initial_tickers.extend(m['ticker'] for m in resp.get('markets', []))
                    except Exception:
                        pass
            logging.info(f"📋 Bootstrap: {len(initial_tickers)} tickers for WS")
        except Exception as e:
            logging.warning(f"Bootstrap failed: {e}")

        # Start background threads
        if self.config.USE_WEBSOCKET and HAS_WEBSOCKETS:
            self._start_websocket(initial_tickers)
            # WS warmup: wait for snapshots to arrive before scanning
            if initial_tickers and self.ws_manager:
                target = int(len(initial_tickers) * 0.80)
                logging.info(f"⏳ WS warmup: waiting for {target}/{len(initial_tickers)} orderbook snapshots...")
                for _wait_i in range(15):  # max 15s wait
                    time.sleep(1)
                    _cached = sum(1 for t in initial_tickers
                                  if self.ws_manager.get_orderbook(t))
                    if _cached >= target:
                        logging.info(f"✅ WS warm: {_cached}/{len(initial_tickers)} books ready")
                        break
                    if _wait_i % 5 == 4:
                        logging.info(f"⏳ WS warming: {_cached}/{len(initial_tickers)} books...")
                else:
                    _cached = sum(1 for t in initial_tickers
                                  if self.ws_manager.get_orderbook(t))
                    logging.warning(f"⚠️ WS warmup timeout: {_cached}/{len(initial_tickers)} books "
                                    f"(proceeding with partial cache)")
        # Spot feed: REST mode is synchronous (fetched in scan cycle), WS is legacy
        if (self.config.CRYPTO_SPOT_DRIVEN and self.spot_rest
                and 'CRYPTO_SCALP' in self.strategies):
            logging.info("📈 Spot REST feed enabled (BTC via Coinbase)")
            # Start multi-exchange feeds (Kraken, Bitstamp, Gemini) for IDX_PROXY
            if self.multi_feed and HAS_WEBSOCKETS:
                def _run_multi_feeds():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.multi_feed.run_all())
                    except Exception as e:
                        logging.error(f"Multi-exchange feed error: {e}")
                self.multi_feed_thread = threading.Thread(
                    target=_run_multi_feeds, daemon=True, name="multi-exchange-feeds")
                self.multi_feed_thread.start()
                logging.info("📡 Multi-exchange feeds started (Kraken, Bitstamp, Gemini → IDX_PROXY)")
        elif (self.config.CRYPTO_REQUIRE_SPOT_CONFIRM and HAS_WEBSOCKETS
                and 'CRYPTO_SCALP' in self.strategies):
            self._start_spot_feed()

        # Main loop
        cycle = 0
        while self.running:
            try:
                cycle += 1
                if max_cycles > 0 and cycle > max_cycles:
                    logging.info(f"Completed {max_cycles} cycles.")
                    break
                trades = self.run_scan_cycle()
                logging.info(f"📈 Cycle #{self.scan_count}: {trades} trades | "
                             f"Total: {self.total_trades} trades, "
                             f"{self.total_signals} signals | "
                             f"Balance: ${self.balance_cents/100:.2f} | "
                             f"TP: {self.take_profit.get_tracked_count()}")
                if self.running:
                    logging.info(f"⏳ Next scan in {self.config.SCAN_INTERVAL_SECONDS}s\n")
                    time.sleep(self.config.SCAN_INTERVAL_SECONDS)
            except KeyboardInterrupt:
                logging.info("\n🛑 Stopped by user")
                self.running = False
                break
            except Exception as e:
                self.audit.log_error('scan_cycle', e)
                logging.error(f"Scan cycle error: {e}")
                time.sleep(10)

        if self.ws_manager:
            self.ws_manager.stop()
        if self.spot_feed:
            self.spot_feed.stop()
        self._print_summary()

    def _print_summary(self):
        status = self.risk.get_status()
        logging.info(f"\n{'═' * 80}")
        logging.info(f"SESSION SUMMARY")
        logging.info(f"{'═' * 80}")
        logging.info(f"  Scan cycles:    {self.scan_count}")
        logging.info(f"  Total signals:  {self.total_signals}")
        logging.info(f"  Total trades:   {self.total_trades}")
        logging.info(f"  Final balance:  ${self.balance_cents/100:.2f}")
        logging.info(f"  Daily PnL:      {status['daily_pnl_cents']}¢")
        logging.info(f"  Open positions: {status['open_positions']}")
        logging.info(f"  TP tracked:     {self.take_profit.get_tracked_count()}")
        logging.info(f"  Audit logs:     {self.audit.session_file}")
        logging.info(f"  Trade logs:     {self.audit.trade_file}")
        logging.info(f"{'═' * 80}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Kalshi AutoTrader v3.0 — Multi-Exchange EMA + IDX Proxy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategies:
  NO_HARVEST   — Safe harvester + upset hunter
  STRUCTURAL   — STALE/FRAGILE book detection
  CROSS_MARKET — Event arbitrage
  CRYPTO_SCALP — Orderbook imbalance + Binance confirmation
  INDEX_SCALP  — S&P/Nasdaq (half fees)
  MARKET_MAKER — Spread capture (both sides)

Examples:
  # Crypto only (your main setup):
  python kalshi_autotrader_v2_final.py --api-key KEY --key-file kalshi.pem \\
      --live --strategy CRYPTO_SCALP --scan-interval 20

  # Crypto + Index:
  python kalshi_autotrader_v2_final.py --api-key KEY --key-file kalshi.pem \\
      --live --strategy CRYPTO_SCALP INDEX_SCALP

  # All strategies with WebSocket:
  python kalshi_autotrader_v2_final.py --api-key KEY --key-file kalshi.pem --live

  # Disable specific upgrades:
  python kalshi_autotrader_v2_final.py --api-key KEY --key-file kalshi.pem \\
      --live --no-websocket --no-binance --no-kelly
        """)

    parser.add_argument('--api-key', required=True, help='Kalshi API key ID')
    parser.add_argument('--key-file', required=True, help='Path to RSA private key PEM')
    parser.add_argument('--live', action='store_true', help='LIVE TRADING (default: demo)')
    parser.add_argument('--cycles', type=int, default=0, help='Scan cycles (0=forever)')

    parser.add_argument('--strategy', nargs='+',
                        choices=['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET',
                                 'CRYPTO_SCALP', 'INDEX_SCALP', 'MARKET_MAKER'],
                        default=['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET',
                                 'CRYPTO_SCALP', 'INDEX_SCALP'],
                        help='Strategies to run')

    parser.add_argument('--max-position', type=int, default=5500)
    parser.add_argument('--max-position-pct', type=float, default=1.0)
    parser.add_argument('--daily-loss-limit', type=int, default=5000)
    parser.add_argument('--max-positions', type=int, default=100)
    parser.add_argument('--scan-interval', type=int, default=20)
    parser.add_argument('--market-orders', action='store_true')
    parser.add_argument('--order-ttl', type=int, default=30)

    # v2.0 toggle flags
    parser.add_argument('--no-websocket', action='store_true', help='Disable WebSocket')
    parser.add_argument('--no-binance', action='store_true', help='Disable spot feed (legacy name)')
    parser.add_argument('--no-spot', action='store_true', help='Disable spot price feed')
    parser.add_argument('--no-kelly', action='store_true', help='Disable Kelly sizing')
    parser.add_argument('--kelly-fraction', type=float, default=0.25)
    parser.add_argument('--no-batch', action='store_true', help='Disable batch orders')
    parser.add_argument('--no-hedge', action='store_true', help='Disable auto-hedging')
    parser.add_argument('--no-take-profit', action='store_true', help='Disable take-profit')
    parser.add_argument('--take-profit-at', type=int, default=92)
    parser.add_argument('--market-maker', action='store_true', help='Enable market making')
    parser.add_argument('--no-index', action='store_true', help='Disable index markets')

    # Calibration upgrades
    parser.add_argument('--no-persistence', action='store_true', help='Disable signal persistence filter')
    parser.add_argument('--persistence-count', type=int, default=1, help='Consecutive scans required (default 1)')
    parser.add_argument('--no-depth-filter', action='store_true', help='Disable minimum depth filter')
    parser.add_argument('--min-depth', type=int, default=50, help='Min total book depth (default 50)')

    args = parser.parse_args()

    config = TradingConfig(
        MAX_POSITION_CENTS=args.max_position,
        MAX_POSITION_PCT=args.max_position_pct,
        MAX_DAILY_LOSS_CENTS=args.daily_loss_limit,
        MAX_OPEN_POSITIONS=args.max_positions,
        USE_LIMIT_ORDERS=not args.market_orders,
        ORDER_TTL_SECONDS=args.order_ttl,
        SCAN_INTERVAL_SECONDS=args.scan_interval,
        USE_WEBSOCKET=not args.no_websocket,
        CRYPTO_REQUIRE_SPOT_CONFIRM=not (args.no_binance or args.no_spot),
        CRYPTO_SPOT_DRIVEN=not (args.no_binance or args.no_spot),
        USE_KELLY=not args.no_kelly,
        KELLY_FRACTION=args.kelly_fraction,
        USE_BATCH_ORDERS=not args.no_batch,
        HEDGE_ENABLED=not args.no_hedge,
        TAKE_PROFIT_ENABLED=not args.no_take_profit,
        TAKE_PROFIT_THRESHOLD=args.take_profit_at,
        MM_ENABLED=args.market_maker,
        INDEX_ENABLED=not args.no_index,
        # Calibration upgrades
        REQUIRE_SIGNAL_PERSISTENCE=not args.no_persistence,
        SIGNAL_PERSISTENCE_COUNT=args.persistence_count,
        MIN_DEPTH_FILTER_ENABLED=not args.no_depth_filter,
        MIN_BOOK_DEPTH=args.min_depth,
    )

    strats = list(args.strategy)
    if args.market_maker and 'MARKET_MAKER' not in strats:
        strats.append('MARKET_MAKER')
    if args.no_index and 'INDEX_SCALP' in strats:
        strats.remove('INDEX_SCALP')

    if args.live:
        print("\n" + "⚠️ " * 30)
        print("  LIVE TRADING v3.4 — REAL MONEY")
        print(f"  Max position: ${config.MAX_POSITION_CENTS/100:.2f}")
        print(f"  Daily loss limit: ${config.MAX_DAILY_LOSS_CENTS/100:.2f}")
        print(f"  Strategies: {', '.join(strats)}")
        print(f"  WebSocket: {'ON' if config.USE_WEBSOCKET else 'OFF'}")
        spot_str = "SPOT REST (primary)" if config.CRYPTO_SPOT_DRIVEN else "OFF"
        print(f"  Spot feed: {spot_str}")
        if config.USE_KELLY:
            print(f"  Kelly: ON (fraction={config.KELLY_FRACTION:.0%})")
        else:
            print(f"  Kelly: OFF (fixed sizing)")
        print(f"  Take profit: {'ON' if config.TAKE_PROFIT_ENABLED else 'OFF'} @ {config.TAKE_PROFIT_THRESHOLD}¢")
        print(f"  Hedging: {'ON' if config.HEDGE_ENABLED else 'OFF'}")
        print(f"  Signal persistence: {'ON' if config.REQUIRE_SIGNAL_PERSISTENCE else 'OFF'}"
              f" ({config.SIGNAL_PERSISTENCE_COUNT} scans)")
        print(f"  Min depth: {'ON' if config.MIN_DEPTH_FILTER_ENABLED else 'OFF'}"
              f" ({config.MIN_BOOK_DEPTH} contracts)")
        print("⚠️ " * 30)
        confirm = input("\nType 'YES I UNDERSTAND' to proceed: ")
        if confirm.strip() != 'YES I UNDERSTAND':
            print("Aborted.")
            return

    if not os.path.exists(args.key_file):
        print(f"❌ Key file not found: {args.key_file}")
        return

    try:
        api = KalshiAPIClient(
            api_key_id=args.api_key, private_key_path=args.key_file, live=args.live)
    except Exception as e:
        print(f"❌ API init failed: {e}")
        return

    trader = AutoTraderV2(api=api, config=config, strategies=strats)
    trader.run(max_cycles=args.cycles)


if __name__ == "__main__":
    main()
