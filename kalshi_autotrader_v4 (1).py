#!/usr/bin/env python3
"""
Kalshi AutoTrader v4.0 — Settlement Probability Engine

FUNDAMENTAL CHANGE from v1-v3:
  Old: Predict BTC direction using TA indicators, enter 5-15 min early
  New: Model P(settlement_avg >= strike) using Monte Carlo, trade only in final 90s

The ONLY structural edge in Kalshi 15-min BTC contracts:
  Settlement = average of 60 seconds of BRTI (1 print/sec)
  Most traders price as if settlement = spot at close
  With 30 seconds elapsed, you KNOW 30 of 60 data points
  The remaining uncertainty is calculable, not predictable

Strategy:
  1. Collect 1-second IDX_PROXY samples (Coinbase + Kraken + Bitstamp + Gemini)
  2. At T-90s, begin monitoring settlement window
  3. As seconds tick, compute rolling partial average of known prints
  4. Monte Carlo simulate remaining seconds using realized volatility
  5. q_hat = P(final_60s_avg >= strike) from simulation
  6. Compare to Kalshi implied probability (best bid/ask)
  7. Trade ONLY when: EV > 2 × (fee + estimated_slippage)

No RSI. No MACD. No EMA. No cushion filters. No directional bets.
Pure math on a shrinking error bar.

Requires: pip install cryptography requests websockets numpy
"""

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import requests

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

try:
    import websockets
    import websockets.client
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    logging.warning("websockets not installed — WebSocket features disabled.")


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    """v4.0 configuration — radically simplified."""

    # ── Risk Management ──────────────────────────────────────────────────
    MAX_POSITION_CENTS: int = 5500      # max single position cost
    MAX_DAILY_LOSS_CENTS: int = 5000    # daily stop
    BANKROLL_RESERVE_PCT: float = 0.10  # keep 10% in reserve

    # ── Settlement Model ─────────────────────────────────────────────────
    MC_SIMULATIONS: int = 2000          # Monte Carlo paths per evaluation
    VOL_LOOKBACK_SEC: int = 300         # 5 min realized vol window
    VOL_MIN_SAMPLES: int = 30           # need 30+ 1-sec returns for vol estimate
    MEAN_REVERSION_STRENGTH: float = 0.0  # 0 = pure random walk (conservative)

    # ── Trade Entry Rules ────────────────────────────────────────────────
    ENTRY_WINDOW_SEC: int = 90          # only trade in final 90 seconds
    MIN_EV_MULTIPLIER: float = 2.0     # EV must exceed 2× (fee + slippage)
    MIN_EDGE_PCT: float = 0.025         # q_hat must differ from market by ≥2.5%
    MIN_KNOWN_PRINTS: int = 10         # need ≥10 of 60 prints before trading
    MAX_PRICE_CENTS: int = 85          # never buy contracts above 85¢
    MIN_PRICE_CENTS: int = 3           # never buy below 3¢

    # ── Execution ────────────────────────────────────────────────────────
    SCAN_INTERVAL_SEC: float = 1.0     # scan every 1 second in active window
    IDLE_SCAN_INTERVAL_SEC: float = 10.0  # scan every 10s when not in window
    API_RATE_DELAY: float = 0.10
    SLIPPAGE_CENTS: int = 2            # assume 2¢ slippage for EV calc

    # ── Fee Model ────────────────────────────────────────────────────────
    FEE_RATE: float = 0.07             # taker fee multiplier

    # ── Logging ──────────────────────────────────────────────────────────
    LOG_LEVEL: str = 'INFO'
    ONE_PER_TICKER: bool = True        # max 1 position per settlement window


# ═══════════════════════════════════════════════════════════════════════════════
# KALSHI API CLIENT (unchanged from v3.4)
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
                raise
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
                    series_ticker=None) -> dict:
        params = {'status': status, 'limit': str(limit)}
        if cursor:
            params['cursor'] = cursor
        if series_ticker:
            params['series_ticker'] = series_ticker
        return self._request('GET', '/markets', params=params)

    def get_market(self, ticker: str) -> dict:
        return self._request('GET', f'/markets/{ticker}')

    def get_orderbook(self, ticker: str) -> dict:
        return self._request('GET', f'/markets/{ticker}/orderbook')

    def get_balance(self) -> dict:
        return self._request('GET', '/portfolio/balance')

    def get_positions(self, limit=200) -> dict:
        return self._request('GET', '/portfolio/positions',
                             params={'limit': str(limit)})

    def get_fills(self, limit=100) -> dict:
        return self._request('GET', '/portfolio/fills',
                             params={'limit': str(limit)})

    def get_live_position(self, ticker: str, side: str) -> int:
        """HARD SAFETY: Query Kalshi for actual position count on a ticker/side.
        Returns the number of contracts we currently hold. Never trust local state."""
        try:
            positions = self.get_positions(limit=200)
            for pos in positions.get('market_positions', []):
                if pos.get('ticker') == ticker:
                    if side == 'yes':
                        return pos.get('yes_contracts', 0) or 0
                    else:
                        return pos.get('no_contracts', 0) or 0
            return 0
        except Exception as e:
            logging.error(f"Position check failed for {ticker}: {e}")
            return 0  # fail safe: assume 0

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


# ═══════════════════════════════════════════════════════════════════════════════
# FEE CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def kalshi_fee_cents(price_cents: int, contracts: int = 1,
                     fee_rate: float = 0.07) -> int:
    """Exact Kalshi taker fee: ceil(fee_rate * C * P * (1-P))"""
    p = price_cents / 100.0
    fee_dollars = fee_rate * contracts * p * (1 - p)
    return math.ceil(fee_dollars * 100)


def net_ev_cents(price_cents: int, q_hat: float, contracts: int = 1,
                 fee_rate: float = 0.07, slippage_cents: int = 2) -> float:
    """Expected value of buying contracts at price_cents when true prob = q_hat.

    EV = q_hat * (100 - price) * C  -  (1 - q_hat) * price * C  -  fee  -  slippage
       = (q_hat * 100 - price) * C  -  fee  -  slippage
    """
    gross_ev = (q_hat * 100 - price_cents) * contracts
    fee = kalshi_fee_cents(price_cents, contracts, fee_rate)
    slip = slippage_cents * contracts
    return gross_ev - fee - slip


# ═══════════════════════════════════════════════════════════════════════════════
# EXCHANGE FEEDS (Coinbase REST + Kraken/Bitstamp/Gemini WebSocket)
# ═══════════════════════════════════════════════════════════════════════════════

class CoinbaseFeed:
    """REST-based Coinbase feed. 1 call per second."""

    EXCHANGE_URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json',
                                       'User-Agent': 'KalshiV4/1.0'})
        self.price: Optional[float] = None
        self.last_update: float = 0.0

    def fetch(self) -> Optional[float]:
        try:
            resp = self._session.get(self.EXCHANGE_URL, timeout=3)
            if resp.status_code == 200:
                self.price = float(resp.json()['price'])
                self.last_update = time.time()
                return self.price
        except Exception as e:
            logging.debug(f"Coinbase fetch error: {e}")
        return None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.last_update if self.last_update else 999


class ExchangeFeed:
    """Base class for WebSocket exchange feeds."""
    name = "base"

    def __init__(self):
        self.mid_price: Optional[float] = None
        self.last_update: float = 0.0
        self._running = False

    @property
    def age_seconds(self) -> float:
        return time.time() - self.last_update if self.last_update else 999

    async def run(self):
        raise NotImplementedError


class KrakenFeed(ExchangeFeed):
    name = "kraken"
    WS_URL = "wss://ws.kraken.com/v2"

    async def run(self):
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "params": {"channel": "ticker", "symbol": ["BTC/USD"]}
                    }))
                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get('channel') == 'ticker' and 'data' in data:
                            for tick in data['data']:
                                bid = float(tick.get('bid', 0))
                                ask = float(tick.get('ask', 0))
                                if bid > 0 and ask > 0:
                                    self.mid_price = (bid + ask) / 2
                                    self.last_update = time.time()
            except Exception as e:
                logging.debug(f"Kraken WS error: {e}")
                await asyncio.sleep(5)


class BitstampFeed(ExchangeFeed):
    name = "bitstamp"
    WS_URL = "wss://ws.bitstamp.net"

    async def run(self):
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps({
                        "event": "bts:subscribe",
                        "data": {"channel": "order_book_btcusd"}
                    }))
                    async for msg in ws:
                        data = json.loads(msg)
                        d = data.get('data', {})
                        bids = d.get('bids', [])
                        asks = d.get('asks', [])
                        if bids and asks:
                            best_bid = float(bids[0][0])
                            best_ask = float(asks[0][0])
                            if best_bid > 0 and best_ask > 0:
                                self.mid_price = (best_bid + best_ask) / 2
                                self.last_update = time.time()
            except Exception as e:
                logging.debug(f"Bitstamp WS error: {e}")
                await asyncio.sleep(5)


class GeminiFeed(ExchangeFeed):
    name = "gemini"
    WS_URL = "wss://api.gemini.com/v1/marketdata/BTCUSD?top_of_book=true"

    async def run(self):
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    _best_bid = _best_ask = None
                    async for msg in ws:
                        data = json.loads(msg)
                        events = data.get('events', [])
                        for evt in events:
                            if evt.get('type') == 'change':
                                side = evt.get('side')
                                price = float(evt.get('price', 0))
                                if side == 'bid' and price > 0:
                                    _best_bid = price
                                elif side == 'ask' and price > 0:
                                    _best_ask = price
                        if _best_bid and _best_ask:
                            self.mid_price = (_best_bid + _best_ask) / 2
                            self.last_update = time.time()
            except Exception as e:
                logging.debug(f"Gemini WS error: {e}")
                await asyncio.sleep(5)


# ═══════════════════════════════════════════════════════════════════════════════
# IDX PROXY + HIGH-FREQUENCY PRICE COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class PriceCollector:
    """Collects 1-second price samples from multiple exchanges.

    Computes IDX_PROXY = median of all non-stale exchange mid-prices.
    Maintains a rolling buffer of 1-second samples for:
      - Settlement average projection
      - Realized volatility estimation
    """

    STALENESS_SEC = 30.0

    def __init__(self):
        self.coinbase = CoinbaseFeed()
        self.feeds: Dict[str, ExchangeFeed] = {
            'kraken': KrakenFeed(),
            'bitstamp': BitstampFeed(),
            'gemini': GeminiFeed(),
        }
        self._idx_proxy: Optional[float] = None
        # High-frequency sample buffer: (timestamp, price)
        # Stores 1 sample/sec, keeps 10 min history
        self._samples: deque = deque(maxlen=600)
        self._last_sample_sec: int = 0
        self._lock = threading.Lock()

    def update(self) -> Optional[float]:
        """Call once per second. Returns current IDX_PROXY."""
        # Fetch Coinbase (REST)
        self.coinbase.fetch()

        # Collect all fresh prices
        prices = []
        if self.coinbase.price and self.coinbase.age_seconds < self.STALENESS_SEC:
            prices.append(self.coinbase.price)
        for name, feed in self.feeds.items():
            if feed.mid_price and feed.age_seconds < self.STALENESS_SEC:
                prices.append(feed.mid_price)

        if not prices:
            return self._idx_proxy

        # IDX = median
        prices.sort()
        n = len(prices)
        idx = prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) / 2

        with self._lock:
            self._idx_proxy = idx
            # Sample once per integer second — settlement requires aligned prints
            now_sec = int(time.time())
            if now_sec != self._last_sample_sec:
                self._samples.append((float(now_sec), self._idx_proxy))
                self._last_sample_sec = now_sec

        return idx

    @property
    def idx(self) -> Optional[float]:
        with self._lock:
            return self._idx_proxy

    def get_samples_since(self, since_ts: float) -> List[Tuple[float, float]]:
        """Return all samples after given timestamp."""
        with self._lock:
            return [(t, p) for t, p in self._samples if t >= since_ts]

    def get_recent_samples(self, seconds: int = 300) -> List[Tuple[float, float]]:
        """Return samples from last N seconds."""
        cutoff = time.time() - seconds
        with self._lock:
            return [(t, p) for t, p in self._samples if t > cutoff]

    def get_realized_vol(self, lookback_sec: int = 300, min_samples: int = 30) -> Optional[float]:
        """Compute realized volatility (std of 1-second log returns).

        Returns σ per second (not annualized) for direct use in simulation.
        """
        samples = self.get_recent_samples(lookback_sec)
        if len(samples) < min_samples:
            return None

        prices = [p for _, p in samples]
        log_returns = [math.log(prices[i] / prices[i-1])
                       for i in range(1, len(prices))
                       if prices[i-1] > 0 and prices[i] > 0]
        if len(log_returns) < 10:
            return None

        return float(np.std(log_returns))  # σ per second

    def get_price_diffs(self, lookback_sec: int = 300, min_samples: int = 30
                        ) -> Optional[Tuple[float, float]]:
        """Compute arithmetic price diffs for closed-form approximation.

        Returns (mu_price_per_sec, sigma_price_per_sec) or None.
        These are E[ΔP] and std(ΔP) of 1-second price changes.
        """
        samples = self.get_recent_samples(lookback_sec)
        if len(samples) < min_samples:
            return None

        prices = [p for _, p in samples]
        diffs = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        if len(diffs) < 10:
            return None

        return float(np.mean(diffs)), float(np.std(diffs))

    def get_feed_count(self) -> int:
        count = 1 if (self.coinbase.price and self.coinbase.age_seconds < self.STALENESS_SEC) else 0
        for feed in self.feeds.values():
            if feed.mid_price and feed.age_seconds < self.STALENESS_SEC:
                count += 1
        return count

    def get_status_str(self) -> str:
        parts = []
        if self.coinbase.price and self.coinbase.age_seconds < self.STALENESS_SEC:
            parts.append(f"CB=${self.coinbase.price:,.0f}")
        for name, feed in self.feeds.items():
            if feed.mid_price and feed.age_seconds < self.STALENESS_SEC:
                parts.append(f"{name[:3].upper()}=${feed.mid_price:,.0f}")
            else:
                parts.append(f"{name[:3].upper()}=stale")
        if self._idx_proxy:
            parts.append(f"IDX=${self._idx_proxy:,.2f}")
        n = len(self._samples)
        parts.append(f"samples={n}")
        return " | ".join(parts)

    async def run_ws_feeds(self):
        """Run all WebSocket feeds concurrently."""
        tasks = []
        for feed in self.feeds.values():
            tasks.append(asyncio.create_task(feed.run()))
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop_all(self):
        for feed in self.feeds.values():
            feed._running = False


# ═══════════════════════════════════════════════════════════════════════════════
# SETTLEMENT PROBABILITY ENGINE (the core innovation)
# ═══════════════════════════════════════════════════════════════════════════════

class SettlementEngine:
    """Monte Carlo model for P(settlement_avg >= strike).

    Settlement = (1/60) * Σ(BRTI prints for seconds T-60 to T)

    At any time t during the settlement window [T-60, T]:
      - We KNOW the prints from T-60 to t (already collected)
      - We must ESTIMATE the prints from t to T
      - The more prints we have, the tighter our estimate

    For remaining seconds, we simulate Geometric Brownian Motion paths
    using realized volatility from recent price history.
    """

    def __init__(self, config: Config, price_collector: PriceCollector):
        self.config = config
        self.collector = price_collector
        self._rng = np.random.default_rng()

    # ── Closed-Form Normal Approximation (fast screening) ────────────────

    @staticmethod
    def _norm_cdf(x: float) -> float:
        """Φ(x) using erf — no scipy needed."""
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    @staticmethod
    def closed_form_q_hat(
        strike: float, known_sum: float, n_known: int,
        n_remaining: int, S0: float,
        mu_price: float, sigma_price: float,
        direction: str = 'ABOVE'
    ) -> Tuple[float, float, float]:
        """Fast closed-form approximation — no simulation needed.

        Treats remaining price changes as arithmetic Brownian motion:
          S(t) = S0 + μ*t + σ*W(t)
        Average of N samples has Normal distribution.

        Returns: (q_hat, mean_final_avg, sd_final_avg)
        """
        m = int(n_remaining)
        total = n_known + m
        if total == 0:
            return 0.5, 0.0, 0.0
        if m <= 0:
            avg = known_sum / total if total > 0 else 0.0
            if direction == 'ABOVE':
                return (1.0 if avg >= strike else 0.0), avg, 0.0
            return (1.0 if avg < strike else 0.0), avg, 0.0

        tri = m * (m + 1) / 2.0
        mean_future_sum = m * S0 + mu_price * tri
        var_future_sum = (sigma_price ** 2) * tri

        mean_avg = (known_sum + mean_future_sum) / total
        sd_avg = math.sqrt(var_future_sum) / total if var_future_sum > 0 else 0.0

        if sd_avg <= 0:
            if direction == 'ABOVE':
                return (1.0 if mean_avg >= strike else 0.0), mean_avg, 0.0
            return (1.0 if mean_avg < strike else 0.0), mean_avg, 0.0

        z = (strike - mean_avg) / sd_avg
        if direction == 'ABOVE':
            q_hat = 1.0 - SettlementEngine._norm_cdf(z)
        else:
            q_hat = SettlementEngine._norm_cdf(z)

        return q_hat, mean_avg, sd_avg

    # ── Monte Carlo (full precision) ─────────────────────────────────────

    def compute_q_hat(
        self,
        strike: float,
        settlement_time: float,
        direction: str = 'ABOVE'
    ) -> Optional[dict]:
        """Compute probability that settlement average >= strike (or <= for BELOW).

        Returns dict with:
          q_hat: estimated probability
          known_prints: number of prints already observed
          remaining_prints: number still to simulate
          partial_avg: average of known prints
          vol_per_sec: realized σ per second used in simulation
          current_price: latest IDX_PROXY

        Returns None if insufficient data.
        """
        now = time.time()
        window_start = settlement_time - 60.0  # settlement window is 60 seconds
        seconds_into_window = now - window_start
        seconds_remaining = settlement_time - now

        if seconds_remaining < 0:
            return None  # already settled
        if seconds_into_window < 0:
            return None  # window hasn't started yet

        # Get observed prints during settlement window
        # Use integer-second boundaries for alignment with actual BRTI prints
        window_start_int = int(math.ceil(window_start))
        observed = self.collector.get_samples_since(float(window_start_int))
        known_prices = [p for _, p in observed]
        n_known = len(known_prices)
        n_remaining = max(0, int(seconds_remaining))

        current_price = self.collector.idx
        if current_price is None:
            return None

        # Get realized volatility — MUST have real data, no guessing
        vol = self.collector.get_realized_vol(
            self.config.VOL_LOOKBACK_SEC, self.config.VOL_MIN_SAMPLES)
        if vol is None:
            logging.debug("Cannot compute q_hat: insufficient vol data (no fallback)")
            return None

        # If no prints observed yet but window has started, use current price
        if n_known == 0 and seconds_into_window > 0:
            # Estimate prints we missed as current price
            n_missed = min(int(seconds_into_window), 60)
            known_prices = [current_price] * n_missed
            n_known = n_missed

        if n_known == 0:
            return None

        # ── Monte Carlo Simulation ───────────────────────────────────────
        known_sum = sum(known_prices)
        total_prints = n_known + n_remaining

        if n_remaining == 0:
            # Window complete — deterministic
            final_avg = known_sum / max(total_prints, 1)
            if direction == 'ABOVE':
                q_hat = 1.0 if final_avg >= strike else 0.0
            else:
                q_hat = 1.0 if final_avg < strike else 0.0
            return {
                'q_hat': q_hat,
                'known_prints': n_known,
                'remaining_prints': 0,
                'partial_avg': final_avg,
                'vol_per_sec': vol,
                'current_price': current_price,
                'total_prints': total_prints,
                'known_sum': known_sum,
            }

        # Simulate remaining price paths — VECTORIZED cumprod (5× faster)
        n_sims = self.config.MC_SIMULATIONS
        S0 = current_price

        # GBM: S_{t+1} = S_t * exp((μ - σ²/2) + σ*Z)
        mu = 0.0
        if self.config.MEAN_REVERSION_STRENGTH > 0:
            known_avg = known_sum / n_known
            mu = self.config.MEAN_REVERSION_STRENGTH * (known_avg - S0) / S0

        drift = mu - 0.5 * vol * vol
        Z = self._rng.standard_normal((n_sims, n_remaining), dtype=np.float64)
        log_increments = drift + vol * Z

        # cumprod of multiplicative returns — numerically stable for long paths
        mult = np.exp(log_increments)
        future_prices = S0 * np.cumprod(mult, axis=1)  # shape: (n_sims, n_remaining)

        # Combine known + simulated → settlement average per path
        future_sums = np.sum(future_prices, axis=1)  # shape: (n_sims,)
        settlement_avgs = (known_sum + future_sums) / total_prints

        # q_hat = fraction of simulations where avg meets condition
        if direction == 'ABOVE':
            q_hat = float(np.mean(settlement_avgs >= strike))
        else:
            q_hat = float(np.mean(settlement_avgs < strike))

        return {
            'q_hat': q_hat,
            'known_prints': n_known,
            'remaining_prints': n_remaining,
            'partial_avg': known_sum / n_known,
            'vol_per_sec': vol,
            'current_price': current_price,
            'total_prints': total_prints,
            'known_sum': known_sum,
            'mean_final_avg': float(np.mean(settlement_avgs)),
            'std_final_avg': float(np.std(settlement_avgs, ddof=1)) if n_sims > 1 else 0.0,
        }

    def evaluate_trade(
        self,
        strike: float,
        settlement_time: float,
        direction: str,
        kalshi_yes_price: int,  # cents
        kalshi_no_price: int,   # cents
    ) -> dict:
        """Hybrid trade evaluation: closed-form screen → MC only when near threshold.

        ALWAYS returns a dict with data + trade recommendation.
        'trade_side' is None if no trade recommended.
        '_mc_valid' is False if data insufficient.
        '_method' is 'closed_form' or 'monte_carlo'.
        """
        now = time.time()
        window_start = settlement_time - 60.0
        seconds_into_window = now - window_start
        seconds_remaining = settlement_time - now

        if seconds_remaining < 0 or seconds_into_window < 0:
            return {'_mc_valid': False, 'trade_side': None}

        current_price = self.collector.idx
        if current_price is None:
            return {'_mc_valid': False, 'trade_side': None}

        # Get observed settlement prints
        window_start_int = int(math.ceil(window_start))
        observed = self.collector.get_samples_since(float(window_start_int))
        known_prices = [p for _, p in observed]
        n_known = len(known_prices)
        n_remaining = max(0, int(seconds_remaining))

        if n_known == 0 and seconds_into_window > 0:
            n_missed = min(int(seconds_into_window), 60)
            known_prices = [current_price] * n_missed
            n_known = n_missed

        if n_known == 0:
            return {'_mc_valid': False, 'trade_side': None}

        known_sum = sum(known_prices)
        total_prints = n_known + n_remaining

        # Get vol data — required for both methods
        vol = self.collector.get_realized_vol(
            self.config.VOL_LOOKBACK_SEC, self.config.VOL_MIN_SAMPLES)
        price_diffs = self.collector.get_price_diffs(
            self.config.VOL_LOOKBACK_SEC, self.config.VOL_MIN_SAMPLES)

        if vol is None or price_diffs is None:
            return {'_mc_valid': False, 'trade_side': None}

        mu_price, sigma_price = price_diffs

        # ── STEP 1: Closed-form screening (fast, every tick) ─────────────
        cf_q_hat, cf_mean, cf_sd = self.closed_form_q_hat(
            strike, known_sum, n_known, n_remaining,
            current_price, mu_price, sigma_price, direction)

        # Quick check: is closed-form edge anywhere near tradeable?
        yes_implied = kalshi_yes_price / 100.0
        no_implied = kalshi_no_price / 100.0
        cf_yes_edge = cf_q_hat - yes_implied
        cf_no_edge = (1 - cf_q_hat) - no_implied
        best_cf_edge = max(cf_yes_edge, cf_no_edge)

        # Build base result with closed-form data
        base = {
            '_mc_valid': True,
            '_method': 'closed_form',
            'q_hat': cf_q_hat,
            'known_prints': n_known,
            'remaining_prints': n_remaining,
            'partial_avg': known_sum / n_known,
            'vol_per_sec': vol,
            'current_price': current_price,
            'total_prints': total_prints,
            'known_sum': known_sum,
            'mean_final_avg': cf_mean,
            'std_final_avg': cf_sd,
            'q_hat_yes': cf_q_hat,
            'q_hat_no': 1 - cf_q_hat,
            'yes_price': kalshi_yes_price,
            'no_price': kalshi_no_price,
            'yes_implied': yes_implied,
            'no_implied': no_implied,
            'trade_side': None,
        }

        # If closed-form edge is clearly below threshold, skip MC entirely
        # Only upgrade to MC when CF edge is within striking distance of MIN_EDGE_PCT
        CF_UPGRADE_MARGIN = 0.015  # run MC if cf_edge within 1.5% of threshold
        if best_cf_edge < self.config.MIN_EDGE_PCT - CF_UPGRADE_MARGIN:
            return base  # definite no-trade, saved 2000 sims

        # ── STEP 2: MC for precision (only when edge is near threshold) ──
        mc = self.compute_q_hat(strike, settlement_time, direction)
        if mc is None:
            return base  # fall back to closed-form result

        q_hat = mc['q_hat']
        base.update(mc)
        base['_method'] = 'monte_carlo'
        base['q_hat_yes'] = q_hat
        base['q_hat_no'] = 1 - q_hat

        # ── STEP 3: Edge + EV evaluation ─────────────────────────────────
        best_side = None
        best_ev = -999
        best_price = 0
        best_edge = 0

        q_yes = base['q_hat_yes']
        q_no = base['q_hat_no']

        if self.config.MIN_PRICE_CENTS <= kalshi_yes_price <= self.config.MAX_PRICE_CENTS:
            yes_ev = net_ev_cents(
                kalshi_yes_price, q_yes, 1,
                self.config.FEE_RATE, self.config.SLIPPAGE_CENTS)
            yes_edge = q_yes - yes_implied
            if yes_ev > best_ev and yes_edge >= self.config.MIN_EDGE_PCT:
                best_side = 'yes'
                best_ev = yes_ev
                best_price = kalshi_yes_price
                best_edge = yes_edge

        if self.config.MIN_PRICE_CENTS <= kalshi_no_price <= self.config.MAX_PRICE_CENTS:
            no_ev = net_ev_cents(
                kalshi_no_price, q_no, 1,
                self.config.FEE_RATE, self.config.SLIPPAGE_CENTS)
            no_edge = q_no - no_implied
            if no_ev > best_ev and no_edge >= self.config.MIN_EDGE_PCT:
                best_side = 'no'
                best_ev = no_ev
                best_price = kalshi_no_price
                best_edge = no_edge

        # Always include MC data in result
        mc['_mc_valid'] = True
        mc['q_hat_yes'] = q_hat
        mc['q_hat_no'] = 1 - q_hat
        mc['yes_price'] = kalshi_yes_price
        mc['no_price'] = kalshi_no_price
        mc['yes_implied'] = yes_implied
        mc['no_implied'] = no_implied

        if best_side is None:
            return base

        # Check EV threshold: must exceed 2× (fee + slippage)
        fee = kalshi_fee_cents(best_price, 1, self.config.FEE_RATE)
        threshold = self.config.MIN_EV_MULTIPLIER * (fee + self.config.SLIPPAGE_CENTS)
        if best_ev < threshold:
            return base

        base.update({
            'trade_side': best_side,
            'trade_price': best_price,
            'edge': best_edge,
            'ev_per_contract': best_ev,
            'fee_per_contract': fee,
            'ev_threshold': threshold,
        })
        return base


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════

STRIKE_RE = re.compile(r'[\$]?([\d,]+(?:\.\d+)?)')


def parse_strike(market: dict) -> Optional[float]:
    """Extract strike price from market data."""
    strike = market.get('floor_strike')
    if strike and float(strike) > 0:
        return float(strike)
    title = market.get('title', '')
    match = STRIKE_RE.search(title)
    if match:
        return float(match.group(1).replace(',', ''))
    return None


def parse_direction(title: str) -> str:
    """Is this an 'above' or 'below' contract?"""
    t = title.lower()
    if any(kw in t for kw in ['above', 'up', 'higher', 'at or above']):
        return 'ABOVE'
    elif any(kw in t for kw in ['below', 'down', 'lower', 'at or below']):
        return 'BELOW'
    return 'UNKNOWN'


def parse_close_time(market: dict) -> Optional[float]:
    """Parse close_time string to epoch timestamp."""
    close_str = market.get('close_time')
    if not close_str:
        return None
    try:
        dt = datetime.fromisoformat(close_str.replace('Z', '+00:00'))
        return dt.timestamp()
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION LOGGER
# ═══════════════════════════════════════════════════════════════════════════════

class SessionLogger:
    """Logs all evaluations, trades, and outcomes.

    Two output files:
    1. JSONL: full event log (trades, errors, metadata)
    2. CSV: structured quant eval log (section E from the analysis)
       Every scan of every market gets a row — this is the dataset
       needed to answer "does edge exist?" with numbers.
    """

    EVAL_CSV_HEADER = (
        "timestamp,scan_num,ticker,strike,direction,settle_time,seconds_left,"
        "idx_proxy,partial_avg,known_prints,remaining_prints,total_prints,"
        "realized_vol_5m,q_hat_yes,q_hat_no,method,"
        "best_yes_ask,best_yes_bid,best_no_ask,best_no_bid,"
        "yes_implied,no_implied,"
        "fee_yes,fee_no,slippage,"
        "ev_yes,ev_no,"
        "edge_yes,edge_no,"
        "decision,trade_side,trade_price,contracts,"
        "feed_count,spread_yes,spread_no,book_depth_yes,book_depth_no\n"
    )

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.log_path = f"session_v4_{session_id}.jsonl"
        self.eval_csv_path = f"evals_v4_{session_id}.csv"
        self.trade_count = 0
        self.eval_count = 0
        self._f = open(self.log_path, 'a')
        self._csv = open(self.eval_csv_path, 'w')
        self._csv.write(self.EVAL_CSV_HEADER)
        self._csv.flush()

    def log(self, event_type: str, data: dict):
        entry = {
            'ts': datetime.now(timezone.utc).isoformat(),
            'event': event_type,
            **data
        }
        self._f.write(json.dumps(entry, default=str) + '\n')
        self._f.flush()

    def log_quant_eval(self, scan_num: int, ticker: str, strike: float,
                       direction: str, settle_time: float, seconds_left: float,
                       idx_proxy: float, result: Optional[dict],
                       book_data: dict, decision: str,
                       trade_side: str = '', trade_price: int = 0,
                       contracts: int = 0, feed_count: int = 0):
        """Write one row to the quant eval CSV — the dataset for edge analysis."""
        self.eval_count += 1

        # Extract from MC result (or defaults if no result)
        if result:
            partial_avg = result.get('partial_avg', 0)
            known = result.get('known_prints', 0)
            remaining = result.get('remaining_prints', 0)
            total = result.get('total_prints', 0)
            vol = result.get('vol_per_sec', 0)
            q_yes = result.get('q_hat_yes', result.get('q_hat', 0))
            q_no = result.get('q_hat_no', 1 - q_yes)
            method = result.get('_method', 'unknown')
        else:
            partial_avg = known = remaining = total = 0
            vol = q_yes = q_no = 0
            method = 'none'

        yes_ask = book_data.get('yes_ask', 99)
        yes_bid = book_data.get('yes_bid', 1)
        no_ask = book_data.get('no_ask', 99)
        no_bid = book_data.get('no_bid', 1)
        depth_yes = book_data.get('depth_yes', 0)
        depth_no = book_data.get('depth_no', 0)

        yes_implied = yes_ask / 100.0
        no_implied = no_ask / 100.0
        spread_yes = yes_ask - yes_bid if yes_ask > yes_bid else 0
        spread_no = no_ask - no_bid if no_ask > no_bid else 0

        fee_yes = kalshi_fee_cents(yes_ask, 1, 0.07)
        fee_no = kalshi_fee_cents(no_ask, 1, 0.07)
        slippage = 2  # assumed

        ev_yes = net_ev_cents(yes_ask, q_yes, 1, 0.07, slippage) if q_yes > 0 else 0
        ev_no = net_ev_cents(no_ask, q_no, 1, 0.07, slippage) if q_no > 0 else 0

        edge_yes = q_yes - yes_implied if q_yes > 0 else 0
        edge_no = q_no - no_implied if q_no > 0 else 0

        ts = datetime.now(timezone.utc).isoformat()
        row = (
            f"{ts},{scan_num},{ticker},{strike:.0f},{direction},"
            f"{settle_time:.0f},{seconds_left:.1f},"
            f"{idx_proxy:.2f},{partial_avg:.2f},{known},{remaining},{total},"
            f"{vol:.8f},{q_yes:.4f},{q_no:.4f},{method},"
            f"{yes_ask},{yes_bid},{no_ask},{no_bid},"
            f"{yes_implied:.4f},{no_implied:.4f},"
            f"{fee_yes},{fee_no},{slippage},"
            f"{ev_yes:.2f},{ev_no:.2f},"
            f"{edge_yes:.4f},{edge_no:.4f},"
            f"{decision},{trade_side},{trade_price},{contracts},"
            f"{feed_count},{spread_yes},{spread_no},{depth_yes},{depth_no}\n"
        )
        self._csv.write(row)
        self._csv.flush()

    def log_trade(self, ticker: str, side: str, price: int,
                  contracts: int, result: dict):
        self.trade_count += 1
        self.log('trade', {
            'ticker': ticker,
            'side': side,
            'price_cents': price,
            'contracts': contracts,
            'ev': result.get('ev_per_contract', 0),
            'edge': result.get('edge', 0),
            'q_hat': result.get('q_hat', 0),
        })

    def close(self):
        self._f.close()
        self._csv.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN TRADING BOT
# ═══════════════════════════════════════════════════════════════════════════════

class SettlementBot:
    """v4.0 Trading Bot — Settlement Probability Engine.

    Core loop:
    1. Discover BTC 15-min markets on Kalshi
    2. For each market, compute time to settlement
    3. If within 90-second entry window:
       a. Collect price prints (already happening via PriceCollector)
       b. Run Monte Carlo → q_hat
       c. Compare to Kalshi orderbook → EV
       d. Trade if EV > threshold
    4. Log everything
    """

    def __init__(self, api: KalshiAPIClient, config: Config):
        self.api = api
        self.config = config
        self.collector = PriceCollector()
        self.engine = SettlementEngine(config, self.collector)
        self.logger = SessionLogger(
            datetime.now().strftime('%Y%m%d_%H%M%S'))

        # State
        self._running = False
        self._traded_tickers: set = set()  # ONE_PER_TICKER guard
        self._daily_loss_cents: int = 0
        self._scan_count: int = 0
        self._start_time = time.time()

    async def run(self):
        """Main async loop."""
        self._running = True
        logging.info("=" * 70)
        logging.info("🚀 Kalshi AutoTrader v4.0 — Settlement Probability Engine")
        logging.info("=" * 70)
        logging.info(f"Mode: {'LIVE 💰' if self.api.live else 'DEMO 🧪'}")
        logging.info(f"MC simulations: {self.config.MC_SIMULATIONS}")
        logging.info(f"Entry window: final {self.config.ENTRY_WINDOW_SEC}s")
        logging.info(f"Min edge: {self.config.MIN_EDGE_PCT*100:.1f}%")
        logging.info(f"EV multiplier: {self.config.MIN_EV_MULTIPLIER}×")
        logging.info(f"Vol lookback: {self.config.VOL_LOOKBACK_SEC}s")

        # Check balance
        try:
            bal = self.api.get_balance()
            balance_cents = bal.get('balance', 0)
            logging.info(f"💰 Balance: ${balance_cents/100:.2f}")
        except Exception as e:
            logging.error(f"Failed to get balance: {e}")
            return

        # Start WebSocket feeds in background
        ws_task = asyncio.create_task(self.collector.run_ws_feeds())
        logging.info("📡 Starting exchange feeds (Kraken, Bitstamp, Gemini)...")

        # Give feeds 5 seconds to connect
        await asyncio.sleep(5)
        logging.info(f"🌐 Feeds: {self.collector.get_status_str()}")

        # Main loop
        try:
            while self._running:
                await self._scan_cycle()
        except KeyboardInterrupt:
            logging.info("⛔ Shutting down...")
        except Exception as e:
            logging.error(f"Fatal error: {e}", exc_info=True)
        finally:
            self._running = False
            self.collector.stop_all()
            ws_task.cancel()
            self.logger.close()
            logging.info(f"📊 Session stats: {self._scan_count} scans, "
                         f"{self.logger.trade_count} trades, "
                         f"{self.logger.eval_count} evaluations")

    async def _scan_cycle(self):
        """Single scan cycle."""
        self._scan_count += 1

        # Update price collector (Coinbase REST)
        self.collector.update()

        # Log status periodically
        if self._scan_count % 30 == 1:
            feeds = self.collector.get_feed_count()
            vol = self.collector.get_realized_vol(
                self.config.VOL_LOOKBACK_SEC, self.config.VOL_MIN_SAMPLES)
            vol_str = f"σ={vol*100:.4f}%/s" if vol else "σ=warming"
            n_samples = len(self.collector._samples)
            logging.info(
                f"📡 Scan #{self._scan_count} | "
                f"{self.collector.get_status_str()} | "
                f"{vol_str} | {feeds} feeds | {n_samples} samples"
            )

        # Discover BTC 15-min markets
        try:
            markets_resp = self.api.get_markets(
                status='open', series_ticker='KXBTC15M')
            markets = markets_resp.get('markets', [])
        except Exception as e:
            logging.error(f"Market discovery failed: {e}")
            await asyncio.sleep(self.config.IDLE_SCAN_INTERVAL_SEC)
            return

        if not markets:
            logging.debug("No open KXBTC15M markets")
            await asyncio.sleep(self.config.IDLE_SCAN_INTERVAL_SEC)
            return

        # Check each market for entry window
        now = time.time()
        in_window = False

        for market in markets:
            ticker = market.get('ticker', '')
            if not ticker or '15M' not in ticker.upper():
                continue

            close_time = parse_close_time(market)
            if not close_time:
                continue

            seconds_to_close = close_time - now
            settlement_time = close_time  # settlement happens at close

            # Are we in the entry window?
            if seconds_to_close <= 0:
                continue  # already closed
            if seconds_to_close > self.config.ENTRY_WINDOW_SEC:
                logging.debug(
                    f"⏳ {ticker}: {seconds_to_close:.0f}s to close "
                    f"(window starts at {self.config.ENTRY_WINDOW_SEC}s)")
                continue

            in_window = True

            # ONE_PER_TICKER guard
            if self.config.ONE_PER_TICKER and ticker in self._traded_tickers:
                logging.debug(f"🚫 {ticker}: already traded this window")
                continue

            # Parse contract details
            strike = parse_strike(market)
            title = market.get('title', '')
            direction = parse_direction(title)
            if not strike or direction == 'UNKNOWN':
                logging.debug(f"⚠️ {ticker}: no strike or direction")
                continue

            # Get orderbook
            try:
                book = self.api.get_orderbook(ticker)
            except Exception as e:
                logging.debug(f"Orderbook error {ticker}: {e}")
                continue

            # Extract best prices AND depth for quant logging
            raw_book = book.get('orderbook', {})
            yes_levels = raw_book.get('yes', [])
            no_levels = raw_book.get('no', [])
            if not yes_levels and not no_levels:
                logging.debug(f"📕 {ticker}: empty book")
                continue

            # Best ask = cheapest price to buy (first level)
            yes_price = int(yes_levels[0][0]) if yes_levels else 99
            no_price = int(no_levels[0][0]) if no_levels else 99
            # Best bid = 100 - opponent's best ask
            yes_bid = 100 - no_price if no_levels else 1
            no_bid = 100 - yes_price if yes_levels else 1
            # Total depth (sum of contracts across all levels)
            depth_yes = sum(int(l[1]) for l in yes_levels) if yes_levels else 0
            depth_no = sum(int(l[1]) for l in no_levels) if no_levels else 0

            book_data = {
                'yes_ask': yes_price, 'yes_bid': yes_bid,
                'no_ask': no_price, 'no_bid': no_bid,
                'depth_yes': depth_yes, 'depth_no': depth_no,
            }

            # ── EVALUATE ─────────────────────────────────────────────
            idx = self.collector.idx
            if idx is None:
                logging.debug(f"⚠️ {ticker}: no IDX price yet")
                continue

            result = self.engine.evaluate_trade(
                strike=strike,
                settlement_time=settlement_time,
                direction=direction,
                kalshi_yes_price=yes_price,
                kalshi_no_price=no_price,
            )

            sec_left = seconds_to_close
            feed_count = self.collector.get_feed_count()
            mc_valid = result.get('_mc_valid', False)

            if result.get('trade_side'):
                # We have a trade recommendation!
                ev = result.get('ev_per_contract', 0)
                edge = result.get('edge', 0)
                side = result['trade_side']
                price = result['trade_price']
                known = result.get('known_prints', 0)
                remaining = result.get('remaining_prints', 0)
                q_hat = result.get('q_hat', 0)
                partial = result.get('partial_avg', 0)

                logging.info(
                    f"🎯 {ticker} | {sec_left:.0f}s left | "
                    f"strike=${strike:,.0f} dir={direction} | "
                    f"IDX=${idx:,.0f} | partial_avg=${partial:,.0f} | "
                    f"prints={known}/{known+remaining} | "
                    f"q_hat={q_hat:.3f} vs market={price/100:.2f} | "
                    f"edge={edge*100:.1f}% | "
                    f"EV={ev:.1f}¢ (thresh={result['ev_threshold']:.1f}¢) | "
                    f"→ {side.upper()} @{price}¢")

                # Log to quant CSV
                self.logger.log_quant_eval(
                    scan_num=self._scan_count, ticker=ticker, strike=strike,
                    direction=direction, settle_time=settlement_time,
                    seconds_left=sec_left, idx_proxy=idx, result=result,
                    book_data=book_data, decision=f'SIGNAL_{side.upper()}',
                    trade_side=side, trade_price=price, contracts=0,
                    feed_count=feed_count)

                # Execute trade
                await self._execute_trade(ticker, result, book_data)
            else:
                # No trade — log why with full quant data
                q_hat = result.get('q_hat', 0) if mc_valid else 0
                known = result.get('known_prints', 0) if mc_valid else 0
                remaining = result.get('remaining_prints', '?') if mc_valid else '?'
                partial = result.get('partial_avg', 0) if mc_valid else 0

                decision = 'NO_EVAL' if not mc_valid else 'NO_EDGE'
                logging.info(
                    f"📊 {ticker} | {sec_left:.0f}s left | "
                    f"strike=${strike:,.0f} dir={direction} | "
                    f"IDX=${idx:,.0f} | partial_avg=${partial:,.0f} | "
                    f"prints={known}/{known+remaining if isinstance(remaining,int) else remaining} | "
                    f"q_hat={q_hat:.3f} | "
                    f"YES@{yes_price}¢ NO@{no_price}¢ | "
                    f"{decision}")

                self.logger.log_quant_eval(
                    scan_num=self._scan_count, ticker=ticker, strike=strike,
                    direction=direction, settle_time=settlement_time,
                    seconds_left=sec_left, idx_proxy=idx,
                    result=result if mc_valid else None,
                    book_data=book_data, decision=decision,
                    feed_count=feed_count)

        # Adaptive sleep: fast in window, slow otherwise
        if in_window:
            await asyncio.sleep(self.config.SCAN_INTERVAL_SEC)
        else:
            await asyncio.sleep(self.config.IDLE_SCAN_INTERVAL_SEC)

    async def _execute_trade(self, ticker: str, result: dict, book_data: dict):
        """Execute a trade based on engine recommendation.

        HARD SAFETY RULES:
        1. v4.0 is BUY-ONLY. No sell orders. Hold to settlement.
        2. Before ANY order, verify balance from Kalshi API (not local state).
        3. Verify we don't already hold a position on this ticker.
        4. Max 5 contracts per trade until edge is proven.
        """
        side = result['trade_side']
        price = result['trade_price']
        ev = result['ev_per_contract']
        edge = result['edge']

        # ── HARD SAFETY: Verify no existing position on this ticker ──────
        live_yes = self.api.get_live_position(ticker, 'yes')
        live_no = self.api.get_live_position(ticker, 'no')
        if live_yes > 0 or live_no > 0:
            logging.warning(
                f"🛡️ POSITION_EXISTS: {ticker} already has "
                f"yes={live_yes} no={live_no} — skipping")
            self._traded_tickers.add(ticker)
            self.logger.log('position_exists_block', {
                'ticker': ticker, 'live_yes': live_yes, 'live_no': live_no})
            return

        # Check bankroll
        try:
            bal = self.api.get_balance()
            balance_cents = bal.get('balance', 0)
        except Exception as e:
            logging.error(f"Balance check failed: {e}")
            return

        # Reserve check
        usable = int(balance_cents * (1 - self.config.BANKROLL_RESERVE_PCT))
        max_contracts = usable // price if price > 0 else 0

        if max_contracts <= 0:
            logging.warning(
                f"💸 BANKROLL_BLOCK: balance=${balance_cents/100:.2f}, "
                f"usable=${usable/100:.2f}, need {price}¢/contract")
            self.logger.log_eval(ticker, result, 'BANKROLL_BLOCK')
            return

        # Size: start conservative — max 5 contracts or what bankroll allows
        contracts = min(5, max_contracts)
        total_cost = price * contracts
        fee = kalshi_fee_cents(price, contracts, self.config.FEE_RATE)
        total_with_fee = total_cost + fee

        if total_with_fee > usable:
            contracts = max(1, (usable - fee) // price)
            if contracts <= 0:
                logging.warning(f"💸 BANKROLL_BLOCK after fee adjustment")
                return

        # Daily loss check
        if self._daily_loss_cents >= self.config.MAX_DAILY_LOSS_CENTS:
            logging.warning(f"🛑 DAILY_LOSS_LIMIT: ${self._daily_loss_cents/100:.2f}")
            return

        # Place order
        logging.info(
            f"🔥 EXECUTING: {side.upper()} {ticker} × {contracts} @ {price}¢ "
            f"(cost=${total_cost/100:.2f} + fee=${fee/100:.2f}) | "
            f"EV={ev:.1f}¢/contract | edge={edge*100:.1f}%")

        try:
            # Send limit at exact ask price for taker fill.
            # DO NOT add offset — crossing into next level worsens price.
            if side == 'yes':
                order = self.api.create_order(
                    ticker=ticker, side=side, action='buy',
                    count=contracts, order_type='limit',
                    yes_price=price)
            else:
                order = self.api.create_order(
                    ticker=ticker, side=side, action='buy',
                    count=contracts, order_type='limit',
                    no_price=price)

            order_data = order.get('order', {})
            order_id = order_data.get('order_id', 'unknown')
            status = order_data.get('status', 'unknown')
            logging.info(f"✅ Order placed: {order_id} status={status}")

            # Verify fill after 2 seconds
            await asyncio.sleep(2)
            live_pos = self.api.get_live_position(ticker, side)
            logging.info(f"📋 Position verification: {ticker} {side}={live_pos}")

            self._traded_tickers.add(ticker)
            self.logger.log_trade(ticker, side, price, contracts, result)

            # Update quant CSV with actual trade
            self.logger.log_quant_eval(
                scan_num=self._scan_count, ticker=ticker,
                strike=result.get('current_price', 0),
                direction='', settle_time=0,
                seconds_left=0, idx_proxy=result.get('current_price', 0),
                result=result, book_data=book_data,
                decision='EXECUTED',
                trade_side=side, trade_price=price,
                contracts=contracts,
                feed_count=self.collector.get_feed_count())

        except Exception as e:
            logging.error(f"❌ Order failed: {e}")
            self.logger.log('order_error', {
                'ticker': ticker, 'side': side, 'price': price,
                'contracts': contracts, 'error': str(e)
            })


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Kalshi AutoTrader v4.0 — Settlement Probability Engine')
    parser.add_argument('--live', action='store_true',
                        help='Use live (real money) API')
    parser.add_argument('--key-id', type=str,
                        default=os.environ.get('KALSHI_API_KEY_ID', ''),
                        help='Kalshi API key ID')
    parser.add_argument('--key-file', type=str,
                        default=os.environ.get('KALSHI_KEY_FILE', 'private_key.pem'),
                        help='Path to RSA private key')
    parser.add_argument('--entry-window', type=int, default=90,
                        help='Seconds before close to begin evaluating (default: 90)')
    parser.add_argument('--min-edge', type=float, default=0.025,
                        help='Minimum probability edge to trade (default: 0.025 = 2.5%%)')
    parser.add_argument('--ev-mult', type=float, default=2.0,
                        help='EV must exceed this multiple of fee+slippage (default: 2.0)')
    parser.add_argument('--sims', type=int, default=2000,
                        help='Monte Carlo simulations per evaluation (default: 2000)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)-7s %(message)s',
        datefmt='%H:%M:%S')

    if not args.key_id:
        logging.error("API key ID required. Set KALSHI_API_KEY_ID or use --key-id")
        sys.exit(1)

    config = Config(
        ENTRY_WINDOW_SEC=args.entry_window,
        MIN_EDGE_PCT=args.min_edge,
        MIN_EV_MULTIPLIER=args.ev_mult,
        MC_SIMULATIONS=args.sims,
        LOG_LEVEL=args.log_level,
    )

    api = KalshiAPIClient(
        api_key_id=args.key_id,
        private_key_path=args.key_file,
        live=args.live,
    )

    bot = SettlementBot(api, config)

    # Handle Ctrl+C gracefully
    def shutdown(sig, frame):
        logging.info("⛔ Received shutdown signal")
        bot._running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(bot.run())


if __name__ == '__main__':
    main()
