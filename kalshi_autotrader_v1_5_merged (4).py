#!/usr/bin/env python3
"""
Kalshi AutoTrader v1.5 (MERGED) — Best of V1 + V2

PROVEN STRATEGIES FROM V1:
  1. NO Harvester   — Safe harvester (91-99¢) + Upset hunter (1-9¢)
  2. Structural Arb — STALE + FRAGILE book detection (0.8 confidence - WORKS!)
  3. Cross-Market   — Event arbitrage (mutually exclusive markets)

NEW STRATEGY FROM V2:
  4. Crypto Scalper — Orderbook imbalance on crypto binaries (25-65¢)

Why merge? V1's STRUCTURAL strategy found 11 signals in one scan with real
STALE detection. V2's Crypto Scalper adds fast-turnover crypto trades using
orderbook depth analysis. Best of both worlds!

Safety features (all V1 safeguards maintained):
  - Demo mode by default (no real money)
  - Position size limits (max $ and % of bankroll per trade)
  - Daily loss circuit breaker
  - Requires explicit --live flag for real trading
  - Full audit log of every decision and order
  - Stale order cancellation (30s TTL)

Requires: pip install cryptography requests
"""

import requests
import json
import math
import time
import uuid
import hashlib
import base64
import argparse
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

# RSA signing for Kalshi API auth
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradingConfig:
    """All tunable parameters in one place."""
    
    # ── Risk Management ──────────────────────────────────────────────────
    MAX_POSITION_CENTS: int = 300        # Max $3 per position
    MAX_POSITION_PCT: float = 0.02       # Max 2% of bankroll per trade
    MAX_DAILY_LOSS_CENTS: int = 1000     # $10 daily loss limit → halt
    MAX_OPEN_POSITIONS: int = 50         # Max concurrent positions
    MAX_OPEN_ORDERS: int = 50            # Max resting orders
    
    # ── Strategy 1: NO Harvester ─────────────────────────────────────────
    # Buy NO at 1-5¢ (sell YES at 95-99¢) on near-certain outcomes
    NO_HARVEST_MIN_PRICE: int = 91       # Min YES price (= max NO cost 9¢)
    NO_HARVEST_MAX_PRICE: int = 99       # Max YES price (= min NO cost 1¢)
    NO_HARVEST_MIN_VOLUME: int = 500     # Only harvest liquid markets
    NO_HARVEST_MIN_MINUTES: int = 60     # At least 1hr to close
    NO_HARVEST_MAX_CONTRACTS: int = 10   # Max contracts per harvest trade
    
    # ── Strategy 2: Structural Arb (from scanner) ────────────────────────
    STRUCT_MIN_ROI: float = 0.20         # 20% ROI minimum
    STRUCT_MIN_SPREAD: int = 10          # 10¢ spread minimum
    STRUCT_MIN_VOLUME: int = 100         # Must have some volume
    STRUCT_REQUIRE_STALE: bool = True    # Require STALE flag
    STRUCT_MAX_CONTRACTS: int = 5        # Conservative sizing
    
    # ── Strategy 3: Cross-Market Correlation ─────────────────────────────
    CROSS_MIN_EDGE_CENTS: int = 5        # Min edge after fees
    CROSS_MIN_VOLUME: int = 200          # Both markets need liquidity
    CROSS_MAX_CONTRACTS: int = 5         # Conservative sizing
    
    # ── Strategy 4: Crypto Scalper (NEW from V2) ────────────────────────
    CRYPTO_ENABLED: bool = True          # Toggle crypto scalper on/off
    CRYPTO_MIN_PRICE: int = 25           # Min ask price in cents
    CRYPTO_MAX_PRICE: int = 65           # Max ask price in cents
    CRYPTO_SIZE_DOLLARS: float = 4.0     # $ per crypto trade
    CRYPTO_TICKERS: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL", "CRYPTO"])
    CRYPTO_KEYWORDS: List[str] = field(default_factory=lambda: ["up", "down", "above", "below", "higher", "lower"])
    CRYPTO_MIN_VOLUME: int = 1           # Min contracts traded (1 for new markets)
    CRYPTO_MAX_SPREAD: int = 8           # Max bid-ask spread in cents
    CRYPTO_MIN_IMBALANCE: float = 0.05   # Min orderbook imbalance (5% - relaxed for low-volume markets)
    
    # ── Execution ────────────────────────────────────────────────────────
    USE_LIMIT_ORDERS: bool = True        # Limit orders (not market)
    LIMIT_OFFSET_CENTS: int = 1          # Place limit 1¢ better than book
    ORDER_TTL_SECONDS: int = 30          # Cancel unfilled orders after 30s
    SCAN_INTERVAL_SECONDS: int = 60      # Scan every 60 seconds
    API_RATE_DELAY: float = 0.15         # 150ms between API calls
    
    # ── Fee Model ────────────────────────────────────────────────────────
    FEE_RATE: float = 0.07               # Kalshi taker fee rate


# ═══════════════════════════════════════════════════════════════════════════════
# KALSHI API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class KalshiAPIClient:
    """
    Authenticated Kalshi API client with RSA-PSS signing.
    Supports both demo and production environments.
    """
    
    PROD_BASE = "https://api.elections.kalshi.com/trade-api/v2"
    DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"
    
    def __init__(self, api_key_id: str, private_key_path: str, live: bool = False):
        self.api_key_id = api_key_id
        self.base_url = self.PROD_BASE if live else self.DEMO_BASE
        self.live = live
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        
        # Load RSA private key
        self.private_key = self._load_key(private_key_path)
        
        self.request_count = 0
        self.last_request_time = 0.0
    
    def _load_key(self, path: str):
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
    
    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """RSA-PSS signature: sign(timestamp + method + path)"""
        message = (timestamp_ms + method + path).encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    
    def _headers(self, method: str, path: str) -> dict:
        """Generate auth headers for a request."""
        ts = str(int(time.time() * 1000))
        # Strip query params from path for signing
        sign_path = path.split('?')[0]
        sig = self._sign(ts, method, sign_path)
        return {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': ts,
        }
    
    def _rate_limit(self):
        """Basic rate limiting — 100ms minimum between requests."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _request(self, method: str, path: str, data: dict = None,
                 params: dict = None) -> dict:
        """Make an authenticated API request."""
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
            logging.error(f"API error {method} {path}: {e} — {e.response.text if e.response else ''}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error {method} {path}: {e}")
            raise
    
    # ── Public Data (no auth needed but we sign anyway) ──────────────────
    
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
    
    # ── Portfolio (auth required) ────────────────────────────────────────
    
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
        """
        Place an order.
        
        Args:
            ticker: Market ticker (e.g., 'KXPGATOUR-ATPBP26-SSCH')
            side: 'yes' or 'no'
            action: 'buy' or 'sell'
            count: Number of contracts
            order_type: 'limit' or 'market'
            yes_price: Limit price in cents for YES side (1-99)
            no_price: Limit price in cents for NO side (1-99)
        """
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
        """Submit up to 20 orders in a batch."""
        return self._request('POST', '/portfolio/orders/batched',
                             data={'orders': orders})


# ═══════════════════════════════════════════════════════════════════════════════
# FEE CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def kalshi_fee_cents(price_cents: int, contracts: int = 1,
                     fee_rate: float = 0.07) -> int:
    """Kalshi taker fee: ceil(fee_rate * C * P * (1-P)) in cents."""
    p = price_cents / 100.0
    fee_dollars = fee_rate * contracts * p * (1 - p)
    return math.ceil(fee_dollars * 100)


def net_profit_cents(buy_price_cents: int, contracts: int = 1,
                     fee_rate: float = 0.07) -> int:
    """Net profit in cents if the contract wins (pays 100¢)."""
    gross = (100 - buy_price_cents) * contracts
    fee = kalshi_fee_cents(buy_price_cents, contracts, fee_rate)
    return gross - fee


def roi(buy_price_cents: int, contracts: int = 1,
        fee_rate: float = 0.07) -> float:
    """Return on investment if contract wins."""
    cost = buy_price_cents * contracts
    if cost <= 0:
        return 0.0
    return net_profit_cents(buy_price_cents, contracts, fee_rate) / cost


# ═══════════════════════════════════════════════════════════════════════════════
# TRADE SIGNAL
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeSignal:
    """A potential trade identified by a strategy engine."""
    strategy: str           # 'NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET'
    ticker: str
    side: str               # 'yes' or 'no'
    action: str             # 'buy' or 'sell'
    price_cents: int        # Limit price
    contracts: int
    edge_cents: int         # Expected profit in cents
    edge_roi: float         # Expected ROI
    confidence: float       # 0.0 to 1.0
    reason: str             # Human-readable explanation
    market_data: dict = field(default_factory=dict)  # Raw data for audit
    timestamp: float = field(default_factory=time.time)


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: NO HARVESTER
# ═══════════════════════════════════════════════════════════════════════════════

class NoHarvester:
    """
    Strategy: Buy NO on near-certain outcomes for small guaranteed profits.
    
    When YES is trading at 95-99¢, NO costs 1-5¢.
    If the "obvious" outcome happens, NO pays 0¢ (lose 1-5¢).
    If the upset happens, NO pays 100¢ (gain 95-99¢).
    
    The REAL edge: On Kalshi, you can also SELL the NO position before 
    settlement if the price moves. But the core play is:
    
    Buy NO at 1-5¢ on events where the YES outcome is near-certain.
    You LOSE most of the time (small loss), but the rare wins are huge.
    
    HOWEVER — the swisstony approach is actually the OPPOSITE:
    Buy YES at 95-99¢ on near-certain outcomes. You win 1-5¢ per contract
    almost every time. It's like selling insurance.
    
    We implement BOTH sub-strategies:
    a) "Safe Harvester" — Buy YES at high prices (small, consistent wins)
    b) "Upset Hunter"  — Buy NO at low prices (rare, large wins)
    """
    
    def __init__(self, config: TradingConfig):
        self.config = config
    
    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        """Scan for NO harvest opportunities."""
        signals = []
        
        for market in markets:
            ticker = market.get('ticker', '')
            volume = market.get('volume', 0)
            
            # Volume filter
            if volume < self.config.NO_HARVEST_MIN_VOLUME:
                continue
            
            # Time filter
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
            
            # Get orderbook
            book = orderbooks.get(ticker)
            if not book:
                continue
            
            yes_bids = book.get('yes', [])
            no_bids = book.get('no', [])
            
            if not yes_bids or not no_bids:
                continue
            
            # Best YES bid (what someone will pay for YES)
            best_yes_bid_price = max(b[0] for b in yes_bids) if yes_bids else 0
            best_yes_bid_qty = sum(b[1] for b in yes_bids if b[0] == best_yes_bid_price)
            
            # Best NO bid (what someone will pay for NO)
            best_no_bid_price = max(b[0] for b in no_bids) if no_bids else 0
            
            # Cost to buy YES = 100 - best_NO_bid
            buy_yes_cost = 100 - best_no_bid_price if best_no_bid_price > 0 else None
            # Cost to buy NO = 100 - best_YES_bid
            buy_no_cost = 100 - best_yes_bid_price if best_yes_bid_price > 0 else None
            
            # ── Sub-strategy A: Safe Harvester ───────────────────────
            # Buy YES at 91-99¢ — wins small almost every time
            if buy_yes_cost and self.config.NO_HARVEST_MIN_PRICE <= buy_yes_cost <= self.config.NO_HARVEST_MAX_PRICE:
                profit = net_profit_cents(buy_yes_cost, 1, self.config.FEE_RATE)
                trade_roi = roi(buy_yes_cost, 1, self.config.FEE_RATE)
                
                if profit > 0:
                    # Size based on available liquidity and config
                    max_qty = min(
                        self.config.NO_HARVEST_MAX_CONTRACTS,
                        best_no_bid_price  # Available on NO side
                    )
                    if max_qty < 1:
                        continue
                    
                    signals.append(TradeSignal(
                        strategy='NO_HARVEST_SAFE',
                        ticker=ticker,
                        side='yes',
                        action='buy',
                        price_cents=buy_yes_cost,
                        contracts=max_qty,
                        edge_cents=profit * max_qty,
                        edge_roi=trade_roi,
                        confidence=0.90,  # High confidence (near-certain)
                        reason=f"Buy YES@{buy_yes_cost}¢ — "
                               f"win {profit}¢/contract if resolves YES "
                               f"(vol={volume:,}, {mins:.0f}min to close)",
                        market_data={
                            'volume': volume,
                            'best_yes_bid': best_yes_bid_price,
                            'best_no_bid': best_no_bid_price,
                            'buy_yes_cost': buy_yes_cost,
                            'minutes_to_close': mins,
                        }
                    ))
            
            # ── Sub-strategy B: Upset Hunter ─────────────────────────
            # Buy NO at 1-9¢ — rare wins but huge payoff
            if buy_no_cost and 1 <= buy_no_cost <= (100 - self.config.NO_HARVEST_MIN_PRICE):
                profit = net_profit_cents(buy_no_cost, 1, self.config.FEE_RATE)
                trade_roi = roi(buy_no_cost, 1, self.config.FEE_RATE)
                
                if profit > 0 and trade_roi > 5.0:  # Only if ROI > 500%
                    max_qty = min(
                        self.config.NO_HARVEST_MAX_CONTRACTS,
                        3  # Very conservative on upset bets
                    )
                    
                    signals.append(TradeSignal(
                        strategy='NO_HARVEST_UPSET',
                        ticker=ticker,
                        side='no',
                        action='buy',
                        price_cents=buy_no_cost,
                        contracts=max_qty,
                        edge_cents=profit * max_qty,
                        edge_roi=trade_roi,
                        confidence=0.10,  # Low confidence (upset)
                        reason=f"Buy NO@{buy_no_cost}¢ — "
                               f"win {profit}¢/contract if upset "
                               f"(vol={volume:,}, ROI={trade_roi:.0%})",
                        market_data={
                            'volume': volume,
                            'buy_no_cost': buy_no_cost,
                            'minutes_to_close': mins,
                        }
                    ))
        
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: STRUCTURAL ARB
# ═══════════════════════════════════════════════════════════════════════════════

class StructuralArb:
    """
    Strategy: Detect stale/fragile orderbooks and trade before MMs update.
    
    Uses the scanner's detection logic:
    - STALE: orderbook hasn't changed between scans (MM bot down?)
    - FRAGILE_BOOK: thin top-of-book with large gap to next level
    - WIDE_SPREAD: bid-ask spread > 10¢
    
    The combination of STALE + FRAGILE + VOLUME is the gold signal:
    someone's limit order is sitting there unchanged while the market
    consensus has moved.
    """
    
    def __init__(self, config: TradingConfig):
        self.config = config
        self.previous_books: Dict[str, Tuple[str, float]] = {}  # ticker -> (sig, time)
    
    def _book_signature(self, orderbook: dict) -> str:
        """Hash the top 5 levels of the orderbook."""
        yes_top5 = sorted(orderbook.get('yes', []),
                          key=lambda x: x[0], reverse=True)[:5]
        no_top5 = sorted(orderbook.get('no', []),
                         key=lambda x: x[0], reverse=True)[:5]
        raw = json.dumps({'y': yes_top5, 'n': no_top5}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    
    def _is_stale(self, ticker: str, book: dict) -> bool:
        """Check if orderbook is stale (unchanged for >60s)."""
        sig = self._book_signature(book)
        now = time.time()
        
        if ticker in self.previous_books:
            prev_sig, prev_time = self.previous_books[ticker]
            age = now - prev_time
            if age > 60 and sig == prev_sig:
                self.previous_books[ticker] = (sig, prev_time)  # Keep old timestamp
                return True
        
        self.previous_books[ticker] = (sig, now)
        return False
    
    def _is_fragile(self, levels: list, min_gap: int = 30,
                    max_qty: int = 500) -> bool:
        """Check if top-of-book is fragile (thin + gap)."""
        if len(levels) < 2:
            return False
        sorted_levels = sorted(levels, key=lambda x: x[0], reverse=True)
        best_price, best_qty = sorted_levels[0]
        second_price, _ = sorted_levels[1]
        gap = best_price - second_price
        return gap >= min_gap and best_qty <= max_qty
    
    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        """Scan for structural arbitrage opportunities."""
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
            
            # Check staleness
            is_stale = self._is_stale(ticker, book)
            
            # Check fragility
            yes_fragile = self._is_fragile(yes_bids)
            no_fragile = self._is_fragile(no_bids)
            
            # Calculate best trade
            best_yes_bid = max(b[0] for b in yes_bids)
            best_no_bid = max(b[0] for b in no_bids)
            
            buy_yes_cost = 100 - best_no_bid
            buy_no_cost = 100 - best_yes_bid
            spread = buy_yes_cost - best_yes_bid  # Should equal buy_no_cost - best_no_bid
            
            if spread < self.config.STRUCT_MIN_SPREAD:
                continue
            
            # Evaluate both sides
            for side, cost, fragile in [
                ('yes', buy_yes_cost, no_fragile),
                ('no', buy_no_cost, yes_fragile)
            ]:
                if cost <= 0 or cost >= 100:
                    continue
                
                profit = net_profit_cents(cost, 1, self.config.FEE_RATE)
                trade_roi = roi(cost, 1, self.config.FEE_RATE)
                
                if trade_roi < self.config.STRUCT_MIN_ROI:
                    continue
                
                # Score the signal
                score = 0
                flags = []
                
                if is_stale:
                    score += 3
                    flags.append('STALE')
                if fragile:
                    score += 2
                    flags.append('FRAGILE')
                if spread >= 20:
                    score += 1
                    flags.append(f'SPREAD={spread}¢')
                if volume >= 1000:
                    score += 1
                    flags.append(f'VOL={volume}')
                
                # Only signal if we have structural evidence
                if self.config.STRUCT_REQUIRE_STALE and not is_stale:
                    continue
                if score < 3:
                    continue
                
                contracts = min(
                    self.config.STRUCT_MAX_CONTRACTS,
                    5  # Conservative
                )
                
                signals.append(TradeSignal(
                    strategy='STRUCTURAL',
                    ticker=ticker,
                    side=side,
                    action='buy',
                    price_cents=cost,
                    contracts=contracts,
                    edge_cents=profit * contracts,
                    edge_roi=trade_roi,
                    confidence=min(0.3 + score * 0.1, 0.8),
                    reason=f"STRUCTURAL: Buy {side.upper()}@{cost}¢ "
                           f"[{','.join(flags)}] "
                           f"spread={spread}¢ ROI={trade_roi:.0%}",
                    market_data={
                        'volume': volume,
                        'spread': spread,
                        'is_stale': is_stale,
                        'fragile': fragile,
                        'score': score,
                        'flags': flags,
                    }
                ))
        
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: CROSS-MARKET CORRELATION
# ═══════════════════════════════════════════════════════════════════════════════

class CrossMarketArb:
    """
    Strategy: Find mispriced markets within the same event.
    
    Kalshi events contain multiple markets that are logically related.
    For mutually exclusive events (e.g., "Who will win X?"), the sum
    of all YES prices should equal ~100¢. If they don't, there's an edge.
    
    Example: "Who wins the Super Bowl?"
      - Team A YES: 45¢
      - Team B YES: 40¢  
      - Team C YES: 20¢
      - Sum: 105¢ (should be ~100¢)
      - Edge: Buy NO on overpriced side
    
    Also detects: If a market has moved but a correlated market hasn't
    updated yet (logic arbitrage).
    """
    
    def __init__(self, config: TradingConfig):
        self.config = config
    
    def scan(self, events: List[dict], orderbooks: dict) -> List[TradeSignal]:
        """Scan events for cross-market arbitrage."""
        signals = []
        
        for event in events:
            if not event.get('mutually_exclusive', False):
                continue
            
            markets = event.get('markets', [])
            if len(markets) < 2:
                continue
            
            # Get current prices for all markets in this event
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
                    'ticker': ticker,
                    'title': m.get('title', '')[:40],
                    'volume': m.get('volume', 0),
                    'best_yes_bid': best_yes_bid,
                    'best_no_bid': best_no_bid,
                    'buy_yes': buy_yes,
                    'buy_no': buy_no,
                    'mid_yes': (buy_yes + best_yes_bid) / 2 if buy_yes and best_yes_bid else None,
                })
            
            if not all_have_books or not market_prices:
                continue
            
            # Check if all markets have mid prices
            valid_prices = [mp for mp in market_prices if mp['mid_yes'] is not None]
            if len(valid_prices) < 2:
                continue
            
            # Sum of implied YES probabilities
            sum_yes = sum(mp['mid_yes'] for mp in valid_prices)
            
            # For mutually exclusive events, sum should be ~100
            # If sum > 100: market is overpriced overall → buy NO on most overpriced
            # If sum < 100: market is underpriced overall → buy YES on most underpriced
            
            overpricing = sum_yes - 100  # Positive = overpriced
            
            if abs(overpricing) < self.config.CROSS_MIN_EDGE_CENTS:
                continue
            
            # Find the most mispriced market
            if overpricing > 0:
                # Market is overpriced — find the most overpriced individual market
                # and buy NO on it (or sell YES)
                # Sort by highest mid_yes (most overpriced)
                valid_prices.sort(key=lambda x: x['mid_yes'], reverse=True)
                target = valid_prices[0]
                
                if target['buy_no'] is None:
                    continue
                
                # Edge = how much the sum exceeds 100, divided among markets
                # Conservative: only claim half the edge
                edge = max(1, int(overpricing / 2))
                
                if target['volume'] < self.config.CROSS_MIN_VOLUME:
                    continue
                
                profit = net_profit_cents(target['buy_no'], 1, self.config.FEE_RATE)
                if profit < self.config.CROSS_MIN_EDGE_CENTS:
                    continue
                
                trade_roi = roi(target['buy_no'], 1, self.config.FEE_RATE)
                
                signals.append(TradeSignal(
                    strategy='CROSS_MARKET',
                    ticker=target['ticker'],
                    side='no',
                    action='buy',
                    price_cents=target['buy_no'],
                    contracts=min(self.config.CROSS_MAX_CONTRACTS, 3),
                    edge_cents=edge,
                    edge_roi=trade_roi,
                    confidence=0.6,
                    reason=f"CROSS-MKT: Sum={sum_yes:.0f}¢ (should be ~100) "
                           f"→ Buy NO@{target['buy_no']}¢ on {target['ticker'][:25]} "
                           f"(overpriced by {overpricing:.0f}¢)",
                    market_data={
                        'event_ticker': event.get('event_ticker'),
                        'sum_yes': sum_yes,
                        'overpricing': overpricing,
                        'num_markets': len(valid_prices),
                        'target_volume': target['volume'],
                    }
                ))
            
            elif overpricing < 0:
                # Market is underpriced — buy YES on cheapest
                valid_prices.sort(key=lambda x: x['mid_yes'])
                target = valid_prices[0]
                
                if target['buy_yes'] is None:
                    continue
                
                edge = max(1, int(abs(overpricing) / 2))
                
                if target['volume'] < self.config.CROSS_MIN_VOLUME:
                    continue
                
                profit = net_profit_cents(target['buy_yes'], 1, self.config.FEE_RATE)
                if profit < self.config.CROSS_MIN_EDGE_CENTS:
                    continue
                
                trade_roi = roi(target['buy_yes'], 1, self.config.FEE_RATE)
                
                signals.append(TradeSignal(
                    strategy='CROSS_MARKET',
                    ticker=target['ticker'],
                    side='yes',
                    action='buy',
                    price_cents=target['buy_yes'],
                    contracts=min(self.config.CROSS_MAX_CONTRACTS, 3),
                    edge_cents=edge,
                    edge_roi=trade_roi,
                    confidence=0.6,
                    reason=f"CROSS-MKT: Sum={sum_yes:.0f}¢ (should be ~100) "
                           f"→ Buy YES@{target['buy_yes']}¢ on {target['ticker'][:25]} "
                           f"(underpriced by {abs(overpricing):.0f}¢)",
                    market_data={
                        'event_ticker': event.get('event_ticker'),
                        'sum_yes': sum_yes,
                        'overpricing': overpricing,
                        'num_markets': len(valid_prices),
                        'target_volume': target['volume'],
                    }
                ))
        
        return signals


# ── Strategy 4: Crypto Scalper (NEW from V2) ────────────────────────────────

class CryptoScalper:
    """
    NEW STRATEGY from V2: Orderbook imbalance on crypto binaries.
    
    Inspired by top Polymarket trader k9Q2mX4L8A7ZP3R who trades crypto
    up/down binaries at mid-range prices (25-65¢) based on orderbook depth.
    
    Signal: If bid depth > ask depth significantly → buying pressure → price likely to rise
    
    Example:
      - Market: "Will BTC close above $65,000?"
      - Current price: YES@45¢
      - YES orderbook: 5000 contracts bid, 1000 contracts offered
      - Imbalance: (5000-1000)/(5000+1000) = 0.67 = strong buy signal
      - Trade: Buy YES@45¢, target quick flip or settlement
    
    Complements existing strategies with fast-turnover crypto trades.
    """
    
    def __init__(self, config: TradingConfig):
        self.config = config
    
    def scan(self, markets: List[dict], orderbooks: dict) -> List[TradeSignal]:
        """Scan for crypto binary opportunities based on orderbook imbalance."""
        if not self.config.CRYPTO_ENABLED:
            return []
        
        signals = []
        crypto_kws = [k.upper() for k in self.config.CRYPTO_TICKERS]
        directional_kws = [k.lower() for k in self.config.CRYPTO_KEYWORDS]
        
        for market in markets:
            ticker = market.get('ticker', '')
            title = market.get('title', '').lower()
            volume = market.get('volume', 0) or 0
            
            # Must be crypto-related
            if not any(kw in ticker.upper() for kw in crypto_kws):
                continue
            
            # Must be directional (up/down/above/below)
            if not any(kw in title for kw in directional_kws):
                continue
            
            # Need minimum volume
            if volume < self.config.CRYPTO_MIN_VOLUME:
                continue
            
            book = orderbooks.get(ticker)
            if not book:
                continue
            
            # Analyze both YES and NO sides for imbalance
            for side in ['yes', 'no']:
                signal = self._analyze_side(book, side, ticker, title, volume)
                if signal:
                    signals.append(signal)
        
        # Sort by confidence (highest first)
        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals
    
    def _analyze_side(self, book: dict, side: str, ticker: str,
                      title: str, volume: int) -> Optional[TradeSignal]:
        """Analyze one side (yes or no) for orderbook imbalance signal.
        
        Kalshi orderbook: {"yes": [[price, qty], ...], "no": [[price, qty], ...]}
        All quantities are positive. Arrays represent bid depth for that side.
        """
        try:
            # Get YES and NO levels
            yes_levels = book.get("yes", []) or []
            no_levels = book.get("no", []) or []
            
            if not yes_levels or not no_levels:
                return None
            
            # Extract best bid and ask based on which side we're trading
            if side == "yes":
                # Best YES bid = highest price someone will pay for YES
                best_bid = max(lv[0] for lv in yes_levels) if yes_levels else 0
                # Best YES ask = what we pay to buy YES = 100 - best NO bid
                best_no_bid = max(lv[0] for lv in no_levels) if no_levels else 0
                best_ask = 100 - best_no_bid if best_no_bid > 0 else 100
            else:  # side == "no"
                # Best NO bid = highest price someone will pay for NO
                best_bid = max(lv[0] for lv in no_levels) if no_levels else 0
                # Best NO ask = what we pay to buy NO = 100 - best YES bid
                best_yes_bid = max(lv[0] for lv in yes_levels) if yes_levels else 0
                best_ask = 100 - best_yes_bid if best_yes_bid > 0 else 100
            
            # Check if price is in target range
            if best_ask < self.config.CRYPTO_MIN_PRICE or best_ask > self.config.CRYPTO_MAX_PRICE:
                return None
            
            # Check spread
            spread = best_ask - best_bid
            if spread > self.config.CRYPTO_MAX_SPREAD or spread <= 0:
                return None
            
            # Calculate orderbook depth imbalance
            # Sum all quantities (all are positive)
            yes_depth = sum(lv[1] for lv in yes_levels)
            no_depth = sum(lv[1] for lv in no_levels)
            
            if yes_depth + no_depth == 0:
                return None
            
            # Imbalance: if buying YES, want more YES depth vs NO depth
            if side == "yes":
                imbalance = (yes_depth - no_depth) / (yes_depth + no_depth)
            else:
                imbalance = (no_depth - yes_depth) / (yes_depth + no_depth)
            
            # Need positive imbalance (more demand on our side)
            if imbalance < self.config.CRYPTO_MIN_IMBALANCE:
                return None
            
            # Trade parameters
            buy_price = best_ask
            contracts = max(1, int(self.config.CRYPTO_SIZE_DOLLARS * 100 / buy_price))
            
            # Calculate profit
            profit = net_profit_cents(buy_price, contracts, self.config.FEE_RATE)
            trade_roi = roi(buy_price, contracts, self.config.FEE_RATE)
            
            # Confidence scoring
            # Components:
            # 1. Imbalance strength (0-0.4)
            # 2. Tight spread (0-0.15)  
            # 3. High volume (0-0.1)
            # Base: 0.35
            spread_score = max(0, 1 - spread / self.config.CRYPTO_MAX_SPREAD)
            vol_score = min(1.0, volume / 500.0)
            confidence = min(1.0, 0.35 + imbalance * 0.4 + spread_score * 0.15 + vol_score * 0.1)
            
            # Determine direction from title
            direction = "UP" if "above" in title or "up" in title or "higher" in title else "DOWN"
            
            return TradeSignal(
                strategy='CRYPTO_SCALP',
                ticker=ticker,
                side=side,
                action='buy',
                price_cents=buy_price,
                contracts=contracts,
                edge_cents=profit,
                edge_roi=trade_roi,
                confidence=confidence,
                reason=f"Crypto {direction} | imbal={imbalance:.2f} "
                       f"yes_depth={yes_depth} no_depth={no_depth} "
                       f"spread={spread}¢ vol={volume}",
                market_data={
                    'imbalance': imbalance,
                    'yes_depth': yes_depth,
                    'no_depth': no_depth,
                    'spread': spread,
                    'volume': volume,
                    'direction': direction,
                }
            )
        
        except Exception as e:
            logging.debug(f"Crypto scan error {ticker}: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# RISK MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class RiskManager:
    """
    Enforces position limits, daily loss limits, and circuit breakers.
    Every trade must pass through the risk manager before execution.
    """
    
    def __init__(self, config: TradingConfig):
        self.config = config
        self.daily_pnl_cents: int = 0
        self.open_position_count: int = 0
        self.open_order_count: int = 0
        self.positions: Dict[str, dict] = {}  # ticker -> position info
        self.trade_log: List[dict] = []
        self.halted: bool = False
        self.halt_reason: str = ''
        self.day_start = datetime.now(timezone.utc).date()
    
    def reset_daily(self):
        """Reset daily counters at start of new day."""
        today = datetime.now(timezone.utc).date()
        if today > self.day_start:
            logging.info(f"New day — resetting daily PnL (was {self.daily_pnl_cents}¢)")
            self.daily_pnl_cents = 0
            self.day_start = today
            if self.halted and 'daily_loss' in self.halt_reason:
                self.halted = False
                self.halt_reason = ''
                logging.info("Circuit breaker reset for new day")
    
    def update_positions(self, positions_response: dict):
        """Sync positions from API response."""
        self.positions = {}
        market_positions = positions_response.get('market_positions', [])
        for pos in market_positions:
            ticker = pos.get('ticker', '')
            if ticker:
                self.positions[ticker] = pos
        self.open_position_count = len(self.positions)
    
    def update_orders(self, orders_response: dict):
        """Sync open order count from API response."""
        orders = orders_response.get('orders', [])
        self.open_order_count = len([o for o in orders
                                     if o.get('status') == 'resting'])
    
    def record_fill(self, ticker: str, side: str, action: str,
                    price_cents: int, contracts: int, fee_cents: int):
        """Record a filled trade for PnL tracking."""
        # For buys: cost is negative (money out)
        # For settlements: profit is positive (money in)
        if action == 'buy':
            cost = price_cents * contracts + fee_cents
            self.daily_pnl_cents -= cost
        elif action == 'sell':
            revenue = price_cents * contracts - fee_cents
            self.daily_pnl_cents += revenue
        
        self.trade_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'ticker': ticker,
            'side': side,
            'action': action,
            'price_cents': price_cents,
            'contracts': contracts,
            'fee_cents': fee_cents,
            'daily_pnl_after': self.daily_pnl_cents,
        })
        
        # Check circuit breaker
        if self.daily_pnl_cents <= -self.config.MAX_DAILY_LOSS_CENTS:
            self.halted = True
            self.halt_reason = f'daily_loss: {self.daily_pnl_cents}¢ <= -{self.config.MAX_DAILY_LOSS_CENTS}¢'
            logging.critical(f"🛑 CIRCUIT BREAKER: {self.halt_reason}")
    
    def check_trade(self, signal: TradeSignal, balance_cents: int) -> Tuple[bool, str]:
        """
        Validate a trade signal against all risk rules.
        Returns (approved, reason).
        """
        self.reset_daily()
        
        # Circuit breaker
        if self.halted:
            return False, f"HALTED: {self.halt_reason}"
        
        # Position limit
        if self.open_position_count >= self.config.MAX_OPEN_POSITIONS:
            return False, f"Max positions reached ({self.open_position_count})"
        
        # Order limit
        if self.open_order_count >= self.config.MAX_OPEN_ORDERS:
            return False, f"Max open orders reached ({self.open_order_count})"
        
        # Already in this market?
        if signal.ticker in self.positions:
            return False, f"Already have position in {signal.ticker}"
        
        # Position size — absolute limit
        trade_cost = signal.price_cents * signal.contracts
        if trade_cost > self.config.MAX_POSITION_CENTS:
            return False, (f"Trade cost {trade_cost}¢ > max {self.config.MAX_POSITION_CENTS}¢")
        
        # Position size — percentage of bankroll
        max_by_pct = int(balance_cents * self.config.MAX_POSITION_PCT)
        if trade_cost > max_by_pct:
            return False, (f"Trade cost {trade_cost}¢ > {self.config.MAX_POSITION_PCT:.0%} "
                          f"of bankroll ({max_by_pct}¢)")
        
        # Sufficient balance
        if trade_cost > balance_cents:
            return False, f"Insufficient balance: need {trade_cost}¢, have {balance_cents}¢"
        
        # Daily loss proximity check — don't trade if close to limit
        remaining_loss_budget = self.config.MAX_DAILY_LOSS_CENTS + self.daily_pnl_cents
        if trade_cost > remaining_loss_budget:
            return False, (f"Trade cost {trade_cost}¢ exceeds remaining daily loss "
                          f"budget {remaining_loss_budget}¢")
        
        # Price sanity check
        if signal.price_cents < 1 or signal.price_cents > 99:
            return False, f"Price {signal.price_cents}¢ out of range (1-99)"
        
        return True, "APPROVED"
    
    def get_status(self) -> dict:
        """Return current risk status."""
        return {
            'halted': self.halted,
            'halt_reason': self.halt_reason,
            'daily_pnl_cents': self.daily_pnl_cents,
            'open_positions': self.open_position_count,
            'open_orders': self.open_order_count,
            'trades_today': len(self.trade_log),
            'remaining_loss_budget': self.config.MAX_DAILY_LOSS_CENTS + self.daily_pnl_cents,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT LOGGER
# ═══════════════════════════════════════════════════════════════════════════════

class AuditLogger:
    """
    Logs every decision and trade to both console and file.
    Creates a JSON audit trail for post-analysis.
    """
    
    def __init__(self, log_dir: str = 'logs'):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_file = self.log_dir / f'session_{timestamp}.jsonl'
        self.trade_file = self.log_dir / f'trades_{timestamp}.jsonl'
        
        # Console logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger('autotrader')
    
    def _append_jsonl(self, filepath: Path, record: dict):
        """Append a JSON record to a JSONL file."""
        with open(filepath, 'a') as f:
            f.write(json.dumps(record, default=str) + '\n')
    
    def log_scan(self, strategy: str, markets_scanned: int,
                 signals_found: int, duration_ms: float):
        record = {
            'type': 'scan',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': strategy,
            'markets_scanned': markets_scanned,
            'signals_found': signals_found,
            'duration_ms': round(duration_ms, 1),
        }
        self._append_jsonl(self.session_file, record)
        self.logger.info(f"📊 {strategy}: scanned {markets_scanned} → "
                        f"{signals_found} signals ({duration_ms:.0f}ms)")
    
    def log_signal(self, signal: TradeSignal, approved: bool, reason: str):
        record = {
            'type': 'signal',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': signal.strategy,
            'ticker': signal.ticker,
            'side': signal.side,
            'price_cents': signal.price_cents,
            'contracts': signal.contracts,
            'edge_cents': signal.edge_cents,
            'edge_roi': signal.edge_roi,
            'confidence': signal.confidence,
            'approved': approved,
            'reason': reason,
            'signal_reason': signal.reason,
        }
        self._append_jsonl(self.session_file, record)
        
        icon = "✅" if approved else "❌"
        self.logger.info(f"{icon} {signal.strategy} {signal.ticker} "
                        f"{signal.side.upper()}@{signal.price_cents}¢ x{signal.contracts} "
                        f"edge={signal.edge_cents}¢ — {reason}")
    
    def log_order(self, signal: TradeSignal, order_response: dict,
                  status: str):
        record = {
            'type': 'order',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'strategy': signal.strategy,
            'ticker': signal.ticker,
            'side': signal.side,
            'action': signal.action,
            'price_cents': signal.price_cents,
            'contracts': signal.contracts,
            'order_id': order_response.get('order', {}).get('order_id', ''),
            'status': status,
        }
        self._append_jsonl(self.trade_file, record)
        self.logger.info(f"📝 ORDER {status}: {signal.ticker} "
                        f"{signal.action.upper()} {signal.side.upper()} "
                        f"@{signal.price_cents}¢ x{signal.contracts}")
    
    def log_error(self, context: str, error: Exception):
        record = {
            'type': 'error',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'context': context,
            'error': str(error),
            'error_type': type(error).__name__,
        }
        self._append_jsonl(self.session_file, record)
        self.logger.error(f"💥 {context}: {error}")
    
    def log_status(self, risk_status: dict, balance_cents: int):
        record = {
            'type': 'status',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'balance_cents': balance_cents,
            **risk_status,
        }
        self._append_jsonl(self.session_file, record)


# ═══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class OrderManager:
    """
    Manages order lifecycle: place, track, cancel stale orders.
    """
    
    def __init__(self, api: KalshiAPIClient, config: TradingConfig):
        self.api = api
        self.config = config
        # Track orders we've placed: order_id -> {signal, placed_at, status}
        self.active_orders: Dict[str, dict] = {}
    
    def place_order(self, signal: TradeSignal) -> Optional[dict]:
        """
        Place an order based on a trade signal.
        Returns the API response or None on failure.
        """
        try:
            # Determine limit price
            if self.config.USE_LIMIT_ORDERS:
                order_type = 'limit'
                # Place limit at the current price (no improvement needed
                # since we already validated the edge)
                if signal.side == 'yes':
                    yes_price = signal.price_cents
                    no_price = None
                else:
                    yes_price = None
                    no_price = signal.price_cents
            else:
                order_type = 'market'
                yes_price = None
                no_price = None
            
            response = self.api.create_order(
                ticker=signal.ticker,
                side=signal.side,
                action=signal.action,
                count=signal.contracts,
                order_type=order_type,
                yes_price=yes_price,
                no_price=no_price,
            )
            
            order = response.get('order', {})
            order_id = order.get('order_id', '')
            
            if order_id:
                self.active_orders[order_id] = {
                    'signal': signal,
                    'placed_at': time.time(),
                    'status': order.get('status', 'unknown'),
                    'ticker': signal.ticker,
                }
            
            return response
            
        except Exception as e:
            logging.error(f"Order placement failed for {signal.ticker}: {e}")
            return None
    
    def cancel_stale_orders(self) -> int:
        """Cancel orders that have been resting too long."""
        cancelled = 0
        now = time.time()
        stale_ids = []
        
        for order_id, info in self.active_orders.items():
            age = now - info['placed_at']
            if age > self.config.ORDER_TTL_SECONDS and info['status'] == 'resting':
                try:
                    self.api.cancel_order(order_id)
                    stale_ids.append(order_id)
                    cancelled += 1
                    logging.info(f"🗑️  Cancelled stale order {order_id[:12]}... "
                               f"({info['ticker']}, age={age:.0f}s)")
                except Exception as e:
                    logging.warning(f"Failed to cancel {order_id[:12]}...: {e}")
        
        for oid in stale_ids:
            del self.active_orders[oid]
        
        return cancelled
    
    def sync_orders(self):
        """Sync order statuses from API."""
        try:
            response = self.api.get_orders(status='resting')
            api_orders = {o['order_id']: o
                         for o in response.get('orders', [])}
            
            # Update tracked orders
            filled_ids = []
            for order_id in list(self.active_orders.keys()):
                if order_id in api_orders:
                    self.active_orders[order_id]['status'] = \
                        api_orders[order_id].get('status', 'unknown')
                else:
                    # Order no longer resting — filled, cancelled, or expired
                    filled_ids.append(order_id)
            
            for oid in filled_ids:
                info = self.active_orders.pop(oid, None)
                if info:
                    logging.info(f"📦 Order {oid[:12]}... no longer resting "
                               f"({info['ticker']})")
        
        except Exception as e:
            logging.warning(f"Order sync failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# AUTOTRADER ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class AutoTrader:
    """
    Main orchestrator: runs scan loops, evaluates signals, executes trades.
    """
    
    def __init__(self, api: KalshiAPIClient, config: TradingConfig,
                 strategies: List[str] = None):
        self.api = api
        self.config = config
        self.risk = RiskManager(config)
        self.orders = OrderManager(api, config)
        self.audit = AuditLogger()
        
        # Initialize strategy engines
        self.strategies = {}
        enabled = strategies or ['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET', 'CRYPTO_SCALP']
        
        if 'NO_HARVEST' in enabled:
            self.strategies['NO_HARVEST'] = NoHarvester(config)
        if 'STRUCTURAL' in enabled:
            self.strategies['STRUCTURAL'] = StructuralArb(config)
        if 'CROSS_MARKET' in enabled:
            self.strategies['CROSS_MARKET'] = CrossMarketArb(config)
        if 'CRYPTO_SCALP' in enabled:
            self.strategies['CRYPTO_SCALP'] = CryptoScalper(config)
        
        self.running = False
        self.scan_count = 0
        self.total_signals = 0
        self.total_trades = 0
        self.balance_cents = 0
    
    def _fetch_balance(self) -> int:
        """Get current balance in cents."""
        try:
            resp = self.api.get_balance()
            # Kalshi returns balance in cents
            self.balance_cents = resp.get('balance', 0)
            return self.balance_cents
        except Exception as e:
            self.audit.log_error('fetch_balance', e)
            return self.balance_cents  # Return last known
    
    def _fetch_markets(self, max_markets: int = 2000) -> List[dict]:
        """Fetch all open single-event markets."""
        all_markets = []
        cursor = None
        
        while len(all_markets) < max_markets:
            try:
                resp = self.api.get_markets(
                    limit=min(1000, max_markets - len(all_markets)),
                    cursor=cursor
                )
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
        """Fetch crypto markets by series ticker - OPTIMIZED for 15-min markets only."""
        # SPEED: Only fetch 15M series (not daily/hourly)
        crypto_series = ['KXBTC15M', 'KXETH15M', 'KXSOL15M']
        all_crypto = []
        
        import re
        from datetime import datetime, timedelta
        
        for series in crypto_series:
            try:
                resp = self.api.get_markets(series_ticker=series, limit=200)
                markets = resp.get('markets', [])
                
                # FILTER: Only keep 15M markets expiring in next 90 minutes
                filtered = []
                for m in markets:
                    ticker = m.get('ticker', '')
                    # Extract time from ticker (e.g., KXBTC15M-26FEB101945 -> hour=19, min=45)
                    match = re.search(r'(\d{2})(\d{2})(?:-|$)', ticker)
                    if match:
                        try:
                            hour = int(match.group(1))
                            minute = int(match.group(2))
                            now = datetime.now()
                            expiry = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                            
                            # Handle next-day expiry
                            if expiry < now:
                                expiry += timedelta(days=1)
                            
                            mins_away = (expiry - now).total_seconds() / 60
                            
                            # Keep markets expiring in next 90 minutes
                            if 0 <= mins_away <= 90:
                                filtered.append(m)
                        except (ValueError, AttributeError):
                            # Skip if time parsing fails
                            continue
                
                all_crypto.extend(filtered)
                time.sleep(self.config.API_RATE_DELAY)
                
            except Exception as e:
                logging.debug(f"No markets for series {series}: {e}")
                continue
        
        logging.info(f"📊 Fetched {len(all_crypto)} crypto markets from {len(crypto_series)} series")
        return all_crypto
    
    def _fetch_orderbooks(self, tickers: List[str]) -> dict:
        """Fetch orderbooks for a list of tickers."""
        books = {}
        for ticker in tickers:
            try:
                resp = self.api.get_orderbook(ticker)
                book = resp.get('orderbook', {})
                if book:
                    books[ticker] = book
                time.sleep(self.config.API_RATE_DELAY)
            except Exception:
                continue  # Skip failed fetches
        return books
    
    def _fetch_events(self) -> List[dict]:
        """Fetch all open events with their markets."""
        all_events = []
        cursor = None
        
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
    
    def _prioritize_signals(self, signals: List[TradeSignal]) -> List[TradeSignal]:
        """
        Rank signals by expected value and confidence.
        Score = edge_cents * confidence * roi_factor
        """
        for s in signals:
            roi_factor = min(s.edge_roi, 5.0) / 5.0  # Cap at 5x
            s._score = s.edge_cents * s.confidence * (1 + roi_factor)
        
        signals.sort(key=lambda s: s._score, reverse=True)
        return signals
    
    def run_scan_cycle(self) -> int:
        """
        Run one complete scan cycle across all strategies.
        Returns number of trades executed.
        """
        self.scan_count += 1
        cycle_trades = 0
        
        logging.info(f"\n{'━' * 80}")
        logging.info(f"🔄 SCAN CYCLE #{self.scan_count} — "
                    f"{datetime.now().strftime('%H:%M:%S')}")
        logging.info(f"{'━' * 80}")
        
        # Sync state
        balance = self._fetch_balance()
        
        try:
            pos_resp = self.api.get_positions()
            self.risk.update_positions(pos_resp)
        except Exception as e:
            self.audit.log_error('sync_positions', e)
        
        try:
            ord_resp = self.api.get_orders(status='resting')
            self.risk.update_orders(ord_resp)
        except Exception as e:
            self.audit.log_error('sync_orders', e)
        
        # Log status
        status = self.risk.get_status()
        self.audit.log_status(status, balance)
        logging.info(f"💰 Balance: ${balance/100:.2f} | "
                    f"PnL today: {status['daily_pnl_cents']}¢ | "
                    f"Positions: {status['open_positions']} | "
                    f"Orders: {status['open_orders']}")
        
        if status['halted']:
            logging.warning(f"🛑 HALTED: {status['halt_reason']}")
            return 0
        
        # Cancel stale orders
        cancelled = self.orders.cancel_stale_orders()
        if cancelled:
            logging.info(f"🗑️  Cancelled {cancelled} stale orders")
        
        # Fetch market data
        markets = self._fetch_markets(max_markets=2000)
        if not markets:
            logging.warning("No markets fetched")
            return 0
        
        logging.info(f"📊 Fetched {len(markets)} markets")
        
        # Fetch crypto markets separately (they're not in first 2000)
        if 'CRYPTO_SCALP' in self.strategies:
            crypto_markets = self._fetch_crypto_markets()
            if crypto_markets:
                # Add crypto markets that aren't already in the list
                existing_tickers = {m['ticker'] for m in markets}
                new_crypto = [m for m in crypto_markets if m['ticker'] not in existing_tickers]
                markets.extend(new_crypto)
                logging.info(f"📊 Added {len(new_crypto)} crypto markets (total: {len(markets)})")
        
        # Collect all signals
        all_signals = []
        orderbooks = {}  # Initialize empty, will be populated by strategies that need it
        
        # ── Strategy 1 & 2: Need orderbooks ──────────────────────────
        if 'NO_HARVEST' in self.strategies or 'STRUCTURAL' in self.strategies or 'CRYPTO_SCALP' in self.strategies:
            # Pre-filter markets that might have opportunities
            # to avoid fetching all orderbooks (expensive)
            candidate_tickers = []
            crypto_tickers_force_fetch = []  # Force-fetch crypto regardless of volume
            
            for m in markets:
                vol = m.get('volume', 0)
                ticker = m.get('ticker', '')
                
                # Force-fetch ALL crypto markets (they're new, won't have volume yet)
                if 'CRYPTO_SCALP' in self.strategies:
                    ticker_upper = ticker.upper()
                    # Crypto 15-min and daily markets: KXBTC15M, KXBTCD, KXETH15M, KXSOL15M, etc.
                    crypto_patterns = ['KXBTC15M', 'KXBTCD', 'KXETH15M', 'KXETHD', 'KXSOL15M', 'KXSOLD']
                    if any(pattern in ticker_upper for pattern in crypto_patterns):
                        crypto_tickers_force_fetch.append(ticker)
                        continue  # Skip volume check for crypto
                
                # Only fetch books for non-crypto markets with some activity
                if vol >= 50:
                    candidate_tickers.append(ticker)
            
            # Combine: crypto tickers (forced) + high-volume tickers
            all_tickers = list(set(crypto_tickers_force_fetch + candidate_tickers))
            
            # Limit to avoid rate limit issues
            all_tickers = all_tickers[:500]
            
            logging.info(f"📖 Fetching {len(all_tickers)} orderbooks ({len(crypto_tickers_force_fetch)} crypto forced)...")
            t0 = time.time()
            orderbooks = self._fetch_orderbooks(all_tickers)
            fetch_ms = (time.time() - t0) * 1000
            logging.info(f"📖 Got {len(orderbooks)} orderbooks ({fetch_ms:.0f}ms)")
            
            # Run NO Harvester
            if 'NO_HARVEST' in self.strategies:
                t0 = time.time()
                harvest_signals = self.strategies['NO_HARVEST'].scan(
                    markets, orderbooks)
                ms = (time.time() - t0) * 1000
                self.audit.log_scan('NO_HARVEST', len(markets),
                                   len(harvest_signals), ms)
                all_signals.extend(harvest_signals)
            
            # Run Structural Arb
            if 'STRUCTURAL' in self.strategies:
                t0 = time.time()
                struct_signals = self.strategies['STRUCTURAL'].scan(
                    markets, orderbooks)
                ms = (time.time() - t0) * 1000
                self.audit.log_scan('STRUCTURAL', len(markets),
                                   len(struct_signals), ms)
                all_signals.extend(struct_signals)
            
            # Run Crypto Scalper (NEW from V2)
            if 'CRYPTO_SCALP' in self.strategies:
                t0 = time.time()
                crypto_signals = self.strategies['CRYPTO_SCALP'].scan(
                    markets, orderbooks)
                ms = (time.time() - t0) * 1000
                self.audit.log_scan('CRYPTO_SCALP', len(markets),
                                   len(crypto_signals), ms)
                all_signals.extend(crypto_signals)
        
        # ── Strategy 3: Cross-market needs events ────────────────────
        if 'CROSS_MARKET' in self.strategies:
            t0 = time.time()
            events = self._fetch_events()
            
            # Need orderbooks for event markets
            event_tickers = []
            for evt in events:
                for m in evt.get('markets', []):
                    t_str = m.get('ticker', '')
                    if t_str:
                        event_tickers.append(t_str)
            
            # Fetch any books we don't already have
            missing = [t for t in event_tickers if t not in orderbooks]
            if missing:
                missing = missing[:200]  # Limit
                event_books = self._fetch_orderbooks(missing)
                orderbooks.update(event_books)
            
            cross_signals = self.strategies['CROSS_MARKET'].scan(
                events, orderbooks)
            ms = (time.time() - t0) * 1000
            self.audit.log_scan('CROSS_MARKET', len(events),
                               len(cross_signals), ms)
            all_signals.extend(cross_signals)
        
        self.total_signals += len(all_signals)
        
        if not all_signals:
            logging.info("😴 No signals this cycle")
            return 0
        
        # Prioritize and execute
        ranked = self._prioritize_signals(all_signals)
        logging.info(f"🎯 {len(ranked)} signals — top 5:")
        for s in ranked[:5]:
            logging.info(f"   {s.strategy} {s.ticker[:30]} "
                        f"{s.side.upper()}@{s.price_cents}¢ "
                        f"edge={s.edge_cents}¢ conf={s.confidence:.0%}")
        
        # Execute top signals (respect limits)
        max_trades_per_cycle = 5  # Don't go crazy
        
        for signal in ranked[:max_trades_per_cycle]:
            # Risk check
            approved, reason = self.risk.check_trade(signal, balance)
            self.audit.log_signal(signal, approved, reason)
            
            if not approved:
                continue
            
            # Execute
            response = self.orders.place_order(signal)
            if response:
                order_status = response.get('order', {}).get('status', 'unknown')
                self.audit.log_order(signal, response, order_status)
                
                # If immediately filled, record it
                if order_status == 'executed':
                    fee = kalshi_fee_cents(signal.price_cents,
                                          signal.contracts,
                                          self.config.FEE_RATE)
                    self.risk.record_fill(
                        signal.ticker, signal.side, signal.action,
                        signal.price_cents, signal.contracts, fee
                    )
                
                cycle_trades += 1
                self.total_trades += 1
                
                # Refresh balance after trade
                balance = self._fetch_balance()
            else:
                self.audit.log_error(f'order_failed_{signal.ticker}',
                                    Exception('No response from API'))
        
        return cycle_trades
    
    def run(self, max_cycles: int = 0):
        """
        Main loop. Runs scan cycles until stopped.
        max_cycles=0 means run forever.
        """
        self.running = True
        mode = "🔴 LIVE" if self.api.live else "🟢 DEMO"
        
        logging.info(f"\n{'#' * 80}")
        logging.info(f"# KALSHI AUTOTRADER v1.5 (MERGED) — {mode}")
        logging.info(f"# Strategies: {', '.join(self.strategies.keys())}")
        logging.info(f"# Scan interval: {self.config.SCAN_INTERVAL_SECONDS}s")
        logging.info(f"# Max position: ${self.config.MAX_POSITION_CENTS/100:.2f} "
                    f"({self.config.MAX_POSITION_PCT:.0%} of bankroll)")
        logging.info(f"# Daily loss limit: ${self.config.MAX_DAILY_LOSS_CENTS/100:.2f}")
        logging.info(f"# Base URL: {self.api.base_url}")
        logging.info(f"{'#' * 80}\n")
        
        # Verify connection
        try:
            status = self.api.get_exchange_status()
            if not status.get('trading_active'):
                logging.warning("⚠️  Exchange trading is NOT active")
            else:
                logging.info("✅ Exchange is active and trading")
            
            balance = self._fetch_balance()
            logging.info(f"💰 Starting balance: ${balance/100:.2f}")
            
        except Exception as e:
            logging.critical(f"❌ Cannot connect to Kalshi API: {e}")
            logging.critical("Check your API key and private key path.")
            return
        
        cycle = 0
        while self.running:
            try:
                cycle += 1
                if max_cycles > 0 and cycle > max_cycles:
                    logging.info(f"Completed {max_cycles} cycles. Stopping.")
                    break
                
                trades = self.run_scan_cycle()
                
                logging.info(f"📈 Cycle #{self.scan_count}: "
                            f"{trades} trades | "
                            f"Total: {self.total_trades} trades, "
                            f"{self.total_signals} signals | "
                            f"Balance: ${self.balance_cents/100:.2f}")
                
                # Wait for next cycle
                if self.running:
                    logging.info(f"⏳ Next scan in {self.config.SCAN_INTERVAL_SECONDS}s "
                               f"(Ctrl+C to stop)\n")
                    time.sleep(self.config.SCAN_INTERVAL_SECONDS)
                
            except KeyboardInterrupt:
                logging.info("\n🛑 Stopped by user")
                self.running = False
                break
            except Exception as e:
                self.audit.log_error('scan_cycle', e)
                logging.error(f"Scan cycle error: {e}")
                # Wait and retry
                time.sleep(10)
        
        # Final summary
        self._print_summary()
    
    def _print_summary(self):
        """Print session summary."""
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
        logging.info(f"  Halted:         {status['halted']}")
        logging.info(f"  Audit logs:     {self.audit.session_file}")
        logging.info(f"  Trade logs:     {self.audit.trade_file}")
        logging.info(f"{'═' * 80}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Kalshi AutoTrader v1.5 (MERGED) — Best of V1 + V2 Strategies',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategies:
  NO_HARVEST   — Safe harvester + upset hunter (V1 proven)
  STRUCTURAL   — STALE/FRAGILE book detection (V1 proven - 11 signals found!)
  CROSS_MARKET — Event arbitrage (V1 proven)
  CRYPTO_SCALP — Orderbook imbalance on crypto (V2 new - fast turnover)

Examples:
  # Demo mode (safe, no real money):
  python kalshi_autotrader_v1_5_merged.py --api-key YOUR_KEY --key-file kalshi.pem

  # Single scan cycle test:
  python kalshi_autotrader_v1_5_merged.py --api-key YOUR_KEY --key-file kalshi.pem --cycles 1

  # Only structural arb + crypto scalper:
  python kalshi_autotrader_v1_5_merged.py --api-key YOUR_KEY --key-file kalshi.pem \\
      --strategy STRUCTURAL CRYPTO_SCALP

  # LIVE trading (real money!):
  python kalshi_autotrader_v1_5_merged.py --api-key YOUR_KEY --key-file kalshi.pem --live

  # Custom risk limits:
  python kalshi_autotrader_v1_5_merged.py --api-key YOUR_KEY --key-file kalshi.pem \\
      --max-position 500 --daily-loss-limit 2000
        """)
    
    # Required
    parser.add_argument('--api-key', required=True,
                        help='Kalshi API key ID')
    parser.add_argument('--key-file', required=True,
                        help='Path to RSA private key PEM file')
    
    # Mode
    parser.add_argument('--live', action='store_true',
                        help='⚠️  LIVE TRADING with real money (default: demo)')
    parser.add_argument('--cycles', type=int, default=0,
                        help='Number of scan cycles (0=run forever)')
    
    # Strategy selection
    parser.add_argument('--strategy', nargs='+',
                        choices=['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET', 'CRYPTO_SCALP'],
                        default=['NO_HARVEST', 'STRUCTURAL', 'CROSS_MARKET', 'CRYPTO_SCALP'],
                        help='Strategies to run (default: all)')
    
    # Risk parameters
    parser.add_argument('--max-position', type=int, default=300,
                        help='Max position size in cents (default: 300 = $3)')
    parser.add_argument('--max-position-pct', type=float, default=0.02,
                        help='Max position as %% of bankroll (default: 0.02 = 2%%)')
    parser.add_argument('--daily-loss-limit', type=int, default=1000,
                        help='Daily loss limit in cents (default: 1000 = $10)')
    parser.add_argument('--max-positions', type=int, default=20,
                        help='Max concurrent positions (default: 20)')
    
    # Execution parameters
    parser.add_argument('--scan-interval', type=int, default=60,
                        help='Seconds between scan cycles (default: 60)')
    parser.add_argument('--market-orders', action='store_true',
                        help='Use market orders instead of limit orders')
    parser.add_argument('--order-ttl', type=int, default=30,
                        help='Cancel unfilled limit orders after N seconds (default: 30)')
    
    args = parser.parse_args()
    
    # ── Build config ─────────────────────────────────────────────────
    config = TradingConfig(
        MAX_POSITION_CENTS=args.max_position,
        MAX_POSITION_PCT=args.max_position_pct,
        MAX_DAILY_LOSS_CENTS=args.daily_loss_limit,
        MAX_OPEN_POSITIONS=args.max_positions,
        USE_LIMIT_ORDERS=not args.market_orders,
        ORDER_TTL_SECONDS=args.order_ttl,
        SCAN_INTERVAL_SECONDS=args.scan_interval,
    )
    
    # ── Safety check for live mode ───────────────────────────────────
    if args.live:
        print("\n" + "⚠️ " * 30)
        print("  YOU ARE ABOUT TO START LIVE TRADING WITH REAL MONEY")
        print(f"  Max position: ${config.MAX_POSITION_CENTS/100:.2f}")
        print(f"  Daily loss limit: ${config.MAX_DAILY_LOSS_CENTS/100:.2f}")
        print(f"  Strategies: {', '.join(args.strategy)}")
        print("⚠️ " * 30)
        confirm = input("\nType 'YES I UNDERSTAND' to proceed: ")
        if confirm.strip() != 'YES I UNDERSTAND':
            print("Aborted.")
            return
    
    # ── Validate key file exists ─────────────────────────────────────
    if not os.path.exists(args.key_file):
        print(f"❌ Key file not found: {args.key_file}")
        print("Generate an API key at https://kalshi.com/account/profile")
        return
    
    # ── Initialize API client ────────────────────────────────────────
    try:
        api = KalshiAPIClient(
            api_key_id=args.api_key,
            private_key_path=args.key_file,
            live=args.live,
        )
    except Exception as e:
        print(f"❌ Failed to initialize API client: {e}")
        return
    
    # ── Initialize and run trader ────────────────────────────────────
    trader = AutoTrader(
        api=api,
        config=config,
        strategies=args.strategy,
    )
    
    trader.run(max_cycles=args.cycles)


if __name__ == "__main__":
    main()
