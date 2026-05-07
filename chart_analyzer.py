"""
chart_analyzer.py
=================

Parses TradingView MCP server output (data_get_pine_boxes / _lines / _labels /
data_get_ohlcv) and scores the current setup against the proven Databento
findings.

Drop-in usage in a next Claude Code session that has the TradingView MCP wired:

    from chart_analyzer import ChartReader, analyze_setup, format_report

    # raw outputs from MCP tools
    boxes  = data_get_pine_boxes()
    lines  = data_get_pine_lines()
    labels = data_get_pine_labels()
    bars_5m = data_get_ohlcv(symbol="MNQ1!", timeframe="5m", bars=2000)
    bars_1h = data_get_ohlcv(symbol="MNQ1!", timeframe="1h", bars=400)

    reader = ChartReader(boxes, lines, labels, bars_5m, bars_1h)
    report = analyze_setup(reader)
    print(format_report(report))

Schema-tolerant: the MCP server's exact JSON shape isn't pinned in the README,
so every parser uses a fallback chain on common field names.

Proven findings encoded (from prior research on 1 yr MNQ Databento):
  • 9:45 wick first-touch reverses 84-96%
  • 9:45 HIGH approached from above reverses 96.4% (n=110)
  • 9:45 body direction is CONTRARIAN — it is the manipulation candle
  • 9AM 1H bias wins 59.8% when it DISAGREES with 9:45
  • 63% of first moves go to a wick (L0 or L100)
  • Chain rate across 9:45 ranges: 99.6%
  • Median expansion 54 min after 09:50
  • 10:00 hour absorbs 38% of all level touches
  • 13:00 touches on 9AM zones reverse 90%
  • FVG broken then 9AM zone reached: 72.5% bounce
  • First touch strongest (73.2%); degrades on retest
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Constants — encoded findings
# ---------------------------------------------------------------------------

FINDINGS = {
    "wick_first_touch_low":   0.84,   # range 84–96% — pessimistic floor
    "wick_first_touch_high":  0.96,
    "wick_high_from_above":   0.964,
    "h1_disagree_winrate":    0.598,
    "first_move_to_wick":     0.63,
    "chain_rate":             0.996,
    "median_expansion_min":   54,
    "ten_oclock_touches":     0.38,
    "thirteen_oclock_rev":    0.90,
    "fvg_break_to_9am":       0.725,
    "first_touch_winrate":    0.732,
}

WICK_TOL_PT = 2.0           # "near wick" tolerance for confluence checks
NEAR_LEVEL_PT = 5.0         # FVG / line proximity to a candle edge
SD_MULTIPLIERS = [-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75,
                  1.0, 1.25, 1.5, 1.75, 2.0]


# ---------------------------------------------------------------------------
# Schema-tolerant accessors
# ---------------------------------------------------------------------------

def _get(d: Any, *names, default=None):
    """First non-None lookup across alias names. Accepts dict-likes."""
    if d is None:
        return default
    if isinstance(d, dict):
        for n in names:
            if n in d and d[n] is not None:
                return d[n]
    return default


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _normalize_box(b: Any) -> Optional[Dict[str, Any]]:
    """A Pine box typically has top, bottom, left/right time, color, label."""
    top    = _to_float(_get(b, "top", "y2", "high", "price_top"))
    bottom = _to_float(_get(b, "bottom", "y1", "low", "price_bottom"))
    if top is None or bottom is None:
        return None
    if top < bottom:
        top, bottom = bottom, top
    return {
        "top": top,
        "bottom": bottom,
        "mid": (top + bottom) / 2,
        "size": top - bottom,
        "left":  _get(b, "left", "x1", "left_time", "time1", "start"),
        "right": _get(b, "right", "x2", "right_time", "time2", "end"),
        "label": _get(b, "label", "text", "name", "title", default=""),
        "color": _get(b, "color", "bgcolor", "fillcolor", default=""),
        "raw":   b,
    }


def _normalize_line(ln: Any) -> Optional[Dict[str, Any]]:
    y1 = _to_float(_get(ln, "y1", "price1", "start_price", "price"))
    y2 = _to_float(_get(ln, "y2", "price2", "end_price", "price"))
    if y1 is None and y2 is None:
        return None
    if y1 is None: y1 = y2
    if y2 is None: y2 = y1
    return {
        "y1": y1, "y2": y2,
        "price": (y1 + y2) / 2,           # treat as a single horizontal level
        "horizontal": abs(y1 - y2) < 1e-6,
        "x1": _get(ln, "x1", "time1", "start_time"),
        "x2": _get(ln, "x2", "time2", "end_time"),
        "label": _get(ln, "label", "text", "name", default=""),
        "color": _get(ln, "color", default=""),
        "raw":  ln,
    }


def _normalize_label(lb: Any) -> Optional[Dict[str, Any]]:
    y = _to_float(_get(lb, "y", "price", "y1"))
    text = str(_get(lb, "text", "label", "name", default="") or "")
    if y is None and not text:
        return None
    return {
        "price": y, "text": text,
        "x": _get(lb, "x", "time", "x1"),
        "raw": lb,
    }


def _normalize_bar(b: Any) -> Optional[Dict[str, Any]]:
    o = _to_float(_get(b, "open", "o"))
    h = _to_float(_get(b, "high", "h"))
    l = _to_float(_get(b, "low",  "l"))
    c = _to_float(_get(b, "close", "c"))
    t = _get(b, "time", "timestamp", "ts", "datetime")
    if any(v is None for v in (o, h, l, c)):
        return None
    return {"time": t, "open": o, "high": h, "low": l, "close": c,
            "volume": _to_float(_get(b, "volume", "v"))}


def _coerce_list(payload: Any) -> List[Any]:
    """MCP tools sometimes wrap arrays in {data: [...]} or {boxes: [...]}."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "boxes", "lines", "labels", "bars",
                    "items", "result", "values", "ohlcv"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # dict of lists keyed by indicator name → flatten
        flat: List[Any] = []
        for v in payload.values():
            if isinstance(v, list):
                flat.extend(v)
        return flat
    return []


def _ts_to_dt(t: Any) -> Optional[datetime]:
    if t is None:
        return None
    if isinstance(t, datetime):
        return t
    if isinstance(t, (int, float)):
        # heuristic: ms vs s
        v = float(t)
        if v > 1e12: v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OSError, ValueError):
            return None
    if isinstance(t, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d"):
            try:
                return datetime.strptime(t, fmt)
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Indicator classifier — tag boxes/labels with which indicator likely produced them
# ---------------------------------------------------------------------------

INDICATOR_KEYWORDS = {
    "fvg":          ["fvg", "fair value gap", "imbalance", "ifvg", "bisi", "sibi"],
    "crt":          ["crt", "candle range theory", "ce", "p2k", "p3k"],
    "turtle_soup":  ["turtle", "soup", "ts buy", "ts sell"],
    "fractal":      ["fractal", "fr ", "fr_", "range"],
}


def _classify(text: str) -> List[str]:
    if not text:
        return []
    t = text.lower()
    return [k for k, kws in INDICATOR_KEYWORDS.items() if any(kw in t for kw in kws)]


# ---------------------------------------------------------------------------
# ChartReader — central object built from raw MCP outputs
# ---------------------------------------------------------------------------

@dataclass
class ChartReader:
    raw_boxes:  Any = None
    raw_lines:  Any = None
    raw_labels: Any = None
    raw_5m:     Any = None
    raw_1h:     Any = None

    boxes:  List[Dict[str, Any]] = field(default_factory=list)
    lines:  List[Dict[str, Any]] = field(default_factory=list)
    labels: List[Dict[str, Any]] = field(default_factory=list)
    bars_5m: List[Dict[str, Any]] = field(default_factory=list)
    bars_1h: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        self.boxes  = [b for b in (_normalize_box(x)  for x in _coerce_list(self.raw_boxes))  if b]
        self.lines  = [l for l in (_normalize_line(x) for x in _coerce_list(self.raw_lines))  if l]
        self.labels = [l for l in (_normalize_label(x) for x in _coerce_list(self.raw_labels)) if l]
        self.bars_5m = [b for b in (_normalize_bar(x) for x in _coerce_list(self.raw_5m)) if b]
        self.bars_1h = [b for b in (_normalize_bar(x) for x in _coerce_list(self.raw_1h)) if b]

    # ---------- last-bar helpers ----------
    @property
    def current_price(self) -> Optional[float]:
        if self.bars_5m: return self.bars_5m[-1]["close"]
        if self.bars_1h: return self.bars_1h[-1]["close"]
        return None

    def bars_for_session(self, target_date: datetime,
                         start: dtime, end: dtime) -> List[Dict[str, Any]]:
        """Return 5m bars whose timestamp falls in [start, end) on target_date."""
        out = []
        for b in self.bars_5m:
            dt = _ts_to_dt(b["time"])
            if not dt: continue
            if dt.date() != target_date.date(): continue
            if not (start <= dt.time() < end): continue
            out.append(b)
        return out

    # ---------- candle extraction ----------
    def candle_9am_1h(self, target_date: datetime
                      ) -> Optional[Dict[str, float]]:
        """9:00 1H candle. Prefer native 1H bar; fall back to aggregated 5m."""
        # native
        for b in self.bars_1h:
            dt = _ts_to_dt(b["time"])
            if dt and dt.date() == target_date.date() and dt.time() == dtime(9, 0):
                return {"open": b["open"], "high": b["high"],
                        "low": b["low"], "close": b["close"]}
        # aggregate from 5m
        bars = self.bars_for_session(target_date, dtime(9, 0), dtime(10, 0))
        if len(bars) < 8:
            return None
        return {
            "open":  bars[0]["open"],
            "close": bars[-1]["close"],
            "high":  max(b["high"] for b in bars),
            "low":   min(b["low"]  for b in bars),
        }

    def candle_945_5m(self, target_date: datetime
                      ) -> Optional[Dict[str, float]]:
        bars = self.bars_for_session(target_date, dtime(9, 45), dtime(9, 50))
        if not bars: return None
        b = bars[0]
        return {"open": b["open"], "high": b["high"],
                "low":  b["low"],  "close": b["close"]}

    # ---------- proximity queries ----------
    def boxes_near(self, price: float, tol: float = NEAR_LEVEL_PT
                   ) -> List[Dict[str, Any]]:
        out = []
        for b in self.boxes:
            if (b["bottom"] - tol) <= price <= (b["top"] + tol):
                tags = _classify(str(b["label"]))
                out.append({**b, "tags": tags,
                            "dist_to_top": abs(price - b["top"]),
                            "dist_to_bottom": abs(price - b["bottom"])})
        return out

    def lines_near(self, price: float, tol: float = NEAR_LEVEL_PT
                   ) -> List[Dict[str, Any]]:
        return [{**l, "tags": _classify(str(l["label"])),
                 "distance": abs(l["price"] - price)}
                for l in self.lines if abs(l["price"] - price) <= tol]

    def labels_near(self, price: float, tol: float = NEAR_LEVEL_PT
                    ) -> List[Dict[str, Any]]:
        out = []
        for lb in self.labels:
            if lb["price"] is None: continue
            if abs(lb["price"] - price) <= tol:
                out.append({**lb, "tags": _classify(lb["text"]),
                            "distance": abs(lb["price"] - price)})
        return out


# ---------------------------------------------------------------------------
# SD level projection (mirrors sd_alignment_backtest.py)
# ---------------------------------------------------------------------------

def project_levels(low: float, high: float) -> Dict[float, float]:
    rng = high - low
    return {m: low + m * rng for m in SD_MULTIPLIERS}


# ---------------------------------------------------------------------------
# Setup analyzer
# ---------------------------------------------------------------------------

@dataclass
class SetupSignal:
    name: str
    direction: str        # "bull", "bear", "neutral"
    strength: float       # 0..1
    finding_pct: float    # historical hit rate this signal references
    note: str


@dataclass
class WatchLevel:
    price: float
    source: str           # "9:45_high", "9:45_low", "9am_high", etc.
    finding_pct: float
    confluence: List[str] = field(default_factory=list)
    note: str = ""


@dataclass
class SetupReport:
    target_date: Optional[datetime]
    current_price: Optional[float]
    h1_9am: Optional[Dict[str, float]]
    m5_945: Optional[Dict[str, float]]
    h1_bias: str = "unknown"
    m5_bias: str = "unknown"
    bias_call: str = "unknown"
    bias_confidence: float = 0.0
    quality: str = "low"           # "high" / "medium" / "low" / "no-trade"
    signals: List[SetupSignal] = field(default_factory=list)
    watch_levels: List[WatchLevel] = field(default_factory=list)
    no_trade_reasons: List[str] = field(default_factory=list)
    contradictions: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


def _bias(c: Dict[str, float]) -> str:
    if c["close"] > c["open"]: return "bull"
    if c["close"] < c["open"]: return "bear"
    return "flat"


def _add_confluence(level: WatchLevel, reader: ChartReader, tol: float = NEAR_LEVEL_PT):
    for b in reader.boxes_near(level.price, tol):
        for t in b["tags"]:
            level.confluence.append(f"{t}_box:{b['label']}".strip(":"))
    for ln in reader.lines_near(level.price, tol):
        for t in ln["tags"]:
            level.confluence.append(f"{t}_line:{ln['label']}".strip(":"))
    for lb in reader.labels_near(level.price, tol):
        for t in lb["tags"]:
            level.confluence.append(f"{t}_label:{lb['text']}".strip(":"))


def analyze_setup(reader: ChartReader,
                  target_date: Optional[datetime] = None,
                  prior_945_levels: Optional[Sequence[Dict[float, float]]] = None
                  ) -> SetupReport:
    """
    Score the current intraday setup against the proven findings.

    target_date: defaults to the date of the most recent 5m bar.
    prior_945_levels: list of prior days' 9:45 SD-level dicts (multiplier→price)
        used for chain/alignment scoring. Optional.
    """
    if target_date is None:
        if reader.bars_5m:
            target_date = _ts_to_dt(reader.bars_5m[-1]["time"]) or datetime.now()
        else:
            target_date = datetime.now()

    rep = SetupReport(target_date=target_date,
                      current_price=reader.current_price,
                      h1_9am=reader.candle_9am_1h(target_date),
                      m5_945=reader.candle_945_5m(target_date))

    # ---- bias ----
    if rep.h1_9am: rep.h1_bias = _bias(rep.h1_9am)
    if rep.m5_945: rep.m5_bias = _bias(rep.m5_945)
    if rep.h1_bias != "unknown" and rep.m5_bias != "unknown":
        # 9:45 body is contrarian; bias FROM 9:45 = opposite of body
        m5_signal = "bear" if rep.m5_bias == "bull" else (
                    "bull" if rep.m5_bias == "bear" else "flat")
        if rep.h1_bias == m5_signal:
            rep.bias_call = rep.h1_bias
            rep.bias_confidence = 0.66           # both layers agree
            rep.signals.append(SetupSignal(
                "9AM_agrees_with_945_contrarian", rep.h1_bias, 0.66,
                FINDINGS["h1_disagree_winrate"],
                "9AM 1H bias matches the contrarian read of the 9:45 manipulation candle"))
        elif rep.h1_bias != rep.m5_bias:
            # 9AM disagrees with 9:45 body direction → trade with 9AM
            rep.bias_call = rep.h1_bias
            rep.bias_confidence = FINDINGS["h1_disagree_winrate"]
            rep.signals.append(SetupSignal(
                "9AM_disagrees_with_945_body", rep.h1_bias,
                FINDINGS["h1_disagree_winrate"],
                FINDINGS["h1_disagree_winrate"],
                "9AM 1H bias disagrees with 9:45 body — historical edge to 9AM (59.8%)"))
        else:
            rep.bias_call = rep.h1_bias
            rep.bias_confidence = 0.5
            rep.contradictions.append(
                "9AM bias matches 9:45 body — but 9:45 body is contrarian; ambiguous")

    # ---- watch levels: 9:45 wicks (top finding) ----
    if rep.m5_945:
        for src, price, note, pct in [
            ("9:45_high", rep.m5_945["high"],
             "9:45 high — first touch reverses 84-96%; 96.4% if approached from above",
             FINDINGS["wick_high_from_above"]),
            ("9:45_low",  rep.m5_945["low"],
             "9:45 low — first touch reverses 84-96%",
             FINDINGS["wick_first_touch_low"]),
        ]:
            wl = WatchLevel(price=price, source=src, finding_pct=pct, note=note)
            _add_confluence(wl, reader)
            rep.watch_levels.append(wl)

    # ---- 9AM zone wicks (13:00 reversal + FVG-break bounce) ----
    if rep.h1_9am:
        for src, price, note, pct in [
            ("9am_high", rep.h1_9am["high"],
             "9AM 1H high — 13:00 touches reverse 90%; FVG-break bounce 72.5%",
             FINDINGS["thirteen_oclock_rev"]),
            ("9am_low",  rep.h1_9am["low"],
             "9AM 1H low — 13:00 touches reverse 90%; FVG-break bounce 72.5%",
             FINDINGS["thirteen_oclock_rev"]),
        ]:
            wl = WatchLevel(price=price, source=src, finding_pct=pct, note=note)
            _add_confluence(wl, reader)
            rep.watch_levels.append(wl)

    # ---- prior-day 9:45 alignment (chain-rate) ----
    if prior_945_levels and rep.h1_9am:
        today_h1 = project_levels(rep.h1_9am["low"], rep.h1_9am["high"])
        clusters: Dict[float, List[str]] = {}
        for prior_idx, prior in enumerate(prior_945_levels):
            for m_p, p_p in prior.items():
                for m_t, p_t in today_h1.items():
                    dist = abs(p_t - p_p)
                    if dist <= 5.0:
                        clusters.setdefault(round(p_t, 1), []).append(
                            f"prior[-{prior_idx+1}d] 9:45 m={m_p:+.2f} (dist={dist:.1f}pt)")
        for price, hits in clusters.items():
            if len(hits) < 1: continue
            wl = WatchLevel(price=price, source="9am_x_prior_945_align",
                            finding_pct=FINDINGS["chain_rate"],
                            note=f"chain-rate confluence: today's 9AM SD level "
                                 f"aligns with {len(hits)} prior 9:45 level(s)")
            wl.confluence.extend(hits)
            _add_confluence(wl, reader)
            rep.watch_levels.append(wl)
        if clusters:
            rep.signals.append(SetupSignal(
                "chain_alignment_present", rep.bias_call,
                min(1.0, 0.5 + 0.1 * sum(len(v) for v in clusters.values())),
                FINDINGS["chain_rate"],
                f"{len(clusters)} aligned levels — chain rate across 9:45 ranges is 99.6%"))

    # ---- FVG-broken-then-9AM signal ----
    if rep.h1_9am and reader.current_price is not None:
        fvgs = [b for b in reader.boxes if "fvg" in _classify(str(b["label"]))]
        for f in fvgs:
            broken = (reader.current_price > f["top"]) or (reader.current_price < f["bottom"])
            in_9am_zone = (rep.h1_9am["low"] - 5
                           <= reader.current_price
                           <= rep.h1_9am["high"] + 5)
            if broken and in_9am_zone:
                rep.signals.append(SetupSignal(
                    "fvg_broken_into_9am_zone",
                    "bull" if reader.current_price <= f["bottom"] else "bear",
                    FINDINGS["fvg_break_to_9am"],
                    FINDINGS["fvg_break_to_9am"],
                    "FVG broken and price now inside 9AM 1H zone — 72.5% bounce"))

    # ---- 10:00 absorption / 13:00 windows ----
    now_dt = _ts_to_dt(reader.bars_5m[-1]["time"]) if reader.bars_5m else None
    if now_dt:
        t = now_dt.time()
        if dtime(10, 0) <= t < dtime(11, 0):
            rep.notes.append("Currently in 10:00 hour — historically absorbs 38% of all level touches")
        if dtime(13, 0) <= t < dtime(14, 0):
            rep.notes.append("Currently in 13:00 window — touches on 9AM zones reverse 90%")
        if rep.m5_945 and t < dtime(10, 45):
            rep.notes.append("Within 54-min median expansion window after 09:50")

    # ---- no-trade conditions ----
    if not rep.h1_9am: rep.no_trade_reasons.append("9AM 1H candle missing — incomplete data")
    if not rep.m5_945: rep.no_trade_reasons.append("9:45 5m candle missing — incomplete data")
    if rep.h1_9am and rep.m5_945:
        h1_rng = rep.h1_9am["high"] - rep.h1_9am["low"]
        m5_rng = rep.m5_945["high"] - rep.m5_945["low"]
        if h1_rng < 20:
            rep.no_trade_reasons.append(f"9AM 1H range {h1_rng:.1f} pt — too compressed (<20)")
        if m5_rng < 5:
            rep.no_trade_reasons.append(f"9:45 5m range {m5_rng:.1f} pt — manipulation candle "
                                        f"too small to project from")
    if rep.bias_confidence > 0 and rep.bias_confidence < 0.55:
        rep.no_trade_reasons.append("Layer-disagreement edge below 55% — borderline")

    # ---- quality ----
    sig_score = sum(s.strength for s in rep.signals)
    n_align = sum(1 for w in rep.watch_levels if w.source == "9am_x_prior_945_align")
    if rep.no_trade_reasons:
        rep.quality = "no-trade"
    elif sig_score >= 1.5 and n_align >= 1:
        rep.quality = "high"
    elif sig_score >= 0.6:
        rep.quality = "medium"
    else:
        rep.quality = "low"

    return rep


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def format_report(rep: SetupReport) -> str:
    L: List[str] = []
    L.append("=" * 72)
    L.append("CHART READ — SETUP REPORT")
    L.append("=" * 72)
    L.append(f"target_date:    {rep.target_date}")
    L.append(f"current_price:  {rep.current_price}")
    L.append(f"quality:        {rep.quality.upper()}    "
             f"bias_call: {rep.bias_call}    "
             f"confidence: {rep.bias_confidence:.0%}")

    L.append("\n-- candles --")
    if rep.h1_9am:
        c = rep.h1_9am
        L.append(f"9AM 1H:   O={c['open']:.2f}  H={c['high']:.2f}  "
                 f"L={c['low']:.2f}  C={c['close']:.2f}  "
                 f"range={c['high']-c['low']:.2f}  bias={rep.h1_bias}")
    else:
        L.append("9AM 1H:   <missing>")
    if rep.m5_945:
        c = rep.m5_945
        L.append(f"9:45 5m:  O={c['open']:.2f}  H={c['high']:.2f}  "
                 f"L={c['low']:.2f}  C={c['close']:.2f}  "
                 f"range={c['high']-c['low']:.2f}  body={rep.m5_bias} "
                 f"(contrarian read: {('bear' if rep.m5_bias=='bull' else 'bull' if rep.m5_bias=='bear' else 'flat')})")
    else:
        L.append("9:45 5m:  <missing>")

    if rep.signals:
        L.append("\n-- signals --")
        for s in rep.signals:
            L.append(f"  [{s.direction:>4}]  {s.name:<35}  "
                     f"strength={s.strength:.2f}  finding={s.finding_pct:.1%}  — {s.note}")

    if rep.watch_levels:
        L.append("\n-- watch levels (sorted by finding %) --")
        for w in sorted(rep.watch_levels, key=lambda x: -x.finding_pct):
            conf = (" | confluence: " + ", ".join(w.confluence)) if w.confluence else ""
            L.append(f"  {w.price:>10.2f}  {w.source:<26}  "
                     f"finding={w.finding_pct:.1%}{conf}")
            if w.note: L.append(f"             ↳ {w.note}")

    if rep.contradictions:
        L.append("\n-- contradictions --")
        for c in rep.contradictions: L.append(f"  ! {c}")

    if rep.notes:
        L.append("\n-- notes --")
        for n in rep.notes: L.append(f"  • {n}")

    if rep.no_trade_reasons:
        L.append("\n-- NO-TRADE conditions --")
        for r in rep.no_trade_reasons: L.append(f"  ✗ {r}")

    L.append("\n" + "=" * 72)
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Convenience: load prior 9:45 levels from sd_alignment_backtest output
# ---------------------------------------------------------------------------

def load_prior_945_from_csv(path: str, n: int = 5
                            ) -> List[Dict[float, float]]:
    """Read artifacts/daily_levels.csv and return the most recent N days'
    9:45 level dicts (multiplier→price)."""
    import csv
    from collections import defaultdict
    by_date: Dict[str, Dict[float, float]] = defaultdict(dict)
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("layer") != "m5":
                continue
            try:
                m = float(r["multiplier"]); p = float(r["price"])
            except (KeyError, ValueError):
                continue
            by_date[r["date"]][m] = p
    dates = sorted(by_date)[-n:]
    return [by_date[d] for d in dates]


# ---------------------------------------------------------------------------
# Smoke test on synthetic MCP-shaped input
# ---------------------------------------------------------------------------

def _smoke_test() -> int:
    today = datetime(2025, 5, 6, 10, 30)
    base_open = 18000.0
    bars_5m = []
    for hour in range(6, 16):
        for m in range(0, 60, 5):
            stamp = today.replace(hour=hour, minute=m, second=0)
            o = base_open + (hour - 9) * 5 + m * 0.1
            h = o + 7; l = o - 5; c = o + 2
            bars_5m.append({"time": stamp.isoformat() + "Z",
                            "open": o, "high": h, "low": l, "close": c, "volume": 100})
    bars_1h = [{"time": today.replace(hour=9, minute=0).isoformat() + "Z",
                "open": 18005, "high": 18045, "low": 17995, "close": 18030}]
    boxes = [{"label": "FVG bull", "top": 18020, "bottom": 18015,
              "left": "2025-05-06T07:00:00Z", "right": "2025-05-06T12:00:00Z"},
             {"text": "CRT manip", "top": 18044, "bottom": 18040}]
    lines = [{"label": "Fractal Range high", "y1": 18070, "y2": 18070}]
    labels = [{"text": "Turtle Soup buy", "price": 17993, "time": "2025-05-06T09:50:00Z"}]
    reader = ChartReader(boxes, lines, labels, bars_5m, bars_1h)

    fake_prior = [{m: 18000 + m * 12 for m in SD_MULTIPLIERS} for _ in range(5)]
    rep = analyze_setup(reader, target_date=today, prior_945_levels=fake_prior)
    print(format_report(rep))
    assert rep.h1_9am is not None, "h1 candle should be detected"
    assert rep.m5_945 is not None, "m5 candle should be detected"
    assert rep.watch_levels, "should produce at least one watch level"
    print("\n[smoke] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(_smoke_test())
