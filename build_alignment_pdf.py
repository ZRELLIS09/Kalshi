"""
Build a single PDF report from out_range_alignment/ outputs.

Sections:
  1. Title + study parameters
  2. Headline summary (aligned vs non-aligned)
  3. Significance tests
  4. Scenario comparison (both / only_1h / only_5m / none)
  5. Tolerance sweep
  6. Lookback sweep
  7. Top level pairs
  8. Day of week, range bucket, level rankings
  9. Cluster-size effect
 10. Hour-of-touch reaction (13:00 claim test)
 11. Spot-check dates
 12. Charts (5 PNGs)
 13. Interpretation / caveats / next steps

Usage:
  python3 build_alignment_pdf.py [--indir out_range_alignment] \
                                  [--out alignment_report.pdf]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd


def _read(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


def _table_page(pdf: PdfPages, title: str, df: pd.DataFrame,
                subtitle: str = "", float_fmt: str = "{:.3f}",
                col_widths: dict | None = None) -> None:
    """Render a DataFrame as a page in the PDF."""
    if df.empty:
        fig, ax = plt.subplots(figsize=(11, 8.5))
        ax.axis("off")
        ax.text(0.5, 0.5, f"{title}\n(no data)", ha="center", va="center",
                fontsize=14, transform=ax.transAxes)
        pdf.savefig(fig); plt.close(fig)
        return

    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_float_dtype(df[c]):
            df[c] = df[c].apply(lambda v: "" if pd.isna(v) else float_fmt.format(v))

    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.96)
    if subtitle:
        ax.text(0.5, 0.92, subtitle, ha="center", va="top",
                fontsize=10, transform=fig.transFigure, style="italic")

    n_rows = min(len(df), 32)
    show = df.head(n_rows)
    tbl = ax.table(cellText=show.values, colLabels=show.columns,
                   loc="center", cellLoc="center", colLoc="center")
    tbl.auto_set_font_size(False)
    fontsize = 9 if len(show.columns) <= 8 else 7
    tbl.set_fontsize(fontsize)
    tbl.scale(1, 1.3)
    for j in range(len(show.columns)):
        tbl[(0, j)].set_facecolor("#cccccc")
        tbl[(0, j)].set_text_props(weight="bold")
    if len(df) > n_rows:
        ax.text(0.5, 0.05, f"(showing {n_rows} of {len(df)} rows)",
                ha="center", transform=ax.transAxes, fontsize=9, style="italic")
    pdf.savefig(fig); plt.close(fig)


def _text_page(pdf: PdfPages, title: str, paragraphs: list[str]) -> None:
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.96)
    y = 0.88
    for para in paragraphs:
        ax.text(0.06, y, para, ha="left", va="top", wrap=True,
                fontsize=10, transform=fig.transFigure)
        y -= 0.04 + 0.018 * para.count("\n")
        if y < 0.05:
            break
    pdf.savefig(fig); plt.close(fig)


def _image_page(pdf: PdfPages, title: str, img_path: Path) -> None:
    if not img_path.exists():
        return
    fig, ax = plt.subplots(figsize=(11, 8.5))
    img = plt.imread(img_path)
    ax.imshow(img); ax.axis("off")
    fig.suptitle(title, fontsize=12, fontweight="bold", y=0.97)
    pdf.savefig(fig); plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", default="out_range_alignment")
    ap.add_argument("--out", default="alignment_report.pdf")
    args = ap.parse_args()

    indir = Path(args.indir)
    out = Path(args.out)

    if not indir.exists():
        print(f"Input dir not found: {indir}")
        return 2

    summary = _read(indir / "summary_stats.csv")
    sig = _read(indir / "significance_tests.csv")
    scen = _read(indir / "scenario_comparison.csv")
    tol = _read(indir / "tolerance_sweep.csv")
    lb = _read(indir / "lookback_sweep.csv")
    lvl = _read(indir / "level_respect_stats.csv")
    spot = _read(indir / "spot_check_dates.csv")
    by_dow = _read(indir / "extra_by_dow.csv")
    by_rng = _read(indir / "extra_by_range_bucket.csv")
    per_today = _read(indir / "extra_per_today_level.csv")
    per_prior = _read(indir / "extra_per_prior_level.csv")
    cluster = _read(indir / "extra_by_cluster_size.csv")
    hour = _read(indir / "touch_by_hour.csv")
    daily = _read(indir / "daily_levels.csv")

    n_sessions = len(daily) if not daily.empty else 0
    date_min = daily["date"].min() if "date" in daily.columns else "?"
    date_max = daily["date"].max() if "date" in daily.columns else "?"

    with PdfPages(out) as pdf:
        # Title page
        _text_page(pdf, "NQ/MNQ Range Alignment Backtest", [
            "9:00 AM 1H candle (BIAS) and 9:45 AM 5m candle (PRECISION)",
            "Range level alignment vs continuation predictiveness",
            "",
            f"Symbol:        MNQ continuous front-month (MNQ.c.0)",
            f"Source:        Databento GLBX.MDP3, ohlcv-1m schema",
            f"Sessions:      {n_sessions} valid trading days",
            f"Window:        {date_min} -> {date_max}",
            f"Default:       lookback=5d, tolerance=5pts (NY tz, DST-aware)",
            "",
            "Levels tested per candle: 0, 0.25, 0.50, 0.75, 1.00",
            "(low wick, lower-quartile, midpoint/CE, upper-quartile, high wick)",
            "",
            "An 'aligned' day = today's 9AM 1H level falls within tolerance of",
            "any prior-day's 9:45 5m level (within lookback window).",
            "",
            "Outcome metric: post-10:00 close-to-close move and MFE/MAE in",
            "5 windows: 10-11, 10-12, 10-13, 10-15, 10-16 (NY).",
            "",
            "Primary post-10:00 window for headline stats: 10:00 - 13:00.",
        ])

        _table_page(pdf, "1. Headline summary (aligned vs non-aligned)",
                    summary,
                    subtitle="Primary window = 10:00 - 13:00 NY",
                    float_fmt="{:.2f}")

        _table_page(pdf, "2. Significance tests (two-prop z / Welch t)",
                    sig,
                    subtitle="None of the comparisons reach p < 0.05",
                    float_fmt="{:.4f}")

        _table_page(pdf, "3. Scenario comparison",
                    scen,
                    subtitle="both = 1H+9:45 aligned, only_1h, only_5m, none",
                    float_fmt="{:.3f}")

        _table_page(pdf, "4. Tolerance sweep",
                    tol, float_fmt="{:.4f}",
                    subtitle="Tighter tolerance (2pt) gives the cleanest cohort")

        _table_page(pdf, "5. Lookback sweep",
                    lb, float_fmt="{:.3f}",
                    subtitle="1-day lookback outperforms 5/10/20")

        _table_page(pdf, "6. Top level pairs by avg |move|",
                    lvl, float_fmt="{:.2f}",
                    subtitle="today_1h_level x prior_5m_level (lookback=5, tol=5)")

        _table_page(pdf, "7a. Day of week",
                    by_dow, float_fmt="{:.3f}",
                    subtitle="0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri")
        _table_page(pdf, "7b. Range bucket (small/med/large 9AM range)",
                    by_rng, float_fmt="{:.3f}")
        _table_page(pdf, "7c. Per today_1h_level reaction",
                    per_today, float_fmt="{:.2f}")
        _table_page(pdf, "7d. Per prior_5m_level reaction",
                    per_prior, float_fmt="{:.2f}")

        _table_page(pdf, "8. Cluster-size effect",
                    cluster, float_fmt="{:.3f}",
                    subtitle="More aligned levels per day -> higher continuation? "
                    "(7+ hits = 100% on n=5)")

        _table_page(pdf, "9. Hour-of-touch reversal (prior-zone touch test)",
                    hour, float_fmt="{:.3f}",
                    subtitle="Tests prior 90% reversal claim at 13:00 -> NOT replicated")

        _table_page(pdf, "10. Spot-check dates (manual TradingView review)",
                    spot, float_fmt="{:.2f}")

        # Charts
        for fname, title in [
            ("01_avg_move_aligned_vs_not.png", "Chart 1 — Avg |move| aligned vs non-aligned"),
            ("02_continuation_by_tolerance.png", "Chart 2 — Continuation rate vs tolerance"),
            ("03_level_pair_ranking.png", "Chart 3 — Top level pair ranking"),
            ("04_distance_vs_move_scatter.png", "Chart 4 — Alignment distance vs realized move"),
            ("05_boxplot_aligned_vs_not.png", "Chart 5 — Distribution of post-10:00 moves"),
        ]:
            _image_page(pdf, title, indir / fname)

        # Interpretation
        _text_page(pdf, "Interpretation", [
            "1. HYPOTHESIS: weakly directional but not statistically significant.",
            "   Aligned days had higher continuation (78.3% vs 74.5%) and larger",
            "   average |moves| (115.2 vs 99.9 pts), but every comparison has",
            "   p > 0.20. With n=253 sessions we cannot reject the null.",
            "",
            "2. RANGE LEVELS RANKED (FIRST TOUCH OF TODAY'S 9AM):",
            "   1h_L0  (low wick):  cont=82.5%  avg|move|=104.6  best",
            "   1h_L100 (high wick): cont=82.4%  avg|move|=117.7  best",
            "   1h_L25:              cont=76.4%  avg|move|=95.6",
            "   1h_L50 (CE):         cont=77.8%  avg|move|=98.1",
            "   1h_L75:              cont=74.0%  avg|move|=89.8  worst",
            "   -> WICKS dominate, NOT body levels. CE is mid-pack here.",
            "",
            "3. ALIGNMENT IMPROVEMENT VS NON-ALIGNMENT:",
            "   Continuation +3.8pp, |move| +15.3 pts. Real direction, not",
            "   significant at p < 0.05. Effect size suggests ~700 sessions",
            "   (3 years) needed for confirmation.",
            "",
            "4. DOES NON-ALIGNMENT = CHOP? NO.",
            "   Chop rate < 1% in BOTH cohorts. Non-aligned days still trended.",
            "   The chop framing is REJECTED.",
            "",
            "5. BEST TOLERANCE: 2pt (continuation 80.0%) - tighter is better.",
            "6. BEST LOOKBACK: 1 day (continuation 80.8%) - yesterday matters",
            "   most, decays with distance.",
            "",
            "7. DOES 9:45 5m ADD PRECISION? NO.",
            "   both (1H+5m) cont=79.1% vs only_1h cont=80.8%. The 5m precision",
            "   layer does not earn its keep when stacked on the 1H bias.",
            "   only_5m (n=14) had +84 pt avg signed move - intriguing but tiny.",
            "",
            "8. PRIOR 13:00 -> 90% REVERSAL CLAIM: NOT REPLICATED.",
            "   On prior-day 9AM 1H range levels, 13:00 touches reverse 47.9%",
            "   (n=119). 14:00 is the strongest hour at 62.4% (n=93, p=0.082).",
            "   The 90% number likely refers to FVG touches, not 1H range.",
            "",
            "9. CLUSTER EFFECT (unexpected): 7+ aligned levels -> 100% (n=5).",
            "   Worth filtering for, but n is too small to trade on yet.",
        ])

        _text_page(pdf, "Caveats and next steps", [
            "DATA CAVEATS",
            "  - One year of MNQ continuous front-month, n=253 sessions.",
            "  - 3 days flagged 'degraded' by Databento (2025-09-17, 2025-09-24,",
            "    2025-11-28) - kept in the run.",
            "  - Continuous-contract roll boundaries can introduce small range",
            "    distortions on changeover days.",
            "",
            "METHOD CAVEATS",
            "  - 'Continuation' = today's 9AM hi/lo hit first, post-10:00.",
            "    Days that grind sideways without touching either side count",
            "    as 'inside' (neither cont nor reversal).",
            "  - Significance is two-sided z and Welch t with normal-CDF",
            "    approximation (no scipy dependency). For df > 30 this is",
            "    accurate; for the small subgroups (n=14, n=26) treat the",
            "    p-values as approximate.",
            "  - Reaction window for prior-zone touches: 60 minutes after",
            "    first touch. Reversal threshold: > tolerance pts.",
            "",
            "RECOMMENDED NEXT STEPS",
            "  A. Pull 3-5 years of MNQ 1m (~$5) and re-run alignment test.",
            "  B. Test 9AM FVGs specifically (3-bar imbalance inside 9:00-9:59",
            "     window) - this is what the prior 78.8%/74.4%/90% findings",
            "     measured. (Implemented in nq_9am_fvg_backtest.py.)",
            "  C. Drill into 14:00 reversal hour - is it level-dependent?",
            "     Does it stack with alignment?",
            "  D. Add cluster-count filter (>= 4 hits) and test as a tradable",
            "     signal across more data.",
            "",
            "FILES",
            "  Script:    nq_range_alignment_backtest.py",
            "  Outputs:   out_range_alignment/  (18 files: 5 PNG, 13 CSV)",
            "  Branch:    claude/nq-range-alignment-backtest-TTkFT",
        ])

    print(f"PDF written: {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
