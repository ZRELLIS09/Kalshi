#!/usr/bin/env python3
"""
Kalshi v4.0 Eval CSV Analyzer
Answers: "Does edge exist?" with numbers, not opinions.

Usage:
  python3 analyze_evals.py evals_v4_SESSION.csv
  python3 analyze_evals.py evals_v4_SESSION.csv --outcomes outcomes.csv

The eval CSV is produced by v4.0's SessionLogger.
The outcomes CSV (optional) maps ticker → outcome_yes (0/1).
"""

import argparse
import sys
import pandas as pd
import numpy as np


def load_eval(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if 'timestamp' in df.columns:
        df['ts'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df


def add_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Add implied probs, spreads, and buckets."""
    # Implied probabilities from ask prices
    for col in ['best_yes_ask', 'best_no_ask']:
        if col in df.columns:
            df[col.replace('best_', 'p_')] = df[col] / 100.0

    # Buckets for analysis
    if 'seconds_left' in df.columns:
        df['sec_bucket'] = pd.cut(
            df['seconds_left'],
            bins=[0, 5, 10, 20, 30, 45, 60, 90],
            right=True, include_lowest=True)

    if 'spread_yes' in df.columns:
        df['spread_bucket'] = pd.cut(
            df['spread_yes'],
            bins=[-1, 0, 2, 4, 6, 10, 20, 100],
            right=True)

    if 'q_hat_yes' in df.columns:
        df['q_bucket'] = pd.cut(
            df['q_hat_yes'],
            bins=np.linspace(0, 1, 11),
            include_lowest=True)

    if 'realized_vol_5m' in df.columns:
        vol_nz = df['realized_vol_5m'][df['realized_vol_5m'] > 0]
        if len(vol_nz) > 10:
            df['vol_bucket'] = pd.cut(
                df['realized_vol_5m'],
                bins=vol_nz.quantile([0, .25, .5, .75, 1.0]).values,
                include_lowest=True)

    return df


def summary_stats(df: pd.DataFrame):
    """Basic dataset summary."""
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    print(f"Total rows: {len(df)}")
    if 'ts' in df.columns:
        print(f"Time range: {df['ts'].min()} → {df['ts'].max()}")
    print(f"Unique tickers: {df['ticker'].nunique()}")

    if 'decision' in df.columns:
        print(f"\nDecisions:")
        print(df['decision'].value_counts().to_string())

    if 'method' in df.columns:
        print(f"\nMethods:")
        print(df['method'].value_counts().to_string())


def edge_distribution(df: pd.DataFrame):
    """Where does the model diverge from market?"""
    print("\n" + "=" * 60)
    print("EDGE DISTRIBUTION (model q_hat vs market implied)")
    print("=" * 60)

    cols = [c for c in ['edge_yes', 'edge_no', 'ev_yes', 'ev_no',
                         'spread_yes', 'spread_no'] if c in df.columns]
    if cols:
        print(df[cols].describe(
            percentiles=[.01, .05, .10, .25, .50, .75, .90, .95, .99]))


def edge_by_seconds(df: pd.DataFrame):
    """Where does edge concentrate by time-to-settle?"""
    if 'sec_bucket' not in df.columns or 'ev_yes' not in df.columns:
        return
    print("\n" + "=" * 60)
    print("MEAN EV BY SECONDS REMAINING")
    print("=" * 60)
    grouped = df.groupby('sec_bucket', observed=True).agg(
        n=('ev_yes', 'size'),
        mean_ev_yes=('ev_yes', 'mean'),
        mean_ev_no=('ev_no', 'mean'),
        mean_edge_yes=('edge_yes', 'mean'),
        mean_edge_no=('edge_no', 'mean'),
        mean_spread=('spread_yes', 'mean'),
    ).reset_index()
    print(grouped.to_string(index=False))


def edge_by_vol(df: pd.DataFrame):
    """Edge vs volatility regime."""
    if 'vol_bucket' not in df.columns or 'ev_yes' not in df.columns:
        return
    print("\n" + "=" * 60)
    print("MEAN EV BY VOLATILITY REGIME")
    print("=" * 60)
    grouped = df.groupby('vol_bucket', observed=True).agg(
        n=('ev_yes', 'size'),
        mean_ev_yes=('ev_yes', 'mean'),
        mean_ev_no=('ev_no', 'mean'),
        mean_spread=('spread_yes', 'mean'),
    ).reset_index()
    print(grouped.to_string(index=False))


def edge_heatmap(df: pd.DataFrame):
    """2D: seconds_remaining × spread → mean EV."""
    if 'sec_bucket' not in df.columns or 'spread_bucket' not in df.columns:
        return
    if 'ev_yes' not in df.columns:
        return
    print("\n" + "=" * 60)
    print("EV HEATMAP: seconds_remaining × spread")
    print("=" * 60)

    # YES side
    heat_yes = df.pivot_table(
        index='sec_bucket', columns='spread_bucket',
        values='ev_yes', aggfunc='mean', observed=True)
    if not heat_yes.empty:
        print("\nMean EV (YES):")
        print(heat_yes.round(2))

    # NO side
    heat_no = df.pivot_table(
        index='sec_bucket', columns='spread_bucket',
        values='ev_no', aggfunc='mean', observed=True)
    if not heat_no.empty:
        print("\nMean EV (NO):")
        print(heat_no.round(2))


def calibration(df: pd.DataFrame, outcome_col: str = 'outcome_yes'):
    """Calibration: is q_hat real? Compare predicted vs realized."""
    if outcome_col not in df.columns:
        print("\n⚠️  No outcome column — cannot compute calibration.")
        print("   Add 'outcome_yes' column (0/1) to enable.")
        return

    print("\n" + "=" * 60)
    print("CALIBRATION: q_hat_yes vs realized win rate")
    print("=" * 60)

    if 'q_bucket' in df.columns:
        calib = df.groupby('q_bucket', observed=True).agg(
            n=('q_hat_yes', 'size'),
            q_mean=('q_hat_yes', 'mean'),
            win_rate=(outcome_col, 'mean'),
        ).reset_index()
        calib['gap'] = calib['q_mean'] - calib['win_rate']
        print(calib.to_string(index=False))

        # Brier score
        brier = np.mean((df['q_hat_yes'] - df[outcome_col]) ** 2)
        baseline_brier = np.mean((df[outcome_col].mean() - df[outcome_col]) ** 2)
        print(f"\nBrier score: {brier:.4f} (baseline: {baseline_brier:.4f})")
        print(f"Brier skill: {1 - brier/baseline_brier:.4f}" if baseline_brier > 0 else "")


def realized_pnl(df: pd.DataFrame, outcome_col: str = 'outcome_yes'):
    """Compute hypothetical PnL if we'd traded every signal."""
    if outcome_col not in df.columns:
        return

    signals = df[df['decision'].str.startswith('SIGNAL', na=False)].copy()
    if len(signals) == 0:
        print("\n⚠️  No SIGNAL rows found.")
        return

    print("\n" + "=" * 60)
    print(f"HYPOTHETICAL PnL ({len(signals)} signals)")
    print("=" * 60)

    # For YES signals
    yes_signals = signals[signals['trade_side'] == 'yes'].copy()
    if len(yes_signals) > 0:
        yes_signals['pnl'] = (
            100 * yes_signals[outcome_col]
            - yes_signals['trade_price']
            - yes_signals['fee_yes']
            - yes_signals['slippage']
        )
        print(f"\nYES trades ({len(yes_signals)}):")
        print(yes_signals['pnl'].describe(percentiles=[.05, .25, .5, .75, .95]))
        print(f"Total PnL: {yes_signals['pnl'].sum():.0f}¢")

    # For NO signals
    no_signals = signals[signals['trade_side'] == 'no'].copy()
    if len(no_signals) > 0:
        no_outcome = 1 - no_signals[outcome_col]
        no_signals['pnl'] = (
            100 * no_outcome
            - no_signals['trade_price']
            - no_signals['fee_no']
            - no_signals['slippage']
        )
        print(f"\nNO trades ({len(no_signals)}):")
        print(no_signals['pnl'].describe(percentiles=[.05, .25, .5, .75, .95]))
        print(f"Total PnL: {no_signals['pnl'].sum():.0f}¢")


def method_comparison(df: pd.DataFrame):
    """Compare closed_form vs monte_carlo evaluations."""
    if 'method' not in df.columns:
        return
    print("\n" + "=" * 60)
    print("METHOD COMPARISON")
    print("=" * 60)
    for method in df['method'].unique():
        subset = df[df['method'] == method]
        signals = subset[subset['decision'].str.startswith('SIGNAL', na=False)]
        print(f"\n{method}: {len(subset)} evals, {len(signals)} signals "
              f"({len(signals)/len(subset)*100:.1f}% signal rate)")
        if 'ev_yes' in subset.columns:
            print(f"  Mean EV(YES): {subset['ev_yes'].mean():.2f}¢")
            print(f"  Mean EV(NO):  {subset['ev_no'].mean():.2f}¢")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze v4.0 eval CSV — does edge exist?')
    parser.add_argument('eval_csv', help='Path to evals_v4_*.csv')
    parser.add_argument('--outcomes', help='Optional: CSV with ticker,outcome_yes columns')
    args = parser.parse_args()

    df = load_eval(args.eval_csv)

    # Merge outcomes if provided
    if args.outcomes:
        outcomes = pd.read_csv(args.outcomes)
        df = df.merge(outcomes[['ticker', 'outcome_yes']], on='ticker', how='left')
        print(f"Merged {outcomes['outcome_yes'].notna().sum()} outcomes")

    df = add_derived(df)

    summary_stats(df)
    edge_distribution(df)
    edge_by_seconds(df)
    edge_by_vol(df)
    edge_heatmap(df)
    method_comparison(df)
    calibration(df)
    realized_pnl(df)

    print("\n" + "=" * 60)
    print("DONE — Review the numbers above.")
    print("If calibration is tight and EV is positive in specific buckets,")
    print("you have a statistically defensible edge.")
    print("=" * 60)


if __name__ == '__main__':
    main()
