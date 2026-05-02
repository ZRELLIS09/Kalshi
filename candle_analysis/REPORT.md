# NQ 9AM 1H × 9:45 5m SD-Level Alignment Analysis

**Source:** Yahoo Finance (`NQ=F` continuous front-month). TradingView export was not available in this environment.
**Data:** 5-minute bars, 2026-03-04 to 2026-05-01 ET.
**Days with valid 9AM 1H + 9:45 5m candles:** 42
**Continuation window:** 10:00 ET (open) → 16:00 ET (close), regular session.
**Primary alignment tolerance:** 1.0% of today's 9AM 1H range.

## TL;DR — Executive summary

**The stated hypothesis is NOT supported in this 42-day NQ sample.**
Aligned days do not show higher-conviction continuation; if anything they
show *lower* continuation than non-aligned days, but the difference is not
statistically significant given the small non-aligned sample.

- Aligned days (n=34): mean directional move = -34.1 pts, continuation win-rate = 38.2%
- Non-aligned days (n=8): mean directional move = +71.8 pts, continuation win-rate = 50.0%
- Welch t-test t=-1.41, df≈12 (not significant at p<0.05)
- corr(alignment hit count, |directional move|) = -0.077

**Findings that ARE supported by the data (these were not in the hypothesis):**

1. **9:45 5m is always fully inside the 9AM 1H range** — 100% of 42 days. Median 5m range is ~39% of the 1H range. The 9:45 5m never breaks the 9AM hour's high or low while it is forming.
2. **9AM 1H range is almost always violated post-10AM**: 73.8% of days take out the 9AM high, 57.1% take out the low, 33.3% sweep BOTH sides, and only 2.4% (1 day) stay inside the 9AM range until 16:00.
3. **The 9AM 1H midpoint and upper-half levels are the most-respected.** +0.50 (midpoint) is touched on 85.7% of days; +0.75 on 83.3%; +0.25 on 76.2%. Below-zero extensions are touched far less (-0.50: 33%, -1.00: 21%).
4. **Both the 9AM 1H bias and the 9:45 5m direction are weak / contrarian predictors of the 10:00→16:00 move** (40.5% and 42.9% win rates respectively). When the two AGREE, continuation is actually slightly *worse* (38.5% win rate) than when they DISAGREE (46.7% win rate).
5. **9AM range size is a weak predictor of intraday range** (corr ≈ 0.28); the 9:45 5m range is marginally better (corr ≈ 0.33).

**Caveats:**

- Sample is 42 trading days from 2026-03-04 → 2026-05-01. Recent NQ has been net down ~2.7% over this window with elevated vol; results may not generalize to other regimes.
- Tolerance choice strongly affects 'alignment'. With 13 SD-level grids for today's 1H × up to 30 prior days × 13 prior 5m levels = up to 5,070 pair candidates per day. Even at 1% tolerance, alignment is common; at 5% it is near-universal.
- TradingView export was not accessible from this environment. Source is Yahoo Finance NQ=F (continuous front-month). Re-running on actual TV export of CME NQ may shift results by 0.25-pt tick discrepancies but should not change the qualitative findings.

---


## 1. Alignment frequency

- Days with ≥1 alignment hit (5% tol):  34 / 42 (81.0%)
- Mean hits/day:  3.19
  - At 0.5% tol: 69.0% of days have ≥1 hit, mean hits/day = 1.76
  - At 1% tol: 81.0% of days have ≥1 hit, mean hits/day = 3.19
  - At 2% tol: 88.1% of days have ≥1 hit, mean hits/day = 6.55
  - At 5% tol: 88.1% of days have ≥1 hit, mean hits/day = 16.62

## 2. Continuation: post-9AM directional move (10:00→16:00 ET)

`cont_directional_pts` is signed move in the direction of the 9AM 1H bias (close-open).
Positive = move continued in the bias direction; negative = reversed.

- All days:                   n=42 mean=-13.9 median=-28.2 std=214.7
- Aligned days (≥1 hit):      n=34 mean=-34.1 median=-33.6 std=218.8
- Non-aligned days (0 hits):  n=8 mean=+71.8 median=+74.0 std=184.4

- Aligned MFE (excursion with bias):    n=34 mean=+132.5 median=+80.2 std=115.5
- Non-aligned MFE:                      n=8 mean=+190.2 median=+223.5 std=116.2
- Aligned MAE (excursion against bias): n=34 mean=+190.9 median=+148.8 std=144.7
- Non-aligned MAE:                      n=8 mean=+126.1 median=+109.9 std=115.8

- % days where price continued in 9AM 1H bias direction:
  - Aligned:     38.2%
  - Non-aligned: 50.0%

## 3. Chop analysis

`chop_ratio = |directional move| / continuation range`. Lower = choppier.

- Aligned chop_ratio:     n=34 mean=+0.5 median=+0.5 std=0.3
- Non-aligned chop_ratio: n=8 mean=+0.5 median=+0.6 std=0.3

## 4. SD-level respect (touch counts in 10:00–16:00 ET, summed across days)

9AM 1H levels — % of days touched in 10:00-16:00 window:
  -1.00:  21.4%  (total touches: 48)
  -0.75:  28.6%  (total touches: 86)
  -0.50:  33.3%  (total touches: 119)
  -0.25:  50.0%  (total touches: 173)
  +0.00:  57.1%  (total touches: 301)
  +0.25:  76.2%  (total touches: 287)
  +0.50:  85.7%  (total touches: 288)
  +0.75:  83.3%  (total touches: 277)
  +1.00:  73.8%  (total touches: 232)
  +1.25:  57.1%  (total touches: 143)
  +1.50:  40.5%  (total touches: 120)
  +1.75:  35.7%  (total touches: 131)
  +2.00:  31.0%  (total touches: 161)

9:45 5m levels — % of days touched in 10:00-16:00 window:
  -1.00:  59.5%  (total touches: 266)
  -0.75:  59.5%  (total touches: 305)
  -0.50:  59.5%  (total touches: 303)
  -0.25:  71.4%  (total touches: 271)
  +0.00:  78.6%  (total touches: 270)
  +0.25:  78.6%  (total touches: 259)
  +0.50:  78.6%  (total touches: 258)
  +0.75:  76.2%  (total touches: 256)
  +1.00:  78.6%  (total touches: 256)
  +1.25:  78.6%  (total touches: 271)
  +1.50:  76.2%  (total touches: 247)
  +1.75:  76.2%  (total touches: 212)
  +2.00:  69.0%  (total touches: 208)

## 5. Per-day spot-check (most-aligned and least-aligned days)

Top 10 most-aligned days:
```
      date weekday  h1_range  h1_bias  align_hits_primary  cont_directional_pts  cont_excursion_with  cont_excursion_against
2026-03-12     Thu    222.25       -1                  10                103.00               116.00                   88.75
2026-04-06     Mon    171.00        1                  10                -48.50                40.75                  208.75
2026-03-23     Mon    215.50       -1                   8                201.25               292.00                   89.50
2026-04-30     Thu    337.00       -1                   7               -330.25                84.00                  374.75
2026-03-17     Tue    187.50        1                   7                -82.25                 5.25                  146.50
2026-03-25     Wed    165.25       -1                   6                 -3.50                82.00                  151.00
2026-03-18     Wed    134.25        1                   6               -317.50                15.75                  327.25
2026-04-02     Thu    230.25        1                   6                323.00               369.00                   38.75
2026-04-01     Wed    158.75        1                   5                 78.75               226.50                   13.00
2026-03-20     Fri    247.75       -1                   5                172.75               314.50                   71.00
```

Up to 10 zero-alignment days:
```
      date weekday  h1_range  h1_bias  align_hits_primary  cont_directional_pts  cont_excursion_with  cont_excursion_against
2026-03-04     Wed    144.25        1                   0                201.25               298.50                   27.75
2026-03-06     Fri    133.75        1                   0                -24.00               235.00                   90.25
2026-03-27     Fri    194.00       -1                   0                172.00               212.00                  129.50
2026-04-14     Tue    112.00        1                   0                279.75               281.75                    9.50
2026-04-15     Wed     93.25        1                   0                294.25               308.25                   20.75
2026-04-16     Thu    177.75       -1                   0               -162.50                30.75                  242.75
2026-04-17     Fri    188.25       -1                   0                -95.75                17.25                  153.00
2026-04-23     Thu    137.75        1                   0                -90.75               138.50                  335.50
```

## 6. Which (today_1H, prior_5m) frac pairs align most?

Total alignment hit records: 134
Top 15 frac pairs (today_1H_frac, prior_5m_frac, count):
```
 today_h1_frac  prior_m5_frac  n
         -1.00          -0.75  3
          0.75          -0.50  3
          0.75           1.25  3
          0.25           0.75  3
          0.00          -0.50  3
          1.00           2.00  3
          1.50          -0.75  3
          2.00           1.25  3
          1.75           1.50  3
         -1.00          -0.25  2
         -1.00          -0.50  2
         -1.00           0.25  2
         -1.00           0.75  2
          0.00           0.00  2
          0.25          -0.75  2
```

Today 1H levels most frequently part of an alignment:
  -1.00: 14
  +0.75: 14
  +1.50: 13
  -0.25: 12
  +0.25: 12
  +2.00: 11
  +1.25: 11
  +0.00: 10
  +1.00: 9
  +1.75: 9
  -0.75: 8
  -0.50: 7
  +0.50: 4

Prior 5m levels most frequently part of an alignment:
  -0.50: 16
  +1.25: 15
  -0.25: 14
  +0.75: 13
  -0.75: 12
  +1.50: 10
  +1.75: 9
  +2.00: 9
  -1.00: 8
  +0.00: 7
  +1.00: 7
  +0.25: 7
  +0.50: 7

## 7. Unbidden patterns (statistically interesting findings)

- corr(9AM 1H range, 10-16 range) = 0.285
- corr(9:45 5m range, 10-16 range) = 0.332
  → 9AM 1H range is the better predictor of the day's continuation volatility.

- 9:45 5m inside 9AM 1H range:
  Inside  (n=42): mean directional = -13.9, |dir| = 167.2
  Outside (n=0): mean directional = +nan, |dir| = nan

- By weekday (mean directional move, win rate continuing in 9AM bias):
  Fri: n=8, mean_dir=-52.1, win_rate=25.0%
  Mon: n=8, mean_dir=-0.5, win_rate=50.0%
  Thu: n=9, mean_dir=-48.3, win_rate=44.4%
  Tue: n=8, mean_dir=+4.4, win_rate=25.0%
  Wed: n=9, mean_dir=+26.2, win_rate=55.6%

- By 9AM 1H bias direction:
  bias=-1: n=17, mean_dir=-62.9, win_rate=35.3%
  bias=+1: n=25, mean_dir=+19.4, win_rate=44.0%

- 9AM 1H high taken out in 10-16 window: 73.8% of days
- 9AM 1H low taken out in 10-16 window:  57.1% of days
- BOTH taken out (sweep both sides):     33.3% of days
- NEITHER taken out (range stays inside): 2.4% of days

- corr(alignment hits, directional move)       = -0.026
- corr(alignment hits, |directional move|)     = -0.077
- corr(alignment hits, excursion with bias)    = -0.101

- Continuation by alignment-hit bucket (1% tol):
  0: n=8 mean_dir=+71.8 |dir|=165.0 mfe=190.2 win_rate=50.0%
  1: n=9 mean_dir=-100.9 |dir|=185.9 mfe=97.8 win_rate=22.2%
  2-3: n=5 mean_dir=+109.3 |dir|=172.9 mfe=207.8 win_rate=60.0%
  4-5: n=12 mean_dir=-53.6 |dir|=146.1 mfe=131.7 win_rate=41.7%
  6+: n=8 mean_dir=-19.3 |dir|=176.2 mfe=125.6 win_rate=37.5%

- 9AM 1H bias predicts 10-16 direction:  win_rate = 40.5% (mean continuation = -13.9 pts)
- 9:45 5m direction predicts 10-16 dir:  win_rate = 42.9% (mean continuation = -26.4 pts)
- 1H and 5m AGREE  (n=26): mean cont = -30.4, win_rate = 38.5%
- 1H and 5m DISAGREE (n=15): mean cont (signed by 1H) = +21.4, win_rate = 46.7%

- Welch t-test, aligned vs non-aligned directional pts: t=-1.41, df≈12
  (|t|>2 ≈ p<0.05; tiny non-aligned sample limits power)

- 9:45 5m candle is fully inside 9AM 1H range: 100.0% of days (median 5m_range/1H_range = 0.39)
