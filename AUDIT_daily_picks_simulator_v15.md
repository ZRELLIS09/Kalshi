# Structural Invariant Audit: daily_picks_simulator (15).py

**Date:** 2026-02-28
**Scope:** 10-section deterministic invariant verification
**File:** `daily_picks_simulator (15).py` (1218 lines)
**Supporting files reviewed:** `build_calibrator.py`, `team_name_mapping.py`

---

## VERIFIED CORRECT:

### Section 1 — Spread Orientation Integrity
1. **Spread is consistently home-team perspective.** `normalize_home_spread()` (L323-333) extracts the home team's point from the spread map; `game["spread"]` is always the home team's line. Convention: spread > 0 = home dog, spread < 0 = home favorite. Consistent through ingestion, validation, simulation, and output.
2. **μ is always home margin.** `build_features_for_game(home, away, ...)` (L172-205) computes all diffs as (home − away). Ridge target is `g["margin"]` from game_stats, validated at startup to be home margin via location=H check (L1042-1048) and result-string cross-check (L1055-1079).
3. **μ-gap computed as (μ + spread).** L593: `z_mugap = mu + spread`. Traced: mu=home margin, spread=home's line. z > 0 means model predicts home covers. Consistent with MC check `sims + spread > 0` at L590.
4. **No double sign inversions.** Swap logic (L1171) flips spread exactly once (`-old_spread`) when home/away is corrected. No other code path flips sign of an already-flipped value.
5. **pick_side and pick_spread match spread orientation.** L616-625: HOME pick → `pick_spread = spread` (home's line). AWAY pick → `pick_spread = -spread` (away's line). Correct and consistent.
6. **Full pipeline trace: ingestion → z_mugap → simulation → edge → output.** OddsAPI spread → `normalize_home_spread` → `game["spread"]` → `z_mugap = mu + spread` → `calibrator.predict(z_mugap)` → `cover_prob_home` → `pick_conf` → `edge = pick_conf - BREAKEVEN_110` → unit assignment → output. All steps maintain home perspective. No orientation breaks found.

### Section 2 — Snapshot Binding
7. **Missing teams raise visible errors.** `build_features_for_game` (L175-176) returns `None` for missing teams. `predict_games` (L557-560) converts this to an explicit error record with `"error": "Team not found"`. Not silent.
8. **Normalization: explicit mapping dict + strip().** `odds_to_torvik()` uses `ODDS_TO_TORVIK.get(name.strip(), name.strip())` (team_name_mapping.py L377-379). Unmapped names fall through as raw strings, which will not match the snapshot and produce a visible error.

### Section 3 — Temporal Integrity
9. **Snapshot date < game_date for training.** `find_nearest_snapshot()` (L164): `valid = [d for d in snapshot_dates if pd.to_datetime(d) < gd]`. Strict `<`.
10. **No future snapshot leakage.** Training: strict `<` via `find_nearest_snapshot`. Prediction: `get_latest_snapshot(before_date=date_str)` uses `<=` (L155), allowing same-day snapshot for prediction only. Correct — today's data is available at prediction time.
11. **Training only uses games before prediction date.** L1104: `gs_train = gs[gs["_gd"] < pd.to_datetime(date_str)]`. Strict `<`.

### Section 4 — OOS Sigma Estimation
12. **Walk-forward OOS residuals confirmed.** L282-296: Each fold trains a fresh Ridge on `dates < current`, predicts on `[current, fold_end)`. Residuals collected from test set only.
13. **No in-sample residual contamination.** `tr_mask = dates_all < current` and `te_mask = (dates_all >= current) & (dates_all < fold_end)`. Train and test have zero overlap.
14. **Folds do not overlap.** Each fold covers `[current, current + fold_days)`. Next fold starts at the previous fold's end. No overlap by construction.
15. **Residual collection boundaries correct.** `warmup_cutoff = dates_all[min_train - 1]` (L276) ensures minimum training games. Explicit `min_train` and `min_holdout` checks (L287) guard against degenerate folds. Hard failure at L300-303 if insufficient OOS residuals.

### Section 5 — Monte Carlo Engine
16. **Simulation mean = μ.** L588: `sims = np.random.normal(mu, sigma, MC_SIMS)`. Mean is `mu` (the averaged home margin).
17. **σ used is OOS σ.** `sigma` from `train_model` (L305: `float(np.std(oos_residuals))`) is passed through to `predict_games` (L1195) and used at L588. Confirmed OOS, not in-sample.
18. **Independent simulation per game.** L588 is inside the `for game in games` loop. Each game gets a fresh draw of 10,000 samples.
19. **No shared RNG arrays across games.** Each `np.random.normal()` call produces a new array. Sequential draws from global RNG are independent conditional on the seed.

### Section 6 — Calibration (partial — see VERIFIED BROKEN)
20. **Isotonic calibrator is not refit during prediction.** Loaded at L1121 (`joblib.load`), type-checked at L1122-1126. Only `calibrator.predict()` is called during prediction (L609). No `.fit()` calls during prediction.
21. **z values outside training range handled safely.** `build_calibrator.py` L290: `IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip")`. Out-of-range z values are clipped to nearest learned endpoint.

### Section 7 — Edge Calculation
22. **edge = calibrated_cover_prob − implied_prob.** L627: `edge = pick_conf - BREAKEVEN_110`. `pick_conf` uses calibrated probability when calibrator is present (L609, L617-618), raw MC probability otherwise (L613).
23. **Implied probability calculation correct.** L68: `BREAKEVEN_110 = 1.1 / 2.1` = 0.52381. Standard breakeven at −110 vig: risk 110 to win 100, breakeven = 110/210.
24. **Raw and calibrated edges not mixed in unit logic.** `edge` (L627) always uses `pick_conf` (calibrated when available). `edge_raw` (L634) is stored for audit trail only — never enters unit logic (L655-683).

### Section 8 — Unit Assignment Logic
25. **Pocket and board mutually exclusive.** L643-646: POCKET requires `7.0 <= abs_sp <= 9.5 AND pick_is_dog`. Everything else → BOARD. Exhaustive and non-overlapping.
26. **Unit thresholds match printed rules.** Calibrated POCKET: ≥5% → 2u, 3–4.9% → 1u, <3% → 0u (L658-665 vs L744). Calibrated BOARD: ≥10% → 3u, 8–9.9% → 2u, <8% → 0u (L667-673 vs L745). Uncalibrated: ≥7.6% → 3u, 3.6–7.5% → 2u, 0–3.5% → 1u, <0% → 0u (L676-683 vs L750). All match.
27. **No units assigned below threshold.** POCKET floor = 3% edge. BOARD floor = 8% edge. Uncalibrated floor = 0% edge. Below each respective floor → `units = 0`.
28. **Blocked games cannot receive units.** Z-gap blocked: L655-656 forces `units = 0`, overriding all other logic. Spread-sanity blocked: removed from `games` entirely at L1191 before `predict_games` is called.

### Section 9 — Spread Sanity Gate
29. **Discrepancy formula correct.** L478: `discrepancy = abs(implied_home_margin - (-spread))` = `abs(rating_diff + spread)`. When ratings and market agree, rating_diff ≈ −spread, so discrepancy ≈ 0.
30. **Direction flip detection correct.** L481-488: Fires when market says home is dog but ratings say home is much stronger (>5pt), or vice versa. Correctly captures fundamental market/ratings disagreement.
31. **Blocked games removed before simulation.** L1177-1191: `validate_spreads()` returns (clean, flagged, blocked). `games = clean_games + flagged_games` — blocked are excluded. Simulation at L1195 sees only clean + flagged.
32. **Flagged vs blocked thresholds: block dominates.** L495-500: `≥18 → blocked` (checked first), `≥12 or direction_flip → flagged` (elif), else → clean. Block threshold checked before flag threshold.

### Section 10 — Flip-Swing Logic
33. **Swap margin math is symmetric.** L578: `mu = (mu_home - mu_away) / 2`. If teams are swapped, mu_home and mu_away swap, yielding `−mu`. Sign-symmetric.
34. **No intercept double-counting.** Algebraically: `(mu_home − mu_away)/2 = ((coefs·diffs + c_t·tempo + b) − (−coefs·diffs + c_t·tempo + b))/2 = coefs·diffs`. Intercept, tempo, and constant HCA cancel exactly. No residual intercept contribution.
35. **flip_swing cannot distort μ.** `flip_swing` (L579) is computed AFTER `mu` is set (L578). It is a read-only diagnostic metric — never fed back into μ, z_mugap, or any simulation input.
36. **flip_swing does not affect betting decisions.** Used only in output warnings (L789-802) and stored in results dict (L709). Not referenced in unit logic (L655-683), edge calculation (L627), or pick selection (L616-625).

---

## VERIFIED BROKEN:

1. **Calibrator z-distribution mismatch (Section 6 / cross-cutting).**
   - **Location:** `build_calibrator.py` L259 vs `daily_picks_simulator` L578, L593.
   - **Finding:** The calibrator trains on `z = μ_single + spread` where `μ_single` is a single-orientation Ridge prediction (`mu_te = m.predict(X_all[te_mask])`). This μ includes the model intercept (~2–3 pts phantom HCA) and the `c_tempo × tempo_avg` term. The daily runner feeds the calibrator `z = μ_averaged + spread` where `μ_averaged = (mu_home − mu_away) / 2`, which algebraically cancels the intercept and tempo_avg term.
   - **Algebra:** `μ_single = coefs·diffs + c_tempo·tempo_avg + intercept`. `μ_averaged = coefs·diffs`. Difference = `c_tempo·tempo_avg + intercept ≈ 2–3 points`.
   - **Effect:** On a z-scale where typical values range [−10, +10], the ~2–3 point systematic shift moves predictions through a materially different region of the calibration curve. The calibrator systematically receives z values lower than what it was trained on, causing it to underestimate home cover probability and overestimate away cover probability.

---

## POTENTIAL RISK AREAS:

1. **No snapshot team uniqueness guard (Section 2).** `build_features_for_game` does `snapshot_df.set_index("team")` (L174) without verifying index uniqueness. If a snapshot contained duplicate team rows, `snap.loc[team]` would return a DataFrame instead of a Series, causing a downstream crash with an uninformative error. Not a silent mismatch, but no explicit guard and no clear error message.

2. **No NaN guard before calibrator input (Section 6).** No explicit NaN check on `z_mugap` before `calibrator.predict([z_mugap])` at L609. Upstream guards (NaN check in `build_features_for_game` L182-187, spread validation in `normalize_home_spread`) make this practically unreachable, but `IsotonicRegression.predict([NaN])` would return NaN, which would silently propagate through `pick_conf`, `edge`, and unit logic without raising an exception.

3. **Calibrator exception silently degrades to raw probability (Section 6).** L608-611: `try: cover_prob_home = calibrator.predict(...) except: cover_prob_home = raw_cover`. Any calibrator exception silently falls back to the uncalibrated MC probability with no log message. The unit logic at L657-673 (calibrated thresholds) still applies but operates on uncalibrated probabilities, which could produce incorrect unit assignments for the affected game.

4. **OddsAPI bookmaker selection ignores priority order (Section 2 / cross-cutting).** L386-402: The code iterates bookmakers in API response order, accepting the first book that appears in `BOOK_PRIORITY`. It does not iterate in `BOOK_PRIORITY` rank order. If FanDuel appears after DraftKings in the API response, DraftKings is used regardless of FanDuel's higher priority. Spread values may differ by 0.5–1 point between books.

5. **Flip-averaging cancels tempo_avg contribution (Section 10).** The formula `(mu_home − mu_away)/2` cancels not only the intercept (intended — removes phantom HCA) but also `c_tempo × tempo_avg`. If the tempo coefficient is nonzero, the tempo effect is removed from all predictions. This is a structural consequence of the averaging approach, not a sign error, but means μ reflects only differential features and excludes pace effects.

6. **Push games (margin + spread = 0) treated as non-covers (Section 6 / calibrator).** `build_calibrator.py` L210: `home_covered = 1.0 if (margin + spread > 0) else 0.0`. Daily runner uses `sims + spread > 0` (L590). Both use strict `> 0`, so pushes count as non-covers. Consistent between calibrator and simulator, but introduces a small systematic downward bias on cover probabilities since pushes are typically returned (no loss) in actual betting.

7. **Global RNG seed is fragile (Section 5).** L547: `np.random.seed(42)` sets global numpy state. Results are order-dependent — if the game list order changes, each game draws different random values. Any future code change inserting numpy random calls between games would silently alter all downstream simulations.
