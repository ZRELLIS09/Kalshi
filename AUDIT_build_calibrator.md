# Structural Invariant Audit: build_calibrator.py

**Date:** 2026-02-28
**Scope:** 6-section OOS walk-forward calibration logic verification
**File:** `build_calibrator.py` (323 lines)
**Cross-referenced:** `daily_picks_simulator (15).py` (Sections 2, 4)

---

## VERIFIED CORRECT:

### Section 1 — Data Cut-Off
1. **Games are strictly before cutoff date.** L185: `gs = gs[gs["_gd"] < pd.to_datetime(cutoff)].copy()`. Strict `<`. All downstream operations inherit this filter.
2. **No future games leak into OOS pairs.** All rows in `df` satisfy `_gd < cutoff`. Walk-forward folds operate entirely within this filtered set. `date_max` (L238) is bounded by the latest pre-cutoff game.
3. **Correct train/test segmentation.** L243: `tr_mask = dates_all < current` (strictly before boundary). L244: `te_mask = (dates_all >= current) & (dates_all < fold_end)` (within fold window). No overlap.

### Section 3 — Walk-Forward Logic
4. **Folds are sequential.** `current` starts at `warmup_cutoff` and advances by `fold_days` every iteration. Each fold boundary is strictly after the previous.
5. **No overlapping OOS segments.** Fold k: `[current_k, current_k + fold_days)`. Fold k+1: `[current_k + fold_days, current_k + 2*fold_days)`. Contiguous, never overlapping.
6. **Residuals are truly out-of-sample.** Ridge fit on `dates < current`, predictions on `dates >= current`. Model never sees test data.
7. **Minimum training and test sizes enforced.** Per-fold: L249 checks `n_tr < MIN_TRAIN or n_te < MIN_HOLDOUT`. Global: L274 checks `len(oos_z) < MIN_OOS_TOTAL`.

### Section 4 — Z Value Computation (partial)
8. **σ is in-sample reference only.** L283-285: `full_model` is fit on all data. `insample_sigma` is printed at L286 as diagnostic. Never used in z computation, isotonic fit, or output artifact.
9. **z is stored correctly for isotonic training.** Parallel `oos_z.extend()` and `oos_outcomes.extend()` at L262-263 within the same fold iteration. Ordering maintained. Fed to `iso.fit()` at L291.

### Section 5 — Isotonic Fit
10. **Isotonic fit only on OOS pairs.** L291: `iso.fit(oos_z, oos_outcomes)`. Arrays contain exclusively walk-forward OOS data.
11. **Monotonicity enforced.** `IsotonicRegression` enforces non-decreasing by definition (default `increasing=True`). `y_min=0.01, y_max=0.99` clamp output.
12. **No refitting during save.** Between `iso.fit()` (L291) and `joblib.dump()` (L310), only `iso.predict()` is called (spot-check). No fit calls after L291.
13. **Model persistence correct.** `joblib.dump(iso, args.output)` serializes fitted object. Daily runner validates type on load.

### Section 6 — Output Artifact
14. **Artifact contains only calibration mapping.** Bare `IsotonicRegression` object — stores piecewise-linear breakpoints, not raw training data.
15. **No embedded training data.** `X_all`, `y_all`, `oos_z`, `oos_outcomes` are not retained in the serialized object.
16. **File saving logic writes only to user-specified path.** Single `joblib.dump()` to `args.output`.

---

## VERIFIED BROKEN:

1. **z is NOT (μ + spread) / σ. z is μ + spread (raw μ-gap, no σ normalization).**
   - **Location:** L260: `z_te = mu_te + spread_all[te_mask]` with comment `# μ-gap = mu + spread`.
   - **Finding:** No σ division anywhere in the z computation pipeline. σ (L285) is computed as a diagnostic only.
   - **Cross-check:** Simulator L593: `z_mugap = mu + spread` — also no σ division. Calibrator and simulator agree: z = μ + spread. Neither implements (μ + spread) / σ.
   - **Consequence:** Calibration curve is implicitly conditioned on the historical σ of the training data. If σ drifts materially, the mapping needs rebuilding.

2. **μ used for calibration does NOT match simulator μ. Calibrator: single-orientation. Simulator: flip-averaged.**
   - **Location:** Calibrator L259: `mu_te = m.predict(X_all[te_mask])`. Simulator L578: `mu = (mu_home - mu_away) / 2`.
   - **Algebra:** `μ_single = coefs·diffs + c_tempo·tempo_avg + intercept`. `μ_avg = coefs·diffs`. Difference = `c_tempo·tempo_avg + intercept ≈ 2–3 points`.
   - **Consequence:** Systematic shift of ~2–3 points between calibrator training z-distribution and simulator prediction z-distribution. The calibrator operates on a materially different region of its curve at prediction time.

3. **Feature set mismatch: calibrator uses all 15 FEATURE_COLS; simulator drops constant features.**
   - **Location:** Calibrator L220: `X_all = df[FEATURE_COLS].values` (15 features, including constant hca=1.0). Simulator L241-248: drops features with zero variance (`active_cols` ≈ 14 features, hca removed).
   - **Finding:** Calibrator Ridge models train on 15-feature matrix. Simulator Ridge model trains on ~14-feature matrix. Different model families produce different μ values for identical inputs.
   - **Magnitude:** Small in isolation (regularization on one constant feature), but compounds with finding #2.

---

## POTENTIAL RISK AREAS:

1. **NaN filter discrepancy between calibrator and simulator.** Calibrator `build_features_for_game` (L125-127) checks 14 columns. Simulator version (simulator L182-184) checks 15 columns including `"rating"` (not in FEATURE_COLS, not used as a feature). Games with NaN `rating` but valid feature columns would be included in calibrator training but excluded from simulator training — creating a population mismatch.

2. **No guard against narrow isotonic training range.** If OOS z values cluster narrowly, `out_of_bounds="clip"` produces flat tails. No diagnostic checks z-range coverage against expected prediction-time inputs.

3. **Overwrite of existing calibrator without backup.** L310: `joblib.dump()` silently overwrites. A degraded build destroys the previous artifact with no versioning or checksum.

4. **Walk-forward warmup boundary is approximate under date ties.** L235: `warmup_cutoff = dates_all[MIN_TRAIN - 1]`. Multiple games on that date may reduce effective training count below MIN_TRAIN. The per-fold `n_tr < MIN_TRAIN` check (L249) catches this, but the warmup point is not guaranteed to yield exactly MIN_TRAIN games on the first viable fold.

5. **Diagnostic σ is optimistically biased.** L283-284: `full_model.fit(X_all, y_all)` trains on ALL data, including walk-forward test folds. The printed in-sample σ is lower than true OOS σ. Labeled as reference only, but could mislead if compared to the simulator's OOS σ.
