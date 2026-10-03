"""Hand-oracle unit tests for the PUBLISHED CIs and significance p-values.

The 95% confidence intervals (``get_confidence_interval``) and the one-sided comparison p-values
(``get_comparison_p_val``: "Supers > Forecaster?" / "Forecaster > Public?") are otherwise untested.
These tests pin them against oracles computed BY HAND from minimal frames:

* Percentile CI (the production default ``method="percentile"``) == ``np.quantile`` at 0.025/0.975.
* Comparison p-value (the production default ``is_centered=False``) == the fraction of bootstrap
  replicates where the *comparison* model scores at/above the *target* model, with the non-obvious
  ``HUMAN_PUBLIC`` direction flip (``1 - p``) and the comparison-model sentinel row pinned
  explicitly.

The BCa branch is selectable but data-entangled (``theta_hat`` + ``norm.ppf`` + ``np.percentile``),
so it is exercised only for a structural invariant (ordered, and equal to percentile when the
bias-correction is zero); what is NOT pinned there is called out in the test docstring.
"""

import numpy as np
import pandas as pd
import pytest

from leaderboard.main import (
    BRIER_INDEX_COL_PREFIX,
    HUMAN_PUBLIC,
    HUMAN_SUPERFORECASTER,
    get_comparison_p_val,
    get_comparison_p_val_col,
    get_confidence_interval,
    two_way_fixed_effects,
)

# ---------------------------------------------------------------------------
# Frame builders. ``df_simulated_scores`` is indexed by model_pk with one column per bootstrap
# replicate; ``df_leaderboard`` has a model_pk column plus a brier_index_<qtype> mean-score column
# and the identity columns (model/organization/model_organization) used to select a comparison.
# ---------------------------------------------------------------------------


def _human_row(comparison: dict, model_pk: str) -> dict:
    """Return a leaderboard identity row matching one of the HUMAN_MODELS comparison dicts."""
    return {
        "model_pk": model_pk,
        "model": comparison["model"],
        "organization": comparison["organization"],
        "model_organization": comparison["model_organization"],
    }


def _make_frames(question_type: str, replicates: dict, mean_scores: dict, identities: dict):
    """Build (df_leaderboard, df_simulated_scores) for the given question type.

    Args:
        question_type (str): "dataset", "market", or "overall".
        replicates (dict): model_pk -> list of bootstrap replicate scores (one per column).
        mean_scores (dict): model_pk -> observed mean score (the brier_index column).
        identities (dict): model_pk -> dict with model/organization/model_organization columns.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: leaderboard and simulated-scores frames.
    """
    score_col = f"{BRIER_INDEX_COL_PREFIX}_{question_type}"
    n_cols = len(next(iter(replicates.values())))
    sim = pd.DataFrame(
        replicates,
        index=[f"b{i}" for i in range(n_cols)],
    ).T  # rows = model_pk, cols = replicates
    sim.index.name = "model_pk"

    rows = []
    for model_pk in replicates:
        row = {"model_pk": model_pk, score_col: mean_scores[model_pk]}
        row.update(identities.get(model_pk, {}))
        rows.append(row)
    leaderboard = pd.DataFrame(rows)
    return leaderboard, sim


# ---------------------------------------------------------------------------
# Percentile confidence interval.
# ---------------------------------------------------------------------------


def test_percentile_ci_equals_np_quantile_at_025_975():
    """CI lower/upper == ``np.quantile(replicates, [0.025, 0.975])`` per model (percentile branch).

    Oracle: for a model whose 9 replicate scores are 0.1..0.9, pandas ``quantile`` (used by the
    function) and ``np.quantile`` share the default linear interpolation, so both endpoints match
    exactly. Exercises ``method="percentile"`` — the production default.
    """
    qtype = "overall"
    a_scores = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    b_scores = [0.05, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.95]
    leaderboard, sim = _make_frames(
        qtype,
        replicates={"A": a_scores, "B": b_scores},
        mean_scores={"A": 0.5, "B": 0.2},
        identities={},
    )

    out = get_confidence_interval(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        question_type=qtype,
        primary_scoring_func=two_way_fixed_effects,
        method="percentile",
    )

    lower_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_lower"
    upper_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_upper"
    by_pk = out.set_index("model_pk")

    for pk, scores in {"A": a_scores, "B": b_scores}.items():
        exp_lower, exp_upper = np.quantile(scores, [0.025, 0.975])
        assert by_pk.loc[pk, lower_col] == exp_lower
        assert by_pk.loc[pk, upper_col] == exp_upper
        # Sanity: the published interval is ordered and brackets the bulk of the replicates.
        assert by_pk.loc[pk, lower_col] < by_pk.loc[pk, upper_col]


def test_percentile_ci_maps_by_model_pk_not_row_order():
    """CI columns are joined on model_pk, so a leaderboard row order != sim order still matches.

    Oracle: build the simulated frame in the OPPOSITE row order from the leaderboard; the function
    maps lower/upper through ``df_leaderboard["model_pk"]``, so each model must still receive its own
    quantiles. Guards the model_pk join (a silent positional bug would mis-assign CIs).
    """
    qtype = "dataset"
    a_scores = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    b_scores = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8]
    leaderboard, sim = _make_frames(
        qtype,
        replicates={"A": a_scores, "B": b_scores},
        mean_scores={"A": 0.4, "B": 1.4},
        identities={},
    )
    # Reverse the simulated frame's row order relative to the leaderboard.
    sim = sim.iloc[::-1]

    out = get_confidence_interval(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        question_type=qtype,
        primary_scoring_func=two_way_fixed_effects,
        method="percentile",
    )
    by_pk = out.set_index("model_pk")
    lower_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_lower"
    upper_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_upper"

    for pk, scores in {"A": a_scores, "B": b_scores}.items():
        exp_lower, exp_upper = np.quantile(scores, [0.025, 0.975])
        assert by_pk.loc[pk, lower_col] == exp_lower
        assert by_pk.loc[pk, upper_col] == exp_upper


def test_bca_ci_is_ordered_and_reduces_to_percentile_when_unbiased():
    """BCa is a structural-invariant test (NOT a full hand oracle).

    The BCa branch uses ``theta_hat`` (the observed mean score), ``z0 = norm.ppf(P[bs < theta])``,
    and per-row ``np.percentile`` at bias-shifted alphas — too entangled to hand-verify in general.
    What IS pinned here: when the observed mean equals the replicate median, the bias-correction
    ``z0`` is 0, so the shifted alphas collapse back to (2.5, 97.5) and the BCa endpoints equal the
    plain ``np.percentile`` quantiles. We also assert lower < upper. What is NOT pinned: the
    bias-corrected endpoints for a biased ``theta_hat`` (the data-dependent shift).
    """
    qtype = "overall"
    # Symmetric replicates whose median (0.5) equals the observed mean -> exactly half are < mean.
    scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    leaderboard, sim = _make_frames(
        qtype,
        replicates={"A": scores},
        mean_scores={"A": 0.55},  # mean of the 10 values -> P(bs < theta) == 0.5 -> z0 == 0
        identities={},
    )

    out = get_confidence_interval(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        question_type=qtype,
        primary_scoring_func=two_way_fixed_effects,
        method="bca",
    )
    by_pk = out.set_index("model_pk")
    lower_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_lower"
    upper_col = f"{BRIER_INDEX_COL_PREFIX}_{qtype}_ci_upper"

    assert by_pk.loc["A", lower_col] < by_pk.loc["A", upper_col]
    # With z0 == 0 the alpha shift vanishes; BCa endpoints collapse to the plain percentiles.
    # ``approx`` only absorbs the float dust from BCa re-deriving 2.5/97.5 via norm.cdf(norm.ppf(.));
    # the invariant (no bias shift -> percentile endpoints) is exact.
    assert by_pk.loc["A", lower_col] == pytest.approx(np.percentile(scores, 2.5), abs=1e-9)
    assert by_pk.loc["A", upper_col] == pytest.approx(np.percentile(scores, 97.5), abs=1e-9)


# ---------------------------------------------------------------------------
# Comparison p-values.
# ---------------------------------------------------------------------------

# Shared construction for the p-value tests.
#   target "T"  replicates: [ 5, 15, 15, 15]
#   comparison "C" (a human) replicates: [10, 10, 10, 10]
# The function computes, per replicate, whether the TARGET model scores >= the COMPARISON model:
#   T >= C ?  ->  [5>=10, 15>=10, 15>=10, 15>=10] = [F, T, T, T]  -> mean = 0.75
# For SUPERFORECASTER that 0.75 is reported directly; for PUBLIC it is flipped to 1 - 0.75 = 0.25.
_T_SCORES = [5.0, 15.0, 15.0, 15.0]
_C_SCORES = [10.0, 10.0, 10.0, 10.0]
_EXPECTED_FRAC = 0.75  # fraction of replicates with T >= C


def _pval_frames(comparison: dict):
    """Build frames where 'C' is the comparison human model and 'T' is the target model."""
    return _make_frames(
        "overall",
        replicates={"T": _T_SCORES, "C": _C_SCORES},
        mean_scores={"T": 10.0, "C": 10.0},
        identities={
            "T": {
                "model": "Some LLM",
                "organization": "OrgA",
                "model_organization": "OrgA",
            },
            "C": _human_row(comparison, "C"),
        },
    )


def test_superforecaster_pval_is_fraction_target_ge_comparison():
    """SUPERFORECASTER p-value == fraction of replicates where target >= comparison (no flip).

    Oracle: T>=C over replicates is [F,T,T,T] -> 0.75. This is reported as-is for the
    Superforecaster comparison ("Supers > Forecaster?"). The comparison model's OWN row is the
    sentinel -1 at this stage.
    """
    leaderboard, sim = _pval_frames(HUMAN_SUPERFORECASTER)
    out = get_comparison_p_val(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        comparison=HUMAN_SUPERFORECASTER,
        question_type="overall",
    )
    out_col = get_comparison_p_val_col(HUMAN_SUPERFORECASTER)
    by_pk = out.set_index("model_pk")

    assert by_pk.loc["T", out_col] == _EXPECTED_FRAC  # 0.75, no direction flip
    # The comparison's own row carries no p-value (the display layer renders it as "—"): any
    # placeholder outside [0, 1] or NaN is acceptable, a real-looking probability is not.
    assert not 0 <= by_pk.loc["C", out_col] <= 1


def test_public_pval_is_one_minus_fraction_direction_flip():
    """PUBLIC p-value == 1 - fraction (the non-obvious HUMAN_PUBLIC direction flip).

    Oracle: the raw fraction is 0.75; HUMAN_PUBLIC flips direction ("Forecaster > Public?") via
    ``df[out_col] = 1 - df[out_col]``, so the target reports 1 - 0.75 = 0.25. The comparison's own
    row must still carry no p-value (the display code renders it as '—').
    """
    leaderboard, sim = _pval_frames(HUMAN_PUBLIC)
    out = get_comparison_p_val(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        comparison=HUMAN_PUBLIC,
        question_type="overall",
    )
    out_col = get_comparison_p_val_col(HUMAN_PUBLIC)
    by_pk = out.set_index("model_pk")

    # Target: flipped fraction.
    assert by_pk.loc["T", out_col] == 1 - _EXPECTED_FRAC  # 0.25
    # Comparison's own row: still not a probability after the flip.
    assert not 0 <= by_pk.loc["C", out_col] <= 1


def test_super_and_public_pvals_are_complementary_for_same_target():
    """Cross-check: for an identical target/comparison pair, PUBLIC p == 1 - SUPERFORECASTER p.

    Pins the flip as the ONLY difference between the two human comparisons (same ge() fraction,
    opposite reported direction), so the flip cannot silently no-op or apply twice.
    """
    super_lb, super_sim = _pval_frames(HUMAN_SUPERFORECASTER)
    public_lb, public_sim = _pval_frames(HUMAN_PUBLIC)

    super_out = get_comparison_p_val(
        df_leaderboard=super_lb,
        df_simulated_scores=super_sim,
        comparison=HUMAN_SUPERFORECASTER,
        question_type="overall",
    )
    public_out = get_comparison_p_val(
        df_leaderboard=public_lb,
        df_simulated_scores=public_sim,
        comparison=HUMAN_PUBLIC,
        question_type="overall",
    )
    super_p = super_out.set_index("model_pk").loc[
        "T", get_comparison_p_val_col(HUMAN_SUPERFORECASTER)
    ]
    public_p = public_out.set_index("model_pk").loc["T", get_comparison_p_val_col(HUMAN_PUBLIC)]

    assert public_p == 1 - super_p


def test_comparison_pval_uses_ge_not_gt_at_ties():
    """The comparison uses ``>=`` (ties count as the target reaching the comparison), not ``>``.

    Oracle: with target replicates exactly equal to the comparison's, every replicate satisfies
    ``T >= C``, so the Superforecaster fraction is 1.0. A ``>`` implementation would yield 0.0,
    making this a discriminating test of the boundary direction.
    """
    leaderboard, sim = _make_frames(
        "overall",
        replicates={"T": [10.0, 10.0, 10.0, 10.0], "C": [10.0, 10.0, 10.0, 10.0]},
        mean_scores={"T": 10.0, "C": 10.0},
        identities={
            "T": {"model": "Tie LLM", "organization": "OrgA", "model_organization": "OrgA"},
            "C": _human_row(HUMAN_SUPERFORECASTER, "C"),
        },
    )
    out = get_comparison_p_val(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        comparison=HUMAN_SUPERFORECASTER,
        question_type="overall",
    )
    out_col = get_comparison_p_val_col(HUMAN_SUPERFORECASTER)
    assert out.set_index("model_pk").loc["T", out_col] == 1.0
