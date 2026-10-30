"""Hand-oracle unit tests for the published confidence intervals and comparison p-values.

The percentile CI equals ``np.quantile`` at 0.025/0.975; the p-value is the fraction of bootstrap
replicates where the target scores at or above the comparison, flipped (``1 - p``) for
``HUMAN_PUBLIC``. The BCa branch is pinned only for a structural invariant.
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
        replicates (dict): Bootstrap replicate scores per model_pk (one per column).
        mean_scores (dict): Observed mean score per model_pk (the brier_index column).
        identities (dict): Identity columns per model_pk (model, organization,
            model_organization), which select a comparison.

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


def test_percentile_ci_equals_np_quantile_at_025_975():
    """The percentile CI endpoints equal ``np.quantile(replicates, [0.025, 0.975])`` per model.

    pandas ``quantile`` and ``np.quantile`` share linear interpolation, so they match exactly.
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
        assert by_pk.loc[pk, lower_col] < by_pk.loc[pk, upper_col]


def test_percentile_ci_maps_by_model_pk_not_row_order():
    """CI columns are joined on model_pk, so a simulated frame in reverse order still matches."""
    qtype = "dataset"
    a_scores = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    b_scores = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8]
    leaderboard, sim = _make_frames(
        qtype,
        replicates={"A": a_scores, "B": b_scores},
        mean_scores={"A": 0.4, "B": 1.4},
        identities={},
    )
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
    """BCa endpoints are ordered and equal the plain percentiles when the bias correction is zero.

    With the observed mean at the replicate median, ``z0 = norm.ppf(P[bs < theta])`` is 0 and the
    shifted alphas collapse to (2.5, 97.5). The endpoints for a biased ``theta_hat`` are not pinned.
    """
    qtype = "overall"
    # Symmetric replicates: exactly half lie below the observed mean.
    scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    leaderboard, sim = _make_frames(
        qtype,
        replicates={"A": scores},
        mean_scores={"A": 0.55},  # the mean of the 10 values
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
    # ``approx`` absorbs only the float error of re-deriving 2.5/97.5 through norm.cdf(norm.ppf(.)).
    assert by_pk.loc["A", lower_col] == pytest.approx(np.percentile(scores, 2.5), abs=1e-9)
    assert by_pk.loc["A", upper_col] == pytest.approx(np.percentile(scores, 97.5), abs=1e-9)


# Per replicate, the target T scores >= the human comparison C in [F, T, T, T], a fraction of 0.75;
# SUPERFORECASTER reports it as is, PUBLIC as 1 - 0.75.
_T_SCORES = [5.0, 15.0, 15.0, 15.0]
_C_SCORES = [10.0, 10.0, 10.0, 10.0]
_EXPECTED_FRAC = 0.75


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
    """The Superforecaster p-value is the fraction of replicates where target >= comparison."""
    leaderboard, sim = _pval_frames(HUMAN_SUPERFORECASTER)
    out = get_comparison_p_val(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        comparison=HUMAN_SUPERFORECASTER,
        question_type="overall",
    )
    out_col = get_comparison_p_val_col(HUMAN_SUPERFORECASTER)
    by_pk = out.set_index("model_pk")

    assert by_pk.loc["T", out_col] == _EXPECTED_FRAC
    # The comparison's own row carries no p-value (the display layer renders it as "—"): any
    # placeholder outside [0, 1] or NaN is acceptable, a real-looking probability is not.
    assert not 0 <= by_pk.loc["C", out_col] <= 1


def test_public_pval_is_one_minus_fraction_direction_flip():
    """The Public p-value is ``1 - fraction`` ("Forecaster > Public?")."""
    leaderboard, sim = _pval_frames(HUMAN_PUBLIC)
    out = get_comparison_p_val(
        df_leaderboard=leaderboard,
        df_simulated_scores=sim,
        comparison=HUMAN_PUBLIC,
        question_type="overall",
    )
    out_col = get_comparison_p_val_col(HUMAN_PUBLIC)
    by_pk = out.set_index("model_pk")

    assert by_pk.loc["T", out_col] == 1 - _EXPECTED_FRAC
    # The comparison's own row stays a non-probability placeholder after the flip.
    assert not 0 <= by_pk.loc["C", out_col] <= 1


def test_super_and_public_pvals_are_complementary_for_same_target():
    """For the same pair, PUBLIC p is 1 - SUPERFORECASTER p: the flip neither no-ops nor doubles."""
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
    """Ties count for the target (``>=``): equal replicates give 1.0, where ``>`` would give 0.0."""
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
