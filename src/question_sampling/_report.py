"""Log and plot how the sampled questions compare with the available ones."""

import logging
from datetime import datetime

import pandas as pd

from _fb_types import QuestionSetTarget

from ._binning import (
    MARKET_VALUE_CONFIG,
    TIME_HORIZON_CONFIG,
    add_bin_columns,
    get_bin_label,
)
from ._prepare import is_market_source

logger = logging.getLogger(__name__)


def _log_bin_dimension(
    dim_name: str,
    bin_configs: list[dict],
    bin_type: str,
    sampled_series: pd.Series,
    available_series: pd.Series,
    n_target: int,
    prefix: str,
) -> list[tuple[str, int, int]]:
    """Log sampling results for a single bin dimension and return shortfall descriptions.

    Args:
        dim_name (str): Display name for the dimension (e.g., "Market Value")
        bin_configs (list[dict]): Bin config dicts with min, max, weight keys
        bin_type (str): Bin type for get_bin_label ("market_value" or "time_horizon")
        sampled_series (pd.Series): Bin labels for sampled questions
        available_series (pd.Series): Bin labels for all available questions
        n_target (int): Total number of questions targeted for this source
        prefix (str): Short prefix for shortfall labels (e.g., "MV", "TH")

    Returns
        shortfalls (list[tuple[str, int, int]]): (label, got, want) for bins below target
    """
    shortfalls = []
    sampled_counts = sampled_series.value_counts()
    available_counts = available_series.value_counts()
    max_label_len = max(len(get_bin_label(c, bin_type)) for c in bin_configs)
    max_label_len = max(max_label_len, len(dim_name))

    logger.info("")
    logger.info(f"  {dim_name:<{max_label_len}}  Got/Want [Avail]")
    for config in bin_configs:
        bin_label = get_bin_label(config, bin_type)
        want = int(round(n_target * config["weight"]))
        got = int(sampled_counts.get(bin_label, 0))
        avail = int(available_counts.get(bin_label, 0))
        marker = " *" if got < want and avail <= want else ""
        logger.info(f"  {bin_label:<{max_label_len}}  {got:>3}/{want:<3} [{avail:>5}]{marker}")
        if got < want:
            shortfalls.append((f"{prefix} {bin_label}", got, want))

    got_unknown = int(sampled_counts.get("unknown", 0))
    avail_unknown = int(available_counts.get("unknown", 0))
    if avail_unknown > 0 or got_unknown > 0:
        logger.info(
            f"  {'unknown':<{max_label_len}}  {got_unknown:>3}/{'0':<3} [{avail_unknown:>5}]"
        )

    return shortfalls


def log_source_sampling(
    source: str,
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
    forecast_datetime: datetime,
) -> list[tuple[str, int, int]]:
    """Log compact sampling results for a single source.

    For market sources, shows market value and time horizon bin distributions.
    For data sources, shows category distribution.

    Args:
        source (str): Source name (e.g., "polymarket", "acled")
        df_sampled (pd.DataFrame): Sampled questions
        df_available (pd.DataFrame): All available questions for this source
        n_target (int): Number of questions targeted for this source
        forecast_datetime (datetime): The forecast due date the time horizon is measured from

    Returns
        shortfalls (list[tuple[str, int, int]]): (label, got, want) for bins below target
    """
    got = len(df_sampled)
    shortfalls = []

    logger.info("")
    logger.info(f"{source}  {got}/{n_target} sampled from {len(df_available):,} available")

    if is_market_source(source):
        df_sampled_bins = add_bin_columns(df_sampled.copy(), forecast_datetime)
        df_available_bins = add_bin_columns(df_available.copy(), forecast_datetime)

        shortfalls.extend(
            _log_bin_dimension(
                "Market Value",
                MARKET_VALUE_CONFIG,
                "market_value",
                df_sampled_bins["market_value_bin"],
                df_available_bins["market_value_bin"],
                n_target,
                prefix="MV",
            )
        )
        shortfalls.extend(
            _log_bin_dimension(
                "Time Horizon",
                TIME_HORIZON_CONFIG,
                "time_horizon",
                df_sampled_bins["time_horizon_bin"],
                df_available_bins["time_horizon_bin"],
                n_target,
                prefix="TH",
            )
        )
    else:
        # Data source - show category distribution
        categories = sorted(df_available["category"].unique())
        n_categories = len(categories)
        ideal_per_cat = n_target / n_categories if n_categories > 0 else 0

        sampled_counts = df_sampled["category"].value_counts()
        available_counts = df_available["category"].value_counts()

        max_cat_len = max((len(c) for c in categories), default=8)
        max_cat_len = max(max_cat_len, len("Category"))

        logger.info("")
        logger.info(f"  {'Category':<{max_cat_len}}  Got/Want [Avail]")
        for cat in categories:
            got_cat = int(sampled_counts.get(cat, 0))
            avail_cat = int(available_counts.get(cat, 0))
            want_cat = int(round(ideal_per_cat))
            marker = " *" if got_cat < want_cat and avail_cat <= want_cat else ""
            logger.info(
                f"  {cat:<{max_cat_len}}  {got_cat:>3}/{want_cat:<3} [{avail_cat:>5}]{marker}"
            )
            if got_cat < want_cat:
                shortfalls.append((cat, got_cat, want_cat))

    return shortfalls


def log_all_market_source_sampling(
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
    question_set_target: QuestionSetTarget,
    forecast_datetime: datetime,
) -> None:
    """Log market-value/time-horizon sampling results across all market sources combined.

    Args:
        df_sampled (pd.DataFrame): Sampled market questions
        df_available (pd.DataFrame): All available market questions
        n_target (int): Number of market questions targeted
        question_set_target (QuestionSetTarget): Question set target
        forecast_datetime (datetime): The forecast due date the time horizon is measured from
    """
    separator_len = 65
    logger.info("")
    logger.info(f"{'=' * separator_len}")
    logger.info(
        f"ALL MARKET SOURCES ({question_set_target.name}) "
        f"{len(df_sampled)}/{n_target} sampled from {len(df_available):,} available"
    )
    logger.info(f"{'=' * separator_len}")

    df_sampled_bins = add_bin_columns(df_sampled.copy(), forecast_datetime)
    df_available_bins = add_bin_columns(df_available.copy(), forecast_datetime)

    _log_bin_dimension(
        "Market Value",
        MARKET_VALUE_CONFIG,
        "market_value",
        df_sampled_bins["market_value_bin"],
        df_available_bins["market_value_bin"],
        n_target,
        prefix="MV",
    )
    _log_bin_dimension(
        "Time Horizon",
        TIME_HORIZON_CONFIG,
        "time_horizon",
        df_sampled_bins["time_horizon_bin"],
        df_available_bins["time_horizon_bin"],
        n_target,
        prefix="TH",
    )


def log_sampling_summary(
    source_summaries: list[tuple[str, int, int, list]],
    question_set_target: QuestionSetTarget,
) -> None:
    """Log a compact summary table of all sources.

    Args:
        source_summaries (list[tuple]): (source, got, want, shortfalls) per source
        question_set_target (QuestionSetTarget): Question set target
    """
    header = f"SAMPLING SUMMARY ({question_set_target.name})"

    max_name = max((len(s[0]) for s in source_summaries), default=10)
    max_name = max(max_name, len("Source"))
    separator_len = max_name + 55

    logger.info("")
    logger.info(f"{'=' * separator_len}")
    logger.info(header)
    logger.info(f"{'=' * separator_len}")
    logger.info(f"  {'Source':<{max_name}}   Got/Want  Biggest shortfall")
    logger.info(f"  {'-' * (separator_len - 2)}")

    total_got = 0
    total_want = 0
    for source, got, want, shortfalls in source_summaries:
        total_got += got
        total_want += want
        if shortfalls:
            # Find the biggest shortfall by absolute gap (want - got)
            biggest = max(shortfalls, key=lambda s: s[2] - s[1])
            shortfall_str = f"* {biggest[0]}: {biggest[1]}/{biggest[2]}"
        else:
            shortfall_str = ""
        logger.info(f"  {source:<{max_name}}  {got:>4}/{want:<4} {shortfall_str}")

    logger.info(f"  {'-' * (separator_len - 2)}")
    logger.info(f"  {'Total':<{max_name}}  {total_got:>4}/{total_want:<4}")


def plot_sampling_distribution(
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
    source_name: str | None = None,
    source_counts: list[tuple[str, int, int]] | None = None,
) -> None:
    """Plot the realized vs expected distribution for market question sampling.

    Creates line charts comparing Available, Selected, and Target distributions
    for both market value bins and time horizon bins. Meant for local runs only; plotly is
    imported here so that it is not a dependency of the deployed job.

    Args:
        df_sampled (pd.DataFrame): Sampled questions (must include bin columns)
        df_available (pd.DataFrame): All available questions (must include bin columns)
        n_target (int): Number of questions requested
        source_name (str | None): Name for the source (used in chart title)
        source_counts (list[tuple[str, int, int]] | None): (source, sampled, target) per source,
            charted as an extra row when given
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Ensure bin columns exist
    required_cols = ["market_value_bin", "time_horizon_bin"]
    for col in required_cols:
        if col not in df_sampled.columns or col not in df_available.columns:
            logger.warning(f"Column '{col}' missing. Skipping plot.")
            return

    title = "Sampling Distribution"
    if source_name:
        title = f"Sampling Distribution: {source_name}"
    title += (
        f"<br><sub>{len(df_sampled):,}/{n_target:,} sampled "
        f"from {len(df_available):,} available</sub>"
    )

    subplot_titles = ["Market Value", "Time Horizon", "Category"]
    if source_counts:
        subplot_titles.append("Questions by Source")

    fig = make_subplots(
        rows=len(subplot_titles),
        cols=1,
        subplot_titles=tuple(subplot_titles),
        vertical_spacing=0.1,
    )

    def get_distribution_data(
        bin_configs: list[dict],
        bin_type: str,
        col_name: str,
    ) -> tuple[list[str], list[int], list[int], list[float]]:
        """Extract distribution data for a dimension."""
        bin_labels = [get_bin_label(config, bin_type) for config in bin_configs]
        weights = {get_bin_label(c, bin_type): c["weight"] for c in bin_configs}

        # Check if unknown is present
        has_unknown = (
            "unknown" in df_sampled[col_name].values or "unknown" in df_available[col_name].values
        )
        if has_unknown:
            bin_labels.append("unknown")
            weights["unknown"] = 0

        # Count occurrences in each bin
        available_counts = df_available[col_name].value_counts()
        selected_counts = df_sampled[col_name].value_counts()

        available_data = [int(available_counts.get(lbl, 0)) for lbl in bin_labels]
        selected_data = [int(selected_counts.get(lbl, 0)) for lbl in bin_labels]
        target_data = [n_target * weights.get(lbl, 0) for lbl in bin_labels]

        return bin_labels, available_data, selected_data, target_data

    def add_line_chart(
        bin_labels: list[str],
        available_data: list[int],
        selected_data: list[int],
        target_data: list[float],
        row: int,
        show_legend: bool,
    ) -> None:
        """Add line chart with spline interpolation."""
        x_numeric = list(range(len(bin_labels)))

        fig.add_trace(
            go.Scatter(
                name="Available",
                x=x_numeric,
                y=available_data,
                mode="lines+markers",
                line=dict(color="gray", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="available",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="Selected",
                x=x_numeric,
                y=selected_data,
                mode="lines+markers",
                line=dict(color="steelblue", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="selected",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="Target",
                x=x_numeric,
                y=target_data,
                mode="lines+markers",
                line=dict(color="coral", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="target",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )

        # Update x-axis to show bin labels
        fig.update_xaxes(
            tickmode="array",
            tickvals=x_numeric,
            ticktext=bin_labels,
            tickangle=45,
            row=row,
            col=1,
        )

    # Market value distribution
    mv_labels, mv_avail, mv_sel, mv_target = get_distribution_data(
        MARKET_VALUE_CONFIG, "market_value", "market_value_bin"
    )
    add_line_chart(mv_labels, mv_avail, mv_sel, mv_target, row=1, show_legend=True)

    # Time horizon distribution
    th_labels, th_avail, th_sel, th_target = get_distribution_data(
        TIME_HORIZON_CONFIG, "time_horizon", "time_horizon_bin"
    )
    add_line_chart(th_labels, th_avail, th_sel, th_target, row=2, show_legend=False)

    # Category distribution (bar chart)
    categories = sorted(df_available["category"].unique())
    avail_counts = df_available["category"].value_counts()
    sampled_counts = df_sampled["category"].value_counts()
    cat_avail = [int(avail_counts.get(cat, 0)) for cat in categories]
    cat_sampled = [int(sampled_counts.get(cat, 0)) for cat in categories]

    fig.add_trace(
        go.Bar(
            name="Available",
            x=categories,
            y=cat_avail,
            marker=dict(color="gray", opacity=0.5),
            legendgroup="available",
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Selected",
            x=categories,
            y=cat_sampled,
            marker=dict(color="steelblue"),
            legendgroup="selected",
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.update_xaxes(tickangle=45, row=3, col=1)

    # Number of questions sampled from each source
    if source_counts:
        source_row = len(subplot_titles)
        source_labels = [source for source, _, _ in source_counts]
        # The target bar is never shorter than the selected bar, so labeling it keeps the
        # sampled/target counts clear of both bars.
        fig.add_trace(
            go.Bar(
                name="Target",
                x=source_labels,
                y=[target for _, _, target in source_counts],
                text=[f"{sampled}/{target}" for _, sampled, target in source_counts],
                textposition="outside",
                marker=dict(color="coral", opacity=0.5),
                legendgroup="target",
                showlegend=False,
            ),
            row=source_row,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                name="Selected",
                x=source_labels,
                y=[sampled for _, sampled, _ in source_counts],
                marker=dict(color="steelblue"),
                legendgroup="selected",
                showlegend=False,
            ),
            row=source_row,
            col=1,
        )
        fig.update_xaxes(tickangle=45, row=source_row, col=1)

    fig.update_layout(
        title_text=title,
        height=250 + 250 * len(subplot_titles),
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig.show()
