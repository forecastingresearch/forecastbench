"""Launch google cloud functions serially."""

import os
import sys
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import nightly_update_workflow_helper as nightly_update  # noqa: E402

from helpers import constants, dates, question_curation  # noqa: E402

metadata = [
    [
        ("func-metadata-tag-questions", False),
        ("func-metadata-validate-questions", False),
    ]
]
resolve_and_leaderboard = [
    [
        ("func-resolve-forecasts", True),
        ("func-leaderboard", True),
    ]
]


def get_curate_questions():
    """Create question set if it's the right day to do so.

    If today is a multiple of 2 weeks after the original freeze date, create a question set.
    """
    original_freeze_date = constants.BENCHMARK_TOURNAMENT_START_DATE_DATETIME_DATE - timedelta(
        days=question_curation.FREEZE_WINDOW_IN_DAYS
    )
    n_days_since_original_freeze_date = (dates.get_date_today() - original_freeze_date).days
    if n_days_since_original_freeze_date % 14 == 0:
        return [
            [
                ("func-curate-questions", True),
            ],
        ]


def get_fetch_and_update():
    """Dynamically add acled to list of functions to call dending on the day of the week."""
    sources = [
        "dbnomics",
        "fred",
        "infer",
        "manifold",
        "metaculus",
        "polymarket",
        "wikipedia",
        "yfinance",
    ]
    day_of_week = dates.get_datetime_today().strftime("%A")
    if day_of_week in ["Tuesday", "Wednesday"]:
        # Add acled to sources to fetch if it's Tuesday or Wednesday.
        # See Issue #115.
        sources += [
            "acled",
        ]
    return [
        [
            (f"func-data-{source}-fetch", True),
            (f"func-data-{source}-update-questions", True),
        ]
        for source in sources
    ]


def sequential_cloud_run_jobs(functions_to_call):
    """Run these cloud jobs in `functions_to_call` sequentially.

    Each entry in `functions_to_call` is a tuple.
    * The first entry is the Cloud Run Job to call.
    * The second is a boolean saying whether or not to quit processing if an error is encountered.
    """
    for function, exit_on_error in functions_to_call:
        operation = nightly_update.cloud_run_job(
            job_name=function,
        )
        nightly_update.block_and_check_job_result(
            operation=operation,
            name=function,
            exit_on_error=exit_on_error,
        )


def main():
    """Given env variables, launch the associtaed Cloud Run Functions.

    Env variables:
    CLOUD_RUN_TASK_INDEX: automatically set by Cloud Run Jobs
    DICT_TO_USE: one of `fetch_and_update`, `metadata`, `resolve_and_leaderboard`.
    """
    dict_mapping = {
        "fetch_and_update": get_fetch_and_update(),
        "metadata": metadata,
        "curate_questions": get_curate_questions(),
        "resolve_and_leaderboard": resolve_and_leaderboard,
    }

    task_num = None
    try:
        task_num = int(os.getenv("CLOUD_RUN_TASK_INDEX"))
    except Exception as e:
        print("ERROR: Unexpected error. Should not arrive here. Error message...")
        print(e)
        sys.exit(1)

    dict_key = os.getenv("DICT_TO_USE")
    if not dict_key or dict_key not in dict_mapping.keys():
        print("ERROR: `DICT_TO_USE` env variable not set or set incorrectly")
        print(os.getenv("DICT_TO_USE"))
        sys.exit(1)

    dict_to_use = dict_mapping.get(dict_key)
    if not dict_to_use:
        print(f"Nothing to be done for `{dict_key}` task.")
        sys.exit(0)

    if task_num >= len(dict_to_use):
        print(f"task number {task_num} not needed, winding down.")
        sys.exit(0)

    if len(dict_to_use[task_num]) != 2:
        print("ERROR: Dictionary incorrectly defined, should not arrive here.")
        print(dict_to_use[task_num])
        sys.exit(1)

    sequential_cloud_run_jobs(functions_to_call=dict_to_use[task_num])


if __name__ == "__main__":
    main()
