"""Launch google cloud functions serially."""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import cloud_run, dates, question_curation  # noqa: E402

metadata = [
    [
        ("func-metadata-tag-questions", False, cloud_run.timeout_1h, 1),
        ("func-metadata-validate-questions", False, cloud_run.timeout_1h, 1),
    ]
]
resolve_and_leaderboard = [
    [
        ("func-resolve-forecasts", True, cloud_run.timeout_1h * 2, 50),
        ("func-leaderboard", True, cloud_run.timeout_1h * 2, 1),
    ]
]

website = [
    [
        ("func-website", True, cloud_run.timeout_1h, 1),
    ]
]


def get_naive_and_dummy_forecasters():
    """Generate the naive and dummy forecasts if the question set was published today.

    This is in a separate call to the worker because we want to call it after fetch and update.
    """
    return (
        [
            [
                ("func-baseline-naive-and-dummy-forecasters", True, cloud_run.timeout_1h * 2, 1),
            ],
        ]
        if question_curation.is_today_question_set_publication_date()
        else None
    )


def get_create_question_set():
    """Create question set if it's the right day to do so.

    If today is a multiple of 2 weeks after the original freeze date, create a question set.
    """
    return (
        [
            [
                ("func-question-set-create", True, cloud_run.timeout_1h, 1),
            ],
        ]
        if question_curation.is_today_question_curation_date()
        else None
    )


def get_publish_question_set_make_llm_baseline():
    """Publish the question set if it's the right day to do so.

    If today is a multiple of 2 weeks after the original forecast due date, publish the question
    set.
    """
    return (
        [
            [
                ("func-question-set-publish", True, cloud_run.timeout_1h, 1),
                ("func-baseline-llm-forecasts-manager", True, cloud_run.timeout_1h * 24, 1),
            ],
        ]
        if question_curation.is_today_question_set_publication_date()
        else None
    )


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
            (f"func-data-{source}-fetch", True, cloud_run.timeout_1h * 1.5, 1),
            (f"func-data-{source}-update-questions", True, cloud_run.timeout_1h * 1.5, 1),
        ]
        for source in sources
    ]


def sequential_cloud_run_jobs(functions_to_call):
    """Run these cloud jobs in `functions_to_call` sequentially.

    Each entry in `functions_to_call` is a tuple.
    * The first entry is the Cloud Run Job to call.
    * The second is a boolean saying whether or not to quit processing if an error is encountered.
    """
    for function, exit_on_error, timeout, task_count in functions_to_call:
        operation = cloud_run.run_job(
            job_name=function,
            timeout=timeout,
            task_count=task_count,
        )
        cloud_run.block_and_check_job_result(
            operation=operation,
            name=function,
            exit_on_error=exit_on_error,
            timeout=timeout,
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
        "create_question_set": get_create_question_set(),
        "publish_question_set_make_llm_baseline": get_publish_question_set_make_llm_baseline(),
        "resolve_and_leaderboard": resolve_and_leaderboard,
        "naive_and_dummy_forecasters": get_naive_and_dummy_forecasters(),
        "website": website,
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

    sequential_cloud_run_jobs(functions_to_call=dict_to_use[task_num])


if __name__ == "__main__":
    main()
