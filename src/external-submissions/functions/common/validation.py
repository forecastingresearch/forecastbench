"""
ForecastBench forecast file validation logic.

Single source of truth for all validation rules.
Used by both the validate Cloud Function and the on_submission trigger.

Eventually to be PR'd into the main forecastbench repository.
"""

from datetime import datetime

MARKET_SOURCES = ["infer", "manifold", "metaculus", "polymarket"]
DATASET_SOURCES = ["acled", "dbnomics", "fred", "wikipedia", "yfinance"]
COVERAGE_THRESHOLD = 0.95
MAX_SUBMISSIONS_PER_ROUND = 3


def validate_filename(filename, expected_due_date=None):
    errors = []
    if not filename.endswith(".json"):
        errors.append(f"Filename '{filename}' must end with .json")
        return errors

    parts = filename.replace(".json", "").split(".")
    if len(parts) < 3:
        errors.append(
            f"Filename '{filename}' must match {{round_date}}.{{organization}}.{{N}}.json"
        )
        return errors

    date_part, n_part = parts[0], parts[-1]

    try:
        datetime.strptime(date_part, "%Y-%m-%d")
    except ValueError:
        errors.append(f"Filename date '{date_part}' is not a valid ISO date (YYYY-MM-DD)")

    if not n_part.isdigit():
        errors.append(f"Filename submission number '{n_part}' must be a digit")

    if expected_due_date and date_part != expected_due_date:
        errors.append(
            f"Filename date '{date_part}' doesn't match round date '{expected_due_date}'"
        )

    return errors


def validate_top_level(forecast_data):
    errors = []
    for key in ["organization", "model", "model_organization", "question_set", "forecasts"]:
        if key not in forecast_data:
            errors.append(f"Missing required key: '{key}'")

    for key, expected_type in [
        ("organization", str), ("model", str),
        ("model_organization", str), ("question_set", str),
    ]:
        if key in forecast_data and not isinstance(forecast_data[key], expected_type):
            errors.append(f"'{key}' must be a string")

    if "forecasts" in forecast_data:
        if not isinstance(forecast_data["forecasts"], list):
            errors.append("'forecasts' must be an array")
        elif len(forecast_data["forecasts"]) == 0:
            errors.append("'forecasts' array is empty")

    return errors


def validate_forecasts(forecast_data, question_set_data):
    errors = []
    warnings = []

    forecasts = forecast_data.get("forecasts", [])
    questions = question_set_data.get("questions", [])

    def _qkey(qid, source):
        return (tuple(qid) if isinstance(qid, list) else qid, source)
    question_lookup = {_qkey(q["id"], q["source"]): q for q in questions}
    market_questions = [q for q in questions if q["source"] in MARKET_SOURCES]
    dataset_questions = [q for q in questions if q["source"] in DATASET_SOURCES]

    if forecast_data.get("question_set") != question_set_data.get("question_set"):
        errors.append(
            f"'question_set' value '{forecast_data.get('question_set')}' doesn't match "
            f"'{question_set_data.get('question_set')}'"
        )

    market_ids_covered = set()
    dataset_forecasts_covered = set()
    unknown_questions = []

    for f in forecasts:
        fid = f.get("id", "?")
        source = f.get("source", "?")
        label = f"Forecast (id='{fid}', source='{source}')"

        missing = [k for k in ["id", "source", "forecast", "resolution_date"] if k not in f]
        if missing:
            errors.append(f"{label}: missing fields {missing}")
            continue

        if not isinstance(f["forecast"], (int, float)):
            errors.append(f"{label}: forecast must be a number")
        elif not 0 <= f["forecast"] <= 1:
            errors.append(f"{label}: forecast value {f['forecast']} not in [0, 1]")

        if source not in MARKET_SOURCES and source not in DATASET_SOURCES:
            errors.append(f"{label}: unrecognized source '{source}'")
            continue

        if (fid, source) not in question_lookup:
            unknown_questions.append((fid, source))
            continue

        if source in MARKET_SOURCES:
            if f["resolution_date"] is not None:
                errors.append(f"{label}: market questions must have resolution_date = null")
            market_ids_covered.add(fid)

        elif source in DATASET_SOURCES:
            if f["resolution_date"] is None:
                errors.append(f"{label}: dataset questions must have a resolution_date")
            else:
                expected = question_lookup[(fid, source)].get("resolution_dates", [])
                if isinstance(expected, list) and f["resolution_date"] not in expected:
                    warnings.append(
                        f"{label}: resolution_date '{f['resolution_date']}' not in expected dates"
                    )
                dataset_forecasts_covered.add((fid, f["resolution_date"]))

    if unknown_questions:
        warnings.append(
            f"{len(unknown_questions)} forecast(s) reference questions not in the question set "
            f"(first 5: {unknown_questions[:5]})"
        )

    market_total = len(market_questions)
    market_covered = len(market_ids_covered)
    market_pct = market_covered / market_total if market_total else 0

    dataset_ids_covered = set(qid for qid, _ in dataset_forecasts_covered)
    dataset_total = len(dataset_questions)
    dataset_covered = len(dataset_ids_covered)
    dataset_pct = dataset_covered / dataset_total if dataset_total else 0

    if market_total > 0 and market_pct < COVERAGE_THRESHOLD:
        errors.append(
            f"Market coverage {market_pct:.1%} below 95% "
            f"({market_covered}/{market_total}). Will NOT appear on leaderboard."
        )
    if dataset_total > 0 and dataset_pct < COVERAGE_THRESHOLD:
        errors.append(
            f"Dataset coverage {dataset_pct:.1%} below 95% "
            f"({dataset_covered}/{dataset_total}). Will NOT appear on leaderboard."
        )

    return {
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "total_forecasts": len(forecasts),
            "market_coverage": f"{market_pct:.1%}",
            "market_covered": market_covered,
            "market_total": market_total,
            "dataset_coverage": f"{dataset_pct:.1%}",
            "dataset_covered": dataset_covered,
            "dataset_total": dataset_total,
        },
    }


def validate_full(forecast_data, question_set_data, filename=None):
    """
    Runs all validation checks. Returns:
        {"valid": bool, "errors": [...], "warnings": [...], "stats": {...}}
    """
    all_errors = []
    all_warnings = []

    if filename:
        all_errors.extend(
            validate_filename(filename, question_set_data.get("forecast_due_date"))
        )

    all_errors.extend(validate_top_level(forecast_data))

    if not isinstance(forecast_data.get("forecasts"), list):
        return {"valid": False, "errors": all_errors, "warnings": [], "stats": {}}

    result = validate_forecasts(forecast_data, question_set_data)
    all_errors.extend(result["errors"])
    all_warnings.extend(result["warnings"])

    return {
        "valid": len(all_errors) == 0,
        "errors": all_errors,
        "warnings": all_warnings,
        "stats": result["stats"],
    }
