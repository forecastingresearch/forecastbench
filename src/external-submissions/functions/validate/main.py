"""
ForecastBench submission validator — Cloud Function + CLI.

Validates a forecast file against the ForecastBench spec.
Validation logic lives in common/validation.py (single source of truth).

Usage (deployed):
    POST /validate-forecast
    multipart/form-data: file=forecast.json, question_set_file=questions.json
    OR application/json: {"forecast": {...}, "question_set": {...}}

Usage (local CLI):
    python main.py forecast.json question_set.json
"""

import json
import sys
import os

from validation import validate_full


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------

def validate(request):
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    cors = {"Access-Control-Allow-Origin": "*"}

    try:
        if request.content_type and "multipart/form-data" in request.content_type:
            forecast_file = request.files.get("file")
            question_set_file = request.files.get("question_set_file")

            if not forecast_file:
                return (json.dumps({"valid": False, "errors": ["No 'file' provided"]}), 400, cors)
            if not question_set_file:
                return (json.dumps({"valid": False, "errors": ["No 'question_set_file' provided"]}), 400, cors)

            try:
                forecast_data = json.loads(forecast_file.read())
            except json.JSONDecodeError as e:
                return (json.dumps({"valid": False, "errors": [f"Forecast file is not valid JSON: {e}"]}), 400, cors)

            try:
                question_set_data = json.loads(question_set_file.read())
            except json.JSONDecodeError as e:
                return (json.dumps({"valid": False, "errors": [f"Question set file is not valid JSON: {e}"]}), 400, cors)

            filename = forecast_file.filename

        elif request.is_json:
            body = request.get_json()
            forecast_data = body.get("forecast")
            question_set_data = body.get("question_set")
            filename = body.get("filename")

            if not forecast_data or not question_set_data:
                return (json.dumps({"valid": False, "errors": ["Body must contain 'forecast' and 'question_set'"]}), 400, cors)

        else:
            return (json.dumps({"valid": False, "errors": ["Request must be multipart/form-data or application/json"]}), 400, cors)

        result = validate_full(forecast_data, question_set_data, filename)

        try:
            _log_to_firestore(forecast_data, result, filename)
        except Exception:
            pass

        return (json.dumps(result, indent=2), 200 if result["valid"] else 422, cors)

    except Exception as e:
        return (json.dumps({"valid": False, "errors": [f"Internal error: {str(e)}"]}), 500, cors)


def _log_to_firestore(forecast_data, result, filename):
    try:
        from google.cloud import firestore
        db = firestore.Client()
        db.collection("submissions").add({
            "organization": forecast_data.get("organization", "unknown"),
            "model": forecast_data.get("model", "unknown"),
            "filename": filename,
            "valid": result["valid"],
            "error_count": len(result["errors"]),
            "stats": result.get("stats", {}),
            "timestamp": firestore.SERVER_TIMESTAMP,
        })
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <forecast_file.json> <question_set.json>")
        sys.exit(1)

    try:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            forecast_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    try:
        with open(sys.argv[2], "r", encoding="utf-8") as f:
            question_set_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    result = validate_full(forecast_data, question_set_data, os.path.basename(sys.argv[1]))

    print()
    print("=== VALIDATION PASSED ===" if result["valid"] else "=== VALIDATION FAILED ===")

    if result["errors"]:
        print(f"\nERRORS ({len(result['errors'])}):")
        for e in result["errors"]:
            print(f"  - {e}")

    if result["warnings"]:
        print(f"\nWARNINGS ({len(result['warnings'])}):")
        for w in result["warnings"]:
            print(f"  - {w}")

    if result.get("stats"):
        print("\nSTATS:")
        for k, v in result["stats"].items():
            print(f"  {k}: {v}")

    print()
    sys.exit(0 if result["valid"] else 1)


if __name__ == "__main__":
    main()
