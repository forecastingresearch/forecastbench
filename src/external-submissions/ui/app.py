import json
import os
import subprocess
import requests
from flask import Flask, render_template, jsonify, send_file, request
from google.cloud import firestore, storage
import io


app = Flask(__name__)

PROJECT       = "forecastbench-johan"
REGION        = "us-central1"
BASE_URL      = f"https://{REGION}-{PROJECT}.cloudfunctions.net"
UPLOAD_BUCKET = "forecastbench-johan-submissions"

SIM_ORG    = "Test Org"
SIM_MODEL  = "GPT-4"
SIM_MODEL2 = "GPT-3.5"
SIM_MODEL3 = "GPT-o1"
SIM_ORG2   = "OpenAI"
SIM_EMAIL  = "johanimates@gmail.com"
SIM_TEAM   = "team1"
SIM_ROUND_VALID    = "2026-05-29"  # future relative to MOCK_DATE=2026-05-27
SIM_ROUND_LATE     = "2025-05-25"  # past deadline
SIM_ROUND_FUTURE   = "2026-06-05"  # separate round for 2d multi-error
SIM_ROUND_COVERAGE = "2026-07-01"  # separate round for 2c coverage-fail

db  = firestore.Client(project=PROJECT)
gcs = storage.Client(project=PROJECT)


def _token():
    return subprocess.run(
        "gcloud auth print-identity-token",
        capture_output=True, text=True, shell=True
    ).stdout.strip()


def _call(endpoint, payload):
    resp = requests.post(
        f"{BASE_URL}/{endpoint}",
        headers={"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    try:
        return resp.json()
    except Exception:
        return {"success": False, "error": resp.text}


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    teams = [d.to_dict() for d in db.collection("teams").stream()]
    subs  = [d.to_dict() for d in db.collection("submissions")
             .order_by("timestamp", direction=firestore.Query.DESCENDING).limit(50).stream()]
    return render_template("index.html", teams=teams, submissions=subs)


# ---------------------------------------------------------------------------
# Example file download
# ---------------------------------------------------------------------------

@app.route("/example-forecast.json")
def example_forecast():
    example = {
        "organization": "Your Org Name",
        "model": "Your Model Name",
        "model_organization": "Your Model Org",
        "question_set": "2026-05-18",
        "forecasts": [
            {
                "id": "example-question-id",
                "source": "metaculus",
                "forecast": 0.65,
                "resolution_date": "2026-12-31"
            },
            {
                "id": "another-question-id",
                "source": "metaculus",
                "forecast": 0.30,
                "resolution_date": "2026-06-30"
            }
        ]
    }
    return send_file(
        io.BytesIO(json.dumps(example, indent=2).encode()),
        mimetype="application/json",
        as_attachment=True,
        download_name="example-forecast.json"
    )


# ---------------------------------------------------------------------------
# Manual API routes
# ---------------------------------------------------------------------------

@app.route("/api/register", methods=["POST"])
def api_register():
    data = request.get_json()
    emails = [e.strip() for e in data.get("emails", "").split(",") if e.strip()]
    sas    = [e.strip() for e in data.get("service_accounts", "").split(",") if e.strip()]
    return jsonify(_call("onboard-team", {
        "organization":       data.get("organization", "").strip(),
        "model":              data.get("model", "").strip(),
        "model_organization": data.get("model_organization", "").strip(),
        "emails":             emails,
        "service_accounts":   sas,
        "anonymous":          data.get("anonymous", False),
    }))


@app.route("/api/upload", methods=["POST"])
def api_upload():
    team = request.form.get("team_name", "").strip()
    f    = request.files.get("file")
    if not f or not team:
        return jsonify({"success": False, "error": "team_name and file required"})
    try:
        blob = gcs.bucket(UPLOAD_BUCKET).blob(f"{team}/{f.filename}")
        blob.upload_from_file(f, content_type="application/json")
        return jsonify({"success": True, "message": f"Uploaded {f.filename} to {team}/"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/post-round", methods=["POST"])
def api_post_round():
    data = request.get_json()
    return jsonify(_call("post-round", {"round_date": data.get("round_date", "")}))


@app.route("/api/clear", methods=["POST"])
def api_clear():
    for col in ["teams", "submissions", "round_transfers"]:
        for doc in db.collection(col).stream():
            doc.reference.delete()
    for bname in [UPLOAD_BUCKET, "forecastbench-johan-interstitial", "forecastbench-johan-history"]:
        try:
            for blob in gcs.bucket(bname).list_blobs():
                if not blob.name.endswith(".keep"):
                    blob.delete()
        except Exception:
            pass
    return jsonify({"success": True, "message": "All test data cleared."})


# ---------------------------------------------------------------------------
# Simulate routes
# ---------------------------------------------------------------------------

def _sim_org_info():
    """Returns (display_org, model_org) for the current sim team from Firestore."""
    teams = list(db.collection("teams").where("team_name", "==", SIM_TEAM).stream())
    if teams:
        data = teams[0].to_dict()
        org = data.get("organization", SIM_ORG)
        return org, (org if data.get("anonymous") else SIM_ORG2)
    return SIM_ORG, SIM_ORG2


@app.route("/sim/onboard", methods=["POST"])
def sim_onboard():
    data = request.get_json(silent=True) or {}
    anonymous = data.get("anonymous", False)
    return jsonify(_call("onboard-team", {
        "organization": SIM_ORG, "model": SIM_MODEL,
        "model_organization": SIM_ORG2, "emails": [SIM_EMAIL], "anonymous": anonymous,
    }))


@app.route("/sim/onboard-team2", methods=["POST"])
def sim_onboard_team2():
    """Register a second team from the same org — should succeed."""
    return jsonify(_call("onboard-team", {
        "organization": SIM_ORG, "model": "Claude-3.5",
        "model_organization": "Anthropic", "emails": [SIM_EMAIL], "anonymous": False,
    }))


REAL_QUESTION_IDS = [
    {"id": "ZxGMjG8U4zDigZh8zcPo", "source": "manifold"},
    {"id": "0587u5Mk7ng2NaOkgCu1", "source": "manifold"},
    {"id": "K1XU4MmHm6FsVeydpMfQ", "source": "manifold"},
]


@app.route("/sim/upload-valid", methods=["POST"])
def sim_upload_valid():
    """Upload 3 valid submissions — one per model (GPT-4, GPT-3.5, GPT-o1). All should pass."""
    try:
        org, model_org = _sim_org_info()
        uploaded = []
        for n, model in enumerate([SIM_MODEL, SIM_MODEL2, SIM_MODEL3], start=1):
            forecast = {
                "organization": org, "model": model,
                "model_organization": model_org, "question_set": SIM_ROUND_VALID,
                "forecasts": [
                    {**REAL_QUESTION_IDS[0], "forecast": round(0.5 + n*0.1, 2), "resolution_date": None},
                    {**REAL_QUESTION_IDS[1], "forecast": round(0.5 - n*0.1, 2), "resolution_date": None},
                    {**REAL_QUESTION_IDS[2], "forecast": 0.5, "resolution_date": None},
                ]
            }
            fname = f"{SIM_ROUND_VALID}.{SIM_ORG}.{n}.json"
            gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
                json.dumps(forecast), content_type="application/json"
            )
            uploaded.append(f"{fname} ({model})")
        return jsonify({"success": True, "message": f"Uploaded: {', '.join(uploaded)}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/upload-mixed", methods=["POST"])
def sim_upload_mixed():
    """N=1 wrong org (rejected), N=2 valid (passes) — shows a resubmit after fixing."""
    try:
        org, model_org = _sim_org_info()
        # N=1: wrong org — will fail validation
        forecast_bad = {
            "organization": "WRONG ORG NAME", "model": SIM_MODEL,
            "model_organization": model_org, "question_set": SIM_ROUND_VALID,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.5, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.5, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.5, "resolution_date": None},
            ]
        }
        fname1 = f"{SIM_ROUND_VALID}.{SIM_ORG}.1.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname1}").upload_from_string(
            json.dumps(forecast_bad), content_type="application/json"
        )
        # N=2: valid — should pass
        forecast_good = {
            "organization": org, "model": SIM_MODEL,
            "model_organization": model_org, "question_set": SIM_ROUND_VALID,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.6, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.3, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.8, "resolution_date": None},
            ]
        }
        fname2 = f"{SIM_ROUND_VALID}.{SIM_ORG}.2.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname2}").upload_from_string(
            json.dumps(forecast_good), content_type="application/json"
        )
        return jsonify({"success": True, "message": f"Uploaded {fname1} (wrong org, will fail) and {fname2} (valid)"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/coverage-fail", methods=["POST"])
def sim_coverage_fail():
    """Upload a 3-question forecast with _dev_question_set_size=100 so the trigger
    inflates the question set to 100 entries, giving 3% coverage and a real rejection."""
    try:
        org, model_org = _sim_org_info()
        forecast = {
            "organization": org, "model": SIM_MODEL,
            "model_organization": model_org, "question_set": SIM_ROUND_COVERAGE,
            "_dev_question_set_size": 100,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.6, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.3, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.8, "resolution_date": None},
            ],
        }
        fname = f"{SIM_ROUND_COVERAGE}.{SIM_ORG}.1.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
            json.dumps(forecast), content_type="application/json"
        )
        return jsonify({"success": True, "message": f"Uploaded {fname} — 3 forecasts against a 100-question set (3% coverage). Trigger will reject it."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/upload-multi-error", methods=["POST"])
def sim_upload_multi_error():
    """3 files to the future round — each with a distinct error type."""
    try:
        org, model_org = _sim_org_info()
        base = {"model_organization": model_org, "question_set": SIM_ROUND_FUTURE}
        files = [
            # N=1: missing required 'model' field
            (f"{SIM_ROUND_FUTURE}.{SIM_ORG}.1.json", {
                **base,
                "organization": org,
                "forecasts": [{**REAL_QUESTION_IDS[0], "forecast": 0.6, "resolution_date": None},
                              {**REAL_QUESTION_IDS[1], "forecast": 0.3, "resolution_date": None},
                              {**REAL_QUESTION_IDS[2], "forecast": 0.8, "resolution_date": None}],
            }),
            # N=2: wrong org name only
            (f"{SIM_ROUND_FUTURE}.{org}.2.json", {
                **base,
                "organization": "WRONG ORG NAME",
                "model": SIM_MODEL,
                "forecasts": [{**REAL_QUESTION_IDS[0], "forecast": 0.5, "resolution_date": None},
                              {**REAL_QUESTION_IDS[1], "forecast": 0.5, "resolution_date": None},
                              {**REAL_QUESTION_IDS[2], "forecast": 0.5, "resolution_date": None}],
            }),
            # N=3: out-of-range forecast values
            (f"{SIM_ROUND_FUTURE}.{org}.3.json", {
                **base,
                "organization": org,
                "model": SIM_MODEL,
                "forecasts": [{**REAL_QUESTION_IDS[0], "forecast": 1.5,  "resolution_date": None},
                              {**REAL_QUESTION_IDS[1], "forecast": 0.4,  "resolution_date": None},
                              {**REAL_QUESTION_IDS[2], "forecast": -0.1, "resolution_date": None}],
            }),
        ]
        uploaded = []
        for fname, forecast in files:
            gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
                json.dumps(forecast), content_type="application/json"
            )
            uploaded.append(fname)
        return jsonify({"success": True, "message": f"Uploaded 3 files to {SIM_ROUND_FUTURE}: missing model field, wrong org, out-of-range values"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/overwrite", methods=["POST"])
def sim_overwrite():
    """Re-upload N=1 (same filename) with updated forecast values before the deadline."""
    try:
        org, model_org = _sim_org_info()
        forecast = {
            "organization": org, "model": SIM_MODEL,
            "model_organization": model_org, "question_set": SIM_ROUND_VALID,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.99, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.01, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.50, "resolution_date": None},
            ],
        }
        fname = f"{SIM_ROUND_VALID}.{SIM_ORG}.1.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
            json.dumps(forecast), content_type="application/json"
        )
        return jsonify({"success": True, "message": f"Re-uploaded {fname} with updated values (0.99/0.01/0.50) — should overwrite existing entry"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/exceed-limit", methods=["POST"])
def sim_exceed_limit():
    """Upload a 4th file (new model) after already having 3 valid — should be rejected."""
    try:
        org, model_org = _sim_org_info()
        forecast = {
            "organization": org, "model": "Claude-3.5",
            "model_organization": model_org, "question_set": SIM_ROUND_VALID,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.5, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.5, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.5, "resolution_date": None},
            ],
        }
        fname = f"{SIM_ROUND_VALID}.{SIM_ORG}.4.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
            json.dumps(forecast), content_type="application/json"
        )
        return jsonify({"success": True, "message": f"Uploaded {fname} (Claude-3.5) — should be rejected (already 3 valid submissions this round)"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/duplicate-model", methods=["POST"])
def sim_duplicate_model():
    """Re-submit GPT-4 (already submitted in 2a) — should be rejected as duplicate model."""
    try:
        org, model_org = _sim_org_info()
        forecast = {
            "organization": org, "model": SIM_MODEL,
            "model_organization": model_org, "question_set": SIM_ROUND_VALID,
            "forecasts": [
                {**REAL_QUESTION_IDS[0], "forecast": 0.9, "resolution_date": None},
                {**REAL_QUESTION_IDS[1], "forecast": 0.1, "resolution_date": None},
                {**REAL_QUESTION_IDS[2], "forecast": 0.5, "resolution_date": None},
            ],
        }
        fname = f"{SIM_ROUND_VALID}.{SIM_ORG}.5.json"
        gcs.bucket(UPLOAD_BUCKET).blob(f"{SIM_TEAM}/{fname}").upload_from_string(
            json.dumps(forecast), content_type="application/json"
        )
        return jsonify({"success": True, "message": f"Uploaded {fname} (GPT-4 again) — should be rejected (duplicate model this round)"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/upload-late", methods=["POST"])
def sim_upload_late():
    try:
        forecast = {
            "organization": SIM_ORG, "model": SIM_MODEL,
            "model_organization": SIM_ORG2, "question_set": SIM_ROUND_LATE,
            "forecasts": [{"id": "test-q-1", "source": "metaculus",
                           "forecast": 0.4, "resolution_date": None}]
        }
        gcs.bucket(UPLOAD_BUCKET).blob(
            f"{SIM_TEAM}/{SIM_ROUND_LATE}.{SIM_ORG}.1.json"
        ).upload_from_string(json.dumps(forecast), content_type="application/json")
        return jsonify({"success": True, "message": f"Uploaded late file {SIM_ROUND_LATE}.{SIM_ORG}.1.json — should be rejected as past deadline"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/check-trigger")
def sim_check_trigger():
    subs = [d.to_dict() for d in db.collection("submissions")
            .where("team_name", "==", SIM_TEAM).stream()]
    return jsonify({
        "found": len(subs) > 0, "count": len(subs),
        "submissions": [{k: v for k, v in s.items() if k != "timestamp"} for s in subs],
    })


@app.route("/sim/upload-invalid", methods=["POST"])
def sim_upload_invalid():
    try:
        # Wrong org name — will fail org-match check in on-submission
        forecast = {
            "organization": "WRONG ORG NAME",
            "model": SIM_MODEL,
            "model_organization": SIM_ORG2,
            "question_set": SIM_ROUND_VALID,
            "forecasts": [{"id": "test-q-1", "source": "metaculus",
                           "forecast": 0.5, "resolution_date": None}]
        }
        gcs.bucket(UPLOAD_BUCKET).blob(
            f"{SIM_TEAM}/{SIM_ROUND_VALID}.{SIM_ORG}.2.json"
        ).upload_from_string(json.dumps(forecast), content_type="application/json")
        return jsonify({"success": True, "message": f"Uploaded invalid file (wrong org name) as submission #2"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/sim/post-round", methods=["POST"])
def sim_post_round():
    """Processes the actual uploaded round files using force=True to bypass the deadline check."""
    return jsonify(_call("post-round", {"round_date": SIM_ROUND_VALID, "force": True}))


@app.route("/api/files")
def api_files():
    buckets = {
        "submissions":  "forecastbench-johan-submissions",
        "interstitial": "forecastbench-johan-interstitial",
        "history":      "forecastbench-johan-history",
    }
    # Pull submission validity from Firestore for colour coding
    sub_map = {}
    for doc in db.collection("submissions").stream():
        s = doc.to_dict()
        sub_map[s.get("filename", "")] = s.get("valid")

    result = {}
    for key, bucket_name in buckets.items():
        files = []
        try:
            for blob in gcs.bucket(bucket_name).list_blobs():
                if blob.name.endswith(".keep"):
                    continue
                fname = blob.name.split("/")[-1]
                files.append({"name": blob.name, "valid": sub_map.get(fname)})
        except Exception:
            pass
        result[key] = files
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
