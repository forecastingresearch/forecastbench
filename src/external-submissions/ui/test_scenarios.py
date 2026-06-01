"""
Programmatic end-to-end test for all ForecastBench sandbox scenarios.
Run with: python test_scenarios.py
Requires the Flask UI to be running on localhost:5000.
"""

import time
import requests

BASE = "http://localhost:5000"
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

results = []


def post(path, **kwargs):
    r = requests.post(f"{BASE}{path}", **kwargs)
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text, "_status": r.status_code}


def get(path):
    r = requests.get(f"{BASE}{path}")
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text}


def clear():
    post("/api/clear")
    time.sleep(1)


def wait_for_trigger(expected_count, timeout=60):
    """Poll Firestore until expected_count submissions appear or timeout."""
    for _ in range(timeout // 3):
        data = get("/sim/check-trigger")
        count = data.get("count", 0)
        if count >= expected_count:
            return data
        time.sleep(3)
    return data


def files():
    return get("/api/files")


def check(name, condition, detail=""):
    icon = PASS if condition else FAIL
    results.append((name, condition, detail))
    print(f"  {icon} {name}" + (f"  [{detail}]" if detail else ""))
    return condition


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# A: Happy path — register, 3 valid uploads, post-round
# ---------------------------------------------------------------------------
section("A: Happy path -- 3 valid uploads -> post-round")

clear()
r = post("/sim/onboard")
check("A1 register succeeds", r.get("success") is True, r.get("team_name", r.get("error", "")))
check("A1 team_name assigned", "team_name" in r)

r = post("/sim/upload-valid")
check("A2 upload-valid accepted", r.get("success") is True, r.get("message", r.get("error", ""))[:80])

print("  ... waiting for on-submission trigger (~15s) ...")
data = wait_for_trigger(3)
count = data.get("count", 0)
subs = data.get("submissions", [])
check("A3 trigger fired: 3 submissions in Firestore", count == 3, f"got {count}")
valid_count = sum(1 for s in subs if s.get("valid"))
check("A3 all 3 valid", valid_count == 3, f"{valid_count}/3 valid")

r = post("/sim/post-round")
check("A4 post-round succeeds", r.get("success") is True, r.get("error", "")[:80])
teams = (r.get("results") or [{}])[0].get("teams", [])
valid_files = teams[0].get("valid_files", []) if teams else []
check("A4 post-round: 3 valid files processed", len(valid_files) == 3, f"got {len(valid_files)}")

f = files()
check("A5 submissions bucket empty after post-round", len(f.get("submissions", [])) == 0)
check("A5 interstitial has 3 files", len(f.get("interstitial", [])) == 3)
check("A5 history has 3 files", len(f.get("history", [])) == 3)

# ---------------------------------------------------------------------------
# B: Second team, same org
# ---------------------------------------------------------------------------
section("B: Second team under same org")

clear()
post("/sim/onboard")
r = post("/sim/onboard-team2")
check("B1 second team registration succeeds", r.get("success") is True, r.get("error", "")[:80])
check("B1 different team_name assigned", r.get("team_name") == "team2", r.get("team_name", ""))

# ---------------------------------------------------------------------------
# C: Mixed upload — 1 wrong org (rejected), 1 valid
# ---------------------------------------------------------------------------
section("C: Mixed upload — wrong org rejected, valid accepted")

clear()
post("/sim/onboard")
r = post("/sim/upload-mixed")
check("C1 upload-mixed returns success", r.get("success") is True, r.get("error", "")[:80])

print("  ... waiting for trigger ...")
data = wait_for_trigger(2)
subs = data.get("submissions", [])
check("C2 2 submissions in Firestore", data.get("count", 0) == 2)
valid = [s for s in subs if s.get("valid")]
invalid = [s for s in subs if not s.get("valid")]
check("C2 1 valid", len(valid) == 1, [s["filename"] for s in valid])
check("C2 1 invalid (wrong org)", len(invalid) == 1, [s.get("errors") for s in invalid])

r = post("/sim/post-round")
check("C3 post-round succeeds", r.get("success") is True)
teams = (r.get("results") or [{}])[0].get("teams", [])
if teams:
    check("C3 1 valid, 1 invalid reported", len(teams[0].get("valid_files", [])) == 1 and len(teams[0].get("invalid_files", [])) == 1,
          f"valid={teams[0].get('valid_files')} invalid={[x['filename'] for x in teams[0].get('invalid_files',[])]}")

# ---------------------------------------------------------------------------
# D: Edge cases — duplicate model, exceed limit
# ---------------------------------------------------------------------------
section("D: Edge cases — duplicate model + exceed limit (run after valid 3-upload)")

clear()
post("/sim/onboard")
r = post("/sim/upload-valid")
check("D0 upload 3 valid files", r.get("success") is True)
print("  ... waiting for trigger ...")
wait_for_trigger(3)

r = post("/sim/duplicate-model")
check("D1 duplicate model rejected", r.get("success") is True, r.get("message", r.get("error", ""))[:80])

r = post("/sim/exceed-limit")
check("D2 4th file rejected (exceed limit)", r.get("success") is True, r.get("message", r.get("error", ""))[:80])

# Verify Firestore still has only 3 valid submissions
data = get("/sim/check-trigger")
subs = data.get("submissions", [])
valid_count = sum(1 for s in subs if s.get("valid"))
check("D3 still only 3 valid submissions total", valid_count == 3, f"got {valid_count}")

# ---------------------------------------------------------------------------
# E: Overwrite before deadline
# ---------------------------------------------------------------------------
section("E: Overwrite before deadline — re-upload same filename")

clear()
post("/sim/onboard")
post("/sim/upload-valid")
print("  ... waiting for trigger ...")
wait_for_trigger(3)

r = post("/sim/overwrite")
check("E1 overwrite accepted", r.get("success") is True, r.get("message", r.get("error", ""))[:80])
print("  ... waiting for overwrite to register ...")
time.sleep(8)
data = get("/sim/check-trigger")
subs = data.get("submissions", [])
valid_count = sum(1 for s in subs if s.get("valid"))
check("E2 still 3 valid after overwrite (not doubled)", valid_count == 3, f"got {valid_count}")

# ---------------------------------------------------------------------------
# F: Late submission
# ---------------------------------------------------------------------------
section("F: Late submission — past deadline")

clear()
post("/sim/onboard")
r = post("/sim/upload-late")
check("F1 late file upload call succeeds (HTTP)", r.get("success") is True, r.get("message", r.get("error", ""))[:80])
print("  ... waiting for trigger ...")
time.sleep(12)
data = get("/sim/check-trigger")
subs = data.get("submissions", [])
late = [s for s in subs if not s.get("valid")]
check("F2 late submission marked invalid in Firestore", len(late) >= 1,
      [s.get("errors") for s in late])

# ---------------------------------------------------------------------------
# G: Coverage failure
# ---------------------------------------------------------------------------
section("G: Coverage failure")

clear()
post("/sim/onboard")
r = post("/sim/coverage-fail")
check("G1 coverage-fail upload accepted by UI", r.get("success") is True, r.get("message", r.get("error", ""))[:80])
print("  ... waiting for trigger ...")
time.sleep(12)
data = get("/sim/check-trigger")
subs = data.get("submissions", [])
invalid = [s for s in subs if not s.get("valid")]
check("G2 coverage failure marked invalid", len(invalid) >= 1,
      [s.get("errors") for s in invalid])

# ---------------------------------------------------------------------------
# H: Multi-field errors
# ---------------------------------------------------------------------------
section("H: Multi-field errors (3 files, 3 distinct rejection reasons)")

clear()
post("/sim/onboard")
r = post("/sim/upload-multi-error")
check("H1 multi-error upload accepted by UI", r.get("success") is True, r.get("message", r.get("error", ""))[:80])
print("  ... waiting for trigger ...")
data = wait_for_trigger(3, timeout=60)
subs = data.get("submissions", [])
invalid = [s for s in subs if not s.get("valid")]
check("H2 all 3 files rejected", len(invalid) >= 3, f"got {len(invalid)} invalid")
error_msgs = [s.get("errors", []) for s in invalid]
distinct = len(set(tuple(e) for e in error_msgs))
check("H3 distinct error reasons", distinct == len(invalid), error_msgs)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
section("SUMMARY")
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"\n  {passed}/{total} checks passed\n")
for name, ok, detail in results:
    icon = PASS if ok else FAIL
    print(f"  {icon} {name}" + (f"  [{detail}]" if detail and not ok else ""))

if passed < total:
    print(f"\n  {total - passed} check(s) FAILED")
else:
    print("\n  All checks passed!")
