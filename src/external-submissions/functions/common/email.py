"""
ForecastBench email notifications via SMTP.

Reads credentials from environment variables:
  SMTP_USER     sender email address
  SMTP_PASSWORD app password for the sender account
  SMTP_HOST     SMTP server (default: smtp.gmail.com)
  SMTP_PORT     SMTP port (default: 587)
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_HOST     = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
VALIDATE_URL  = os.environ.get(
    "VALIDATE_URL",
    "{VALIDATE_URL}",
)


def _send(to_emails, subject, body):
    if not SMTP_USER or not SMTP_PASSWORD:
        return
    if not to_emails:
        return

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, to_emails, msg.as_string())


def send_welcome(emails, team_name, display_org, upload_bucket, anonymous=False, models=None):
    models_line = "\n".join(f"  - {m}" for m in (models or []))
    subject = "ForecastBench — Your team has been registered"
    body = f"""Hi,

Your team has been registered on ForecastBench.

Team name: {display_org}
Registered models:
{models_line}
Upload folder: gs://{upload_bucket}/{team_name}/

To submit a forecast:
1. Download the question set from https://github.com/forecastingresearch/forecastbench-datasets at 0:00 UTC on the forecast due date.
2. Generate your forecasts.
3. Name your file: {{forecast_due_date}}.{display_org}.{{N}}.json
4. Upload it to your folder using gsutil or the GCP Console:
   gsutil cp your-file.json gs://{upload_bucket}/{team_name}/
   gcloud storage cp your-file.json gs://{upload_bucket}/{team_name}/

To test your upload permissions before the due date, upload any file to your test subfolder:
   gsutil cp test.json gs://{upload_bucket}/{team_name}/test/
   gcloud storage cp test.json gs://{upload_bucket}/{team_name}/test/

Deadline: 23:59:59 UTC on the forecast due date.
Max submissions: 3 per round (1 per model, up to 3 different models).

You can validate your file at any time before submitting:
{VALIDATE_URL}

Submission instructions: https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench

If you have any questions, reply to this email.

ForecastBench team
"""
    if anonymous:
        body += (
            f"\nNote: Your public name is '{display_org}'. "
            f"Use this as both 'organization' and 'model_organization' in your forecast files.\n"
        )
    _send(emails, subject, body)


def send_submission_result(emails, filename, round_date, valid,
                           errors=None, warnings=None, stats=None):
    if valid:
        subject = f"ForecastBench — Submission received: {filename}"
        body = f"""Hi,

Your forecast file has been received and validated successfully.

File: {filename}
Round: {round_date}

"""
        if stats:
            body += "Coverage:\n"
            body += f"  Market:  {stats.get('market_coverage', 'N/A')} ({stats.get('market_covered', '?')}/{stats.get('market_total', '?')} questions)\n"
            body += f"  Dataset: {stats.get('dataset_coverage', 'N/A')} ({stats.get('dataset_covered', '?')}/{stats.get('dataset_total', '?')} questions)\n"
        if warnings:
            body += f"\nWarnings ({len(warnings)}):\n"
            for w in warnings:
                body += f"  - {w}\n"
        body += "\nYour submission will appear on the leaderboard approximately 50 days after the forecast due date.\n"
    else:
        subject = f"ForecastBench — Submission issues: {filename}"
        body = f"""Hi,

Your forecast file was received but has validation issues.

File: {filename}
Round: {round_date}

Your file has been kept in your submission folder. Please fix the issues and re-upload before 23:59:59 UTC on the due date.

Issues:
"""
        for e in (errors or []):
            body += f"  - {e}\n"
        body += "\nYou can validate your file at: {VALIDATE_URL}\n"

    body += "\nForecastBench team\n"
    _send(emails, subject, body)


def send_round_processed(emails, round_date, valid_count, invalid_details=None):
    """Sent to each team after post-round runs."""
    invalid_details = invalid_details or []

    if not invalid_details:
        subject = f"ForecastBench — Round {round_date} processed"
        body = f"""Hi,

Round {round_date} has been processed.

{valid_count} file(s) passed validation and have been submitted for scoring.

Your results will appear on the leaderboard approximately 50 days after the forecast due date.

ForecastBench team
"""
    else:
        subject = f"ForecastBench — Round {round_date}: action may be needed"
        body = f"""Hi,

Round {round_date} has been processed.

{valid_count} file(s) passed validation and have been submitted for scoring.
{len(invalid_details)} file(s) had issues and were not automatically submitted:

"""
        for item in invalid_details:
            body += f"  {item['filename']}:\n"
            for e in item.get("errors", []):
                body += f"    - {e}\n"

        body += "\nThe ForecastBench team has been notified and will review these manually. We will follow up if any action is needed.\n"

    body += "\nForecastBench team\n"
    _send(emails, subject, body)


def send_reminder(emails, round_date, upload_bucket, team_name, display_org):
    subject = f"ForecastBench — Reminder: Round {round_date} closes today at 23:59:59 UTC"
    body = f"""Hi,

This is a reminder that the ForecastBench submission deadline for round {round_date} is today at 23:59:59 UTC.

Upload folder: gs://{upload_bucket}/{team_name}/

To submit:
1. Download the question set from https://github.com/forecastingresearch/forecastbench-datasets
2. Generate your forecasts.
3. Name your file: {round_date}.{display_org}.{{N}}.json
4. Upload before 23:59:59 UTC today:
   gsutil cp your-file.json gs://{upload_bucket}/{team_name}/
   gcloud storage cp your-file.json gs://{upload_bucket}/{team_name}/

Max submissions: 3 per round (1 per model, up to 3 different models).

Validate your file: {VALIDATE_URL}

ForecastBench team
"""
    _send(emails, subject, body)


def send_round_digest(fri_email, round_date, team_summaries, interstitial_bucket="forecastbench-submissions-interstitial-dev"):
    """Single digest email to FRI with the full round summary."""
    subject = f"ForecastBench — Round {round_date} digest"

    total_valid = sum(len(t["valid_files"]) for t in team_summaries)
    total_invalid = sum(len(t["invalid_files"]) for t in team_summaries)

    body = f"""Round {round_date} post-processing complete.

{len(team_summaries)} team(s) submitted.
{total_valid} file(s) passed — copied to processing.
{total_invalid} file(s) need review — sitting in interstitial bucket.

---

"""
    for t in team_summaries:
        org = t["organization"]
        valid = t["valid_files"]
        invalid = t["invalid_files"]
        body += f"{org} ({t['team_name']}):\n"
        if valid:
            body += f"  Valid ({len(valid)}):\n"
            for f in valid:
                body += f"    + {f}\n"
        if invalid:
            body += f"  Needs review ({len(invalid)}):\n"
            for item in invalid:
                body += f"    x {item['filename']}\n"
                for e in item.get("errors", []):
                    body += f"        - {e}\n"
        body += "\n"

    if total_invalid:
        body += f"Invalid files are in gs://{interstitial_bucket}/ for manual review.\n"

    body += "\nForecastBench team\n"
    _send([fri_email], subject, body)
