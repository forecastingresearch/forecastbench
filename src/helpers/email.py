"""ForecastBench email notifications via SMTP.

Reads credentials from environment variables:
  SMTP_USER     sender email address
  SMTP_PASSWORD app password for the sender account
  SMTP_HOST     SMTP server (default: smtp.gmail.com)
  SMTP_PORT     SMTP port (default: 587)
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))


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


def send_welcome(
    emails: list,
    team_id: str,
    organization: str,
    upload_bucket: str,
    anonymous: bool = False,
    next_due_date: str = "",
) -> None:
    """Send a welcome email to a newly registered team.

    Args:
        emails (list): Recipient email addresses.
        team_id (str): Internal team ID (team1, team2, ...).
        organization (str): Public-facing organization name shown to the team.
        upload_bucket (str): GCS bucket name.
        anonymous (bool): Whether the team is anonymous.
        next_due_date (str): Next forecast due date (YYYY-MM-DD), or empty to omit.
    """
    subject = "ForecastBench — Your team has been registered"

    due_date_line = f"\nNext forecast due date: {next_due_date}\n" if next_due_date else ""

    body = f"""Hi,

Your team has been registered on ForecastBench.

Team: {organization}
Upload folder: gs://{upload_bucket}/{team_id}/
{due_date_line}
To submit a forecast:
1. Download the question set at 0:00 UTC on the forecast due date:
   https://github.com/forecastingresearch/forecastbench-datasets
2. Generate your forecasts.
3. Name your file: {{forecast_due_date}}.{{organization}}.{{N}}.json
4. Upload it to your folder:
   gsutil cp your-file.json gs://{upload_bucket}/{team_id}/
   gcloud storage cp your-file.json gs://{upload_bucket}/{team_id}/

To test your upload permissions before the due date:
   gcloud storage cp test.json gs://{upload_bucket}/{team_id}/test/

Deadline: 23:59:59 UTC on the forecast due date.
Max submissions: 3 per round (one per model).

Submission instructions:
https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench

If you have any questions, reply to this email.

ForecastBench team
"""
    if anonymous:
        body += (
            f"\nNote: Your public name is '{organization}'."
            " Use this as 'organization' in your forecast files.\n"
        )
    _send(emails, subject, body)
