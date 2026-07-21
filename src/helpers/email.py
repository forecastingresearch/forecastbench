"""Send email notifications via SMTP.

`SMTP_PASSWORD` is read from the environment at send time. In deployed jobs it is injected
from Secret Manager via `--set-secrets`; for local runs, export it in the shell. It must not
be added to `variables.mk` or fetched through `helpers.keys`.

Behavior is controlled by `constants.RunMode` (defaults to TEST — safe by default):
  * TEST: no SMTP connection is attempted; returns False.
  * TEST with `send_email_in_test=True`: the email is rerouted to `SMTP_USER` with a "[TEST]"
    subject prefix and the intended recipients listed at the top of the body.
  * PROD: the email is sent normally; `send_email_in_test` has no effect.
"""

import logging
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from . import constants, env

logger = logging.getLogger(__name__)


def send_email(
    to_emails: List[str],
    subject: str,
    body: str,
    run_mode: constants.RunMode = constants.RunMode.TEST,
    send_email_in_test: bool = False,
) -> bool:
    """Send a plain-text email and report whether it was sent.

    Failures are logged, not raised: callers (e.g. team onboarding) should not abort their
    transaction because a notification could not be delivered.

    Args:
        to_emails (List[str]): Recipient email addresses.
        subject (str): Subject line.
        body (str): Plain-text body.
        run_mode (constants.RunMode): TEST (default) attempts no SMTP unless
            `send_email_in_test` is set; PROD sends normally.
        send_email_in_test (bool): In TEST mode, send the email rerouted to `SMTP_USER`
            with a "[TEST]" subject prefix. No effect in PROD.
    """
    if run_mode != constants.RunMode.PROD and not send_email_in_test:
        logger.info("RunMode is TEST and send_email_in_test is off; not sending %r.", subject)
        return False

    smtp_password = os.environ.get("SMTP_PASSWORD", "")
    if not env.SMTP_USER or not smtp_password:
        logger.warning("SMTP_USER or SMTP_PASSWORD not set; not sending email %r.", subject)
        return False
    if not to_emails:
        logger.warning("No recipients; not sending email %r.", subject)
        return False

    if run_mode != constants.RunMode.PROD:
        body = f"[TEST] Intended recipients: {', '.join(to_emails)}\n\n{body}"
        subject = f"[TEST] {subject}"
        to_emails = [env.SMTP_USER]

    msg = MIMEMultipart()
    msg["From"] = env.SMTP_USER
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(env.SMTP_HOST, env.SMTP_PORT, timeout=15) as server:
            # smtplib's default STARTTLS context does not verify certificates.
            server.starttls(context=ssl.create_default_context())
            server.login(env.SMTP_USER, smtp_password)
            server.sendmail(env.SMTP_USER, to_emails, msg.as_string())
    except Exception:
        logger.exception("Failed to send email %r to %s.", subject, to_emails)
        return False
    return True
