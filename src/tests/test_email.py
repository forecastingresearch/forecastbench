"""Tests for helpers/email.py: SMTP notifications controlled by RunMode."""

from unittest.mock import MagicMock

import pytest

from helpers import email, env
from helpers.constants import RunMode

SENDER = "sender@dummy-domain-x92ah8.org"
RECIPIENT = "a@dummy-domain-x92ah8.com"
RECIPIENT_2 = "b@dummy-domain-x92ah8.com"


@pytest.fixture
def smtp_config(monkeypatch):
    """Configure SMTP settings and capture the SMTP connection mock."""
    monkeypatch.setattr(env, "SMTP_USER", SENDER)
    monkeypatch.setattr(env, "SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setattr(env, "SMTP_PORT", 587)
    monkeypatch.setenv("SMTP_PASSWORD", "app-password")
    smtp_class = MagicMock()
    monkeypatch.setattr(email.smtplib, "SMTP", smtp_class)
    return smtp_class.return_value.__enter__.return_value


class TestSendEmail:
    """Test send_email behavior across run modes."""

    def test_default_test_mode_attempts_no_smtp(self, smtp_config):
        assert email.send_email([RECIPIENT], "subject", "body") is False
        smtp_config.sendmail.assert_not_called()

    def test_test_mode_with_flag_reroutes_to_sender(self, smtp_config):
        sent = email.send_email(
            [RECIPIENT], "subject", "body", run_mode=RunMode.TEST, send_email_in_test=True
        )
        assert sent is True
        _, recipients, message = smtp_config.sendmail.call_args[0]
        assert recipients == [SENDER]
        assert "[TEST]" in message
        assert RECIPIENT in message

    def test_prod_sends_to_recipients(self, smtp_config):
        sent = email.send_email([RECIPIENT, RECIPIENT_2], "subject", "body", run_mode=RunMode.PROD)
        assert sent is True
        _, recipients, message = smtp_config.sendmail.call_args[0]
        assert recipients == [RECIPIENT, RECIPIENT_2]
        assert "[TEST]" not in message

    def test_prod_ignores_send_email_in_test(self, smtp_config):
        sent = email.send_email(
            [RECIPIENT], "subject", "body", run_mode=RunMode.PROD, send_email_in_test=True
        )
        assert sent is True
        _, recipients, message = smtp_config.sendmail.call_args[0]
        assert recipients == [RECIPIENT]
        assert "[TEST]" not in message

    def test_returns_false_without_password(self, monkeypatch):
        monkeypatch.setattr(env, "SMTP_USER", SENDER)
        monkeypatch.delenv("SMTP_PASSWORD", raising=False)
        assert email.send_email([RECIPIENT], "subject", "body", run_mode=RunMode.PROD) is False

    def test_returns_false_without_recipients(self, smtp_config):
        assert email.send_email([], "subject", "body", run_mode=RunMode.PROD) is False

    def test_smtp_failure_returns_false(self, smtp_config):
        smtp_config.sendmail.side_effect = OSError("boom")
        assert email.send_email([RECIPIENT], "subject", "body", run_mode=RunMode.PROD) is False
