"""Slack API."""

import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from . import keys  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def send_message(message=""):
    """Send a slack message."""
    client = WebClient(token=keys.API_SLACK_BOT_NOTIFICATION)

    try:
        client.chat_postMessage(channel=keys.API_SLACK_BOT_CHANNEL, text=message)
        logger.info("Slack message sent successfully!")
    except SlackApiError as e:
        logger.info(f"Got an error: {e.response['error']}")
        logger.info(f"Received a response status_code: {e.response.status_code}")
