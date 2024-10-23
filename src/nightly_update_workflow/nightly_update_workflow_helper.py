"""Helper functions for nightly update workflows."""

import logging
import os
import sys

from google.cloud import run_v2
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import env, keys  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

timeout_1h = 3600


def cloud_run_job(job_name, env_vars=None, task_count=1, timeout=timeout_1h):
    """Run the Cloud Run job for the nightly worker.

    env_vars: override default environment variables.
    task_count: override default task count.
    timeout: override default timeout.
    """
    client = run_v2.JobsClient()
    name = client.job_path(
        project=env.PROJECT_ID,
        location=env.CLOUD_DEPLOY_REGION,
        job=job_name,
    )
    request = run_v2.RunJobRequest(
        name=name,
    )

    overrides_args = {}
    if env_vars is not None:
        env_var_overrides = [run_v2.EnvVar(name=k, value=v) for k, v in env_vars.items()]
        container_override = run_v2.RunJobRequest.Overrides.ContainerOverride(
            env=env_var_overrides,
        )
        overrides_args["container_overrides"] = [container_override]
    if timeout is not None:
        overrides_args["timeout"] = f"{timeout}s"
    if task_count is not None:
        overrides_args["task_count"] = task_count

    if overrides_args:
        request.overrides = run_v2.RunJobRequest.Overrides(**overrides_args)

    operation = client.run_job(request=request, timeout=timeout)
    return operation


def send_slack_message(message=""):
    """Send a slack message."""
    client = WebClient(token=keys.API_SLACK_BOT_NOTIFICATION)

    try:
        client.chat_postMessage(channel=keys.API_SLACK_BOT_CHANNEL, text=message)
        logger.info("Slack message sent successfully!")
    except SlackApiError as e:
        logger.info(f"Got an error: {e.response['error']}")
        logger.info(f"Received a response status_code: {e.response.status_code}")


def block_and_check_job_result(operation, name, exit_on_error, timeout=timeout_1h):
    """Blocking check for result of Cloud Run job specified in `operation`."""
    try:
        execution = operation.result(timeout=timeout)
        name = execution.name.rsplit("/", 1)[-1] + f" ({name})"

        conditions = execution.conditions
        succeeded = False
        for condition in conditions:
            if (
                condition.type_ == "Completed"
                and condition.state == run_v2.Condition.State.CONDITION_SUCCEEDED
            ):
                succeeded = True
                break

        # Get start and completion times
        start_time = execution.start_time
        end_time = execution.completion_time

        minutes, seconds = divmod((end_time - start_time).total_seconds(), 60)
        logger.info(f"Job `{name}`")
        logger.info("Succeeded!" if succeeded else "Failed.")
        logger.info(f"Elapsed job time: {minutes}m{seconds}s.\n")

    except Exception as e:
        message = f"Job `{name}` failed with exception:\n\n{e}"
        logger.error(message)
        if exit_on_error:
            send_slack_message(message=message)
            sys.exit(1)
