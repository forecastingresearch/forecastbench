"""Cloud run interacitons."""

import logging
import sys

from google.cloud import run_v2

from . import env, slack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

timeout_1h = 3600


def call_worker(job_name, env_vars, task_count):
    """Invoke a Cloud Run Job.

    Params:
    job_name: the name of the job to run
    env_vars: a dict of the environment variables to overwrite
    task_count: the number of invocations of the job to start
    """
    if (
        not isinstance(job_name, str)
        or not isinstance(env_vars, dict)
        or not isinstance(task_count, int)
    ):
        raise ValueError("One of the arguments to `cloud_run.call_workrer` is incorrect.")

    return run_job(
        job_name=job_name,
        env_vars=env_vars,
        task_count=task_count,
    )


def run_job(job_name, env_vars=None, task_count=1, timeout=timeout_1h):
    """Run the Cloud Run job given by `job_name`.

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
            slack.send_message(message=message)
            sys.exit(1)
