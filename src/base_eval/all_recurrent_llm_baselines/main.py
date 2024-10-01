"""Create a custom job to run."""

import logging
import os
import sys
import time

from google.api_core import exceptions as google_exceptions
from google.cloud import aiplatform

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import env  # noqa: E402

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initializing variables and AI Platform
project_id = env.PROJECT_ID
region = env.CLOUD_DEPLOY_REGION
job_display_name = "all-recurrent-llm-baselines"
image_name = env.LLM_BASELINE_DOCKER_IMAGE_NAME
repo_name = env.LLM_BASELINE_DOCKER_REPO_NAME
staging_bucket = env.LLM_BASELINE_STAGING_BUCKET
service_account = env.LLM_BASELINE_SERVICE_ACCOUNT
aiplatform.init(project=project_id, location=region, staging_bucket=staging_bucket)


def trigger_vertex_ai(event, context):
    """Create a custom job to run."""
    logger.info("Trigger function called.")

    try:
        container_uri = f"{region}-docker.pkg.dev/{project_id}/{repo_name}/{image_name}:latest"
        logger.info(f"Container URI: {container_uri}")

        logger.info("Attempting to create CustomJob...")
        job = aiplatform.CustomJob(
            display_name=job_display_name,
            worker_pool_specs=[
                {
                    "machine_spec": {
                        "machine_type": "n1-standard-4",
                    },
                    "replica_count": 1,
                    "container_spec": {
                        "image_uri": container_uri,
                        "command": [
                            "python3.12",
                            "run_eval.py",
                            "TEST",
                        ],  # Remove the TEST command when we wanna start running it regularly
                    },
                }
            ],
            labels={"env": "latest"},
        )
        logger.info("CustomJob object created successfully")
        logger.info(f"Job details: {job}")

        # Log all attributes of the job for debugging
        logger.info(f"Job attributes: {vars(job)}")

        logger.info("Attempting to submit the job...")
        job.submit(service_account=service_account)

        # Wait for the job resource name to be available (with a timeout)
        timeout = 60  # seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            if job.resource_name:
                break
            time.sleep(5)
        else:
            raise TimeoutError("Timed out waiting for job resource name to be available")

        logger.info(f"Custom job submitted with name: {job.resource_name}")
        logger.info(f"Job resource name: {job.resource_name}")
        logger.info(f"Initial job state: {job.state}")
        logger.info("The job will continue running after this function completes.")

    except google_exceptions.GoogleAPICallError as api_error:
        logger.error(f"Google API call failed: {api_error}")
        logger.error(f"Error details: {api_error.details}")
    except RuntimeError as re:
        logger.error(f"RuntimeError occurred: {str(re)}")
    except ValueError as ve:
        logger.error(f"ValueError: {str(ve)}")
    except TimeoutError as te:
        logger.error(f"TimeoutError: {str(te)}")
    except Exception as e:
        logger.error(f"Failed to create or start job: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")

    logger.info("Function execution completed")


if __name__ == "__main__":
    import base64

    # Simulate the event and context
    event = {
        "data": base64.b64encode(b"Hello World").decode("utf-8"),
        "_comment": "data is base64 encoded string of 'Hello World'",
    }
    context = {
        "event_id": "1234567890",
        "timestamp": "2024-06-27T22:14:56.795Z",
        "event_type": "google.pubsub.topic.publish",
        "resource": {
            "service": "pubsub.googleapis.com",
            "name": f"projects/{project_id}/topics/{env.TOPIC_NAME}",
        },
    }

    # Call the function
    trigger_vertex_ai(event, context)
