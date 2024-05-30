"""utils for key-related tasks in llm-benchmark."""

import os

from google.cloud import secretmanager


def get_secret(secret_name, version_id="latest"):
    """
    Retrieve the payload of a specified secret version from Secret Manager.

    Accesses the Google Cloud Secret Manager to fetch the payload of a secret version
    identified by `project_id`, `secret_name`, and `version_id`. Decodes the payload
    from bytes to a UTF-8 string and returns it.
    """
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get("CLOUD_PROJECT")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
