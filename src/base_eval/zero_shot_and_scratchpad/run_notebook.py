"""Run the zero-shot and scratchpad eval notebook."""

import json
import os
import subprocess
import sys

import papermill as pm

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

# Install dependencies
subprocess.check_call(["pip", "install", "-r", "requirements.txt"])

local_filename = remote_filename = "latest-llm.json"
if not os.path.exists(local_filename):
    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_SETS_BUCKET,
        filename=remote_filename,
        local_filename=local_filename,
    )
else:
    print(f"{local_filename} already exists.")

# Run the notebook
local_output_notebook_path = "/tmp/output_notebook.ipynb"
pm.execute_notebook(
    "notebook.ipynb",
    local_output_notebook_path,  # Save the output to a temporary location
)

with open(local_filename, "r") as file:
    questions_data = json.load(file)

forecast_due_date = questions_data["forecast_due_date"]

gcp.storage.upload(
    bucket_name=env.FORECAST_SETS_BUCKET,
    local_filename=local_output_notebook_path,
    filename=f"individual_forecast_records/output_notebooks/{forecast_due_date}/output_notebook.ipynb",
)

# Delete the question file and the output notebook after the notebook is done running
os.remove(local_filename)
os.remove(local_output_notebook_path)
