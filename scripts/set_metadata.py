"""Set the metadata for all files in the bucket accordingly."""

import os
import shlex
import subprocess
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from helpers import constants  # noqa: E402

bucket = f"gs://{constants.LEADERBOARD_BUCKET_NAME}"
extensions_content_types = {
    ".html": "text/html",
    ".json": "application/json",
    ".jsonl": "application/jsonlines",
    ".pickle": "application/octet-stream",
    ".tar.xz": "application/x-xz",
    ".txt": "text/plain",
    ".py": "text/x-python",
}

# List all files in the bucket
list_files_command = ["gsutil", "ls", "-r", f"{bucket}/**"]
result = subprocess.run(list_files_command, capture_output=True, text=True)

if result.returncode != 0:
    print("Failed to list files")
    print(result.stderr)
    exit(1)

# Parse the list of files
lines = result.stdout.split("\n")
files = []
current_file = ""

for line in lines:
    line = line.strip()
    if line.startswith("gs://"):
        if current_file:
            files.append(current_file)
        current_file = line
    else:
        current_file += line

if current_file:
    files.append(current_file)

for file in files:
    if not file.endswith("/") and not file.endswith(".pickle"):
        _, ext = os.path.splitext(file)
        if ext in extensions_content_types:
            content_type = extensions_content_types[ext]
            setmeta_command = (
                f'gsutil setmeta -h "Content-Type:{content_type}" -h '
                f'"Cache-Control:no-store, max-age=0, no-transform" {shlex.quote(file)}'
            )
            result = subprocess.run(setmeta_command, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Failed to set metadata for {file}")
                print(result.stderr)
            else:
                print(f"Metadata set for {file}")
