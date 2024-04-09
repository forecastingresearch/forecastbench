"""Constants."""

import os

BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")

QUESTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "question": str,
    "background": str,
    "source_resolution_criteria": str,
    "begin_datetime": str,
    "close_datetime": str,
    "url": str,
    "resolution_datetime": str,
    "resolved": bool,
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "datetime": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]

MANIFOLD_TOPIC_SLUGS = ["entertainment", "sports-default", "technology-default"]

METACULUS_CATEGORIES = [
    "geopolitics",
    "natural-sciences",
    "sports-entertainment",
    "health-pandemics",
    "law",
    "computing-and-math",
]
