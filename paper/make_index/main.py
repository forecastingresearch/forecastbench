"""Create index.html for a directory on GCP Storage."""

import os
import sys

from variables import BUCKET_NAME

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

# Get the bucket
prefix = "datasets/forecast_sets"
html_file = "forecast_sets.html"

files = gcp.storage.list_with_prefix(bucket_name=BUCKET_NAME, prefix=prefix)

# Create an HTML file content
html_content = """
<html>
<head>
    <title>File List</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        ul { list-style-type: none; padding: 0; }
        li { margin: 5px 0; }
        a { text-decoration: none; color: #1a73e8; }
        a:hover { text-decoration: underline; }
        .container { max-width: 800px; margin: 0 auto;
        padding: 20px; border: 1px solid #ddd;
        border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
    </style>
</head>
<body>
    <div class="container">
        <h1>File List</h1>
        <ul>
"""
count = 0
for f in files:
    if f.endswith(".json"):
        count += 1
        file_name = f.split("/")[-1]
        file_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{f}"
        html_content += f'<li><a href="{file_url}">{file_name}</a></li>'
html_content += "</ul></body></html>"

print(f"There are {count} files.")

# Write the HTML content to a file
with open(html_file, "w") as file:
    file.write(html_content)

# Upload the HTML file to the same directory
# blob = bucket.blob(prefix + html_file)
# blob.upload_from_filename(html_file)
