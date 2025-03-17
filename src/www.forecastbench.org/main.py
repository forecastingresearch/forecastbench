"""Generate website."""

import logging
import os
import shutil
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import dates, env  # noqa:E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


top = """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="ForecastBench is a dynamic, continuously-updated benchmark for
    evaluating the accuracy of LLMs and machine learning systems on evolving forecasting
    questions. Participate in bi-weekly forecasting rounds to see how your LLM compares to expert
    human forecasters and other LLMs.">
    <link rel="icon" href="ROOT_REPLACEMENT/fri-favicon.png" type="image/png">
    <title>ForecastBench - a dynamic, continuously-updated forecasting LLM Benchmark</title>
    <link rel="stylesheet" type="text/css" href="ROOT_REPLACEMENT/styles.css">
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "WebSite",
      "name": "ForecastBench",
      "url": "https://www.forecastbench.org",
      "description": "A dynamic, continuously-updated benchmark to measure LLM accuracy on
                      forecasting questions.",
      "creator": {
        "@type": "Organization",
        "name": "Forecasting Research Institute"
      }
    }
    </script>
  </head>
  <body>
"""

bottom = """
    <footer>
      <div class="footer-content">
         <div class="footer-item">
           <span class="big-dot"></span>
           &copy; 2024&ndash;<span id="currentYear"></span>
           <a href="https://forecastingresearch.org/">Forecasting Research Institute</a>
         </div>
         <div class="footer-item">
           <span class="big-dot">&middot;</span>
           Contact or questions: forecastbench@forecastingresearch.org</div>
         <div class="footer-item">
           <span class="big-dot">&middot;</span>
           Content licensed under
           <a href="https://creativecommons.org/licenses/by-sa/4.0/legalcode">CC BY-SA 4.0</a>
         </div>
       </div>
    </footer>
    <script>
      const hamburger = document.getElementById('hamburger');
      const navbar = document.getElementById('navbar');

      hamburger.addEventListener('click', function() {
        navbar.classList.toggle('nav-open');
      });
    </script>
    <script>
      document.getElementById('currentYear').textContent = new Date().getFullYear();
    </script>
  </body>
</html>
"""  # noqa: E501

nav = """
    <header>
      <a href="ROOT_REPLACEMENT/" style="text-decoration: none; color: inherit; display:
         flex; align-items: center;">
        <img src="ROOT_REPLACEMENT/fri-logo.png" alt="Forecasting Research Institute Logo"
             class="logo-img" style="height: 50px;">
        <h1>ForecastBench</h1>
      </a>
      <nav id="navbar">
        <a href="paper.html">Paper</a>
        <a href="datasets.html">Datasets</a>
        <a href="https://github.com/forecastingresearch/forecastbench/wiki">Docs</a>
        <a href="https://github.com/forecastingresearch/forecastbench">GitHub</a>
      </nav>
      <div class="hamburger" id="hamburger">
        &#9776;
      </div>
    </header>
"""

LOCAL_FOLDER = "/tmp/website"


def write(content, filename, ignore_header_footer=False):
    """Write HTML content to filename."""
    local_file = f"{LOCAL_FOLDER}/{filename}"
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    with open(local_file, "w") as file:
        entire_page = content if ignore_header_footer else f"{top}{nav}{content}{bottom}"
        file.write(entire_page)


def get_latest_leaderboards():
    """Copy latest leaderboards from private bucket to website bucket."""
    files = [
        "leaderboard_overall.html",
        "human_leaderboard_overall.html",
        "human_combo_leaderboard_overall.html",
    ]
    for f in files:
        local_filename = gcp.storage.download(
            bucket_name=env.PUBLIC_RELEASE_BUCKET,
            filename=f"leaderboards/html/{f}",
        )
        gcp.storage.upload(
            bucket_name=env.WEBSITE_BUCKET,
            local_filename=local_filename,
            destination_folder="leaderboards",
        )


def make_index():
    """Make index.html."""
    content = """
    <main>
      <div class="intro-section">
        <h1>Welcome</h1>
        <p>Forecastbench is a dynamic, continuously-updated benchmark designed to measure the
           accuracy of ML systems on a constantly evolving set of forecasting questions.
        </p>
      </div>
      <div class="abstract-section">
        <p>Forecasts of future events are essential inputs into informed decision-making. ML systems
        have the potential to deliver forecasts at scale, but there is no framework for evaluating
        the accuracy of ML systems on a standardized set of forecasting questions. To address this
        gap, we introduce ForecastBench: a dynamic benchmark that evaluates the accuracy of ML
        systems on an automatically generated and regularly updated set of 1,000 forecasting
        questions. To avoid any possibility of data leakage, ForecastBench is comprised solely of
        questions about future events that have no known answer at the time of submission.</p>
      </div>
      <div class="intro-section">
        <h1>Benchmark your model</h1>
        <p>Would you like to benchmark your model's forecasting capabilities on ForecastBench?</p>
        <p> </p>
        <p>Find out how by following the
           <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench">
           instructions on how to submit</a>.
      </div>
      <div class="intro-section">
        <h1>Leaderboards</h1>
        <p>The leaderboard is updated on a nightly basis. To see past leaderboards, see the
           <a href="https://github.com/forecastingresearch/forecastbench-datasets">
             datasets repository</a>.
        <div class="leaderboard-links">
          <a href="leaderboards/leaderboard_overall.html">LLM Leaderboard</a>
          <a href="leaderboards/human_leaderboard_overall.html">LLM / Human Leaderboard</a>
        </div>
      </div>
    </main>
"""
    write(content=content, filename="index.html")


def make_404():
    """Make 404.html."""
    content = """
    <main>
      <h1>404</h1>
      <p>The bots that predicted this page's existence missed the mark;
         we have updated their Brier scores :)
      </p>
    </main>
"""
    write(content=content, filename="404.html")


def make_forecast_sets_index():
    """Create index for forecast sets located in the `datasets/forecast_sets` directory."""
    prefix = "datasets/forecast_sets"
    files = gcp.storage.list_with_prefix(bucket_name=env.WEBSITE_BUCKET, prefix=prefix)

    content = """
    <main>
      <div class="intro-section">
        <h1>Forecast Sets</h1>
        Forecast sets are submitted by each team for a given question set. They follow this
        <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench#4-submitted-forecast-set-data-dictionary">
        data dictionary</a>.
        <ul>"""  # noqa: B950
    count = 0
    for f in files:
        if f.endswith(".json"):
            count += 1
            file_name = f.split("/")[-1]
            file_url = f"https://storage.googleapis.com/{env.WEBSITE_BUCKET}/{f}"
            content += f'\n        <li><a href="{file_url}">{file_name}</a></li>'
    content += """
         </ul>
      </div>
    </main>"""

    logger.info(f"There are {count} files.")

    write(content=content, filename="datasets_forecast_sets_index.html")


def make_datasets():
    """Make datasets.html."""
    make_forecast_sets_index()
    today_iso = dates.get_date_today_as_iso().replace("-", "--")
    dataset_link = "https://github.com/forecastingresearch/forecastbench-datasets"
    superforecaster_link = (
        f"{dataset_link}/"
        "blob/main/datasets/forecast_sets/2024-07-21/"
        "2024-07-21.ForecastBench.human_super_individual.json"
    )
    public_link = (
        f"{dataset_link}/"
        "blob/main/datasets/forecast_sets/2024-07-21/"
        "2024-07-21.ForecastBench.human_public_individual.json"
    )
    content = f"""
    <main>
    <h2>Question and Resolution Sets</h2>
      <p><a href="{dataset_link}">
         ForecastBench Dataset repository</a> <a href="{dataset_link}">
         <img src="https://img.shields.io/badge/last_updated-{today_iso}-006400"
              alt="arxiv 2409.19839" style="vertical-align: -3px;">
         </a>
      </p>

    <h2>Forecast Sets</h2>
      <p><a href="{superforecaster_link}">Superforecaster Forecasts</a></p>
      <p><a href="{public_link}">General Public Forecasts</a></p>
      <p><a href="datasets_forecast_sets_index.html">LLM and Aggregated Human Forecasts</a></p>
    </main>
"""
    write(content=content, filename="datasets.html")


def robots():
    """Write robots.txt."""
    content = """User-agent: *
Disallow:

Sitemap: https://www.forecastbench.org/sitemap.xml
"""
    write(content=content, filename="robots.txt", ignore_header_footer=True)


def sitemap():
    """Create sitemap to help with indexing."""
    today = datetime.today().strftime("%Y-%m-%d")
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://www.forecastbench.org/</loc>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://www.forecastbench.org/paper.html</loc>
    <priority>0.9</priority>
  </url>
  <url>
    <loc>https://www.forecastbench.org/datasets.html</loc>
    <lastmod>{today}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>
</urlset>
"""
    write(content=content, filename="sitemap.xml", ignore_header_footer=True)


def make_forwarding_pages():
    """Make pages that forward to datasets.html.

    Have to make these to easily anonymize the paper.
    """
    content = """<!DOCTYPE html>
    <html>
    <head>
        <meta http-equiv="refresh" content="0; url=../datasets.html">
        <title>Redirecting...</title>
    </head>
    <body>
      <p>If you are not redirected automatically, <a href="../datasets.html">click here</a>.</p>
    </body>
    </html>
    """
    os.makedirs("datasets", exist_ok=True)
    for filename in [
        "datasets/forecast_sets.html",
        "datasets/question_sets.html",
    ]:
        write(content=content, filename=filename, ignore_header_footer=True)


def make_paper():
    """Make paper.html."""
    content = """
    <main>
      <h2>Paper</h2>
      <p><a href="https://iclr.cc/virtual/2025/poster/28507">ForecastBench: A Dynamic Benchmark of AI
         Forecasting Capabilities</a> <a href="https://iclr.cc/virtual/2025/poster/28507">
         <img src="https://img.shields.io/badge/ICLR-2025-D5FFC1?labelColor=2A363F&logo=data:image/svg%2bxml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9Im5vIj8+CjwhRE9DVFlQRSBzdmcgUFVCTElDICItLy9XM0MvL0RURCBTVkcgMS4xLy9FTiIgImh0dHA6Ly93d3cudzMub3JnL0dyYXBoaWNzL1NWRy8xLjEvRFREL3N2ZzExLmR0ZCI+Cjxzdmcgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgdmlld0JveD0iMCAwIDEwNyA4OSIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4bWw6c3BhY2U9InByZXNlcnZlIiB4bWxuczpzZXJpZj0iaHR0cDovL3d3dy5zZXJpZi5jb20vIiBzdHlsZT0iZmlsbC1ydWxlOmV2ZW5vZGQ7Y2xpcC1ydWxlOmV2ZW5vZGQ7c3Ryb2tlLWxpbmVqb2luOnJvdW5kO3N0cm9rZS1taXRlcmxpbWl0OjI7Ij4KICAgIDxnIHRyYW5zZm9ybT0ibWF0cml4KDEuMzMzMzMsMCwwLC0xLjMzMzMzLC0xLjgxMzMzLDEwMC4xODQpIj4KICAgICAgICA8ZyBpZD0iZzYzIj4KICAgICAgICAgICAgPGcgaWQ9ImczNyIgdHJhbnNmb3JtPSJtYXRyaXgoMSwwLDAsMSw2Ljc3NTY3LC01Ljc0OTA1KSI+CiAgICAgICAgICAgICAgICA8ZyBpZD0iZzM5Ij4KICAgICAgICAgICAgICAgICAgICA8ZyBpZD0iZzQ1Ij4KICAgICAgICAgICAgICAgICAgICAgICAgPGcgaWQ9Imc0NyI+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICA8cGF0aCBpZD0icGF0aDYxIiBkPSJNMTQuNzIzLDQ2LjMwNEwxOS4zNTQsNDYuMzAyTDE5LjM1NCw1MC4yNDlMMjQuMjU3LDU1LjE1M0wzMC42NDgsNTUuMTUzTDMwLjY0OCw2Mi4yMjlMMjMuNTczLDYyLjIyOUwyMy41NzIsNTUuODM4TDE4LjY2Nyw1MC45MzRMMTQuNzIzLDUwLjkzNEwxNC43MjMsNDYuMzA0Wk0xMi42ODIsNjEuMTA4TDEyLjY4Miw2Mi45NTlMMTAuODMsNjIuOTU4TDEwLjgzLDYxLjUzN0w5LjI0OSw1OS45NTVMNi4yOTksNTkuOTU0TDYuMjk4LDU2LjU3N0w5LjY3Nyw1Ni41NzdMOS42NzksNTkuNTI2TDExLjI1OSw2MS4xMDhMMTIuNjgyLDYxLjEwOFpNNDEuNjE4LDQyLjQ0MUw0My44NDgsNDQuNjcyTDQ1Ljg1Miw0NC42NzJMNDUuODUyLDQ3LjI4TDQzLjI0NCw0Ny4yOEw0My4yNDQsNDUuMjc2TDQxLjAxMyw0My4wNDZMMzYuODU1LDQzLjA0NEwzNi44NTUsMzguMjg0TDQxLjYxOCwzOC4yODVMNDEuNjE4LDQyLjQ0MVpNNDguMTU2LDMyLjEwMkw0OC4xNTYsMzYuODYzTDQzLjM5NiwzNi44NjNMNDMuMzk0LDMyLjcwNUw0MS4xNjQsMzAuNDc1TDM5LjE2LDMwLjQ3NUwzOS4xNiwyNy44NjZMNDEuNzcxLDI3Ljg2Nkw0MS43NzEsMjkuODcxTDQzLjk5OSwzMi4xMDFMNDguMTU2LDMyLjEwMlpNNC43NzQsNDcuMTEyTDIuOTc2LDQ1LjMxNkwxLjM2LDQ1LjMxNkwxLjM2LDQzLjIxMUwzLjQ2NCw0My4yMTJMMy40NjQsNDQuODI4TDUuMjYzLDQ2LjYyNkw4LjYxMyw0Ni42MjZMOC42MTMsNTAuNDY2TDQuNzc1LDUwLjQ2Nkw0Ljc3NCw0Ny4xMTJaTTE5LjQ0OSwzNC40NDJMMTYuOTIzLDMxLjkxNUwxMi4yMTMsMzEuOTE1TDEyLjIxMywyNi41MjFMMTcuNjA2LDI2LjUyMUwxNy42MDgsMzEuMjNMMjAuMTM2LDMzLjc1OUwyMi40MDQsMzMuNzU5TDIyLjQwNSwzNi43MTNMMTkuNDQ5LDM2LjcxM0wxOS40NDksMzQuNDQyWk0yOC4xNzYsMzIuMDAzTDI1LjY0OSwyOS40NzdMMjMuMzgxLDI5LjQ3N0wyMy4zNzksMjYuNTIxTDI2LjMzNSwyNi41MjFMMjYuMzM1LDI4Ljc5MkwyOC44NjEsMzEuMzE5TDMzLjU3MSwzMS4zMTlMMzMuNTcxLDM2LjcxM0wyOC4xNzgsMzYuNzEzTDI4LjE3NiwzMi4wMDNaTTQwLjM4NCw1MS43MTdMMzQuNzEsNTEuNzE1TDM0LjcxLDQ2LjcyOEwzMi4xMzksNDQuMTU2TDI4LjE5NCw0NC4xNTZMMjguMTk0LDM5LjUyNUwzMi44MjQsMzkuNTI1TDMyLjgyNCw0My40NzFMMzUuMzk1LDQ2LjA0Mkw0MC4zODQsNDYuMDQyTDQwLjM4NCw1MS43MTdaTTMxLjE4NSw0Ni43MDFMMzEuMTg1LDUxLjY3OUwzNC4yMzYsNTQuNzNMMzkuNjEsNTQuNzNMMzkuNjEsNjAuNzkxTDMzLjU1Miw2MC43OUwzMy41NTIsNTUuNDE2TDMwLjQ5OSw1Mi4zNjNMMjUuNTIxLDUyLjM2M0wyNS41MjEsNDcuMzg4TDIyLjU3OCw0NC40NDFMMTkuNjgyLDQ0LjQ0MUwxOS42OCw0MC44NTlMMjMuMjYyLDQwLjg1OUwyMy4yNjIsNDMuNzU3TDI2LjIwNyw0Ni43MDFMMzEuMTg1LDQ2LjcwMVpNMTkuMTU4LDcyLjcwNEwxNy45MDgsNzIuNzA0TDE3LjkwOCw3MS40NTNMMTkuMTU4LDcxLjQ1M0wxOS4xNTgsNzIuNzA0Wk0xNi45MzcsNjguMzQ5TDEzLjgzNyw2OC4zNDlMMTMuODM3LDY1LjI1TDE2LjkzNyw2NS4yNTFMMTYuOTM3LDY4LjM0OVpNMTMuMDAyLDcxLjQ4OEwxMC43MDMsNzEuNDg4TDEwLjcwMyw2OS4xODlMMTMuMDAyLDY5LjE4OUwxMy4wMDIsNzEuNDg4Wk00Ni4zODIsMjcuNTQ0TDQ4LjQ5NCwyNy41NDRMNDguNDk0LDI5LjY1N0w0Ni4zODIsMjkuNjU3TDQ2LjM4MiwyNy41NDRaTTEyLjY4NSw0NC40MTdMMTAuNTc0LDQ0LjQxN0wxMC41NzQsNDIuMzA2TDEyLjY4NSw0Mi4zMDZMMTIuNjg1LDQ0LjQxN1pNMTguMDI1LDU1LjMyMUwxNS45MTUsNTUuMzIxTDE1LjkxNSw1My4yMTFMMTguMDI1LDUzLjIxMUwxOC4wMjUsNTUuMzIxWk0yMy42NzYsNjkuNzA1TDE5LjMzNCw2OS43MDVMMTkuMzM0LDY1LjM2MUwyMy42NzYsNjUuMzYxTDIzLjY3Niw2OS43MDVaTTM0Ljk0OCw2Ny42ODhMMzAuNzQzLDY3LjY4OEwzMC43NDMsNjMuNDg0TDM0Ljk0OCw2My40ODRMMzQuOTQ4LDY3LjY4OFpNMjAuNzM2LDYyLjkzNUwxNS4xNzIsNjIuOTM1TDE1LjE3Miw1Ny4zN0wyMC43MzYsNTcuMzcxTDIwLjczNiw2Mi45MzVaTTQ4LjQ3Nyw1MS45MDNMNDUuNjgxLDUxLjkwMUw0NS42ODEsNDkuMTA3TDQ4LjQ3Nyw0OS4xMDhMNDguNDc3LDUxLjkwM1pNNDMuNjAyLDU3LjM2Nkw0MS4zNTUsNTcuMzY1TDQxLjM1NCw1NS4xMThMNDMuNjAyLDU1LjExOEw0My42MDIsNTcuMzY2Wk0yOC43MSw2Ny41NjlMMjUuOTY5LDY3LjU2OUwyNS45NjksNjQuODI4TDI4LjcxLDY0LjgyOUwyOC43MSw2Ny41NjlaTTEyLjQ3LDU0LjIzM0wxMC4xNDgsNTQuMjMzTDEwLjE0OCw1MS45MTJMMTIuNDcsNTEuOTEyTDEyLjQ3LDU0LjIzM1pNMzkuNTQ1LDM1LjM2MkwzNy4yMjUsMzUuMzYyTDM3LjIyMywzMy4wMzlMMzkuNTQ3LDMzLjAzOUwzOS41NDUsMzUuMzYyWiIgc3R5bGU9ImZpbGw6dXJsKCNfTGluZWFyMSk7ZmlsbC1ydWxlOm5vbnplcm87Ii8+CiAgICAgICAgICAgICAgICAgICAgICAgIDwvZz4KICAgICAgICAgICAgICAgICAgICA8L2c+CiAgICAgICAgICAgICAgICA8L2c+CiAgICAgICAgICAgIDwvZz4KICAgICAgICAgICAgPGcgaWQ9InBhdGg3MSIgdHJhbnNmb3JtPSJtYXRyaXgoMSwwLDAsMSw2Ljc3NTY3LC01Ljc0OTA1KSI+CiAgICAgICAgICAgICAgICA8cGF0aCBkPSJNNTkuMDUxLDMxLjM5Nkw1Ni4wODYsMzEuMzk2TDU2LjA4NiwzNC4zNjJMNTkuMDUxLDM0LjM2Mkw1OS4wNTEsMzEuMzk2Wk01NC41NzEsNDQuODc4TDUyLjk2NSw0NC44NzhMNTIuOTY1LDQ2LjQ4NEw1NC41NzEsNDYuNDg1TDU0LjU3MSw0NC44NzhaTTQ4LjcyOCw1OS45MDVMNDUuODg2LDU5LjkwNUw0NS44ODYsNjIuNzQ2TDQ4LjcyOCw2Mi43NDZMNDguNzI4LDU5LjkwNVpNNDEuMDQ0LDY3LjY3MUwzOS4zOTEsNjcuNjcxTDM5LjM5MSw2OS4zMjJMNDEuMDQ0LDY5LjMyMkw0MS4wNDQsNjcuNjcxWk02Ni44NzIsMzcuNTRDNjMuMTExLDQxLjE0MSA2NS44OTYsNDEuMTYgNjUuOTcsNDMuMzc4QzY2LjA0Niw0NS41OTUgNjQuMTg5LDQ2LjAwOSA2NC4xODksNDYuMDA5QzY0Ljk5Miw0Ny44OTYgNjUuMDkyLDQ4LjMzMiA2NS4wOTIsNDguMzMyQzYzLjM4Nyw0OS42MzUgNjMuMDg2LDUxLjI0MiA2My4wODYsNTEuMjQyQzY0Ljc5LDUzLjg0OSA2NS45OSw1Ni44NDQgNjUuOTksNTYuODQ0QzY1Ljk5LDU2Ljg0NCA1NC43MjcsNjAuNDUyIDUwLjQ3Niw2My4xNzhDNDguODcyLDYzLjg0NCA0OS4yNzMsNjYuMzIzIDQ5LjI3Myw2Ni4zMjNDMzguMjM5LDgwLjQ2NyAyMS43MDksNzMuMzQ1IDIxLjcwOSw3My4zNDVMMjEuNzA5LDcyLjk0M0MyMS43MDksNzIuOTQzIDIyLjk2NSw3Mi43OTMgMjQuOTYsNzIuMjM0TDI3LjcyOCw3Mi4yMzJMMjcuNzI4LDcxLjMyNkMyOC43NDIsNzAuOTQ1IDI5Ljg0MSw3MC40ODEgMzAuOTkzLDY5LjkxNUwzNy41MTMsNjkuOTE1TDM3LjUxMSw2NS43OEMzOC4yMTksNjUuMjE0IDM4LjkyNCw2NC42MDQgMzkuNjIsNjMuOTQ3TDQzLjIyMSw2My45NDdMNDMuMjIyLDU5Ljk2NEM0NC4xMTEsNTguODExIDQ0Ljk2NCw1Ny41NTcgNDUuNzYyLDU2LjE5QzQ2LjI1Niw1NS40MTcgNDYuNzEzLDU0LjY0NSA0Ny4xNDMsNTMuODdMNTAuMzc3LDUzLjg3TDUwLjM3Nyw0OS4zMjdMNTIuOTY1LDQ5LjMyNUw1Mi45NjUsNDYuNDg1TDUxLjY2OSw0Ni40NjdDNTEuNjY5LDQ2LjQ2NyA1MS42ODMsNDMuNDMzIDUxLjY4NCw0MS45NjlMNDguMjQsNDEuOTY4TDQ4LjI0LDM4LjQ3MUw1MS43MzgsMzguNDcxTDUxLjczOCw0MS44NzdMNTUuMjQsNDEuODc3TDU1LjI0LDM1LjI2NEw1Mi42MzIsMzUuMjY0QzUzLjI3NSwyNi4wMiA1MC45OCwxOS4zNzYgNTAuOTgsMTkuMzc2QzUyLjM4NSwyMy4yODkgNTYuMDk2LDI2LjU0OSA2MS4xODEsMjguMzczQzY1Ljk2OSwzMC4wODggNzAuNjM1LDMzLjkzOSA2Ni44NzIsMzcuNTQiIHN0eWxlPSJmaWxsOnJnYigxNjMsMTYwLDE2MCk7ZmlsbC1ydWxlOm5vbnplcm87Ii8+CiAgICAgICAgICAgIDwvZz4KICAgICAgICA8L2c+CiAgICA8L2c+CiAgICA8ZGVmcz4KICAgICAgICA8bGluZWFyR3JhZGllbnQgaWQ9Il9MaW5lYXIxIiB4MT0iMCIgeTE9IjAiIHgyPSIxIiB5Mj0iMCIgZ3JhZGllbnRVbml0cz0idXNlclNwYWNlT25Vc2UiIGdyYWRpZW50VHJhbnNmb3JtPSJtYXRyaXgoLTM1LjQ4MDksMzkuMTcxNCwtMzkuMTcxNCwtMzUuNDgwOSw0NC4wMTc2LDI0LjU0MikiPjxzdG9wIG9mZnNldD0iMCIgc3R5bGU9InN0b3AtY29sb3I6cmdiKDg4LDIwMSwyMzcpO3N0b3Atb3BhY2l0eToxIi8+PHN0b3Agb2Zmc2V0PSIwLjE1IiBzdHlsZT0ic3RvcC1jb2xvcjpyZ2IoODgsMjAxLDIzNyk7c3RvcC1vcGFjaXR5OjEiLz48c3RvcCBvZmZzZXQ9IjAuNDciIHN0eWxlPSJzdG9wLWNvbG9yOnJnYig5NiwxOTUsNTApO3N0b3Atb3BhY2l0eToxIi8+PHN0b3Agb2Zmc2V0PSIwLjgzIiBzdHlsZT0ic3RvcC1jb2xvcjpyZ2IoMjAyLDIyNywyOCk7c3RvcC1vcGFjaXR5OjEiLz48c3RvcCBvZmZzZXQ9IjEiIHN0eWxlPSJzdG9wLWNvbG9yOnJnYigyMDIsMjI3LDI4KTtzdG9wLW9wYWNpdHk6MSIvPjwvbGluZWFyR3JhZGllbnQ+CiAgICA8L2RlZnM+Cjwvc3ZnPgo=" alt="ICLR 2025" style="vertical-align: -3px;">
         </a>
      </p>
    <div class="citation">@inproceedings{karger2025forecastbench,
      title={ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities},
      author={Ezra Karger and Houtan Bastani and Chen Yueh-Han and Zachary Jacobs and Danny Halawi and Fred Zhang and Philip E. Tetlock},
      year={2025},
      booktitle={International Conference on Learning Representations (ICLR)},
      url={https://iclr.cc/virtual/2025/poster/28507}
}
</div>
      <h2>Leaderboards from paper</h2>
      <p><a href="paper/leaderboards/2025-02-17/human_leaderboard_overall.html">LLM / Human Leaderboard</a></p>
      <p><a href="paper/leaderboards/2025-02-17/leaderboard_overall.html">LLM Leaderboard</a></p>
      <p><a href="paper/leaderboards/2025-02-17/human_combo_leaderboard_overall.html">LLM / Human Combo Leaderboard</a></p>
      <p>Also available in the <a href="https://github.com/forecastingresearch/forecastbench-datasets">forecastbench-datasets repo</a>
         (<a class="commit" href="https://github.com/forecastingresearch/forecastbench-datasets/commit/601f6d9e67952032205147305df0b4db8f13f727">601f6d9</a>).</p>
      <h2>Preprint versions of paper</h2>
      <p><a href="https://arxiv.org/abs/2409.19839">ForecastBench: A Dynamic Benchmark of AI
         Forecasting Capabilities</a> <a href="https://arxiv.org/abs/2409.19839">
         <img src="https://img.shields.io/badge/arXiv-2409.19839-272727?logo=arxiv&labelColor=B31B1B" alt="arXiv:2409.19839" style="vertical-align: -3px;">
      </p>
    </main>
"""  # noqa: B950
    write(content=content, filename="paper.html")


def upload(build_env):
    """Upload all files in the website directory."""
    for root, _, files in os.walk(LOCAL_FOLDER):
        for file in files:
            local_filename = os.path.join(root, file)
            if build_env == "prod":
                destination_folder = os.path.dirname(local_filename).replace(f"{LOCAL_FOLDER}", "")
                if destination_folder.startswith("/"):
                    destination_folder = destination_folder[1:]
                    logger.info(f"{local_filename} --> {destination_folder}")
                gcp.storage.upload(
                    bucket_name=env.WEBSITE_BUCKET,
                    local_filename=local_filename,
                    destination_folder=destination_folder,
                )


def main():
    """Generate HTML files."""
    build_env = os.environ.get("BUILD_ENV", "prod")
    logger.info(f"Build env: {build_env}")
    root_replacement = "" if build_env == "prod" else "."

    if os.path.exists(LOCAL_FOLDER):
        shutil.rmtree(LOCAL_FOLDER)

    global top, nav
    top = top.replace("ROOT_REPLACEMENT", root_replacement)
    nav = nav.replace("ROOT_REPLACEMENT", root_replacement)

    get_latest_leaderboards()
    make_index()
    make_404()
    make_datasets()
    robots()
    sitemap()
    make_forwarding_pages()
    make_paper()

    shutil.copy("styles.css", LOCAL_FOLDER)
    upload(build_env)

    # No need to upload the below files as they don't change regularly.
    # Copy them locally so site looks good in case we're testing locally.
    shutil.copy("fri-favicon.png", LOCAL_FOLDER)
    shutil.copy("fri-logo.png", LOCAL_FOLDER)

    logger.info("Done.")


if __name__ == "__main__":
    main()
