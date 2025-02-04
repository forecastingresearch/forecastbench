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
           &copy; 2024
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
      <p>Oops. Go back to the main page. Maybe we'll have something funny here later?</p>
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
      <p>Superforecaster Forecasts (forthcoming)</p>
      <p>General Public Forecasts (forthcoming)</p>
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
    write(content=content, filename="robots.txt")


def sitemap():
    """Create sitemap to help with indexing."""
    today = datetime.today().strftime("%Y-%m-%d")
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://www.forecastbench.org/</loc>
    <lastmod>{today}</lastmod>
  </url>
  <url>
    <loc>https://www.forecastbench.org/paper.html</loc>
    <lastmod>{today}</lastmod>
  </url>
  <url>
    <loc>https://www.forecastbench.org/datasets.html</loc>
    <lastmod>{today}</lastmod>
  </url>
</urlset>
"""
    write(content=content, filename="sitemap.xml")


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
      <p><a href="https://arxiv.org/abs/2409.19839">ForecastBench: A Dynamic Benchmark of AI
         Forecasting Capabilities</a> <a href="https://arxiv.org/abs/2409.19839">
         <img src="https://img.shields.io/badge/arxiv-2409.19839-A42D25" alt="arxiv 2409.19839"
         style="vertical-align: -3px;">
         </a>
      </p>
    <div class="citation">@misc{karger2024forecastbenchdynamicbenchmarkai,
      title={ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities},
      author={Ezra Karger and Houtan Bastani and Chen Yueh-Han and Zachary Jacobs and Danny Halawi and Fred Zhang and Philip E. Tetlock},
      year={2024},
      eprint={2409.19839},
      archivePrefix={arXiv},
      primaryClass={cs.LG},
      url={https://arxiv.org/abs/2409.19839},
}
</div>
      <h2>Leaderboards from paper</h2>
      <p><a href="paper/leaderboards/human_leaderboard_overall.html">LLM / Human Leaderboard</a></p>
      <p><a href="paper/leaderboards/leaderboard_overall.html">LLM Leaderboard</a></p>
      <p><a href="paper/leaderboards/human_combo_leaderboard_overall.html">
         LLM / Human Combo Leaderboard</a>
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
