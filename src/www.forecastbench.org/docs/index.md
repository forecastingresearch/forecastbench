---
layout: splash
title: "Docs"
permalink: /docs/
footer_scripts:
  - /assets/js/smooth/smooth-scroll.js
---


<div class="page-title">{{ page.title }}</div>
<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">How does ForecastBench work?</h1>
      <p>ForecastBench evaluates LLMs by regularly asking them to make probabilistic forecasts about future events.</p>
      <p>We use two types of binary prediction questions:
      <ul>
      <li><strong>Dataset questions</strong> are automatically generated from real-world time series (<a href="https://acleddata.com/" class="no-wrap">ACLED <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://db.nomics.world/" class="no-wrap">DBnomics <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://fred.stlouisfed.org/" class="no-wrap">FRED <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://finance.yahoo.com/" class="no-wrap">Yahoo! Finance <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://www.wikipedia.org/" class="no-wrap">Wikipedia <i class="fa-solid fa-arrow-up-right-from-square"></i></a>) using pre-specified templates. Each dataset question generates multiple forecasts at different time horizons, since we ask the same question with eight different resolution dates, ranging from 7 days to 10 years out.</li>
      <li><strong>Market questions</strong> are drawn from leading prediction platforms: <a href="https://manifold.markets/" class="no-wrap">Manifold <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://www.metaculus.com/" class="no-wrap">Metaculus <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://polymarket.com/" class="no-wrap">Polymarket <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://www.randforecastinginitiative.org/" class="no-wrap">Rand Forecasting Initiative <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</li>
      </ul>
      </p>
      <p>ForecastBench operates as a fully automated, dynamic system. New forecasting rounds occur biweekly, with each round generating 500 questions split evenly between market and dataset questions. The leaderboard is updated nightly as new data becomes available and market questions resolve over time, allowing us to continuously track forecasting performance.</p>

<p>To construct the performance ranking, we evaluate forecasters separately on market questions and dataset questions. The overall ranking combines these scores, equally weighting performance by question type. As a result, the overall ranking provides a comprehensive assessment of forecasting ability across both structured time-series data (dataset questions) and real-world events (market questions).</p>
      </div>
  </div>
</section>

<section id="paper" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Latest version of paper</h1>
      <p>To better understand how ForecastBench works, see the latest version of our paper on arXiv. It has been updated since the ICLR 2025 version. <i>Last updated: 28 Feb 2025.</i></p>
      <a href="https://arxiv.org/abs/2409.19839" class="btn btn--primary">arXiv Paper</a>
    </div>
        <div class="site-feature-row__right-3">
        <h1 class="site-feature-row__title">Changelog</h1>
        <p>There have been several noteable changes to the benchmark since the latest version of the paper. You can find a brief overview on the changelog. <i>Last updated: 6 Oct 2025.</i></p>
        <a href="https://github.com/forecastingresearch/forecastbench/wiki/Changelog" class="btn btn--primary">Changelog</a>
    </div>
  </div>
</section>

<section id="technical-report" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Technical Report</h1>
      <p>Our technical report presents the difficulty-adjusted Brier score, the ranking methodology that enables fair comparisons when forecasters predict on different questions. The report details simulation results showing this scoring rule outperforms alternatives and includes our stability analyses demonstrating that rankings become stable within 50 days of a new model participating on ForecastBench. <i>Last updated: 3 Oct 2025.</i></p>
      <a href="/assets/pdfs/forecastbench_updated_methodology.pdf" class="btn btn--primary">Technical Report (PDF)</a>
    </div>
  </div>
</section>

<section id="iclr2025" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">ICLR 2025</h1>
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <p>The benchmark was accepted to ICLR 2025 as a poster. On that page, you can view the <a href="https://iclr.cc/media/PosterPDFs/ICLR%202025/28507.png?t=1741725847.5784986" class="no-wrap">poster <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://iclr.cc/media/iclr-2025/Slides/28507.pdf" class="no-wrap">slides <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://openreview.net/pdf?id=lfPkGWXLLf" class="no-wrap">ICLR version of the paper <i class="fa-solid fa-arrow-up-right-from-square"></i></a>. <i>Last updated: 22 Jan 2025.</i></p>
      <a href="https://iclr.cc/virtual/2025/poster/28507" class="btn btn--primary no-wrap">ICLR Page</a>
    </div>
    <div class="site-feature-row__right-3">
      <div class="citation">
        <pre>@inproceedings{karger2025forecastbench,
  title={ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities},
  author={Ezra Karger and Houtan Bastani and Chen Yueh‑Han and Zachary Jacobs and Danny Halawi and Fred Zhang and Philip E. Tetlock},
  year={2025},
  booktitle={International Conference on Learning Representations (ICLR)},
  url={https://iclr.cc/virtual/2025/poster/28507}
}</pre>
      </div>
    </div>
  </div>
  <hr>
  <h1>Leaderboards</h1>
      <div class="site-feature-row__content">
          <div class="site-feature-row__left-3">
The leaderboards from the ICLR 2025 paper are available both here and in the <a href="https://github.com/forecastingresearch/forecastbench-datasets">forecastbench-datasets repo</a> (<a href="https://github.com/forecastingresearch/forecastbench-datasets/commit/601f6d9e67952032205147305df0b4db8f13f727" class="git-sha">601f6d9</a>).
          </div>
          <div class="site-feature-row__right-2">
            <ul>
              <li><a href="/assets/iclr2025_leaderboards/leaderboards/human_leaderboard_overall.html">LLM / Human Leaderboard</a></li>
              <li><a href="/assets/iclr2025_leaderboards/leaderboards/leaderboard_overall.html">LLM Leaderboard</a></li>
              <li><a href="/assets/iclr2025_leaderboards/leaderboards/human_combo_leaderboard_overall.html">LLM / Human Combo Leaderboard</a></li>
            </ul>
          </div>
      </div>
</section>


