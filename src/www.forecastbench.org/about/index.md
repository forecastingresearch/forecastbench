---
layout: splash
title: "About"
permalink: /about/
footer_scripts:
  - /assets/js/smooth-scroll.js
---

<section id="how-does-forecastbench-work" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">How does ForecastBench work?</h1>
      <p>ForecastBench is a dynamic, continuously-updated benchmark designed to measure the accuracy of ML systems on a constantly changing set of forecasting questions.</p>
      <p>We evaluate LLMs by regularly asking them to make probabilistic forecasts about future events, thereby creating a contamination-free benchmark.</p>
      <p>We use two types of binary prediction questions:
      <ul>
      <li><strong>Dataset questions</strong> are automatically generated from real-world time series (<a href="https://acleddata.com/" class="no-wrap">ACLED <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://db.nomics.world/" class="no-wrap">DBnomics <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://fred.stlouisfed.org/" class="no-wrap">FRED <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://finance.yahoo.com/" class="no-wrap">Yahoo! Finance <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://www.wikipedia.org/" class="no-wrap">Wikipedia <i class="fa-solid fa-arrow-up-right-from-square"></i></a>) using pre-specified templates. Each dataset question generates multiple forecasts at different time horizons, since we ask the same question with 8 different resolution dates, ranging from 7 days to 10 years out.</li>
      <li><strong>Market questions</strong> are drawn from leading prediction platforms: <a href="https://manifold.markets/" class="no-wrap">Manifold <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://www.metaculus.com/" class="no-wrap">Metaculus <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://polymarket.com/" class="no-wrap">Polymarket <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://www.randforecastinginitiative.org/" class="no-wrap">Rand Forecasting Initiative <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</li>
      </ul>
      </p>
      <p>ForecastBench operates as a <a href="/docs/#architecture">fully automated, dynamic system</a>. New forecasting rounds occur every two weeks, with each round generating 500 questions split evenly between market and dataset questions. The leaderboard is updated nightly as new data becomes available and market questions resolve over time, allowing us to continuously track forecasting performance.</p>

<p>To construct the performance ranking, we evaluate forecasters separately on market questions and dataset questions. The overall ranking combines these scores, equally weighting performance by question type. As a result, the overall ranking provides a comprehensive assessment of forecasting ability across both structured time-series data (dataset questions) and real-world events (market questions).</p>
      </div>
  </div>
</section>

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Blog</h1>
      <p>For a high-level overview of ForecastBench, including motivation, key design decisions, and early results, see our <a href="https://forecastingresearch.substack.com/p/ai-llm-forecasting-model-forecastbench-benchmark">introductory blog post <i class="fa-solid fa-arrow-up-right-from-square"></i></a> on the <a href="https://forecastingresearch.substack.com/">Forecasting Research Institute Substack <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    </div>
  </div>
</section>

<section class="site-feature-card team-section">
  <h1 class="site-feature-row__title">Team</h1>
  <p class="team-intro">ForecastBench is developed and maintained by the <a href="https://forecastingresearch.org/" class="no-wrap">Forecasting Research Institute <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, a nonprofit research organization dedicated to advancing the science, practice, and use of forecasting. The ForecastBench team is committed to open science and we publicly provide our <a href="/docs/#codebase">code</a>, <a href="/datasets/">datasets</a> (where licensing permits), and <a href="/docs/">methodology</a> to support reproducible research. For correspondence, please contact <a href="mailto:forecastbench@forecastingresearch.org">forecastbench@forecastingresearch.org</a>.</p>

  <div class="team-grid">

    <div class="team-member">
      <img src="/assets/images/team/houtan.jpg" alt="Houtan Bastani" class="team-photo">
      <h3>Houtan Bastani</h3>
      <p class="team-role">Code</p>
      <p class="team-link"><a href="https://www.linkedin.com/in/houtanb/">linkedin <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/simas.jpeg" alt="Simas Kučinskas" class="team-photo">
      <h3>Simas Kučinskas</h3>
      <p class="team-role">Data</p>
      <p class="team-link"><a href="https://www.simaskucinskas.com/">website <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/zach.jpg" alt="Zachary Jacobs" class="team-photo">
      <h3>Zachary Jacobs</h3>
      <p class="team-role">Surveys</p>
      <p class="team-link"><a href="https://www.linkedin.com/in/zachary-jacobs-8b2433218/">linkedin <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/phil.jpg" alt="Philip E. Tetlock" class="team-photo">
      <h3>Philip E. Tetlock</h3>
      <p class="team-role">Advisor</p>
      <p class="team-link"><a href="https://www.sas.upenn.edu/tetlock/">website <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/ezra.png" alt="Ezra Karger" class="team-photo">
      <h3>Ezra Karger</h3>
      <p class="team-role">Principal Investigator</p>
      <p class="team-link"><a href="https://ezrakarger.com/">website <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>
  </div>

  <h2 style="margin-top: 3rem; margin-bottom: 1rem;">Past contributors</h2>

  <div class="team-grid">

    <div class="team-member">
      <img src="/assets/images/team/yueh-han.png" alt="Yueh-Han Chen" class="team-photo">
      <h3 style="margin-bottom: 0.3rem;">Yueh-Han Chen</h3>
      <p class="team-link" style="margin-top: 0 !important;"><a href="https://john-chen.cc/">website <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/danny.jpg" alt="Danny Halawi" class="team-photo">
      <h3 style="margin-bottom: 0.3rem;">Danny Halawi</h3>
      <p class="team-link" style="margin-top: 0 !important;"><a href="https://www.linkedin.com/in/danny-halawi-6944a9205/">linkedin <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

    <div class="team-member">
      <img src="/assets/images/team/fred.jpg" alt="Fred Zhang" class="team-photo">
      <h3 style="margin-bottom: 0.3rem;">Fred Zhang</h3>
      <p class="team-link" style="margin-top: 0 !important;"><a href="https://fredzhang.me/">website <i class="fa-solid fa-arrow-up-right-from-square"></i></a></p>
    </div>

  </div>
</section>

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Funding</h1>
      <p>ForecastBench is supported by <a href="https://www.openphilanthropy.org/grants/forecasting-research-institute-forecasting-benchmark/">a grant from Open Philanthropy <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
      <p>The Forecasting Research Institute's funders exercise no editorial control or influence over our research methodology, findings, or conclusions.</p>
    </div>
  </div>
</section>
