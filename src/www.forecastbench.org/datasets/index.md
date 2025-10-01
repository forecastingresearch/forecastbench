---
layout: splash
title: "Datasets"
permalink: /datasets/
---

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">GitHub</h1>
      <p>We provide as much data as possible via our <a href="https://github.com/forecastingresearch/forecastbench-datasets">datasets repository on GitHub<i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
      <p><strong>Leaderboards</strong>. The <a href="https://github.com/forecastingresearch/forecastbench-datasets/tree/main/leaderboards">leaderboards <i class="fa-solid fa-arrow-up-right-from-square"></i></a> are updated nightly and stored in git, allowing you to track model ranking over time.</p>
      <p><strong>Resolution values</strong>. <a href="https://github.com/forecastingresearch/forecastbench-datasets/tree/main/datasets/resolution_sets">Resolution values <i class="fa-solid fa-arrow-up-right-from-square"></i></a> are also updated nightly and stored in git.</p>
      <p><strong>Question sets</strong>. <a href="https://github.com/forecastingresearch/forecastbench-datasets/tree/main/datasets/question_sets">Question sets <i class="fa-solid fa-arrow-up-right-from-square"></i></a> are released every two weeks through this repository.</p>
      <p><strong>Human survey data</strong>. The <a href="https://github.com/forecastingresearch/forecastbench-datasets/blob/main/datasets/forecast_sets/2024-07-21/2024-07-21.ForecastBench.human_super_individual.json">superforecaster <i class="fa-solid fa-arrow-up-right-from-square"></i></a> and <a href="https://github.com/forecastingresearch/forecastbench-datasets/blob/main/datasets/forecast_sets/2024-07-21/2024-07-21.ForecastBench.human_public_individual.json">general public <i class="fa-solid fa-arrow-up-right-from-square"></i></a> forecast sets from the 2024-07-21 survey round are available as well.</p>
      <p>This repository is <a href="https://huggingface.co/datasets/forecastingresearch/forecastbench-datasets">mirrored to Hugging Face <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    </div>
  </div>
</section>

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Direct Download</h1>
      <p>We provide some other files as direct downloads.</p>
      <p><strong>Forecast sets</strong>. Download all forecasts that have been submitted for evalution <a href="/assets/data/forecast-sets/forecast_sets.tar.gz">here</a>{% if site.data.direct_download_file_sizes.forecast_sets %} ({{ site.data.direct_download_file_sizes.forecast_sets }}B){% endif %}.</p>
      <p><strong>Processed forecast sets</strong>. Download all processed forecast files <a href="/assets/data/processed-forecast-sets/processed_forecast_sets.tar.gz">here</a>{% if site.data.direct_download_file_sizes.processed_forecast_sets %} ({{ site.data.direct_download_file_sizes.processed_forecast_sets }}B){% endif %}.</p>
      <p><strong>Question fixed effect estimates</strong>. For those interested in detailed question-level analysis, we provide the <a href="/datasets/question-fixed-effects/">question fixed effects estimates</a>, generated when updating the leaderboard.</p>
    </div>
  </div>
</section>
