---
layout: splash
title: "Explore"
permalink: /explore/
custom_css: /assets/css/custom.scss
after_footer_scripts:
  - https://cdn.jsdelivr.net/npm/d3@7
  - /assets/js/explore_sota_graph.js
---

<div class="leaderboard-wrapper">
  <h1 class="leaderboard-title">State-of-the-art model forecasting performance over time</h1>
  <p>This interactive visualization charts the evolution of AI forecasting accuracy on ForecastBench.
  Each point represents a model's difficulty-adjusted Brier score across all questions it predicted on (lower is better), plotted by model release date.
  <ul>
  <li>Orange points mark models that were state of the art (SOTA) when released; they had the best benchmark performance given their release date.</li>
  <li>Vertical bars indicate 95% confidence intervals.</li>
  <li>Gray points show non-SOTA models.</li>
  <li>The orange dashed line shows the estimated linear trend for SOTA performance improvement.</li>
  </ul></p>
  <div class="chart-container">
    <div class="controls">
      <div class="control-section">
        <div class="control-label">Score</div>
        <div class="segmented-control">
          <div class="segmented-option">
            <input type="radio" id="type_overall" name="typeSelect" value="overall" checked>
            <label for="type_overall">Overall</label>
          </div>
          <div class="segmented-option">
            <input type="radio" id="type_dataset" name="typeSelect" value="dataset">
            <label for="type_dataset">Dataset</label>
          </div>
          <div class="segmented-option">
            <input type="radio" id="type_market" name="typeSelect" value="market">
            <label for="type_market">Market</label>
          </div>
        </div>
      </div>
      <div class="control-section">
        <div class="control-label">Comparisons</div>
        <div class="tag-selection">
          <div class="tag-option">
            <input type="checkbox" id="bench_public" value="public" checked>
            <label for="bench_public">Public</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_superforecaster" value="superforecaster" checked>
            <label for="bench_superforecaster">Superforecaster</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_always_0.5" value="always_0.5">
            <label for="bench_always_0.5">Always 0.5</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_imputed" value="imputed">
            <label for="bench_imputed">Imputed Forecaster</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_naive" value="naive">
            <label for="bench_naive">Naive Forecaster</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_always_0" value="always_0">
            <label for="bench_always_0">Always 0</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_always_1" value="always_1">
            <label for="bench_always_1">Always 1</label>
          </div>
          <div class="tag-option">
            <input type="checkbox" id="bench_random_uniform" value="random_uniform">
            <label for="bench_random_uniform">Random Uniform</label>
          </div>
        </div>
      </div>
      <div class="control-section">
        <div class="control-label">Options</div>
        <div class="toggle-section">
          <label class="toggle-switch">
            <input type="checkbox" id="includeFreeze" checked>
            <span class="toggle-slider"></span>
          </label>
          <label for="includeFreeze" class="toggle-label">Tournament models</label>
        </div>
        <div class="toggle-section">
          <label class="toggle-switch">
            <input type="checkbox" id="showLegend" checked>
            <span class="toggle-slider"></span>
          </label>
          <label for="showLegend" class="toggle-label">Legend</label>
        </div>
        <div class="toggle-section">
          <label class="toggle-switch">
            <input type="checkbox" id="showIntersection" checked>
            <span class="toggle-slider"></span>
          </label>
          <label for="showIntersection" class="toggle-label">Projected AI-superforecaster parity</label>
        </div>
      </div>
    </div>
    <div id="chart"></div>
    <div class="instruction">Hold Shift and drag to zoom into a region. Press Escape to reset zoom.</div>
  </div>
</div>

<div id="tooltip" class="tooltip"></div>
