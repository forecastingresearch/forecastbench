---
layout: splash
title: "Calibration"
permalink: /calibration/
custom_css: /assets/css/custom.scss
after_footer_scripts:
  - https://cdn.jsdelivr.net/npm/d3@7
  - /assets/js/calibration_chart.js
---

<div id="calibration-page" class="leaderboard-wrapper">
  <h1 class="leaderboard-title">Model Calibration Analysis</h1>
  <p>A well-calibrated forecaster's predicted probabilities match observed frequencies: when it says 70%, the event should occur ~70% of the time.
  This page shows <strong>reliability diagrams</strong> and calibration metrics for each model on ForecastBench.</p>
  <ul>
    <li>Points on the diagonal indicate perfect calibration.</li>
    <li>Points above the diagonal indicate underconfidence (events happen more often than predicted).</li>
    <li>Points below the diagonal indicate overconfidence (events happen less often than predicted).</li>
    <li>Circle size reflects the number of forecasts in each probability bin.</li>
  </ul>

  <div class="chart-container">
    <div class="controls">
      <div class="control-section">
        <div class="control-label">Leaderboard</div>
        <div class="segmented-control">
          <div class="segmented-option">
            <input type="radio" id="lb_baseline" name="lbSelect" value="baseline" checked>
            <label for="lb_baseline">Baseline</label>
          </div>
          <div class="segmented-option">
            <input type="radio" id="lb_tournament" name="lbSelect" value="tournament">
            <label for="lb_tournament">Tournament</label>
          </div>
        </div>
      </div>
      <div class="control-section">
        <div class="control-label">Models</div>
        <div id="model-checkboxes" class="tag-selection"></div>
      </div>
    </div>
    <div id="reliability-diagram"></div>
  </div>

  <h2 class="leaderboard-title" style="margin-top: 2rem;">Calibration Metrics</h2>
  <p>
    <strong>ECE</strong> (Expected Calibration Error): weighted mean absolute gap between predicted probability and observed frequency. Lower is better.
    <strong>Reliability</strong>: weighted mean squared gap (Brier decomposition). Lower is better.
    <strong>Resolution</strong>: how much observed frequencies vary across bins. Higher is better.
    <strong>Uncertainty</strong>: base-rate variance (same for all models). <strong>Sharpness</strong>: spread of forecast probabilities.
  </p>
  <div id="metrics-table-container"></div>
</div>

<div id="tooltip" class="tooltip"></div>
