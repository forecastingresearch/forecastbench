---
layout: splash
title: "Leaderboard"
permalink: /leaderboard/
classes: wide
head_css:
  - https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css
  - https://cdn.datatables.net/1.13.7/css/dataTables.semanticui.min.css
  - https://cdn.datatables.net/responsive/2.4.1/css/responsive.semanticui.min.css
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.css
  - /assets/css/tooltips.css
footer_scripts:
  - /assets/js/smooth/smooth-scroll.js
after_footer_scripts:
  - https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js
  - https://cdn.datatables.net/1.13.7/js/dataTables.semanticui.min.js
  - https://cdn.datatables.net/responsive/2.4.1/js/dataTables.responsive.min.js
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.js
  - /assets/js/leaderboard_full.js
  - /assets/js/tooltip-init.js
---



<div class="page-title">{{ page.title }}<sup><a href="#notes" style="text-decoration:none;">‡</a></sup></div>
<div id="leaderboard-table-full"></div>

<section id="notes" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">‡Notes</h1>
  <div class="site-feature-row__content-small">
    <ul>
    <li>Performance on Dataset and Market questions is scored using a <a href="https://github.com/forecastingresearch/forecastbench/wiki/Changelog#scoring-method-two-way-fixed-effects">two-way fixed effects model <i class="fa-solid fa-arrow-up-right-from-square"></i></a> to account for differences in question difficulty across question sets. The Overall score is then the equal-weighted average of the Dataset and Market scores.</li>
    <li>To ensure leaderboard stability, models are included on the leaderboard <a href="https://github.com/forecastingresearch/forecastbench/wiki/Changelog#100-day-delay-before-a-forecaster-is-included-on-the-leaderboard">100 days after forecast submission <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</li>
    <li>Human comparison groups are highlighted in red.</li>
    <li>The zero shot and scratchpad prompts used for the models run by ForecastBench can be found on <a href="https://github.com/forecastingresearch/forecastbench/blob/main/src/helpers/llm_prompts.py">GitHub <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</li>
    <li>The ForecastBench baseline forecasters are described on the <a href="https://github.com/forecastingresearch/forecastbench/wiki/Changelog#baseline-forecasters">Changelog <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</li>
    <li>The "crowd forecast" provided to models run by ForecastBench were valid 10 days before the forecast due date. This delay exists to allow us to run human surveys periodically. Also note that these crowd forecasts only impact Market questions as there is no crowd forecast for Dataset questions.</li>
    </ul>
  </div>
</section>
