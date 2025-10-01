---
layout: splash
title: "Tournament"
permalink: /tournament/
classes: wide
head_css:
  - https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css
  - https://cdn.datatables.net/1.13.7/css/dataTables.semanticui.min.css
  - https://cdn.datatables.net/responsive/2.4.1/css/responsive.semanticui.min.css
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.css
  - /assets/css/tooltips.css
footer_scripts:
  - /assets/js/smooth-scroll.js
after_footer_scripts:
  - https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js
  - https://cdn.datatables.net/1.13.7/js/dataTables.semanticui.min.js
  - https://cdn.datatables.net/responsive/2.4.1/js/dataTables.responsive.min.js
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.js
  - /assets/js/leaderboard_tournament_full.js
  - /assets/js/tooltip-init.js
---

<div style="display:flex;">
  <div style="flex:3;">
     <div class="leaderboard-wrapper">
         <h1 class="leaderboard-title">Tournament leaderboard<sup><a href="#notes" style="text-decoration:none;">‡</a></sup></h1>
         <p>The tournament leaderboard tracks frontier LLM forecasting accuracy, where teams are free to enhance models in any way they choose&mdash;with tools, added context, fine-tuning, ensembling, or other methods. Its purpose is to capture the forefront of LLM forecasting ability. The models submitted regularly by the ForecastBench team are provided the crowd forecast as context for market questions.</p>
         {% include leaderboard-explainer.html %}
         <p>The Tournament Leaderboard is open to <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench" class="no-wrap">public submissions <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
         <div id="leaderboard-table-full"></div>
     </div>
  </div>
</div>

<section id="notes" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">‡Notes</h1>
  <div class="site-feature-row__content-small">
    <ul>
    {% include leaderboard-tournament-notes.html %}
    </ul>
  </div>
</section>


<section id="participate" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">Benchmark your model!</h1>
    <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
    <p>Would you like to have your model's forecasting capabilities evaluated on ForecastBench? We’re creating a community of forecasters who are engaging with LLMs to discover the forefront of their forecasting abilities. Though your setup does not need to be made <a href="https://github.com/forecastingresearch/forecastbench/wiki/Open-source-participants">open source <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, we do provide a growing list of ForecastBench participants who have left the door open to collaboration in this way. We'll be in touch with top performers to discuss their forecasting strategies and, potentially, feature them on a <a href="https://substack.com/@forecastingresearchinstitute">Forecasting Research Institute blog post <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    <p>To participate, follow the <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench">instructions on how to submit<i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    </div>
  </div>
</section>
