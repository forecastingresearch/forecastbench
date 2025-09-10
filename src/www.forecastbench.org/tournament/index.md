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
         <h1 class="leaderboard-title">Tournament Leaderboard<sup><a href="#notes" style="text-decoration:none;">‡</a></sup></h1>
         <p>The tournament leaderboard tracks how well LLMs can forecast when teams are free to enhance them in any way they choose—with tools, added context, fine-tuning, ensembling, or other methods. Its purpose is to capture the forefront of LLM forecasting ability.</p>
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
    <div>
      <p>Would you like to have your model's forecasting capabilities evaluated on ForecastBench? We'd like to create a community forecasters, engaging with LLMs to discover the forefront of their forecasting abilities. Though your setup need not be made open source, we do provide a growing list of ForecastBench participants who want have left the door open to collaboration in this way. We'll be in touch with top performers to discuss their forecasting strategies and, potentially, feature them in an FRI blog post.</p>
      <p>To participate, follow the <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench">instructions on how to submit<i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    </div>
  </div>
</section>
