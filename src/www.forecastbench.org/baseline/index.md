---
layout: splash
title: "Baseline"
permalink: /baseline/
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
  - /assets/js/leaderboard_baseline_full.js
  - /assets/js/tooltip-init.js
---

<div style="display:flex;">
  <div style="flex:3;">
     <div class="leaderboard-wrapper">
         <h1 class="leaderboard-title">Baseline Leaderboard<sup><a href="#notes" style="text-decoration:none;">‡</a></sup></h1>
         <p>The baseline leaderboard measures how models perform "out of the box," without extra tools, context, or scaffolding. Models are selected by the ForecastBench team for standardized evaluation, hence this leaderboard provides a consistent benchmark for tracking progress in LLM forecasting accuracy.</p>
         <div id="leaderboard-table-full"></div>
     </div>
  </div>
</div>


<section id="notes" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">‡Notes</h1>
  <div class="site-feature-row__content-small">
    <ul>
    {% include leaderboard-baseline-notes.html %}
    </ul>
  </div>
</section>
