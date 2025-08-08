---
permalink: /
layout: splash
head_scripts:
    - https://cdn.jsdelivr.net/npm/particles.js@2.0.0/particles.min.js
footer_scripts:
    - /assets/js/particles.js
head_css:
  - https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css
  - https://cdn.datatables.net/1.13.7/css/dataTables.semanticui.min.css
  - https://cdn.datatables.net/responsive/2.4.1/css/responsive.semanticui.min.css
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.css
  - /assets/css/tooltips.css
after_footer_scripts:
  - https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js
  - https://cdn.datatables.net/1.13.7/js/dataTables.semanticui.min.js
  - https://cdn.datatables.net/responsive/2.4.1/js/dataTables.responsive.min.js
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.js
  - /assets/js/leaderboard_compact.js
  - /assets/js/tooltip-init.js
header:
  overlay_color: "#171e29"
  actions:
    - label: "Full Leaderboard"
      url: "/leaderboard"
excerpt: "A dynamic, continuously-updated benchmark designed to measure the accuracy of ML systems on a constantly evolving set of forecasting questions."
---

<div style="display:flex;">
  <div style="flex:2; padding-right:1rem;">
      <h1>Benchmark your model!</h1>
      <p>Would you like to have your model's forecasting capabilities evaluated on ForecastBench?</p>
      <p>Follow the <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench">instructions on how to submit<i class="fa-solid fa-arrow-up-right-from-square"></i></a> to find out how.</p>
  </div>
  <div style="flex:3;">
      <div id="leaderboard-table"></div>
  </div>
</div>
