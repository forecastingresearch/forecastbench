---
layout: splash
permalink: /
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
  - /assets/css/main.css
after_footer_scripts:
  - https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js
  - https://cdn.datatables.net/1.13.7/js/dataTables.semanticui.min.js
  - https://cdn.datatables.net/responsive/2.4.1/js/dataTables.responsive.min.js
  - https://cdn.jsdelivr.net/npm/@floating-ui/dom@1.6.3/dist/floating-ui.dom.min.js
  - /assets/js/leaderboard_baseline_compact.js
  - /assets/js/leaderboard_tournament_compact.js
  - /assets/js/tooltip-init.js
  - https://cdn.jsdelivr.net/npm/d3@7
  - /assets/js/explore_sota_graph.js
header:
  overlay_color: "#171e29"
excerpt: "A dynamic, contamination-free benchmark of LLM forecasting accuracy with human comparison groups, serving as a valuable proxy for general intelligence."
---

<!-- Baseline leaderboard Section with Background -->
<div class="baseline-section" style="background-color: #d0d8e6; margin: 0 -50vw; padding: 3rem 50vw; margin-top: -2rem; margin-bottom: 0;">
  <div style="display:flex;">
    <div style="flex:2; padding-right:1rem; display:flex; justify-content:flex-end;">
      <div style="width:450px; margin-right:2rem;">
        <h1>Baseline leaderboard</h1>
        <p>Tracks base model LLM forecasting performance <i>without additional tools</i>, comparing against human baselines and showing consistent progress in capabilities since models were first tested.</p>
        <p><a href="/baseline/" class="btn btn--primary btn--large">Baseline leaderboard</a></p>
      </div>
    </div>
     <div style="flex:2;">
       <div class="leaderboard-wrapper-home">
          <div id="leaderboard-baseline-compact"></div>
       </div>
     </div>
  </div>
</div>

<!-- Wave Separator -->
<div class="wave-separator" style="position: relative; height: 100px; margin: -2.5rem -30vw 0 -30vw; padding:0 ; overflow: hidden; z-index: 1;">
  <!-- Deeper blue-gray background (upper part) -->
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #d0d8e6; z-index: 1;"></div>

  <!-- Light slate section with wave clip-path -->
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #e0e4ee; z-index: 2; clip-path: polygon(
    0% 80%,
    8% 70%,
    16% 55%,
    24% 35%,
    32% 20%,
    40% 15%,
    48% 25%,
    56% 45%,
    64% 65%,
    72% 80%,
    80% 85%,
    88% 75%,
    96% 60%,
    100% 45%,
    100% 100%,
    0% 100%
  );"></div>
</div>

<!-- Tournament leaderboard Section with Background -->
<div class="tournament-section" style="background-color: #e0e4ee; margin: 0 -50vw; padding: 3rem 50vw 0 50vw; margin-top: 0; margin-bottom: 0; position: relative; z-index: 3;">
  <div style="display:flex;">
     <div style="flex:2; margin-left:-1rem;">
       <div class="leaderboard-wrapper-home">
          <div id="leaderboard-tournament-compact"></div>
       </div>
     </div>
    <div style="flex:2; padding-left:1rem; display:flex; justify-content:center;">
      <div style="width:450px;">
        <h1>Tournament leaderboard</h1>
        <p>Tracks frontier accuracy by allowing tool use to improve LLM performance. Models can be scaffolded, fine-tuned, ensembled, and so on. Open to <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench" class="no-wrap">public submissions <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
        <p><a href="/tournament/" class="btn btn--primary btn--large">Tournament leaderboard</a></p>
      </div>
    </div>
  </div>
</div>

<!-- Wave Separator (Inverted) -->
<div class="wave-separator-inverted" style="position: relative; height: 100px; margin: 0 -30vw 0 -30vw; padding:0; overflow: hidden; z-index: 1;">
  <!-- Light slate background (upper part) -->
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #e0e4ee; z-index: 1;"></div>

  <!-- Chart section background with inverted wave clip-path -->
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #ececf4; z-index: 2; clip-path: polygon(
    0% 20%,
    8% 30%,
    16% 45%,
    24% 65%,
    32% 80%,
    40% 85%,
    48% 75%,
    56% 55%,
    64% 35%,
    72% 20%,
    80% 15%,
    88% 25%,
    96% 40%,
    100% 55%,
    100% 100%,
    0% 100%
  );"></div>
</div>

<!-- Chart Section with Background -->
<div class="chart-section-home" style="background-color: #ececf4; margin: 0 -50vw; padding: 1rem 50vw 4rem 50vw; margin-top: 0; margin-bottom: -3rem; position: relative; z-index: 3;">
  <div style="display: flex; flex-direction: column; align-items: center;">
    <div style="text-align: center; margin-bottom: 1rem;">
      <h1 style="margin-bottom: 0.5rem;">Projected LLM-superforecaster parity</h1>
      <div style="max-width: 600px; margin: 0 auto; padding: 0 1rem;">
        <p>Explore how LLM forecasting accuracy evolves on ForecastBench. A linear trend projects the date when LLMs reach superforecaster-level performance.</p>
        <p><a href="/explore/" class="btn btn--primary btn--large">Explore chart</a></p>
      </div>
    </div>
    <div class="chart-card-home">
      <div id="chart"></div>
    </div>
  </div>
</div>

<div id="tooltip" class="tooltip"></div>
