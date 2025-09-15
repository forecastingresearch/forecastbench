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
header:
  overlay_color: "#171e29"
excerpt: "A dynamic, continuously-updated, contamination-free benchmark that serves as a valuable proxy for general intelligence."
---

<!-- Baseline Leaderboard Section with Background -->
<div class="baseline-section" style="background-color: #e0e6f0; margin: 0 -50vw; padding: 3rem 50vw; margin-top: -2rem; margin-bottom: 0;">
  <div style="display:flex;">
    <div style="flex:2; padding-right:1rem; display:flex; justify-content:flex-end;">
      <div style="width:450px; margin-right:2rem;">
        <h1>Baseline Leaderboard</h1>
        <p>Tracks vanilla LLM forecasting performance, showing consistent progress in forecasting ability since models were first tested.</p>
        <p><a href="/baseline/" class="btn btn--primary btn--large">Baseline Leaderboard</a></p>
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
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #e0e6f0; z-index: 1;"></div>

  <!-- Light slate section with wave clip-path -->
  <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background-color: #f0f2f8; z-index: 2; clip-path: polygon(
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

<!-- Tournament Leaderboard Section with Background -->
<div class="tournament-section" style="background-color: #f0f2f8; margin: 0 -50vw; padding: 3rem 50vw 3rem 50vw; margin-top: 0; margin-bottom: -3rem; position: relative; z-index: 3;">
  <div style="display:flex;">
     <div style="flex:2; margin-left:-1rem;">
       <div class="leaderboard-wrapper-home">
          <div id="leaderboard-tournament-compact"></div>
       </div>
     </div>
    <div style="flex:2; padding-left:1rem; display:flex; justify-content:center;">
      <div style="width:450px;">
        <h1>Tournament Leaderboard</h1>
        <p>Tracks frontier performance, where tools can be used to improve LLM performance; LLMs can be scaffolded, fine-tuned, ensembled, and on and on.</p>
        <p><a href="/tournament/" class="btn btn--primary btn--large">Tournament Leaderboard</a></p>
      </div>
    </div>
  </div>
</div>
