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
excerpt: "A dynamic, continuously-updated benchmark designed to measure the accuracy of ML systems on a constantly evolving set of forecasting questions."
---

<div style="display:flex;">
  <div style="flex:2; padding-right:1rem;">
<p><i>Like coding or mathematics, accurate forecasting requires a blend of cognitive skills: data gathering, causal reasoning, probabilistic thinking, and information synthesis. We think this makes forecasting a valuable proxy for general intelligence.</i></p>
      <h1>The Baseline Leaderboard ... </h1>
      <p>Tracks vanilla LLM forecasting performance, showing consistent progress in forecasting ability since models were first run.</p>
      <p><a href="/baseline/" class="btn btn--primary btn--large">Baseline Leaderboard</a></p>
  </div>
   <div style="flex:2;">
     <div class="leaderboard-wrapper-home">
        <div id="leaderboard-baseline-compact"></div>
     </div>
   </div>
</div>

<br> <br> <br> <br>

<div style="display:flex;">
   <div style="flex:2;  margin-left:-1rem;">
     <div class="leaderboard-wrapper-home">
        <div id="leaderboard-tournament-compact"></div>
     </div>
   </div>
  <div style="flex:2; padding-left:1rem;">
      <h1>... and a Tournament Leaderboard</h1>
      <p>Tracks frontier performance, where LLMs can be scaffolded, fine-tuned, ensembled, etc.</p>
      <p><a href="/tournament/" class="btn btn--primary btn--large">Tournament Leaderboard</a></p>
  </div>
</div>
