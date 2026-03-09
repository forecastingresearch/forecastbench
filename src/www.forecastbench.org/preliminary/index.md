---
layout: splash
title: "Preliminary"
permalink: /preliminary/
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
  - /assets/js/leaderboard_dataset_full.js
  - /assets/js/tooltip-init.js
---

<div style="display:flex;">
  <div style="flex:3;">
     <div class="leaderboard-wrapper">
         <h1 class="leaderboard-title">Preliminary leaderboard<sup><a href="#notes" style="text-decoration:none;">‡</a></sup></h1>
         <p>The preliminary leaderboard ranks models on all resolved dataset questions, providing early results before a model appears on the <a href="/tournament/">tournament leaderboard</a> (typically ~10 days after first submission, depending on dataset update availability). Performance on dataset questions stabilizes faster than on market questions; once they appear on this leaderboard, the dataset rankings can be used for early comparison (see <a href="/docs/#technical-report">tech report</a>). <strong>Rankings are not finalized</strong>; for official results, see the <a href="/tournament/">tournament leaderboard</a>.</p>
         <p>Performance on <a href="/about/#how-does-forecastbench-work">dataset questions</a> is scored using the <a href="/docs/#technical-report">difficulty-adjusted Brier score</a> to account for differences in question difficulty across question sets. The score is then converted to a <a href="https://forecastingresearch.substack.com/p/introducing-the-brier-index" class="no-wrap">Brier Index <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, an interpretable 0-100% scale where higher is better (100% = perfect accuracy, 50% = maximally uninformed, 0% = maximally wrong).</p>
         <p>Hover over the column titles to see tooltips with further explanations. <a href="#notes">Notes</a> are at the bottom of the page.</p>
         <div id="leaderboard-table-full"></div>
     </div>
  </div>
</div>

<section id="notes" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">‡Notes</h1>
  <div class="site-feature-row__content-small">
    <ul>
    {% include leaderboard-shared-notes.html %}
    </ul>
  </div>
</section>
