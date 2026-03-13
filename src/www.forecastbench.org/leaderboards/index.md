---
layout: splash
title: "Leaderboards"
permalink: /leaderboards/
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
  - /assets/js/leaderboard_baseline_full.js
  - /assets/js/leaderboard_dataset_full.js
  - /assets/js/tooltip-init.js
---

<div style="display:flex;">
  <div style="flex:3;">
    <div class="leaderboard-wrapper">

      <!-- Tab bar -->
      <div id="leaderboard-tabs">
        <span class="lb-title">Leaderboards</span>
        <div class="lb-tab-group">
          <button class="lb-tab active" data-tab="tournament"><i class="fa-solid fa-trophy"></i> Tournament</button>
          <button class="lb-tab" data-tab="preliminary"><i class="fa-solid fa-seedling"></i> Preliminary</button>
          <button class="lb-tab" data-tab="baseline"><i class="fa-solid fa-chart-simple"></i> Baseline</button>
        </div>
      </div>

      <!-- Description sections (one per tab) -->
      <div class="lb-desc" id="desc-tournament">
         <p>The tournament leaderboard captures the frontier of LLM forecasting ability and is <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench" class="no-wrap">open to external submissions<i class="fa-solid fa-arrow-up-right-from-square"></i></a>. Teams may enhance models however they choose&mdash;with tools, added context, fine-tuning, ensembling, or other methods. Models submitted by the ForecastBench team receive the crowd forecast as context for market questions.</p>
         {% include leaderboard-explainer.html %}
      </div>

      <div class="lb-desc" id="desc-baseline" style="display:none;">
         <p>The baseline leaderboard is run solely by the ForecastBench team. Models are evaluated out of the box, without extra tools, context, or scaffolding. It provides a standardized benchmark for tracking progress in raw LLM forecasting accuracy.</p>
         {% include leaderboard-explainer.html %}
      </div>

      <div class="lb-desc" id="desc-preliminary" style="display:none;">
         <p>The preliminary leaderboard ranks models on all resolved dataset questions, providing early results before a model appears on the <a href="/tournament/">tournament leaderboard</a> (typically ~10 days after first submission, depending on dataset update availability). Performance on dataset questions stabilizes faster than on market questions; once they appear on this leaderboard, the dataset rankings can be used for early comparison (see <a href="/docs/#technical-report">tech report</a>). <strong>Rankings are not finalized</strong>; for official results, see the <a href="/tournament/">tournament leaderboard</a>.</p>
         <p>Performance on <a href="/about/#how-does-forecastbench-work">dataset questions</a> is scored using the <a href="/docs/#technical-report">difficulty-adjusted Brier score</a> to account for differences in question difficulty across question sets. The score is then converted to a <a href="https://forecastingresearch.substack.com/p/introducing-the-brier-index" class="no-wrap">Brier Index <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, an interpretable 0-100% scale where higher is better (100% = perfect accuracy, 50% = maximally uninformed, 0% = maximally wrong).</p>
         <p>Hover over the column titles to see tooltips with further explanations. <a href="#notes">Notes</a> are at the bottom of the page.</p>
      </div>

      <!-- Leaderboard table container -->
      <div id="leaderboard-table-full"></div>
    </div>
  </div>
</div>

<!-- Notes sections (one per tab) -->
<section id="notes" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">‡Notes</h1>
  <div class="site-feature-row__content-small">
    <ul>
      <div class="lb-notes" id="notes-tournament">
        {% include leaderboard-tournament-notes.html %}
      </div>
      <div class="lb-notes" id="notes-baseline" style="display:none;">
        {% include leaderboard-baseline-notes.html %}
      </div>
      <div class="lb-notes" id="notes-preliminary" style="display:none;">
        {% include leaderboard-shared-notes.html %}
      </div>
    </ul>
  </div>
</section>

<section id="participate" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">Benchmark your model!</h1>
    <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
    <p>Would you like to have your model's forecasting capabilities evaluated on ForecastBench? We're creating a community of forecasters who are engaging with LLMs to discover the forefront of their forecasting abilities. Though your setup does not need to be made <a href="https://github.com/forecastingresearch/forecastbench/wiki/Open-source-participants">open source <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, we do provide a growing list of ForecastBench participants who have left the door open to collaboration in this way. We'll be in touch with top performers to discuss their forecasting strategies and, potentially, feature them on a <a href="https://substack.com/@forecastingresearchinstitute">Forecasting Research Institute blog post <i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    <p>To participate, follow the <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench">instructions on how to submit<i class="fa-solid fa-arrow-up-right-from-square"></i></a>.</p>
    </div>
  </div>
</section>

<script>
(function() {
  var initFns = {
    tournament:   'initLeaderboard_tournament',
    baseline:     'initLeaderboard_baseline',
    preliminary:  'initLeaderboard_dataset'
  };
  var currentTab = null;

  function switchTab(tab) {
    if (tab === currentTab) return;
    currentTab = tab;

    // Update tab buttons
    document.querySelectorAll('.lb-tab').forEach(function(btn) {
      if (btn.getAttribute('data-tab') === tab) btn.classList.add('active');
      else btn.classList.remove('active');
    });

    // Show/hide description sections
    document.querySelectorAll('.lb-desc').forEach(function(el) {
      el.style.display = 'none';
    });
    var desc = document.getElementById('desc-' + tab);
    if (desc) desc.style.display = '';

    // Show/hide notes sections
    document.querySelectorAll('.lb-notes').forEach(function(el) {
      el.style.display = 'none';
    });
    var notes = document.getElementById('notes-' + tab);
    if (notes) notes.style.display = '';

    // Destroy existing DataTable if present
    var container = document.getElementById('leaderboard-table-full');
    if (container) {
      if (typeof $ !== 'undefined' && $.fn && $.fn.DataTable) {
        var tables = container.querySelectorAll('table');
        tables.forEach(function(tbl) {
          if ($.fn.DataTable.isDataTable(tbl)) {
            $(tbl).DataTable().destroy();
          }
        });
      }
      container.innerHTML = '';
    }

    // Initialize the selected leaderboard
    var fnName = initFns[tab];
    if (window[fnName]) {
      window[fnName]();
    }
  }

  // Bind tab clicks
  document.querySelectorAll('.lb-tab').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var tab = this.getAttribute('data-tab');
      switchTab(tab);
      history.replaceState(null, '', '#' + tab);
    });
  });

  // Initialize tab from URL hash or default to tournament
  document.addEventListener('DOMContentLoaded', function() {
    var hash = window.location.hash.replace('#', '');
    var tab = (hash && initFns[hash]) ? hash : 'tournament';
    switchTab(tab);
  });
})();
</script>
