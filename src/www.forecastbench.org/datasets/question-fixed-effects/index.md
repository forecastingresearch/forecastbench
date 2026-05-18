---
layout: splash
title: "Question Fixed Effects"
permalink: /datasets/question-fixed-effects/
---

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Question fixed effect estimates</h1>
      <p>This page provides access to the estimated question fixed effects for all questions evaluated on the current baseline and tournament leaderboards. These files are a byproduct of producing the leaderboards. We provide them in hopes of evaluating the fixed effects used to score forecasting performance. For transparency, we provide the estimated question fixed effects for download. NB: higher scores imply more difficult questions.</p>
      <p>New files are generated nightly as a byproduct of updating the leaderboards.</p>
    </div>
  </div>
</section>

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">File format</h1>
      <p>The question fixed effects files are provided as JSON files with the following fields:</p>
      <ul>
        <li><code>source</code>: The source from which the question was pulled or generated.</li>
        <li><code>id</code>: The question ID (unique given source).</li>
        <li><code>horizon</code>: The forecast horizon in days (<code>null</code> for market questions)</li>
        <li><code>forecast_due_date</code>: The forecast due date associated with the question set the question comes from</li>
        <li><code>leaderboard_type</code>: The leaderboard associated with the estimate (<code>baseline</code> or <code>tournament</code>).</li>
        <li><code>question_fixed_effect</code>: The question fixed effect estimate.</li>
      </ul>
      <p>For more information about these fields, see the <a href="https://github.com/forecastingresearch/forecastbench/wiki/">wiki</a>.</p>
    </div>
  </div>
</section>

<section class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Available files</h1>
      {% assign question_files = site.static_files | where_exp: "file", "file.path contains 'assets/data/question-fixed-effects/'" %}
      {% if question_files.size > 0 %}
        {% assign sorted_files = question_files | sort: "modified_time" | reverse %}
        <ul>
        {% for file in sorted_files limit: 10 %}
          <li>
            <small><a href="{{ file.path | relative_url }}">{{ file.name }}</a></small>
          </li>
        {% endfor %}
        </ul>
      {% else %}
        <p><em>No question fixed effects files are currently available.</em></p>
        <p><small>Files will appear here when they are uploaded.</small></p>
      {% endif %}
    </div>
  </div>
</section>
