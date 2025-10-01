---
layout: splash
title: "Docs"
permalink: /docs/
footer_scripts:
  - /assets/js/smooth-scroll.js
---

<section id="start-here" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Reader's guide</h1>
      <p>If you're new to ForecastBench, we recommend you start by reading the <a href="#paper-changelog">latest version of the paper</a> to understand how it works. To see the changes we've made since the paper was last updated, refer to the <a href="#paper-changelog">Changelog</a>. If you'd like to take a deep dive into our scoring methodology and stability tests, see <a href="#technical-report">the addendum to the paper</a>. Finally, you can view the <a href="#codebase">codebase</a> and learn more about the ForecastBench <a href="#architecture">architecture</a> on our GitHub repository.</p>
      <p>The <a href="#iclr2025">ICLR 2025 version</a> of the paper is provided as a reference.</p>
    </div>
  </div>
</section>


<section id="paper-changelog" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Latest version of paper</h1>
      <p>To better understand how ForecastBench works, see the latest version of our paper on arXiv. It has been updated since the ICLR 2025 version. <i>Last updated: 28 Feb 2025.</i></p>
      <a href="https://arxiv.org/abs/2409.19839" class="btn btn--primary">arXiv Paper</a>
    </div>
        <div class="site-feature-row__right-3">
        <h1 class="site-feature-row__title">Changelog</h1>
        <p>There have been several notable changes to the benchmark since the latest version of the paper. You can find a brief overview on the changelog. <i>Last updated: 6 Oct 2025.</i></p>
        <a href="https://github.com/forecastingresearch/forecastbench/wiki/Changelog" class="btn btn--primary">Changelog</a>
    </div>
  </div>
</section>

<section id="technical-report" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Addendum to the paper</h1>
      <p>Our technical report presents the difficulty-adjusted Brier score, the ranking methodology that enables fair comparisons when forecasters predict on different questions. The report details simulation results showing this scoring rule outperforms alternatives and includes our stability analyses demonstrating that rankings become stable within 50 days of a new model participating on ForecastBench. <i>Last updated: 3 Oct 2025.</i></p>
      <a href="/assets/pdfs/forecastbench_updated_methodology.pdf" class="btn btn--primary">Technical Report (PDF)</a>
    </div>
  </div>
</section>


<section id="codebase" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Codebase</h1>
      <p>The ForecastBench codebase is open-sourced under an <a href="https://github.com/forecastingresearch/forecastbench/blob/main/LICENSE">MIT license <i class="fa-solid fa-arrow-up-right-from-square"></i></a> and available on GitHub. The repository contains the full pipeline for generating forecasting questions from time-series data, evaluating LLM and human forecasts, computing difficulty-adjusted Brier scores, and maintaining the leaderboard.</p>
      <a href="https://github.com/forecastingresearch/forecastbench/" class="btn btn--primary">GitHub Repository</a>
    </div>
  </div>
</section>


<section id="architecture" class="site-feature-card">
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <h1 class="site-feature-row__title">Architecture</h1>
      <p>ForecastBench is an automated Benchmark. Every night at 00:00 UTC the system ingests new questions and resolution data, runs validation and category tagging, refreshes metadata, resolves forecasts, and updates the leaderboard. Every two weeks it samples balanced question sets for LLMs (1,000) and humans (200) and starts a new forecasting round. To better understand how ForecastBench updates its question bank, creates question sets, and resolved forecasts, see the dedicated page on the wiki.</p>
      <a href="https://github.com/forecastingresearch/forecastbench/wiki/How-does-ForecastBench-work%3F" class="btn btn--primary">GitHub Wiki</a>
    </div>
  </div>
</section>


<section id="iclr2025" class="site-feature-card-row-1">
  <h1 class="site-feature-row__title">ICLR 2025</h1>
  <div class="site-feature-row__content">
    <div class="site-feature-row__left-2">
      <p>The benchmark was accepted to ICLR 2025 as a poster. On that page, you can view the <a href="https://iclr.cc/media/PosterPDFs/ICLR%202025/28507.png?t=1741725847.5784986" class="no-wrap">poster <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, <a href="https://iclr.cc/media/iclr-2025/Slides/28507.pdf" class="no-wrap">slides <i class="fa-solid fa-arrow-up-right-from-square"></i></a>, and <a href="https://openreview.net/pdf?id=lfPkGWXLLf" class="no-wrap">ICLR version of the paper <i class="fa-solid fa-arrow-up-right-from-square"></i></a>. <i>Last updated: 22 Jan 2025.</i></p>
      <a href="https://iclr.cc/virtual/2025/poster/28507" class="btn btn--primary no-wrap">ICLR Page</a>
    </div>
    <div class="site-feature-row__right-3">
      <div class="citation">
        <button class="copy-button" onclick="copyToClipboard(this)" title="Copy citation">
          <i class="fa-regular fa-copy"></i>
        </button>
        <pre>@inproceedings{karger2025forecastbench,
  title={ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities},
  author={Ezra Karger and Houtan Bastani and Chen Yueh‑Han and Zachary Jacobs and Danny Halawi and Fred Zhang and Philip E. Tetlock},
  year={2025},
  booktitle={International Conference on Learning Representations (ICLR)},
  url={https://iclr.cc/virtual/2025/poster/28507}
}</pre>
      </div>
    </div>
  </div>
  <hr>
  <h1 class="site-feature-row__title">Leaderboards</h1>
      <div class="site-feature-row__content">
          <div class="site-feature-row__left-3">
            <p>The leaderboards from the ICLR 2025 paper are available both here and in the <a href="https://github.com/forecastingresearch/forecastbench-datasets">forecastbench-datasets repo</a> (<a href="https://github.com/forecastingresearch/forecastbench-datasets/commit/601f6d9e67952032205147305df0b4db8f13f727" class="git-sha">601f6d9</a>).</p>
          </div>
          <div class="site-feature-row__right-2">
            <ul style="font-size: 0.55rem;">
              <li><a href="/assets/iclr2025/leaderboards/human_leaderboard_overall.html">LLM / Human Leaderboard</a></li>
              <li><a href="/assets/iclr2025/leaderboards/leaderboard_overall.html">LLM Leaderboard</a></li>
              <li><a href="/assets/iclr2025/leaderboards/human_combo_leaderboard_overall.html">LLM / Human Combo Leaderboard</a></li>
            </ul>
          </div>
      </div>
</section>

<script>
function copyToClipboard(button) {
  const citation = button.nextElementSibling.textContent;

  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(citation).then(() => {
      showCopySuccess(button);
    }).catch(() => {
      fallbackCopyTextToClipboard(citation, button);
    });
  } else {
    fallbackCopyTextToClipboard(citation, button);
  }
}

function fallbackCopyTextToClipboard(text, button) {
  const textArea = document.createElement("textarea");
  textArea.value = text;
  textArea.style.position = "fixed";
  textArea.style.left = "-999999px";
  textArea.style.top = "-999999px";
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  try {
    document.execCommand('copy');
    showCopySuccess(button);
  } catch (err) {
    console.error('Fallback: Oops, unable to copy', err);
  }

  document.body.removeChild(textArea);
}

function showCopySuccess(button) {
  button.classList.add('copied');
  setTimeout(() => {
    button.classList.remove('copied');
  }, 2000);
}
</script>
