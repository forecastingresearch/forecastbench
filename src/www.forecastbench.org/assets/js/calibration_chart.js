(function () {
  const CURVES_PATH_BASELINE = '/assets/data/calibration_curves_baseline.json';
  const CURVES_PATH_TOURNAMENT = '/assets/data/calibration_curves_tournament.json';
  const METRICS_PATH_BASELINE = '/assets/data/calibration_metrics_baseline.csv';
  const METRICS_PATH_TOURNAMENT = '/assets/data/calibration_metrics_tournament.csv';

  const MAX_DEFAULT_MODELS = 5;
  const MARGIN = { top: 20, right: 30, bottom: 50, left: 55 };
  const SIZE = 500;

  const colorScale = d3.scaleOrdinal(d3.schemeTableau10);
  const tip = d3.select('#tooltip');

  let curveData = null;
  let metricsData = null;
  let selectedModels = new Set();

  function getLeaderboardType() {
    return document.querySelector('input[name="lbSelect"]:checked').value;
  }

  function getCurvesPath() {
    return getLeaderboardType() === 'tournament' ? CURVES_PATH_TOURNAMENT : CURVES_PATH_BASELINE;
  }

  function getMetricsPath() {
    return getLeaderboardType() === 'tournament' ? METRICS_PATH_TOURNAMENT : METRICS_PATH_BASELINE;
  }

  function modelLabel(d) {
    const org = d.organization || '';
    const model = d.model || d.model_pk || '';
    if (org && org !== model) return org + ' / ' + model;
    return model;
  }

  function loadData() {
    Promise.all([
      fetch(getCurvesPath()).then(r => r.json()),
      d3.csv(getMetricsPath()),
    ]).then(([curves, metrics]) => {
      curveData = curves;
      metricsData = metrics.map(d => ({
        ...d,
        ece: +d.ece,
        reliability: +d.reliability,
        resolution: +d.resolution,
        uncertainty: +d.uncertainty,
        sharpness: +d.sharpness,
        n_forecasts: +d.n_forecasts,
      }));

      // Sort by ECE ascending, pick top N as default
      metricsData.sort((a, b) => a.ece - b.ece);
      const defaultModels = metricsData.slice(0, MAX_DEFAULT_MODELS).map(d => d.model_pk);
      selectedModels = new Set(defaultModels);

      buildModelCheckboxes();
      renderChart();
      renderTable();
    }).catch(err => {
      console.error('Failed to load calibration data:', err);
      d3.select('#reliability-diagram').html(
        '<p style="color:#888;padding:2rem;">Calibration data not yet available. ' +
        'Run the leaderboard pipeline to generate calibration artifacts.</p>'
      );
    });
  }

  function buildModelCheckboxes() {
    const container = d3.select('#model-checkboxes');
    container.html('');
    metricsData.forEach(d => {
      const id = 'model_' + d.model_pk.replace(/[^a-zA-Z0-9]/g, '_');
      const div = container.append('div').attr('class', 'tag-option');
      div.append('input')
        .attr('type', 'checkbox')
        .attr('id', id)
        .attr('value', d.model_pk)
        .property('checked', selectedModels.has(d.model_pk))
        .on('change', function () {
          if (this.checked) {
            selectedModels.add(d.model_pk);
          } else {
            selectedModels.delete(d.model_pk);
          }
          renderChart();
          renderTable();
        });
      div.append('label')
        .attr('for', id)
        .text(modelLabel(d));
    });
  }

  function renderChart() {
    const container = d3.select('#reliability-diagram');
    container.html('');

    const width = SIZE;
    const height = SIZE;

    const svg = container.append('svg')
      .attr('viewBox', `0 0 ${width + MARGIN.left + MARGIN.right} ${height + MARGIN.top + MARGIN.bottom}`)
      .attr('preserveAspectRatio', 'xMidYMid meet')
      .style('max-width', (width + MARGIN.left + MARGIN.right) + 'px')
      .style('width', '100%');

    const g = svg.append('g')
      .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

    const x = d3.scaleLinear().domain([0, 1]).range([0, width]);
    const y = d3.scaleLinear().domain([0, 1]).range([height, 0]);

    // Axes
    g.append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x).ticks(10))
      .append('text')
      .attr('x', width / 2)
      .attr('y', 40)
      .attr('fill', 'currentColor')
      .attr('text-anchor', 'middle')
      .style('font-size', '13px')
      .text('Forecast Probability');

    g.append('g')
      .call(d3.axisLeft(y).ticks(10))
      .append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', -42)
      .attr('fill', 'currentColor')
      .attr('text-anchor', 'middle')
      .style('font-size', '13px')
      .text('Observed Frequency');

    // Perfect calibration diagonal
    g.append('line')
      .attr('x1', x(0)).attr('y1', y(0))
      .attr('x2', x(1)).attr('y2', y(1))
      .attr('stroke', '#888')
      .attr('stroke-dasharray', '6,4')
      .attr('stroke-width', 1.5)
      .attr('opacity', 0.7);

    // Filter curves for selected models
    const filteredCurves = curveData.filter(d => selectedModels.has(d.model_pk));

    // Group by model
    const byModel = d3.group(filteredCurves, d => d.model_pk);

    // Size scale for circles
    const allN = filteredCurves.map(d => d.n_bin);
    const maxN = d3.max(allN) || 1;
    const rScale = d3.scaleSqrt().domain([0, maxN]).range([3, 14]);

    let colorIdx = 0;
    const modelColors = new Map();
    for (const mpk of selectedModels) {
      modelColors.set(mpk, colorScale(colorIdx++));
    }

    // Draw lines and circles for each model
    for (const [modelPk, points] of byModel) {
      const color = modelColors.get(modelPk) || '#999';
      const sorted = [...points].sort((a, b) => a.bin_midpoint - b.bin_midpoint);

      // Line
      const line = d3.line()
        .x(d => x(d.forecast_mean))
        .y(d => y(d.resolution_rate));

      g.append('path')
        .datum(sorted)
        .attr('d', line)
        .attr('fill', 'none')
        .attr('stroke', color)
        .attr('stroke-width', 2)
        .attr('opacity', 0.8);

      // Circles
      g.selectAll(null)
        .data(sorted)
        .enter()
        .append('circle')
        .attr('cx', d => x(d.forecast_mean))
        .attr('cy', d => y(d.resolution_rate))
        .attr('r', d => rScale(d.n_bin))
        .attr('fill', color)
        .attr('fill-opacity', 0.7)
        .attr('stroke', color)
        .attr('stroke-width', 1)
        .on('mouseover', function (event, d) {
          d3.select(this).attr('fill-opacity', 1).attr('stroke-width', 2);
          tip.style('opacity', 1)
            .html(
              `<strong>${modelLabel(d)}</strong><br>` +
              `Bin midpoint: ${d.bin_midpoint}<br>` +
              `Forecast mean: ${d3.format('.3f')(d.forecast_mean)}<br>` +
              `Observed freq: ${d3.format('.3f')(d.resolution_rate)}<br>` +
              `N: ${d.n_bin}`
            )
            .style('left', (event.pageX + 12) + 'px')
            .style('top', (event.pageY - 20) + 'px');
        })
        .on('mouseout', function () {
          d3.select(this).attr('fill-opacity', 0.7).attr('stroke-width', 1);
          tip.style('opacity', 0);
        });
    }

    // Legend
    const legend = g.append('g')
      .attr('transform', `translate(${width - 180}, 10)`);

    let ly = 0;
    for (const [modelPk, color] of modelColors) {
      const meta = metricsData.find(d => d.model_pk === modelPk);
      const label = meta ? modelLabel(meta) : modelPk;
      const row = legend.append('g').attr('transform', `translate(0,${ly})`);
      row.append('rect')
        .attr('width', 12).attr('height', 12)
        .attr('fill', color).attr('rx', 2);
      row.append('text')
        .attr('x', 16).attr('y', 10)
        .style('font-size', '11px')
        .attr('fill', 'currentColor')
        .text(label.length > 22 ? label.slice(0, 20) + '...' : label);
      ly += 18;
    }
  }

  function renderTable() {
    const container = d3.select('#metrics-table-container');
    container.html('');

    const filtered = metricsData.filter(d => selectedModels.has(d.model_pk));
    if (filtered.length === 0) {
      container.append('p').style('color', '#888').text('Select models above to see metrics.');
      return;
    }

    const table = container.append('table').attr('class', 'calibration-table');
    const thead = table.append('thead');
    const tbody = table.append('tbody');

    const columns = [
      { key: 'label', label: 'Model' },
      { key: 'ece', label: 'ECE' },
      { key: 'reliability', label: 'Reliability' },
      { key: 'resolution', label: 'Resolution' },
      { key: 'uncertainty', label: 'Uncertainty' },
      { key: 'sharpness', label: 'Sharpness' },
      { key: 'n_forecasts', label: 'N' },
    ];

    thead.append('tr').selectAll('th')
      .data(columns)
      .enter()
      .append('th')
      .text(d => d.label);

    filtered.forEach(d => {
      const row = tbody.append('tr');
      columns.forEach(col => {
        const val = col.key === 'label' ? modelLabel(d)
          : col.key === 'n_forecasts' ? d3.format(',')(d[col.key])
          : d3.format('.4f')(d[col.key]);
        row.append('td').text(val);
      });
    });
  }

  // Event listeners
  document.querySelectorAll('input[name="lbSelect"]').forEach(el => {
    el.addEventListener('change', loadData);
  });

  // Initial load
  loadData();
})();
