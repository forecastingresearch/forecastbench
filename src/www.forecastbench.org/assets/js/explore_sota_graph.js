(function() {
  const CSV_PATH = '/assets/data/sota_graph_tournament.csv';
  const PARITY_DATE_PATH = '/assets/data/parity_dates.json';
  const Y_DOMAIN = [0.05, 0.35];
  const SHOW_REFS = true;

  const fmt = d3.format('.3f');
  const fmtDate = d3.timeFormat('%Y-%m-%d');
  const fmt3or4 = (v) => {
    const s3 = fmt(v);
    if (s3 === '0.000' || s3 === '-0.000') return d3.format('.4f')(v);
    return s3;
  };

  const tip = d3.select('#tooltip');
  let parityDates = null;

  function shortLabel(name) {
    const base = name.split(' (')[0]; // Remove everything after first parenthesis
    const map = [
      // OpenAI models
      [/gpt-4\.5-preview/i, 'GPT-4.5'],
      [/gpt-4\.1/i, 'GPT-4.1'],
      [/gpt-4o-\d{4}/i, 'GPT-4o'],
      [/gpt-4-turbo/i, 'GPT-4 Turbo'],
      [/gpt-4-\d{4}/i, 'GPT-4'],
      [/gpt-3\.5/i, 'GPT-3.5'],
      [/o4-mini/i, 'o4-mini'],
      [/o3-mini/i, 'o3-mini'],
      [/o3-\d{4}/i, 'o3'],

      // Anthropic Claude models
      [/claude-opus-4/i, 'Claude Opus 4'],
      [/claude-sonnet-4/i, 'Claude Sonnet 4'],
      [/claude-3-7-sonnet/i, 'Claude 3.7 Sonnet'],
      [/claude-3-5-sonnet/i, 'Claude 3.5 Sonnet'],
      [/claude-3-opus/i, 'Claude 3 Opus'],
      [/claude-3-haiku/i, 'Claude 3 Haiku'],
      [/claude-2\.1/i, 'Claude 2.1'],

      // Google models
      [/gemini-2\.5-flash/i, 'Gemini 2.5 Flash'],
      [/gemini-2\.0-flash/i, 'Gemini 2.0 Flash'],
      [/gemini-1\.5-pro/i, 'Gemini 1.5 Pro'],
      [/gemini-1\.5-flash/i, 'Gemini 1.5 Flash'],

      // DeepSeek models
      [/deepseek-r1/i, 'DeepSeek R1'],
      [/deepseek-v3/i, 'DeepSeek V3'],

      // Meta models
      [/meta-llama-3\.1-405b/i, 'Llama 3.1 405B'],
      [/llama-4-scout/i, 'Llama 4 Scout'],
      [/llama-3\.3-70b/i, 'Llama 3.3 70B'],
      [/llama-3-70b/i, 'Llama 3 70B'],
      [/llama-3-8b/i, 'Llama 3 8B'],
      [/llama-2-70b/i, 'Llama 2 70B'],

      // Qwen models
      [/qwen3-235b/i, 'Qwen3 235B'],
      [/qwen2\.5-72b/i, 'Qwen 2.5 72B'],
      [/qwen1\.5-110b/i, 'Qwen 1.5 110B'],
      [/qwq-32b/i, 'QwQ 32B'],

      // Mistral models
      [/magistral-medium/i, 'Magistral Medium'],
      [/mistral-large-2411/i, 'Mistral Large 2411'],
      [/mistral-large-2407/i, 'Mistral Large 2407'],
      [/mistral-large-latest/i, 'Mistral Large'],
      [/mixtral-8x22b/i, 'Mixtral 8x22B'],
      [/mixtral-8x7b/i, 'Mixtral 8x7B'],

      // Other models
      [/kimi-k2/i, 'Kimi K2'],
      [/superforecaster/i, 'Superforecaster'],
      [/public median/i, 'Public'],
      [/imputed forecaster/i, 'Imputed'],
      [/naive forecaster/i, 'Naive'],
      [/always 0\.5/i, 'Always 0.5'],
      [/always 0/i, 'Always 0'],
      [/always 1/i, 'Always 1'],
      [/random uniform/i, 'Random'],
      [/llm crowd/i, 'LLM Crowd'],
    ];

    for (const [re, val] of map) {
      const m = base.match(re);
      if (m) return (typeof val === 'function') ? val(...m) : val;
    }

    // Fallback: take first 2-3 words, max 18 chars
    return base.split(/[-_ ]+/).slice(0, 3).join(' ').slice(0, 18);
  }

  function markSOTA(rows) {
    rows.sort((a, b) => d3.ascending(a.release_date, b.release_date));
    const pad = n => String(n).padStart(2, '0');
    const dateKey = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    const byDate = d3.group(rows, r => dateKey(r.release_date));
    let best = Infinity, tol = 1e-12;

    for (const [, sameDayRows] of Array.from(byDate).sort((a, b) => d3.ascending(a[0], b[0]))) {
      sameDayRows.forEach(r => { r.is_sota_at_release = false; });
      sameDayRows.sort((a, b) => {
        const scoreComp = d3.ascending(a.overall_score, b.overall_score);
        if (scoreComp !== 0) return scoreComp;
        return d3.ascending(a.model, b.model);
      });
      const bestOfDay = sameDayRows[0]?.overall_score;
      if (bestOfDay !== undefined && bestOfDay < best - tol) {
        sameDayRows[0].is_sota_at_release = true;
        best = Math.min(best, bestOfDay);
      }
    }
  }

  let currentXDomain = null;
  let currentYDomain = null;
  let isSelecting = false;
  let selectionStart = null;
  let selectionRect = null;
  let isShiftPressed = false;
  let updateOverlayInteraction = null;

  function draw(data, baselines = {}, showErrorBars = false, currentType = 'overall') {
    // Preserve scroll position during redraw
    const scrollY = window.scrollY;

    const container = document.getElementById('chart');
    container.innerHTML = '';
    const W = Math.min(container.clientWidth || 1100, 1100);
    const H = 520;
    const margin = { top: 16, right: 24, bottom: 60, left: 64 };
    const width = W - margin.left - margin.right;
    const height = H - margin.top - margin.bottom;

    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${W} ${H}`)
      .attr('width', '100%')
      .attr('height', H);

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Add clipping path to prevent elements from extending beyond chart boundaries
    const clipPath = svg.append('defs').append('clipPath')
      .attr('id', 'chart-clip')
      .append('rect')
      .attr('width', width)
      .attr('height', height);

    const chartArea = g.append('g').attr('clip-path', 'url(#chart-clip)');

    const [minDate, maxDate] = d3.extent(data, d => d.release_date);
    const extendedMinDate = new Date(minDate.getFullYear(), minDate.getMonth() - 1, minDate.getDate());
    const extendedMaxDate = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, maxDate.getDate());

    const xDomain = currentXDomain || [extendedMinDate, extendedMaxDate];
    const yDomain = currentYDomain || Y_DOMAIN;

    const x = d3.scaleTime().domain(xDomain).range([0, width]);
    const y = d3.scaleLinear().domain(yDomain).nice().range([height, 0]);

    // Grid removed for cleaner look

    // Chart border
    g.append('rect')
      .attr('width', width)
      .attr('height', height)
      .attr('fill', 'none')
      .attr('class', 'chart-border');

    // Axes
    g.append('g').attr('class', 'axis')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x).tickFormat(d3.timeFormat('%b %Y')));
    g.append('g').attr('class', 'axis').call(d3.axisLeft(y));

    // Labels
    g.append('text').attr('class', 'xlabel')
      .attr('x', width / 2).attr('y', height + 44)
      .attr('text-anchor', 'middle')
      .text('Model release date');
    g.append('text').attr('class', 'ylabel')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2).attr('y', -46)
      .attr('text-anchor', 'middle')
      .text('Difficulty-adjusted Brier score');

    // Reference lines
    if (SHOW_REFS) {
      const selectedBenchmarks = getSelectedBenchmarks();
      const refs = [];

      if (selectedBenchmarks.includes('always_0.5')) refs.push({ y: 0.25, label: 'Always 0.5', key: 'always_0.5' });
      if (selectedBenchmarks.includes('public') && baselines.public !== undefined) refs.push({ y: baselines.public, label: 'Public', key: 'public' });
      if (selectedBenchmarks.includes('superforecaster') && baselines.superforecaster !== undefined) refs.push({ y: baselines.superforecaster, label: 'Superforecaster', key: 'superforecaster' });
      if (selectedBenchmarks.includes('imputed') && baselines.imputed !== undefined) refs.push({ y: baselines.imputed, label: 'Imputed Forecaster', key: 'imputed' });
      if (selectedBenchmarks.includes('naive') && baselines.naive !== undefined) refs.push({ y: baselines.naive, label: 'Naive Forecaster', key: 'naive' });
      if (selectedBenchmarks.includes('always_0') && baselines.always_0 !== undefined) refs.push({ y: baselines.always_0, label: 'Always 0', key: 'always_0' });
      if (selectedBenchmarks.includes('always_1') && baselines.always_1 !== undefined) refs.push({ y: baselines.always_1, label: 'Always 1', key: 'always_1' });
      if (selectedBenchmarks.includes('random_uniform') && baselines.random_uniform !== undefined) refs.push({ y: baselines.random_uniform, label: 'Random Uniform', key: 'random_uniform' });

      refs.forEach(r => {
        chartArea.append('line').attr('class', 'refline')
          .attr('x1', 0).attr('x2', width)
          .attr('y1', y(r.y)).attr('y2', y(r.y));
        chartArea.append('text').attr('class', 'reflabel')
          .attr('x', 4).attr('y', y(r.y) - 4)
          .text(r.label);

        chartArea.append('rect')
          .attr('x', 0).attr('width', width)
          .attr('y', y(r.y) - 8).attr('height', 16)
          .attr('fill', 'transparent')
          .style('cursor', 'pointer')
          .on('mouseenter', (ev) => tipOn(ev, referenceTooltipHTML(r.label, r.y, baselines, currentType, r.key)))
          .on('mouseleave', tipOff);
      });
    }

    function tipOn(event, html) {
      if (isSelecting) return;
      tip.html(html).style('opacity', 1);
      // Use clientX/clientY which are relative to viewport, not affected by scroll
      tip.style('left', (event.clientX + 12) + 'px').style('top', (event.clientY + 12) + 'px');
    }

    function tipOff() {
      if (isSelecting) return;
      tip.style('opacity', 0);
    }

    const rest = data.filter(d => !d.is_sota_at_release);
    const sota = data.filter(d => d.is_sota_at_release);

    function calculateRegression(sotaData) {
      const n = sotaData.length;
      if (n < 3) return null;

      sotaData = [...sotaData].sort((a, b) => a.release_date - b.release_date);
      const firstDate = d3.min(sotaData, d => d.release_date);
      const xVals = sotaData.map(d => (d.release_date - firstDate) / (1000 * 60 * 60 * 24 * 30));
      const yVals = sotaData.map(d => d.overall_score);
      const xMean = d3.mean(xVals);
      const yMean = d3.mean(yVals);
      let numerator = 0, denominator = 0;
      for (let i = 0; i < n; i++) {
        const xDiff = xVals[i] - xMean;
        const yDiff = yVals[i] - yMean;
        numerator += xDiff * yDiff;
        denominator += xDiff * xDiff;
      }
      if (denominator === 0) return null;
      const slope = numerator / denominator;
      const intercept = yMean - slope * xMean;

      let residualSumSquares = 0;
      for (let i = 0; i < n; i++) {
        const predicted = intercept + slope * xVals[i];
        const residual = yVals[i] - predicted;
        residualSumSquares += residual * residual;
      }
      const mse = residualSumSquares / (n - 2);
      const slopeStdError = Math.sqrt(mse / denominator);

      const df = n - 2;
      function tCritical95(df) {
        if (df <= 1) return 12.706;
        if (df === 2) return 4.303;
        if (df === 3) return 3.182;
        if (df === 4) return 2.776;
        if (df === 5) return 2.571;
        if (df <= 7) return 2.447;
        if (df <= 9) return 2.306;
        if (df <= 12) return 2.201;
        if (df <= 20) return 2.086;
        if (df <= 30) return 2.042;
        return 1.96;
      }
      const tValue = tCritical95(df);
      const slopeMarginError = tValue * slopeStdError;

      // Generate band with interpolated points for smoother visualization
      const numPoints = Math.max(n, 50); // At least 50 points for smooth curve
      const xMin = Math.min(...xVals);
      const xMax = Math.max(...xVals);
      // Extend xMax slightly beyond last point to ensure band covers trend line
      const xStep = (xMax - xMin) / (numPoints - 1);
      const band = Array.from({ length: numPoints }, (_, i) => {
        const xi = xMin + i * xStep;
        const seMean = Math.sqrt(mse * (1 / n + ((xi - xMean) * (xi - xMean)) / denominator));
        const yhat = intercept + slope * xi;
        const date = new Date(firstDate.getTime() + xi * (1000 * 60 * 60 * 24 * 30));
        return {
          x: xi,
          date,
          yhat,
          lower: yhat - tValue * seMean,
          upper: yhat + tValue * seMean
        };
      });

      return {
        a: intercept,
        b: slope,
        firstDate,
        n,
        slopeCI: {
          lower: slope - slopeMarginError,
          upper: slope + slopeMarginError
        },
        band
      };
    }

    const regression = calculateRegression(sota);

    if (regression && sota.length >= 3) {
      const [currentXMin, currentXMax] = x.domain();
      const trendStartDate = new Date(Math.max(regression.firstDate.getTime(), currentXMin.getTime()));
      const trendEndDate = new Date(Math.min(maxDate.getTime(), currentXMax.getTime()));

      function yOnTrend(date) {
        const months = (date - regression.firstDate) / (1000 * 60 * 60 * 24 * 30);
        return regression.a + regression.b * months;
      }

      // Calculate intersection with Superforecaster baseline
      let intersectionDate = null;
      if (baselines.superforecaster !== undefined && shouldShowIntersection()) {
        const superforecasterScore = baselines.superforecaster;
        // Solve: regression.a + regression.b * months = superforecasterScore
        // months = (superforecasterScore - regression.a) / regression.b
        const intersectionMonths = (superforecasterScore - regression.a) / regression.b;
        intersectionDate = new Date(regression.firstDate.getTime() + intersectionMonths * (1000 * 60 * 60 * 24 * 30));

        // Calculate confidence interval for intersection date if we have slope CI
        let intersectionCI = null;
        if (regression.slopeCI && regression.b < 0) {
          // For CI bounds, use the same intercept but different slopes
          const intersectionMonthsLower = (superforecasterScore - regression.a) / regression.slopeCI.lower;
          const intersectionMonthsUpper = (superforecasterScore - regression.a) / regression.slopeCI.upper;

          const intersectionDateLower = new Date(regression.firstDate.getTime() + intersectionMonthsLower * (1000 * 60 * 60 * 24 * 30));
          const intersectionDateUpper = new Date(regression.firstDate.getTime() + intersectionMonthsUpper * (1000 * 60 * 60 * 24 * 30));

          intersectionCI = {
            lower: intersectionDateLower,
            upper: intersectionDateUpper
          };
        }

        // Only show if intersection is in a reasonable future timeframe
        const now = new Date();
        const maxFutureDate = new Date(now.getFullYear() + 20, now.getMonth(), now.getDate());

        if (intersectionDate > now && intersectionDate < maxFutureDate && regression.b < 0) {
          // Store intersection date for display in legend/info area
          window.intersectionInfo = {
            date: intersectionDate,
            score: superforecasterScore,
            ci: intersectionCI
          };
        } else {
          window.intersectionInfo = null;
        }
      }

      // Draw confidence band
      chartArea.append('path')
               .datum(regression.band.filter(d => d.date >= trendStartDate && d.date <= trendEndDate))
               .attr('class', 'confidence-band')
               .attr('d', d3.area()
                            .x(d => x(d.date))
                            .y0(d => y(d.lower))
                            .y1(d => y(d.upper)));

      chartArea.append('line')
        .attr('class', 'trend-line')
        .attr('x1', x(trendStartDate))
        .attr('y1', y(yOnTrend(trendStartDate)))
        .attr('x2', x(trendEndDate))
        .attr('y2', y(yOnTrend(trendEndDate)));

      chartArea.append('line')
        .attr('x1', x(trendStartDate))
        .attr('y1', y(yOnTrend(trendStartDate)))
        .attr('x2', x(trendEndDate))
        .attr('y2', y(yOnTrend(trendEndDate)))
        .attr('stroke', 'transparent')
        .attr('stroke-width', 14)
        .style('cursor', 'pointer')
        .on('mouseenter', (ev) => {
          let tooltip = trendTooltipHTML(regression);
          if (intersectionDate) {
            tooltip += `<div><strong>Intersects Superforecaster:</strong> ${d3.timeFormat('%B %Y')(intersectionDate)}`;
            if (window.intersectionInfo && window.intersectionInfo.ci) {
              const lowerDate = d3.timeFormat('%b %Y')(window.intersectionInfo.ci.lower);
              const upperDate = d3.timeFormat('%b %Y')(window.intersectionInfo.ci.upper);
              tooltip += `<br><span style="font-size: 11px; opacity: 0.8;">(95% CI: ${lowerDate} – ${upperDate})</span>`;
            }
            tooltip += '</div>';
          }
          tipOn(ev, tooltip);
        })
        .on('mouseleave', tipOff);
    }

    // Points (draw after trend line so they appear on top)
    chartArea.selectAll('.point-all').data(rest).join('circle')
      .attr('class', 'point-all')
      .attr('r', 4.5)
      .attr('cx', d => x(d.release_date))
      .attr('cy', d => y(d.overall_score))
      .on('mouseenter', (ev, d) => tipOn(ev, tooltipHTML(d)))
      .on('mouseleave', tipOff);

    if (showErrorBars) {
      chartArea.selectAll('.errorbar').data(sota).join('line')
        .attr('class', 'errorbar')
        .attr('x1', d => x(d.release_date)).attr('x2', d => x(d.release_date))
        .attr('y1', d => y(d.conf_int_lb)).attr('y2', d => y(d.conf_int_ub));

      chartArea.selectAll('.errorbar-top').data(sota).join('line')
        .attr('class', 'errorbar')
        .attr('x1', d => x(d.release_date) - 3).attr('x2', d => x(d.release_date) + 3)
        .attr('y1', d => y(d.conf_int_ub)).attr('y2', d => y(d.conf_int_ub));

      chartArea.selectAll('.errorbar-bottom').data(sota).join('line')
        .attr('class', 'errorbar')
        .attr('x1', d => x(d.release_date) - 3).attr('x2', d => x(d.release_date) + 3)
        .attr('y1', d => y(d.conf_int_lb)).attr('y2', d => y(d.conf_int_lb));
    }

    // SOTA points
    const sg = chartArea.append('g');
    sg.selectAll('circle.sota').data(sota).join('circle')
      .attr('class', 'point-sota')
      .attr('r', 4.5)
      .attr('cx', d => x(d.release_date))
      .attr('cy', d => y(d.overall_score))
      .on('mouseenter', (ev, d) => tipOn(ev, tooltipHTML(d)))
      .on('mouseleave', tipOff);

    // SOTA labels
    const labels = sg.selectAll('text.lab').data(sota).join('text')
      .attr('class', 'lab')
      .attr('x', d => x(d.release_date))
      .attr('y', d => y(Number.isFinite(d.conf_int_lb) ? d.conf_int_lb : d.overall_score) + 20)
      .text(d => shortLabel(d.model));

    const nodes = labels.nodes()
      .map((node, i) => ({ node, d: sota[i] }))
      .sort((a, b) => d3.descending(a.d.release_date, b.d.release_date));
    const kept = [];
    for (const { node } of nodes) {
      const bb = node.getBBox();
      const x0 = +node.getAttribute('x') - bb.width / 2;
      const y0 = +node.getAttribute('y') - bb.height / 2;
      const box = { x: x0, y: y0, w: bb.width, h: bb.height };
      const collide = kept.some(k => !(box.x + box.w < k.x || k.x + k.w < box.x || box.y + box.h < k.y || k.y + k.h < box.y));
      if (!collide) kept.push(box);
      else node.style.display = 'none';
    }

    // Legend and intersection display
    const showLegend = shouldShowLegend();

    // Calculate legend items and total width (needed for both legend and intersection)
    const legendItems = [];
    let currentX = 0;

    if (regression && sota.length >= 2) {
      legendItems.push({ type: 'trend', x: currentX });
      currentX += 110; // trend line takes more space
    }

    legendItems.push({ type: 'sota', x: currentX });
    currentX += 90;

    legendItems.push({ type: 'regular', x: currentX });
    currentX += 100;

    const totalWidth = currentX + 20; // Add more padding for text to match left side
    const legendX = width - totalWidth - 15; // Right align with space matching top

    // Show legend if enabled
    if (showLegend) {
      const legend = g.append('g').attr('class', 'legend');
      const legendY = 15;

      legend.attr('transform', `translate(${legendX}, ${legendY})`);

      // Background (color and border handled by CSS)
      legend.append('rect')
        .attr('x', -8).attr('y', -8)
        .attr('width', totalWidth + 16).attr('height', 32)
        .attr('rx', 6);

      // Draw legend items
      legendItems.forEach(item => {
        if (item.type === 'trend') {
          legend.append('line')
            .attr('x1', item.x + 5).attr('x2', item.x + 25)
            .attr('y1', 8).attr('y2', 8)
            .attr('class', 'trend-line');
          legend.append('text')
            .attr('x', item.x + 30).attr('y', 12)
            .attr('class', 'legend-text')
            .text('Linear Trend');
        } else if (item.type === 'sota') {
          legend.append('circle')
            .attr('cx', item.x + 15).attr('cy', 8)
            .attr('r', 4.5)
            .attr('class', 'point-sota');
          legend.append('text')
            .attr('x', item.x + 25).attr('y', 12)
            .attr('class', 'legend-text')
            .text('SOTA Model');
        } else if (item.type === 'regular') {
          legend.append('circle')
            .attr('cx', item.x + 15).attr('cy', 8)
            .attr('r', 4.5)
            .attr('class', 'point-all');
          legend.append('text')
            .attr('x', item.x + 25).attr('y', 12)
            .attr('class', 'legend-text')
            .text('Non-SOTA Model');
        }
      });
    }

    // Show intersection date display if enabled
    if (shouldShowIntersection() && parityDates && baselines.superforecaster !== undefined) {
      // Determine which parity dates to use based on current type and tournament toggle
      const useTournament = shouldIncludeFreeze();
      const dataSource = useTournament ? 'tournament' : 'baseline';

      let parityData = null;
      if (currentType === 'dataset') {
        parityData = parityDates.dataset[dataSource];
      } else if (currentType === 'market') {
        parityData = parityDates.market[dataSource];
      } else {
        parityData = parityDates.overall[dataSource];
      }

      if (parityData) {
        const intersectionDisplay = g.append('g').attr('class', 'intersection-display');

        // Use the parity dates from JSON
        const mainText = `Projected LLM-superforecaster parity: ${parityData.median}`;
        const ciText = `(95% CI: ${parityData.lower} – ${parityData.upper})`;

        const displayY = showLegend ? 55 : 15; // Position below legend if legend is shown

        intersectionDisplay.attr('transform', `translate(${legendX}, ${displayY})`);

        // Use same width as legend box for consistency
        // Calculate height with equal padding above and below
        const boxHeight = 48; // Always show CI text
        const boxWidth = totalWidth + 16; // Match legend width exactly

        // Background - styled by CSS but with same width as legend
        intersectionDisplay.append('rect')
          .attr('x', -8).attr('y', -8)
          .attr('width', boxWidth).attr('height', boxHeight)
          .attr('rx', 6);

        // Find the trend line item's x position to align text with the left edge of the orange line
        const trendItem = legendItems.find(item => item.type === 'trend');
        const textX = trendItem ? trendItem.x + 5 : 8; // Align with left edge of trend line (x1 position), fallback to 8

        // Main text line
        intersectionDisplay.append('text')
          .attr('x', textX).attr('y', 12)
          .attr('class', 'legend-text')
          .style('font-weight', '600')
          .text(mainText);

        // Confidence interval text on second line
        intersectionDisplay.append('text')
          .attr('x', textX).attr('y', 28)
          .attr('class', 'legend-text')
          .style('font-size', '10px')
          .style('opacity', '0.8')
          .text(ciText);
      }
    }

    function tooltipHTML(d) {
      const hasCI = Number.isFinite(d.conf_int_lb) && Number.isFinite(d.conf_int_ub);
      const hasN = Number.isFinite(d.sample_size);
      return `
        <div><strong>${d.model}</strong></div>
        <div>Release date: ${fmtDate(d.release_date)}</div>
        <div>Diff.-adj. Brier: ${fmt(d.overall_score)}</div>
        ${hasCI ? `<div>95% CI: [${fmt(d.conf_int_lb)}, ${fmt(d.conf_int_ub)}]</div>` : ''}
        ${hasN ? `<div>Sample size: ${d.sample_size}</div>` : ''}
      `;
    }

    function referenceTooltipHTML(label, score, baselines, baselineType, key) {
      const benchmarkConfig = {
        'always_0.5': { title: 'Always Predict 0.5', model: null },
        'public': { title: 'Public Median Forecast', model: 'Public median forecast' },
        'superforecaster': { title: 'Superforecaster Median Forecast', model: 'Superforecaster median forecast' },
        'imputed': { title: 'Imputed Forecaster', model: 'Imputed Forecaster' },
        'naive': { title: 'Naive Forecaster', model: 'Naive Forecaster' },
        'always_0': { title: 'Always Predict 0', model: 'Always 0' },
        'always_1': { title: 'Always Predict 1', model: 'Always 1' },
        'random_uniform': { title: 'Random Uniform Prediction', model: 'Random Uniform' }
      };

      const config = benchmarkConfig[key] || { title: label, model: null };
      let html = `
        <div><strong>${config.title}</strong></div>
        <div>Diff-Adj. Brier: ${fmt(score)}</div>
      `;

      if (config.model) {
        const baselineRow = originalRows.find(d => d.model === config.model && (d.type || '').trim().toLowerCase() === baselineType);
        if (baselineRow && baselineType === 'overall') {
          const lbNum = +baselineRow.conf_int_lb;
          const ubNum = +baselineRow.conf_int_ub;
          if (Number.isFinite(lbNum) && Number.isFinite(ubNum)) {
            html += `<div>95% CI: [${fmt(lbNum)}, ${fmt(ubNum)}]</div>`;
          }
        }

        if (baselineRow) {
          const nNum = +baselineRow.sample_size;
          if (Number.isFinite(nNum)) {
            html += `<div>Sample Size: ${nNum}</div>`;
          }
        }
      }

      return html;
    }

    function trendTooltipHTML(reg) {
      return `
        <div><strong>Linear Trend, SOTA Models</strong></div>
        <div>Formula = ${fmt3or4(reg.a)} + (${fmt3or4(reg.b)}) * (months since first model)</div>
        <div>Number of SOTA models: ${reg.n}</div>
      `;
    }

    // Zoom selection
    const selectionOverlay = g.append('rect')
      .attr('width', width)
      .attr('height', height)
      .attr('fill', 'transparent')
      .style('cursor', 'default')
      .style('pointer-events', 'none')
      .on('mousedown', function(event) {
        if (event.button !== 0 || !isShiftPressed) return;
        event.preventDefault();
        isSelecting = true;
        tipOff();

        const [mouseX, mouseY] = d3.pointer(event, this);
        selectionStart = { x: mouseX, y: mouseY };

        if (selectionRect) selectionRect.remove();
        selectionRect = g.append('rect')
          .attr('class', 'selection-rect')
          .attr('x', mouseX).attr('y', mouseY)
          .attr('width', 0).attr('height', 0);
      });

    updateOverlayInteraction = function() {
      selectionOverlay
        .style('pointer-events', isShiftPressed ? 'all' : 'none')
        .style('cursor', isShiftPressed ? 'crosshair' : 'default');
    };

    updateOverlayInteraction();

    svg.on('mousemove', function(event) {
      if (!isSelecting || !selectionStart || !selectionRect) return;

      const [mouseX, mouseY] = d3.pointer(event, g.node());
      const x1 = Math.max(0, Math.min(width, Math.min(selectionStart.x, mouseX)));
      const y1 = Math.max(0, Math.min(height, Math.min(selectionStart.y, mouseY)));
      const x2 = Math.max(0, Math.min(width, Math.max(selectionStart.x, mouseX)));
      const y2 = Math.max(0, Math.min(height, Math.max(selectionStart.y, mouseY)));

      selectionRect
        .attr('x', x1).attr('y', y1)
        .attr('width', x2 - x1).attr('height', y2 - y1);
    })
    .on('mouseup', function(event) {
      if (!isSelecting || !selectionStart || !selectionRect) return;

      const [mouseX, mouseY] = d3.pointer(event, g.node());
      const x1 = Math.max(0, Math.min(width, Math.min(selectionStart.x, mouseX)));
      const y1 = Math.max(0, Math.min(height, Math.min(selectionStart.y, mouseY)));
      const x2 = Math.max(0, Math.min(width, Math.max(selectionStart.x, mouseX)));
      const y2 = Math.max(0, Math.min(height, Math.max(selectionStart.y, mouseY)));

      if (Math.abs(x2 - x1) >= 10 && Math.abs(y2 - y1) >= 10) {
        const newXDomain = [x.invert(x1), x.invert(x2)];
        const newYDomain = [y.invert(y2), y.invert(y1)];
        currentXDomain = newXDomain;
        currentYDomain = newYDomain;
        draw(data, baselines, showErrorBars, currentType);
      }

      if (selectionRect) {
        selectionRect.remove();
        selectionRect = null;
      }
      isSelecting = false;
      selectionStart = null;
    });

    // Restore scroll position after DOM changes
    requestAnimationFrame(() => {
      window.scrollTo(0, scrollY);
    });
  }

  function getSelectedBenchmarks() {
    const checkboxes = document.querySelectorAll('.tag-selection input[type="checkbox"]:checked');
    return Array.from(checkboxes).map(checkbox => checkbox.value);
  }

  function getSelectedType() {
    const radio = document.querySelector('input[name="typeSelect"]:checked');
    return radio ? radio.value : 'overall';
  }

  function shouldIncludeFreeze() {
    const checkbox = document.getElementById('includeFreeze');
    return checkbox ? checkbox.checked : true;
  }

  function shouldShowLegend() {
    const checkbox = document.getElementById('showLegend');
    return checkbox ? checkbox.checked : true;
  }

  function shouldShowIntersection() {
    const checkbox = document.getElementById('showIntersection');
    return checkbox ? checkbox.checked : true;
  }

  function filterFreezeModels(rows) {
    if (shouldIncludeFreeze()) return rows;
    return rows.filter(row =>
      row.team === "ForecastBench" &&
                            !row.model.toLowerCase().includes('with news') &&
                            !row.model.toLowerCase().includes('with crowd forecast') &&
                            !row.model.toLowerCase().includes('with second news')
    );
  }


  let originalRows = [];

  function renderForType(selectedType) {
    const typeNorm = (selectedType || 'overall').trim().toLowerCase();
    const isType = d => (d.type || '').trim().toLowerCase() === typeNorm;

    const filteredRows = filterFreezeModels(originalRows);

    const superforecasterRow = filteredRows.find(d => d.model === 'Superforecaster median forecast' && isType(d));
    const publicRow = filteredRows.find(d => d.model === 'Public median forecast' && isType(d));
    const imputedForecastRow = filteredRows.find(d => d.model === 'Imputed Forecaster' && isType(d));
    const naiveForecastRow = filteredRows.find(d => d.model === 'Naive Forecaster' && isType(d));
    const always0Row = filteredRows.find(d => d.model === 'Always 0' && isType(d));
    const always1Row = filteredRows.find(d => d.model === 'Always 1' && isType(d));
    const randomUniformRow = filteredRows.find(d => d.model === 'Random Uniform' && isType(d));

    const filtered = filteredRows.filter(d => isType(d) && d.release_date && d.release_date.trim() !== '');
    const toNumOrNaN = v => (v === undefined || v === null || String(v).trim() === '') ? NaN : +v;
    const parsed = filtered.map(d => ({
      model: d.model,
      overall_score: +d.diff_adj_brier,
      release_date: new Date(d.release_date),
      conf_int_lb: toNumOrNaN(d.conf_int_lb),
      conf_int_ub: toNumOrNaN(d.conf_int_ub),
      sample_size: toNumOrNaN(d.sample_size)
    }));

    const baselines = {};
    if (superforecasterRow) baselines.superforecaster = +superforecasterRow.diff_adj_brier;
    if (publicRow) baselines.public = +publicRow.diff_adj_brier;
    if (imputedForecastRow) baselines.imputed = +imputedForecastRow.diff_adj_brier;
    if (naiveForecastRow) baselines.naive = +naiveForecastRow.diff_adj_brier;
    if (always0Row) baselines.always_0 = +always0Row.diff_adj_brier;
    if (always1Row) baselines.always_1 = +always1Row.diff_adj_brier;
    if (randomUniformRow) baselines.random_uniform = +randomUniformRow.diff_adj_brier;

    markSOTA(parsed);
    const showErrorBars = true; // Show error bars for all types (dataset, market, overall)
    draw(parsed, baselines, showErrorBars, typeNorm);
  }

  // Transform tournament CSV format to original format
  function transformTournamentData(tournamentRows) {
    const transformedRows = [];

    // Helper function to parse confidence interval strings like "[0.124, 0.136]"
    function parseCI(ciString) {
      if (!ciString || ciString.trim() === '') return { lb: NaN, ub: NaN };
      const matches = ciString.match(/\[(.*?),\s*(.*?)\]/);
      if (matches && matches.length === 3) {
        return { lb: parseFloat(matches[1]), ub: parseFloat(matches[2]) };
      }
      return { lb: NaN, ub: NaN };
    }

    tournamentRows.forEach(row => {
      const team = row.Team;
      const model = row.Model;
      const releaseDate = row['Model release date'];

      // Skip rows without model
      if (!model) return;

      // For baseline models without release date, we still want to include them
      // but they won't be plotted on the chart (only used for reference lines)

      // Create dataset row
      if (row.Dataset && row.Dataset !== '') {
        const datasetCI = parseCI(row['Dataset 95% CI']);
        transformedRows.push({
          team: team,
          model: model,
          type: 'dataset',
          diff_adj_brier: parseFloat(row.Dataset),
          release_date: releaseDate,
          conf_int_lb: datasetCI.lb,
          conf_int_ub: datasetCI.ub,
          sample_size: parseInt(row['N dataset']) || NaN
        });
      }

      // Create market row
      if (row.Market && row.Market !== '') {
        const marketCI = parseCI(row['Market 95% CI']);
        transformedRows.push({
          team: team,
          model: model,
          type: 'market',
          diff_adj_brier: parseFloat(row.Market),
          release_date: releaseDate,
          conf_int_lb: marketCI.lb,
          conf_int_ub: marketCI.ub,
          sample_size: parseInt(row['N market']) || NaN
        });
      }

      // Create overall row
      if (row.Overall && row.Overall !== '') {
        const overallCI = parseCI(row['95% CI']);
        transformedRows.push({
          team: team,
          model: model,
          type: 'overall',
          diff_adj_brier: parseFloat(row.Overall),
          release_date: releaseDate,
          conf_int_lb: overallCI.lb,
          conf_int_ub: overallCI.ub,
          sample_size: parseInt(row.N) || NaN
        });
      }
    });

    return transformedRows;
  }

  function parseRows(rows) {
    originalRows = transformTournamentData(rows);
    const defaultType = getSelectedType();
    renderForType(defaultType);
  }

  function resetZoom() {
    currentXDomain = null;
    currentYDomain = null;
    if (originalRows && originalRows.length) {
      renderForType(getSelectedType());
    }
  }

  // Load parity dates JSON
  fetch(PARITY_DATE_PATH)
    .then(response => response.json())
    .then(data => {
      parityDates = data;
    })
    .catch(err => {
      console.error('Could not load parity dates:', err);
    });

  // Load CSV and setup controls
  d3.csv(CSV_PATH).then(parseRows).catch(err => {
    console.error('Could not load CSV:', err);
  });

  document.addEventListener('DOMContentLoaded', () => {
    // Type radio button handlers
    const typeRadios = document.querySelectorAll('input[name="typeSelect"]');
    typeRadios.forEach(radio => {
      radio.addEventListener('change', () => {
        if (originalRows && originalRows.length) {
          renderForType(getSelectedType());
        }
      });
    });

    // Benchmark checkbox handlers
    const benchmarkCheckboxes = document.querySelectorAll('.tag-selection input[type="checkbox"]');
    benchmarkCheckboxes.forEach(checkbox => {
      checkbox.addEventListener('change', () => {
        if (originalRows && originalRows.length) {
          renderForType(getSelectedType());
        }
      });
    });

    // Freeze checkbox handler
    const freezeCheckbox = document.getElementById('includeFreeze');
    if (freezeCheckbox) {
      freezeCheckbox.addEventListener('change', () => {
        if (originalRows && originalRows.length) {
          renderForType(getSelectedType());
        }
      });
    }

    // Legend checkbox handler
    const legendCheckbox = document.getElementById('showLegend');
    if (legendCheckbox) {
      legendCheckbox.addEventListener('change', () => {
        if (originalRows && originalRows.length) {
          renderForType(getSelectedType());
        }
      });
    }

    // Intersection checkbox handler
    const intersectionCheckbox = document.getElementById('showIntersection');
    if (intersectionCheckbox) {
      intersectionCheckbox.addEventListener('change', () => {
        if (originalRows && originalRows.length) {
          renderForType(getSelectedType());
        }
      });
    }

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        resetZoom();
      }
      if (event.key === 'Shift') {
        isShiftPressed = true;
        if (updateOverlayInteraction) updateOverlayInteraction();
      }
    });

    document.addEventListener('keyup', (event) => {
      if (event.key === 'Shift') {
        isShiftPressed = false;
        if (updateOverlayInteraction) updateOverlayInteraction();
      }
    });
  });
})();
