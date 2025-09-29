function initializeTooltips() {
  // Reuse existing tooltip if present; otherwise create it
  let tooltip = document.querySelector('.tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.className = 'tooltip';
    tooltip.style.position = 'fixed';
    tooltip.style.display = 'none';
    document.body.appendChild(tooltip);
  }

  let currentTimeout = null;

  // Target BOTH header elements and any cell that carries data-tooltip
  const targets = document.querySelectorAll(
    '.column-header-tooltip, .cell-tooltip, td [data-tooltip]'
  );

  // Small helper to compute final tooltip HTML:
  function getTooltipHTML(el) {
    const keyOrText = el.getAttribute('data-tooltip');
    if (!keyOrText) return null;
    return (typeof tooltipContent === 'object' && keyOrText in tooltipContent)
      ? tooltipContent[keyOrText]
      : keyOrText;
  }

  function positionTooltip(el) {
    const rect = el.getBoundingClientRect();
    const tooltipWidth = 300;
    const tooltipHeight = 60;

    let left = rect.left + (rect.width / 2);
    let top = rect.bottom + 10;

    if (left < tooltipWidth / 2 + 10) {
      left = tooltipWidth / 2 + 10;
    } else if (left > window.innerWidth - tooltipWidth / 2 - 10) {
      left = window.innerWidth - tooltipWidth / 2 - 10;
    }
    if (top + tooltipHeight > window.innerHeight - 10) {
      top = rect.top - tooltipHeight - 10;
    }
    if (top < 10) top = 10;

    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
    tooltip.style.transform = 'translateX(-50%)';
  }

  function showTooltipFor(el) {
    const html = getTooltipHTML(el);
    if (!html) return;
    if (currentTimeout) {
      clearTimeout(currentTimeout);
      currentTimeout = null;
    }
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';
    positionTooltip(el);
    setTimeout(() => tooltip.classList.add('show'), 10);
  }

  function hideTooltipSoon() {
    if (currentTimeout) clearTimeout(currentTimeout);
    currentTimeout = setTimeout(() => {
      tooltip.classList.remove('show');
      setTimeout(() => { tooltip.style.display = 'none'; }, 200);
      currentTimeout = null;
    }, 50);
  }

  // Attach listeners, but only once per element
  targets.forEach(el => {
    if (el.dataset.tooltipped === '1') return; // already bound

    // Ensure native browser tooltip won't appear even if someone left a title
    if (el.hasAttribute('title')) el.removeAttribute('title');

    el.addEventListener('mouseenter', () => showTooltipFor(el));
    el.addEventListener('mouseleave', hideTooltipSoon);

    el.dataset.tooltipped = '1';
  });
}
