function initializeTooltips() {
  const tooltip = document.createElement('div');
  tooltip.className = 'tooltip';
  tooltip.style.position = 'fixed';
  tooltip.style.display = 'none';
  document.body.appendChild(tooltip);

  let currentTimeout = null;
  const headers = document.querySelectorAll('.column-header-tooltip');

  headers.forEach(header => {
    const columnName = header.getAttribute('data-tooltip');
    const content = tooltipContent[columnName];
    if (!content) return;

    header.addEventListener('mouseenter', (e) => {
      if (currentTimeout) {
        clearTimeout(currentTimeout);
        currentTimeout = null;
      }
      tooltip.innerHTML = content;
      tooltip.style.display = 'block';
      const rect = header.getBoundingClientRect();
      let left = rect.left + (rect.width / 2);
      let top = rect.bottom + 10;
      const tooltipWidth = 300;
      const tooltipHeight = 60;

      if (left < tooltipWidth / 2 + 10) {
        left = tooltipWidth / 2 + 10;
      } else if (left > window.innerWidth - tooltipWidth / 2 - 10) {
        left = window.innerWidth - tooltipWidth / 2 - 10;
      }
      if (top + tooltipHeight > window.innerHeight - 10) {
        top = rect.top - tooltipHeight - 10;
      }
      if (top < 10) {
        top = 10;
      }

      tooltip.style.left = left + 'px';
      tooltip.style.top = top + 'px';
      tooltip.style.transform = 'translateX(-50%)';

      setTimeout(() => tooltip.classList.add('show'), 10);
    });

    header.addEventListener('mouseleave', () => {
      if (currentTimeout) clearTimeout(currentTimeout);
      currentTimeout = setTimeout(() => {
        tooltip.classList.remove('show');
        setTimeout(() => {
          tooltip.style.display = 'none';
        }, 200);
        currentTimeout = null;
      }, 50);
    });
  });
}
