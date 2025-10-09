// Align the check card with the right edge of the baseline leaderboard
function alignCheckCard() {
  const card = document.querySelector('.check-card');
  const leaderboard = document.querySelector('.leaderboard-wrapper-home');

  // Only align on desktop (> 768px)
  if (window.innerWidth > 768 && card && leaderboard) {
    const leaderboardRect = leaderboard.getBoundingClientRect();
    const wrapp = document.getElementById('wrapp');
    const wrappRect = wrapp.getBoundingClientRect();

    // Calculate the right position relative to the wrapper
    const rightOffset = wrappRect.right - leaderboardRect.right;

    card.style.right = rightOffset + 'px';
  } else if (card) {
    // Reset to default on mobile
    card.style.right = '';
  }
}

// Run on page load
window.addEventListener('load', alignCheckCard);

// Run on window resize
let resizeTimeout;
window.addEventListener('resize', function() {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(alignCheckCard, 100);
});
