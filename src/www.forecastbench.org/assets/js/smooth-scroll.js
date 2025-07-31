// Allow scroll to #notes if someone provides the URL as a link.
window.addEventListener("load", function() {
  if (window.location.hash) {
    const el = document.querySelector(window.location.hash);
    if (el) {
      // delay to let DataTables finish layout
      setTimeout(() => {
        el.scrollIntoView({behavior: "smooth", block: "start"});
      }, 500);
    }
  }
});

