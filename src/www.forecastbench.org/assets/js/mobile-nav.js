// Force all nav items into the hamburger dropdown on mobile.
// Runs after greedy-nav has already initialized.
$(function () {
  var $toggle = $("nav.greedy-nav .greedy-nav__toggle");
  var $vlinks = $("nav.greedy-nav .visible-links");
  var $hlinks = $("nav.greedy-nav .hidden-links");

  function collapseToHamburger() {
    if ($(window).width() <= 768) {
      $vlinks.children().each(function () {
        $(this).prependTo($hlinks);
      });
      $toggle.removeClass("hidden").attr("count", $hlinks.children().length);
    }
  }

  collapseToHamburger();
  $(window).on("resize", collapseToHamburger);
});
