(function () {
  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text);
    }

    return new Promise(function (resolve, reject) {
      const textArea = document.createElement("textarea");
      textArea.value = text;
      textArea.setAttribute("readonly", "");
      textArea.style.position = "fixed";
      textArea.style.left = "-9999px";
      textArea.style.top = "-9999px";
      document.body.appendChild(textArea);
      textArea.select();

      try {
        document.execCommand("copy");
        resolve();
      } catch (error) {
        reject(error);
      } finally {
        document.body.removeChild(textArea);
      }
    });
  }

  function sectionUrlFor(element) {
    const section = element.closest("section");
    const hashTarget = (section && section.id) || element.id;
    const url = new URL(window.location.href);

    if (hashTarget) {
      url.hash = hashTarget;
    }

    return url.toString();
  }

  function clearCopiedTimers(button) {
    if (button._copiedFadeTimer) {
      window.clearTimeout(button._copiedFadeTimer);
      button._copiedFadeTimer = null;
    }

    if (button._copiedResetTimer) {
      window.clearTimeout(button._copiedResetTimer);
      button._copiedResetTimer = null;
    }
  }

  function showCopiedState(button) {
    clearCopiedTimers(button);
    button.classList.remove("copied-fade");
    button.classList.add("copied");
    button.blur();

    button._copiedFadeTimer = window.setTimeout(function () {
      button.classList.add("copied-fade");
    }, 850);

    button._copiedResetTimer = window.setTimeout(function () {
      button.classList.remove("copied", "copied-fade");
    }, 1500);
  }

  function addSectionLinkButton(heading) {
    if (heading.querySelector(".section-link-copy")) {
      return;
    }

    const button = document.createElement("button");
    button.type = "button";
    button.className = "section-link-copy";
    button.title = "Copy link to this section";
    button.setAttribute("aria-label", "Copy link to this section");
    button.innerHTML = '<i class="fa-solid fa-link" aria-hidden="true"></i>';

    button.addEventListener("pointerup", function () {
      button.blur();
    });

    button.addEventListener("click", function () {
      const url = sectionUrlFor(heading);

      copyText(url).then(function () {
        showCopiedState(button);
      }).catch(function (error) {
        console.error("Unable to copy section link", error);
      });
    });

    heading.appendChild(button);
  }

  function initSectionLinks() {
    document.querySelectorAll("[data-section-link]").forEach(addSectionLinkButton);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSectionLinks);
  } else {
    initSectionLinks();
  }
})();
