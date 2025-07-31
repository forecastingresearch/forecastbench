particlesJS('particles-js', {
  "particles": {
    "number": {
      "value": 80,
      "density": { "enable": true, "value_area": 800 }
    },
    "color": { "value": ["#dddddd"] },
    "shape": {
      "type": "polygon",
      "polygon": { "nb_sides": 8 }
    },
    "opacity": {
      "value": 0.3,
      "random": false
    },
    "size": {
      "value": 2,
      "random": true,
      "anim": {
        "enable": true,
        "speed": 1,
        "size_min": 1,
        "sync": false
      }
    },
    "line_linked": {
      "enable": true,
      "distance": 120,
      "color": "#888888",
      "opacity": 0.2,
      "width": 6
    },
    "move": {
      "enable": true,
      "speed": 0.08,
      "direction": "top",
      "random": true,
      "straight": false,
      "out_mode": "out",
      "bounce": false
    }
  },
  "interactivity": {
    "detect_on": "canvas",
    "events": {
      "onhover": { "enable": false, "mode": "repulse" },
      "onclick": { "enable": false, "mode": "push" },
      "resize": true
    },
    "modes": {
      "repulse": { "distance": 200, "duration": 9 },
      "push": { "particles_nb": 2 }
    }
  },
  "retina_detect": true
});
