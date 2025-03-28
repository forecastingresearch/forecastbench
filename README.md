# ForecastBench

[![ICLR 2025](https://img.shields.io/badge/ICLR-2025-D5FFC1?labelColor=2A363F)](https://iclr.cc/virtual/2025/poster/28507) [![arXiv:2409.19839](https://img.shields.io/badge/arXiv-2409.19839-272727?logo=arxiv&labelColor=B31B1B)](https://arxiv.org/abs/2409.19839)

A dynamic, continuously-updated benchmark to evaluate LLM forecasting capabilities. More at [www.forecastbench.org](https://www.forecastbench.org/).

## Datasets

Leaderboards and datasets are updated nightly and available at [github.com/forecastingresearch/forecastbench-datasets](https://github.com/forecastingresearch/forecastbench-datasets).

## Participate in the benchmark

Instructions for how to submit your model to the benchmark can be found here: [How-to-submit-to-ForecastBench](https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench).

## Wiki

Dig into the details of ForecastBench on the [wiki](https://github.com/forecastingresearch/forecastbench/wiki/).

## Citation

```bibtex
@inproceedings{karger2025forecastbench,
      title={ForecastBench: A Dynamic Benchmark of AI Forecasting Capabilities},
      author={Ezra Karger and Houtan Bastani and Chen Yueh-Han and Zachary Jacobs and Danny Halawi and Fred Zhang and Philip E. Tetlock},
      year={2025},
      booktitle={International Conference on Learning Representations (ICLR)},
      url={https://iclr.cc/virtual/2025/poster/28507}
}
```

## Getting started for devs

#### Local setup
1. `git clone --recurse-submodules <repo-url>.git`
1. `cd forecastbench`
1. `cp variables.example.mk variables.mk` and set the values accordingly
1. Setup your Python virtual environment
   1. `make setup-python-env`
   1. `source .venv/bin/activate`

#### Run GCP Cloud Functions locally
1. `cd directory/containing/cloud/function`
1. `eval $(cat path/to/variables.mk | xargs) python main.py`

#### Contributions

Before creating a pull request:
* run `make lint` and fix any errors and warnings
* ensure code has been deployed to Google Cloud Platform and tested (only for our devs, for others,
  we're happy you're contributing and we'll test this on our end).
* fork the repo
* reference the issue number (if one exists) in the commit message
* push to the fork on a branch other than `main`
* create a pull request
