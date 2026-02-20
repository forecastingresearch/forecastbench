# ForecastBench: A dynamic, contamination-free benchmark of LLM forecasting accuracy

The code under `src/` is set up to work both locally _and_ to be deployed as GCP Cloud Run Jobs.

## Project orchestration and structure

### Nightly GCP workflow

The nightly workflow on GCP is described in detail [here](https://github.com/forecastingresearch/forecastbench/wiki/How-does-ForecastBench-work%3F) and is orchestrated by the code under `src/nightly_update_workflow/`.

TL;DR of the nightly workflow is, in sequential order:

1. Pull in new questions and resolution values from forecasting platforms and datasets (code `src/questions/`)
2. Generate question metadata (invalidate and tag questions) (code `src/metadata/`)
3. Resolve forecast files (code `src/resolve_forecasts/`)
4. Create leaderboard (code `src/leaderboard/`)
5. Create website (code `src/www.forecastbench.org/`)

Every two weeks, 10 days before the forecast due date, a new question set is created (code `src/curate_questions/`), sampling from the questions pulled in during step 1.

Every two weeks, on the forecast due date, forecasts are generated with the code under `src/base_eval/`.

### Directory structure
```
├── experiments/                  # Scoring rule experiments
├── paper/                        # Inputs to the paper: https://arxiv.org/abs/2409.19839
├── utils/                        # Git submodule for shared organization-level code
├── src
│   ├── base_eval/                # LLM forecasters and naive forecasts
│   ├── curate_questions/         # Question sampling to create and publish the question set
│   ├── helpers/                  # Helper functions that are used across the codebase
│   ├── leaderboard/              # Generate the leaderboard
│   ├── metadata/                 # Generate metadata: categorize and validate forecast questions that have been pulled in
│   ├── nightly_update_workflow/  # Orchestrate the nightly runs on GCP
│   ├── questions/                # Pull questions and resolution values into the system
│   ├── resolve_forecasts/        # Resolve forecast files
│   └── www.forecastbench.org/    # Jekyll website
```

## Local development

### First time setup

```bash
$ make setup-python-env
```

### Before working

Ensure you have set up your virtual env:

```bash
$ source .venv/bin/activate
```

Authenticate with GCP (once/day):

```bash
$ gcloud config set project forecastbench-dev
$ gcloud auth application-default print-access-token >/dev/null 2>&1 && echo "GCP Access OK" || gcloud auth application-default login
```


### When coding

#### Running Python code
To run the code under `src/`, except for `src/www.forecastbench.org`, you must first load the values in `variables.mk` as environment variables.

For example, to run the leaderboard code:

```bash
$ cd src/leaderboard && eval $(cat ../../variables.mk | xargs) python main.py
```

#### Serving the website

To build and serve the site:

```bash
$ cd src/www.forecastbench.org && bundle exec jekyll s
```
This will serve stand-in files that are under `src/www.forecastbench.org/assets/` where data is required (e.g. the leaderboards and explore chart).

If you want to see recent data on the website, first run the leaderboard code, which will create the folders `src/leaderboard/leaderboards/` and `src/leaderboard/anonymous_logos/`. Then, before serving the website, copy that data over:

```bash
$ cp src/leaderboard/leaderboards/js/* src/www.forecastbench.org/assets/js/
$ cp src/leaderboard/leaderboards/csv/* src/www.forecastbench.org/assets/data/
$ cp src/leaderboard/anonymous_logos/* src/www.forecastbench.org/assets/images/org_logos/
```

NB: `variables.mk` contains both runtime environment variables and variables that are used for deployment. It should not be modified. When running locally, this file will contain the environment variable `RUNNING_LOCALLY=1`.

## Code style

* Formatting: black (line length 100)
* Import sorting: isort (black-compatible profile)
* Linting: flake8
* Docstrings: pydocstyle
* Run `make lint` to apply formatting and check all of the above
* Configuration is in `pyproject.toml` and `setup.cfg`
* Functions should use type hints
* Docstring format:

  ```python
  """Description.

  Args:
    Arg 1 (type 1): Description 1
    ...
  """
  ```
