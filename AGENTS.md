# ForecastBench: A dynamic, contamination-free benchmark of LLM forecasting accuracy

The code under `src/` is set up to work both locally _and_ to be deployed as GCP Cloud Run Jobs.

## Project orchestration and structure

### Nightly GCP workflow

The nightly workflow on GCP is described in detail [here](https://github.com/forecastingresearch/forecastbench/wiki/How-does-ForecastBench-work%3F) and is orchestrated by the code under `src/nightly_update_workflow/`.

TL;DR of the nightly workflow is, in sequential order:

1. Pull in new questions and resolution values from forecasting platforms and datasets (code `src/questions/`)
2. Generate question metadata (invalidate and tag questions) (code `src/metadata/`)
3. Resolve forecast files (code `src/resolve/`)
4. Create leaderboard (code `src/leaderboard/`)
5. Create website (code `src/www.forecastbench.org/`)

Every two weeks, 10 days before the forecast due date, a new question set is created (code `src/curate_questions/`), sampling from the questions pulled in during step 1.

Every two weeks, on the forecast due date, naive forecasts are generated with the code under
`src/base_eval/`, and LLM forecasts are generated with the code under `src/llm_forecaster/`
via Cloud Run entrypoints under `src/orchestration/func_llm_forecaster_*`.

### Directory structure
```
├── experiments/                  # Scoring rule experiments
├── paper/                        # Inputs to the paper: https://arxiv.org/abs/2409.19839
├── src
│   ├── base_eval/                # Naive forecasts and baseline forecasting experiments
│   ├── curate_questions/         # Question sampling to create and publish the question set
│   ├── helpers/                  # Helper functions that are used across the codebase
│   ├── leaderboard/              # Generate the leaderboard
│   ├── llm_forecaster/           # ForecastBench LLM forecast generation
│   ├── metadata/                 # Generate metadata: categorize and validate forecast questions that have been pulled in
│   ├── nightly_update_workflow/  # Orchestrate the nightly runs on GCP
│   ├── orchestration/            # Cloud Run function and job entrypoints
│   ├── questions/                # Pull questions and resolution values into the system
│   ├── resolve/                  # Resolve forecast files
│   └── www.forecastbench.org/    # Jekyll website
```

## Local development

### First time setup

```bash
$ make setup-python-env
```

Shared utilities are installed as the `fri-utils` package from root
`requirements.runtime.txt`; this repository no longer has a root `utils` git submodule.

### Before working

Set up and activate a Python virtual environment before running Python commands.

For automated agents: treat the repository `.venv` directory as user-owned. Do not
create it, delete it, install packages into it, or run Make targets that recreate or
modify it. If `.venv` exists, leave it untouched and create your own environment
outside the repository, for example under `/tmp/forecastbench-agent-venv`.

Authenticate with GCP (once/day):

```bash
$ gcloud config set project forecastbench-dev
$ gcloud auth application-default print-access-token >/dev/null 2>&1 && echo "GCP Access OK" || gcloud auth application-default login
```


### When coding

* Ignore code under directories that begin with the text "upload"; these are staging directories for the deployment to Google Cloud.

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
* New ForecastBench Python files should not add `from __future__ import annotations`
* Prefer f-strings for Python string interpolation, including log messages. Use logger
  %-style only when there is a concrete, necessary reason to defer interpolation.
* Tests should verify behavior, data contracts, or externally meaningful integration points. Do
  not add tests that only assert implementation details such as exact helper signatures, private
  call shapes, or the absence/presence of internal arguments unless that detail is an explicitly
  supported public API.
* Test from the caller's or operator's point of view. Before adding or tightening a test, be able
  to state the broken requirement it would catch without naming private implementation choices. If
  an equivalent implementation should still satisfy the same requirement, the test should keep
  passing; otherwise test a higher-level contract or do not add the test.
* Docstring format:

  ```python
  """Description.

  Args:
    Arg 1 (type 1): Description 1
    ...
  """
  ```

## LLM forecasters

See `src/llm_forecaster/AGENTS.md` for package-specific LLM forecaster rules.

## Commits

* Run `make lint` and fix any linting errors before committing
* Run `make test` and fix any test errors before committing. Automated agents must
  instead run the equivalent pytest command from their own external virtual
  environment, because `make test` manages the repository `.venv`.
* When working on a branch, if you’re revising earlier work, amend the relevant existing commit instead of creating a new one.

### Commit messages
* Use Conventional Commits
* Use backticks when referencing code in a commit message
* Wrap at 100 chars, not before
* Close PRs from commit messages
