## ForecastBench

A forecasting benchmark for LLMs. Leaderboards and datasets available at
[https://www.forecastbench.org](https://www.forecastbench.org/).

### Getting started

#### Local setup
1. `git clone --recurse-submodules <repo-url>.git`
1. `cd llm-benchmark`
1. `cp variables.example.mk variables.mk` and set the values accordingly
1. Setup your Python virtual environment
   1. `make setup-python-env`
   1. `source .venv/bin/activate`
1. Install Docker
1. Run `gcloud auth configure-docker $(CLOUD_DEPLOY_REGION)-docker.pkg.dev`

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
