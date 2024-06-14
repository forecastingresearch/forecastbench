## LLM Benchmark

A forecasting benchmark for LLMs.

### For Devs

#### Local setup
1. `git clone --recurse-submodules <repo-url>.git`
1. `cd llm-benchmark`
1. `cp variables.example.mk variables.mk` and set the values accordingly
1. Setup your Python virtual environment
   1. `make setup-python-env`
   1. `source .venv/bin/activate`

#### Run GCP Cloud Functions locally
1. `cd directory/containing/cloud/function`
1. `eval $(cat path/to/variables.mk | xargs) python main.py`

#### Contributions

Before pushing to this repo:
* run `make lint` and fix any errors/warnings
* ensure code has been deployed to Google Cloud Platform and tested
* fork the repo, push to the fork, and create a pull request
