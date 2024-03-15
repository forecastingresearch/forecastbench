## FRI LLM Benchmark

A forecasting benchmark for LLMs.

### For Devs

#### Local setup
1. `cp variables.example.mk variables.mk` and set the values accordingly
1. Setup your Python virtual environment
   1. `python3 -m venv venv`
   1. `source venv/bin/activate`
   1. `pip install -r requirements.txt`
1. `git submodule update --init`

#### Contributions

Before pushing to this repo:
* run `make lint` and fix any errors/warnings
* ensure code has been deployed to Google Cloud Platform and tested
* fork the repo, push to the fork, and create a pull request
