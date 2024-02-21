## Proof of Concept for LLM Benchmark project

### Overview
1. Select several sources from which to generate questions (Manifold, Wikidata, Metaculus, ...)
1. Get news relevant to the questions
1. Setup several models (GPT, LLAMA, Gemini, Claude, ...) to create forecasts on the questions
1. Get everything running on Google Cloud Platform
1. Create a leaderboard, update daily

---
### For Devs

#### Local setup
1. `cp variables.example.mk variables.mk` and set the values accordingly
1. Setup your Python virtual environment
   1. `python3 -m venv venv`
   1. `source venv/bin/activate`
   1. `pip install -r requirements.txt`
1. `git submodule update --init`

#### Coding
General organization:
* The Google Cloud Functions are located in `src/gcp/functions`
  * Each question source can be in its own folder. See `src/gcp/manifold` for an example setup
* The `gpt` proof of concept is in `src/gpt`
* The question generation tests are in `src/question_generation`

#### Contributions

Before pushing to this repo:
* run `make lint` and fix any errors/warnings
* ensure code has been deployed to Google Cloud Platform and tested
* create a pull request
