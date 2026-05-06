# LLM Forecaster Package

This package owns ForecastBench LLM baseline generation.

- `FB_MODEL_RUN_KEYS` in `fb_model_runs.py` lists models runs that will forecast on the next
  weekly round.
- Select model runs for `FB_MODEL_RUN_KEYS` from `utils.llm.model_runs` by `model_run_key`.
  Do not declare local `ModelRun` registries here.
- Use `utils.llm` for provider calls; do not instantiate provider SDK clients directly here.
- Keep runtime model options in the `ModelRun.options` declaration, except the
  project-specific OpenAI `safety_identifier`, which is injected at runtime from Secret Manager.
- Keep model-run names lower-case and file-safe.
- OK any non-default runtime options with a human.
- Treat LLM forecasting prompts, parsing/extraction behavior, and output schema as explicit
  contracts. Update tests and docs when intentionally changing them.
- Keep Cloud Run entrypoints thin; put behavior in `src/llm_forecaster`.
