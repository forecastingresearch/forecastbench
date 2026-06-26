"""Forecasting prompt text."""

from collections.abc import Mapping
from functools import cache
from string import Formatter
from typing import Any

_FORMATTER = Formatter()


@cache
def _template_field_names(template: str) -> set[str]:
    """Return named fields referenced by a Python format template."""
    field_names = set()
    for _literal_text, field_name, _format_spec, _conversion in _FORMATTER.parse(template):
        if field_name is None:
            continue
        if field_name == "":
            raise ValueError("Anonymous prompt fields are not supported.")
        field_root = field_name.split(".", maxsplit=1)[0].split("[", maxsplit=1)[0]
        field_names.add(field_root)
    return field_names


def render_template(template: str, params: Mapping[str, Any]) -> str:
    """Render a prompt template after validating its exact field contract."""
    required_fields = _template_field_names(template)
    provided_fields = set(params)
    missing_fields = sorted(required_fields - provided_fields)
    if missing_fields:
        raise ValueError(f"Missing prompt fields: {', '.join(missing_fields)}")

    extra_fields = sorted(provided_fields - required_fields)
    if extra_fields:
        raise ValueError(f"Unexpected prompt fields: {', '.join(extra_fields)}")

    return template.format(**params)


# The following zero-shot prompts mainly come from:
# "Approaching Human-Level Forecasting with Language Models" by Halawi et al. (2024)
# Some are modified versions in order to adapt to our needs
# https://arxiv.org/pdf/2402.18563v1
ZERO_SHOT_MARKET_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Today's Date: {today_date}

Question resolution date: {resolution_date}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
Do not output anything else.
Answer: {{ Insert answer here }}
"""  # noqa: B950

ZERO_SHOT_MARKET_WITH_FREEZE_VALUE_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Market value on {freeze_datetime}:
{freeze_datetime_value}

Today's Date: {today_date}

Question resolution date: {resolution_date}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
Do not output anything else.
Answer: {{ Insert answer here }}
"""  # noqa: B950


ZERO_SHOT_DATASET_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

You're going to predict the probability of the following potential outcome "at each of the resolution dates".

Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Current value on {freeze_datetime}:
{freeze_datetime_value}

Value Explanation:
{freeze_datetime_value_explanation}

Today's Date: {today_date}

Question resolution dates: {list_of_resolution_dates}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
Answer: {{ Insert answer here }}
"""  # noqa: B950


FORECAST_EXTRACTION_PROMPT = """
You are extracting probabilities from text.

Your task is to extract probabilistic forecasts from the MODEL RESPONSE only.

Expected number of probabilities: {n_horizons}

Rules:
- Do not make a forecast.
- Do not infer missing values.
- Do not revise, smooth, calibrate, average, or replace any probabilities.
- Extract only probabilities explicitly stated in the MODEL RESPONSE.
- Preserve the order in which the probabilities appear in the MODEL RESPONSE.
- If the MODEL RESPONSE contains final-answer probabilities paired with resolution dates, return them in the same order as those dates appear in the MODEL RESPONSE.
- If the MODEL RESPONSE contains multiple candidate sets of probabilities, use only the final answer set.
- If you cannot identify exactly {n_horizons} final-answer probabilities, return [].

Return only a Python list of decimal probabilities, e.g. [0.1, 0.2, 0.3].
Do not output explanation or any other text.

MODEL RESPONSE:
```text
{model_response}
```
"""  # noqa: B950
