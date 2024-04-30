"""Constants."""

import os
from datetime import timedelta

from . import dates, keys, prompts, resolutions

BENCHMARK_START_YEAR = 2024

BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")
PUBLIC_BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET_QUESTIONS")
PROJECT_ID = os.environ.get("CLOUD_PROJECT")
API_KEY_ANTHROPIC = keys.get_secret("API_KEY_ANTHROPIC")
API_KEY_OPENAI = keys.get_secret("API_KEY_OPENAI")
API_KEY_TOGETHERAI = keys.get_secret("API_KEY_TOGETHERAI")
API_KEY_NEWSCATCHER = keys.get_secret("API_KEY_NEWSCATCHER")
API_KEY_GOOGLE = keys.get_secret("API_KEY_GOOGLE")
OAI_SOURCE = "OAI"
ANTHROPIC_SOURCE = "ANTHROPIC"
TOGETHER_AI_SOURCE = "TOGETHER"
GOOGLE_SOURCE = "GOOGLE"

FREEZE_NUM_LLM_QUESTIONS = 1000

FREEZE_NUM_HUMAN_QUESTIONS = 200

# Assumed in the code
assert FREEZE_NUM_LLM_QUESTIONS > FREEZE_NUM_HUMAN_QUESTIONS

FREEZE_QUESTION_SOURCES = {
    "manifold": {
        "name": "Manifold",  # Name to use in the human prompt
        "human_prompt": prompts.market,  # The human prompt to use
        "resolution_criteria": resolutions.market,
    },
    "metaculus": {
        "name": "Metaculus",
        "human_prompt": prompts.market,
        "resolution_criteria": resolutions.metaculus,
    },
    "acled": {
        "name": "ACLED",
        "human_prompt": prompts.acled,
        "resolution_criteria": resolutions.acled,
    },
    "infer": {
        "name": "INFER",
        "human_prompt": prompts.market,
        "resolution_criteria": resolutions.infer,
    },
}

FREEZE_WINDOW_IN_DAYS = 7

FREEZE_DATETIME = os.environ.get("FREEZE_DATETIME", dates.get_datetime_today()).replace(
    hour=0, minute=0, second=0, microsecond=0
)

FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=FREEZE_WINDOW_IN_DAYS)

FORECAST_DATE = FORECAST_DATETIME.date()

FORECAST_HORIZONS_IN_DAYS = [
    7,  # 1 week
    30,  # 1 month
    90,  # 3 months
    180,  # 6 months
    365,  # 1 year
    1095,  # 3 years
    1825,  # 5 years
    3650,  # 10 years
]

QUESTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "question": str,
    "background": str,
    "source_resolution_criteria": str,
    "source_begin_datetime": str,
    "source_close_datetime": str,
    "url": str,
    "source_resolution_datetime": str,
    "resolved": bool,
    "continual_resolution": bool,
    "forecast_horizons": object,  # list<int>
    "value_at_freeze_datetime": str,
    "value_at_freeze_datetime_explanation": str,
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "date": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]

MANIFOLD_TOPIC_SLUGS = ["entertainment", "sports-default", "technology-default"]

METACULUS_CATEGORIES = [
    "geopolitics",
    "natural-sciences",
    "sports-entertainment",
    "health-pandemics",
    "law",
    "computing-and-math",
]

ACLED_FETCH_COLUMN_DTYPE = {
    "event_id_cnty": str,
    "event_date": str,
    "iso": int,
    "region": str,
    "country": str,
    "admin1": str,
    "event_type": str,
    "fatalities": int,
    "timestamp": str,
}
ACLED_FETCH_COLUMNS = list(ACLED_FETCH_COLUMN_DTYPE.keys())

ACLED_QUESTION_FILE_COLUMN_DTYPE = {
    **QUESTION_FILE_COLUMN_DTYPE,
    "lhs_func": str,
    "lhs_args": object,  # <dict>
    "comparison_operator": str,
    "rhs_func": str,
    "rhs_args": object,  # <dict>
}
ACLED_QUESTION_FILE_COLUMNS = list(ACLED_QUESTION_FILE_COLUMN_DTYPE.keys())

MODEL_TOKEN_LIMITS = {
    "claude-2.1": 200000,
    "claude-2": 100000,
    "claude-3-opus-20240229": 200000,
    "claude-3-sonnet-20240229": 200000,
    "gpt-3.5-turbo-0125": 16385,
    "gpt_4": 8192,
    "gpt-4-turbo-2024-04-09": 128000,
    "gemini-pro": 30720,
    "meta-llama/Llama-2-7b-chat-hf": 4096,
    "meta-llama/Llama-2-13b-chat-hf": 4096,
    "meta-llama/Llama-2-70b-chat-hf": 4096,
    "meta-llama/Llama-3-8b-chat-hf": 8000,
    "meta-llama/Llama-3-70b-chat-hf": 8000,
    "mistralai/Mistral-7B-Instruct-v0.2": 32768,
    "mistralai/Mixtral-8x7B-Instruct-v0.1": 32768,
    "mistralai/Mixtral-8x22B-Instruct-v0.1": 65536,
    "NousResearch/Nous-Hermes-2-Mixtral-8x7B-DPO": 32768,
}

MODEL_NAME_TO_SOURCE = {
    "claude-2.1": ANTHROPIC_SOURCE,
    "claude-2": ANTHROPIC_SOURCE,
    "claude-3-opus-20240229": ANTHROPIC_SOURCE,
    "claude-3-sonnet-20240229": ANTHROPIC_SOURCE,
    "gpt-4": OAI_SOURCE,
    "gpt-3.5-turbo-0125": OAI_SOURCE,
    "gpt-4-turbo-2024-04-09": OAI_SOURCE,
    "gemini-pro": GOOGLE_SOURCE,
    "meta-llama/Llama-2-7b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-2-13b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-2-70b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-3-8b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-3-70b-chat-hf": TOGETHER_AI_SOURCE,
    "mistralai/Mistral-7B-Instruct-v0.2": TOGETHER_AI_SOURCE,
    "mistralai/Mixtral-8x7B-Instruct-v0.1": TOGETHER_AI_SOURCE,
    "mistralai/Mixtral-8x22B-Instruct-v0.1": TOGETHER_AI_SOURCE,
    "NousResearch/Nous-Hermes-2-Mixtral-8x7B-DPO": TOGETHER_AI_SOURCE,
}
