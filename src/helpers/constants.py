"""Constants."""

from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum

BENCHMARK_NAME = "ForecastBench"
BENCHMARK_EMAIL = "forecastbench@forecastingresearch.org"
BENCHMARK_URL = "https://www.forecastbench.org"

BENCHMARK_START_YEAR = 2024
BENCHMARK_START_MONTH = 5
BENCHMARK_START_DAY = 1
BENCHMARK_START_DATE = f"{BENCHMARK_START_YEAR}-{BENCHMARK_START_MONTH}-{BENCHMARK_START_DAY}"
BENCHMARK_START_DATE_DATETIME = datetime.strptime(BENCHMARK_START_DATE, "%Y-%m-%d")
BENCHMARK_START_DATE_DATETIME_DATE = BENCHMARK_START_DATE_DATETIME.date()

BENCHMARK_TOURNAMENT_START_YEAR = 2024
BENCHMARK_TOURNAMENT_START_MONTH = 7
BENCHMARK_TOURNAMENT_START_DAY = 21
BENCHMARK_TOURNAMENT_START_DATE = (
    f"{BENCHMARK_TOURNAMENT_START_YEAR}-"
    f"{BENCHMARK_TOURNAMENT_START_MONTH}-"
    f"{BENCHMARK_TOURNAMENT_START_DAY}"
)
BENCHMARK_TOURNAMENT_START_DATE_DATETIME = datetime.strptime(
    BENCHMARK_TOURNAMENT_START_DATE, "%Y-%m-%d"
)
BENCHMARK_TOURNAMENT_START_DATE_DATETIME_DATE = BENCHMARK_TOURNAMENT_START_DATE_DATETIME.date()

parsed_date = datetime.strptime(BENCHMARK_START_DATE + " 00:00", "%Y-%m-%d %H:%M")
BENCHMARK_START_DATE_EPOCHTIME = int(parsed_date.timestamp())
BENCHMARK_START_DATE_EPOCHTIME_MS = BENCHMARK_START_DATE_EPOCHTIME * 1000

QUESTION_BANK_DATA_STORAGE_START_DATETIME = BENCHMARK_START_DATE_DATETIME - timedelta(days=360)
QUESTION_BANK_DATA_STORAGE_START_DATE = QUESTION_BANK_DATA_STORAGE_START_DATETIME.date()

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
    "url": str,
    "resolved": bool,
    "forecast_horizons": object,
    "freeze_datetime_value": str,
    "freeze_datetime_value_explanation": str,
    "market_info_resolution_criteria": str,
    "market_info_open_datetime": str,
    "market_info_close_datetime": str,
    "market_info_resolution_datetime": str,
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "date": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]

META_DATA_FILE_COLUMN_DTYPE = {
    "source": str,
    "id": str,
    "category": str,
    "valid_question": bool,
}
META_DATA_FILE_COLUMNS = list(META_DATA_FILE_COLUMN_DTYPE.keys())
META_DATA_FILENAME = "question_metadata.jsonl"

QUESTION_CATEGORIES = [
    "Science & Tech",
    "Healthcare & Biology",
    "Economics & Business",
    "Environment & Energy",
    "Politics & Governance",
    "Arts & Recreation",
    "Security & Defense",
    "Sports",
    "Other",
]


class RunMode(str, Enum):
    """Run modes for code execution.

    - TEST: Test/dev runs; use to reduce costs when running models.
    - PROD: Full production runs; execute all models with full question set.

    Construction is case-insensitive (e.g., RunMode("teST") --> RunMode.TEST).
    Invalid values raise ValueError.
    """

    TEST = "TEST"
    PROD = "PROD"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            return cls.__members__.get(value.upper())
        return None


TEST_FORECAST_FILE_PREFIX = RunMode.TEST.value

OAI_SOURCE = "OAI"
ANTHROPIC_SOURCE = "ANTHROPIC"
TOGETHER_AI_SOURCE = "TOGETHER"
GOOGLE_SOURCE = "GOOGLE"
MISTRAL_SOURCE = "MISTRAL"
XAI_SOURCE = "XAI"

ANTHROPIC_ORG = "Anthropic"
DEEPSEEK_ORG = "DeepSeek"
MOONSHOT_ORG = "Moonshot"
GOOGLE_ORG = "Google"
META_ORG = "Meta"
MISTRAL_ORG = "Mistral AI"
MISTRAL_ORG_1 = "Mistral"  # for some forecasts, "Mistral AI" was called "Mistral"
OAI_ORG = "OpenAI"
QWEN_ORG = "Qwen"
XAI_ORG = "xAI"
ZAI_ORG = "Z.ai"

ORG_TO_LOGO = {
    BENCHMARK_NAME: "fri.png",
    ANTHROPIC_ORG: "anthropic.svg",
    DEEPSEEK_ORG: "deepseek.svg",
    MOONSHOT_ORG: "moonshot.svg",
    GOOGLE_ORG: "deepmind.svg",
    META_ORG: "meta.svg",
    MISTRAL_ORG: "mistral.svg",
    MISTRAL_ORG_1: "mistral.svg",
    OAI_ORG: "openai.svg",
    QWEN_ORG: "qwen.svg",
    XAI_ORG: "xai.svg",
    ZAI_ORG: "zai.svg",
}

ZERO_SHOT_AND_SCRATCHPAD_MODELS = {
    # oai context window from: https://platform.openai.com/docs/models/
    "gpt-5-2025-08-07": {
        "source": OAI_SOURCE,
        "org": OAI_ORG,
        "full_name": "gpt-5-2025-08-07",
        "token_limit": 128000,
        # `reasoning_model` is OpenAI specific. It should be true for o1 and o3 class models.
        # See model_eval.get_response_from_oai_model() for use.
        "reasoning_model": True,
        # use_web_search is OpenAI specific. It sets web_search_preview as a tool for the model.
        "use_web_search": False,
    },
    "gpt-5-mini-2025-08-07": {
        "source": OAI_SOURCE,
        "org": OAI_ORG,
        "full_name": "gpt-5-mini-2025-08-07",
        "token_limit": 128000,
        # `reasoning_model` is OpenAI specific. It should be true for o1 and o3 class models.
        # See model_eval.get_response_from_oai_model() for use.
        "reasoning_model": True,
        # use_web_search is OpenAI specific. It sets web_search_preview as a tool for the model.
        "use_web_search": False,
    },
    "gpt-5-nano-2025-08-07": {
        "source": OAI_SOURCE,
        "org": OAI_ORG,
        "full_name": "gpt-5-nano-2025-08-07",
        "token_limit": 128000,
        # `reasoning_model` is OpenAI specific. It should be true for o1 and o3 class models.
        # See model_eval.get_response_from_oai_model() for use.
        "reasoning_model": True,
        # use_web_search is OpenAI specific. It sets web_search_preview as a tool for the model.
        "use_web_search": False,
    },
    "gpt-4.1-2025-04-14": {
        "source": OAI_SOURCE,
        "org": OAI_ORG,
        "full_name": "gpt-4.1-2025-04-14",
        "token_limit": 128000,
        # `reasoning_model` is OpenAI specific. It should be true for o1 and o3 class models.
        # See model_eval.get_response_from_oai_model() for use.
        "reasoning_model": False,
        # use_web_search is OpenAI specific. It sets web_search_preview as a tool for the model.
        "use_web_search": False,
    },
    # together.ai context window from: https://docs.together.ai/docs/serverless-models
    "Qwen3-235B-A22B-fp8-tput": {
        "source": TOGETHER_AI_SOURCE,
        "org": QWEN_ORG,
        "full_name": "Qwen/Qwen3-235B-A22B-fp8-tput",
        "token_limit": 128000,
    },
    "Kimi-K2-Instruct": {
        "source": TOGETHER_AI_SOURCE,
        "org": MOONSHOT_ORG,
        "full_name": "moonshotai/Kimi-K2-Instruct",
        "token_limit": 128000,
    },
    "GLM-4.5-Air-FP8": {
        "source": TOGETHER_AI_SOURCE,
        "org": ZAI_ORG,
        "full_name": "zai-org/GLM-4.5-Air-FP8",
        "token_limit": 131072,
    },
    # Mistral
    "mistral-large-2411": {
        "source": MISTRAL_SOURCE,
        "org": MISTRAL_ORG,
        "full_name": "mistral-large-2411",
        "token_limit": 128000,
    },
    "magistral-medium-2506": {
        "source": MISTRAL_SOURCE,
        "org": MISTRAL_ORG,
        "full_name": "magistral-medium-2506",
        "token_limit": 40000,
    },
    # anthropic context window from: https://docs.anthropic.com/en/docs/about-claude/models
    "claude-3-5-sonnet-20240620": {
        "source": ANTHROPIC_SOURCE,
        "org": ANTHROPIC_ORG,
        "full_name": "claude-3-5-sonnet-20240620",
        "token_limit": 200000,
    },
    "claude-sonnet-4-20250514": {
        "source": ANTHROPIC_SOURCE,
        "org": ANTHROPIC_ORG,
        "full_name": "claude-sonnet-4-20250514",
        "token_limit": 200000,
    },
    "claude-opus-4-1-20250805": {
        "source": ANTHROPIC_SOURCE,
        "org": ANTHROPIC_ORG,
        "full_name": "claude-opus-4-1-20250805",
        "token_limit": 200000,
    },
    # xAI context window from: https://console.x.ai/ -> click on API Models (cube symbol on menu)
    "grok-4-0709": {
        "source": XAI_SOURCE,
        "org": XAI_ORG,
        "full_name": "grok-4-0709",
        "token_limit": 256000,
    },
    "grok-3-mini": {
        "source": XAI_SOURCE,
        "org": XAI_ORG,
        "full_name": "grok-3-mini",
        "token_limit": 131072,
    },
    # google context window from: https://ai.google.dev/gemini-api/docs/models
    "gemini-2.5-pro": {
        "source": GOOGLE_SOURCE,
        "org": GOOGLE_ORG,
        "full_name": "gemini-2.5-pro",
        "token_limit": 1048576,
    },
    "gemini-2.5-flash": {
        "source": GOOGLE_SOURCE,
        "org": GOOGLE_ORG,
        "full_name": "models/gemini-2.5-flash",
        "token_limit": 1048576,
    },
}

MODEL_TOKEN_LIMITS = dict()
MODEL_NAME_TO_ORG = dict()
MODEL_NAME_TO_SOURCE = dict()
ZERO_SHOT_AND_SCRATCHPAD_MODELS_BY_SOURCE = defaultdict(dict)
for key, value in ZERO_SHOT_AND_SCRATCHPAD_MODELS.items():
    MODEL_TOKEN_LIMITS[value["full_name"]] = value["token_limit"]
    MODEL_NAME_TO_SOURCE[value["full_name"]] = value["source"]
    MODEL_NAME_TO_ORG[value["full_name"]] = value["org"]
    MODEL_NAME_TO_ORG[key] = value["org"]
    ZERO_SHOT_AND_SCRATCHPAD_MODELS_BY_SOURCE[value["source"]][key] = value

# "gpt-4o-mini" Model used by metadata functions in question_curation.METADATA_MODEL_NAME
MODEL_TOKEN_LIMITS["gpt-4o-mini"] = 128000
MODEL_NAME_TO_ORG["gpt-4o-mini"] = OAI_ORG
MODEL_NAME_TO_SOURCE["gpt-4o-mini"] = OAI_SOURCE

# remove models with less than ~17000 token limits
SUPERFORECASTER_WITH_NEWS_MODELS = SCRATCHPAD_WITH_NEWS_MODELS = {
    "gpt_4_turbo_0409": {"source": OAI_SOURCE, "full_name": "gpt-4-turbo-2024-04-09"},
    "gpt_4o": {"source": OAI_SOURCE, "full_name": "gpt-4o"},
    "mistral_8x7b_instruct": {
        "source": TOGETHER_AI_SOURCE,
        "full_name": "mistralai/Mixtral-8x7B-Instruct-v0.1",
    },
    "mistral_8x22b_instruct": {
        "source": TOGETHER_AI_SOURCE,
        "full_name": "mistralai/Mixtral-8x22B-Instruct-v0.1",
    },
    "mistral_large": {
        "source": TOGETHER_AI_SOURCE,
        "full_name": "mistral-large-latest",
    },
    "qwen_1p5_110b": {
        "source": TOGETHER_AI_SOURCE,
        "full_name": "Qwen/Qwen1.5-110B-Chat",
    },
    "claude_2p1": {"source": ANTHROPIC_SOURCE, "full_name": "claude-2.1"},
    "claude_3_opus": {"source": ANTHROPIC_SOURCE, "full_name": "claude-3-opus-20240229"},
    "claude_3_haiku": {"source": ANTHROPIC_SOURCE, "full_name": "claude-3-haiku-20240307"},
    "claude_3p5_sonnet": {"source": ANTHROPIC_SOURCE, "full_name": "claude-3-5-sonnet-20240620"},
    "gemini_1p5_flash": {"source": GOOGLE_SOURCE, "full_name": "gemini-1.5-flash"},
    "gemini_1p5_pro": {"source": GOOGLE_SOURCE, "full_name": "gemini-1.5-pro"},
}
