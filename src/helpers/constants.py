"""Constants."""

from datetime import datetime

BENCHMARK_NAME = "ForecastBench"

BENCHMARK_START_YEAR = 2024
BENCHMARK_START_MONTH = 5
BENCHMARK_START_DAY = 1
BENCHMARK_START_DATE = f"{BENCHMARK_START_YEAR}-{BENCHMARK_START_MONTH}-{BENCHMARK_START_DAY}"

parsed_date = datetime.strptime(BENCHMARK_START_DATE + " 00:00", "%Y-%m-%d %H:%M")
BENCHMARK_START_DATE_EPOCHTIME = int(parsed_date.timestamp())
BENCHMARK_START_DATE_EPOCHTIME_MS = BENCHMARK_START_DATE_EPOCHTIME * 1000

OAI_SOURCE = "OAI"
ANTHROPIC_SOURCE = "ANTHROPIC"
TOGETHER_AI_SOURCE = "TOGETHER"
GOOGLE_SOURCE = "GOOGLE"
MISTRAL_SOURCE = "MISTRAL"

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


MODEL_TOKEN_LIMITS = {
    "claude-2.1": 200000,
    "claude-2": 100000,
    "claude-3-opus-20240229": 200000,
    "claude-3-sonnet-20240229": 200000,
    "claude-3-haiku-20240307": 200000,
    "claude-3-5-sonnet-20240620": 200000,
    "gpt-3.5-turbo-0125": 16385,
    "gpt_4": 8192,
    "gpt-4-turbo-2024-04-09": 128000,
    "gpt-4-1106-preview": 128000,
    "gpt-4-0125-preview": 128000,
    "gpt-4o": 128000,
    "gemini-pro": 30720,
    "meta-llama/Llama-2-7b-chat-hf": 4096,
    "meta-llama/Llama-2-13b-chat-hf": 4096,
    "meta-llama/Llama-2-70b-chat-hf": 4096,
    "meta-llama/Llama-3-8b-chat-hf": 8000,
    "meta-llama/Llama-3-70b-chat-hf": 8000,
    "mistralai/Mistral-7B-Instruct-v0.2": 32768,
    "mistralai/Mixtral-8x7B-Instruct-v0.1": 32768,
    "mistralai/Mixtral-8x22B-Instruct-v0.1": 65536,
    "mistral-large-latest": 32000,
    "NousResearch/Nous-Hermes-2-Mixtral-8x7B-DPO": 32768,
    "Qwen/Qwen1.5-110B-Chat": 32768,
    "gemini-1.5-flash": 8000,
    "gemini-1.5-pro": 8000,
}

MODEL_NAME_TO_SOURCE = {
    "claude-2.1": ANTHROPIC_SOURCE,
    "claude-2": ANTHROPIC_SOURCE,
    "claude-3-opus-20240229": ANTHROPIC_SOURCE,
    "claude-3-sonnet-20240229": ANTHROPIC_SOURCE,
    "claude-3-haiku-20240307": ANTHROPIC_SOURCE,
    "claude-3-5-sonnet-20240620": ANTHROPIC_SOURCE,
    "gpt-4": OAI_SOURCE,
    "gpt-3.5-turbo-0125": OAI_SOURCE,
    "gpt-4-turbo-2024-04-09": OAI_SOURCE,
    "gpt-4-1106-preview": OAI_SOURCE,
    "gpt-4-0125-preview": OAI_SOURCE,
    "gpt-4o": OAI_SOURCE,
    "gemini-pro": GOOGLE_SOURCE,
    "gemini-1.5-flash": GOOGLE_SOURCE,
    "gemini-1.5-pro": GOOGLE_SOURCE,
    "meta-llama/Llama-2-7b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-2-13b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-2-70b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-3-8b-chat-hf": TOGETHER_AI_SOURCE,
    "meta-llama/Llama-3-70b-chat-hf": TOGETHER_AI_SOURCE,
    "mistralai/Mistral-7B-Instruct-v0.2": TOGETHER_AI_SOURCE,
    "mistralai/Mixtral-8x7B-Instruct-v0.1": TOGETHER_AI_SOURCE,
    "mistralai/Mixtral-8x22B-Instruct-v0.1": TOGETHER_AI_SOURCE,
    "mistral-large-latest": MISTRAL_SOURCE,
    "NousResearch/Nous-Hermes-2-Mixtral-8x7B-DPO": TOGETHER_AI_SOURCE,
    "Qwen/Qwen1.5-110B-Chat": TOGETHER_AI_SOURCE,
    "Qwen/Qwen1.5-110B-Chat": TOGETHER_AI_SOURCE,
}


ZERO_SHOT_AND_SCRATCHPAD_MODELS = {
    "gpt_3p5_turbo_0125": {"source": "OAI", "full_name": "gpt-3.5-turbo-0125"},
    # "gpt_4": {"source": "OAI", "full_name": "gpt-4"},
    # "gpt_4_turbo_0409": {"source": "OAI", "full_name": "gpt-4-turbo-2024-04-09"},
    # "gpt_4_1106_preview": {"source": "OAI", "full_name": "gpt-4-1106-preview"},
    # "gpt_4_0125_preview": {"source": "OAI", "full_name": "gpt-4-0125-preview"},
    # "gpt_4o": {"source": "OAI", "full_name": "gpt-4o"},
    # "llama_2_70b": {
    #     "source": "TOGETHER",
    #     "full_name": "meta-llama/Llama-2-70b-chat-hf",
    # },
    # "llama_3_8b": {
    #     "source": "TOGETHER",
    #     "full_name": "meta-llama/Llama-3-8b-chat-hf",
    # },
    # "llama_3_70b": {
    #     "source": "TOGETHER",
    #     "full_name": "meta-llama/Llama-3-70b-chat-hf",
    # },
    # "mistral_8x7b_instruct": {
    #     "source": "TOGETHER",
    #     "full_name": "mistralai/Mixtral-8x7B-Instruct-v0.1",
    # },
    # "mistral_8x22b_instruct": {
    #     "source": "TOGETHER",
    #     "full_name": "mistralai/Mixtral-8x22B-Instruct-v0.1",
    # },
    # "mistral_large": {
    #     "source": "MISTRAL",
    #     "full_name": "mistral-large-latest",
    # },
    # "qwen_1p5_110b": {
    #     "source": "TOGETHER",
    #     "full_name": "Qwen/Qwen1.5-110B-Chat",
    # },
    # "claude_2p1": {"source": "ANTHROPIC", "full_name": "claude-2.1"},
    # "claude_3_opus": {"source": "ANTHROPIC", "full_name": "claude-3-opus-20240229"},
    # "claude_3_haiku": {"source": "ANTHROPIC", "full_name": "claude-3-haiku-20240307"},
    "claude_3p5_sonnet": {"source": "ANTHROPIC", "full_name": "claude-3-5-sonnet-20240620"},
    # "gemini_1p5_flash": {"source": "GOOGLE", "full_name": "gemini-1.5-flash"},
    "gemini_1p5_pro": {"source": "GOOGLE", "full_name": "gemini-1.5-pro"},
}
