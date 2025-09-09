"""LLM-related util."""

import asyncio
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

import anthropic
import openai
import together
from google import genai
from google.genai import types
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage

from . import (
    constants,
    data_utils,
    env,
    keys,
    llm_crowd_prompts,
    llm_prompts,
    question_curation,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))  # noqa: E402
from utils import gcp  # noqa: E402

anthropic_console = anthropic.Anthropic(api_key=keys.API_KEY_ANTHROPIC)
anthropic_async_client = anthropic.AsyncAnthropic(api_key=keys.API_KEY_ANTHROPIC)
oai_async_client = openai.AsyncOpenAI(api_key=keys.API_KEY_OPENAI)
oai = openai.OpenAI(api_key=keys.API_KEY_OPENAI)
together.api_key = keys.API_KEY_TOGETHERAI
google_ai_client = genai.Client(api_key=keys.API_KEY_GOOGLE)
togetherai_client = openai.OpenAI(
    api_key=keys.API_KEY_TOGETHERAI,
    base_url="https://api.together.xyz/v1",
)
xai_client = openai.OpenAI(
    api_key=keys.API_KEY_XAI,
    base_url="https://api.x.ai/v1",
)
mistral_client = MistralClient(api_key=keys.API_KEY_MISTRAL)
HUMAN_JOINT_PROMPTS = [
    llm_prompts.HUMAN_JOINT_PROMPT_1,
    llm_prompts.HUMAN_JOINT_PROMPT_2,
    llm_prompts.HUMAN_JOINT_PROMPT_3,
    llm_prompts.HUMAN_JOINT_PROMPT_4,
]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
TODAY_DATE = datetime.today().strftime("%Y-%m-%d")


def infer_model_source(model_name):
    """
    Infer the model source from the model name.

    Args:
    - model_name (str): The name of the model.
    """
    if "ft:gpt" in model_name:  # fine-tuned GPT-3 or 4
        return constants.OAI_SOURCE
    if model_name not in constants.MODEL_NAME_TO_SOURCE:
        raise ValueError(f"Invalid model name: {model_name}")
    return constants.MODEL_NAME_TO_SOURCE[model_name]


def get_model_org(model_name):
    """
    Get the model org given the model.

    Args:
    - model_name (str): The name of the model.
    """
    if model_name not in constants.MODEL_NAME_TO_ORG:
        raise ValueError(f"Invalid model name: {model_name}")
    return constants.MODEL_NAME_TO_ORG[model_name]


def get_response_with_retry(api_call, wait_time, error_msg):
    """
    Make an API call and retry on failure after a specified wait time.

    Args:
        api_call (function): API call to make.
        wait_time (int): Time to wait before retrying, in seconds.
        error_msg (str): Error message to print on failure.
    """
    while True:
        try:
            return api_call()
        except Exception as e:
            if "repetitive patterns" in str(e):
                logger.info(
                    "Repetitive patterns detected in the prompt. Modifying prompt and retrying..."
                )
                return "need_a_new_reformat_prompt"

            logger.info(f"{error_msg}: {e}")
            logger.info(f"Waiting for {wait_time} seconds before retrying...")

            time.sleep(wait_time)


def get_response_from_oai_model(
    model_name, prompt, system_prompt, max_tokens, temperature, wait_time
):
    """
    Make an API call to the OpenAI API and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "gpt-4").
        prompt (str): Fully specififed prompt to use for the API call.
        system_prompt (str): Prompt to use for system prompt.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        """
        Make an API call to the OpenAI API, without retrying on failure.

        Returns:
            str: Response string from the API call.
        """

        def get_bool_param_from_model_def(p):
            return constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS.get(model_name, {}).get(p, False)

        is_reasoning_model = get_bool_param_from_model_def("reasoning_model")
        use_web_search = get_bool_param_from_model_def("use_web_search")

        if is_reasoning_model and system_prompt:
            print(system_prompt)
            logger.error("OpenAI reasoning models do NOT support system prompts.")
            sys.exit(1)

        model_input = [{"role": "system", "content": system_prompt}] if system_prompt else []
        model_input.append({"role": "user", "content": prompt})

        params = {
            "model": model_name,
        }
        if use_web_search:
            params["input"] = prompt
            params["tools"] = [{"type": "web_search_preview"}]
            params["tool_choice"] = {"type": "web_search_preview"}
            if not is_reasoning_model:
                params["temperature"] = temperature

            response = oai.responses.create(**params)
            return response.output_text
        else:
            params["messages"] = model_input
            if not is_reasoning_model:
                params["temperature"] = temperature
                params["max_tokens"] = max_tokens

            response = oai.chat.completions.create(**params)
            return response.choices[0].message.content

    return get_response_with_retry(api_call, wait_time, "OpenAI API request exceeded rate limit.")


def get_response_from_xai_model(model_name, prompt, max_tokens, temperature, wait_time):
    """
    Make an API call to the xAI API and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "claude-2").
        prompt (str): Fully specififed prompt to use for the API call.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        response = xai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response.choices[0].message.content

    return get_response_with_retry(api_call, wait_time, "xAI API request exceeded rate limit.")


def get_response_from_anthropic_model(model_name, prompt, max_tokens, temperature, wait_time):
    """
    Make an API call to the Anthropic API and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "claude-2").
        prompt (str): Fully specififed prompt to use for the API call.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        completion = anthropic_console.messages.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=1024,
        )
        return completion.content[0].text

    return get_response_with_retry(
        api_call, wait_time, "Anthropic API request exceeded rate limit."
    )


def get_response_from_mistral_model(model_name, prompt, max_tokens, temperature, wait_time):
    """
    Make an API call to the OpenAI API and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "gpt-4").
        prompt (str): Fully specififed prompt to use for the API call.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        """
        Make an API call to the OpenAI API, without retrying on failure.

        Returns:
            str: Response string from the API call.
        """
        messages = [ChatMessage(role="user", content=prompt)]

        # No streaming
        chat_response = mistral_client.chat(
            model=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        return chat_response.choices[0].message.content

    return get_response_with_retry(api_call, wait_time, "Mistral API request exceeded rate limit.")


def get_response_from_together_ai_model(model_name, prompt, max_tokens, temperature, wait_time):
    """
    Make an API call to the Together AI API and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "togethercomputer/
        llama-2-13b-chat").
        prompt (str): Fully specififed prompt to use for the API call.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        nonlocal max_tokens  # Allow modification of max_tokens

        # Get the token limit for this model
        model_token_limit = constants.MODEL_TOKEN_LIMITS.get(model_name)

        try:
            chat_completion = togetherai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "user", "content": prompt},
                ],
                temperature=temperature,
            )
            response = chat_completion.choices[0].message.content
            return response
        except Exception as e:
            error_message = str(e)
            if "Input validation error" in error_message:
                # Extract the number of input tokens from the error message
                match = re.search(r"Given: (\d+) `inputs` tokens", error_message)
                if match:
                    input_tokens = int(match.group(1))
                    # Adjust max_tokens based on the model's limit
                    max_tokens = model_token_limit - input_tokens - 50  # Subtracting 50 for safety
                    logger.info(f"Adjusted max_tokens to {max_tokens}")
                    if max_tokens <= 0:
                        raise ValueError(
                            f"Prompt is too long for model {model_name}. It uses {input_tokens} tokens, "
                            f"which exceeds or equals the model's limit of {model_token_limit} tokens."
                        )
                    # Retry the API call with adjusted max_tokens
                    return api_call()
            # If it's not the token limit error or we couldn't parse it, re-raise
            raise

    return get_response_with_retry(
        api_call, wait_time, "Together AI API request exceeded rate limit."
    )


def get_response_from_google_model(model_name, prompt, max_tokens, temperature, wait_time):
    """
    Make an API call to the Together AI API and retry on failure after a specified wait time.

    Args:
        model (str): Name of the model to use (such as "gemini-pro").
        prompt (str): Initial prompt for the API call.
        max_tokens (int): Maximum number of tokens to sample.
        temperature (float): Sampling temperature.
        wait_time (int): Time to wait before retrying, in seconds.

    Returns:
        str: Response string from the API call.
    """

    def api_call():
        response = google_ai_client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                candidate_count=1,
                temperature=temperature,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(
                    disable=True,
                ),
            ),
        )
        return response.text

    return get_response_with_retry(
        api_call, wait_time, "Google AI API request exceeded rate limit."
    )


def get_response_from_model(
    model_name,
    prompt,
    system_prompt="",
    max_tokens=2000,
    temperature=0.8,
    wait_time=30,
):
    """
    Make an API call to the specified model and retry on failure after a specified wait time.

    Args:
        model_name (str): Name of the model to use (such as "gpt-4").
        prompt (str): Fully specififed prompt to use for the API call.
        system_prompt (str, optional): Prompt to use for system prompt.
        max_tokens (int, optional): Maximum number of tokens to generate.
        temperature (float, optional): Sampling temperature.
        wait_time (int, optional): Time to wait before retrying, in seconds.
    """
    model_source = infer_model_source(model_name)
    if model_source == constants.OAI_SOURCE:
        return get_response_from_oai_model(
            model_name, prompt, system_prompt, max_tokens, temperature, wait_time
        )
    elif model_source == constants.ANTHROPIC_SOURCE:
        return get_response_from_anthropic_model(
            model_name, prompt, max_tokens, temperature, wait_time
        )
    elif model_source == constants.TOGETHER_AI_SOURCE:
        return get_response_from_together_ai_model(
            model_name, prompt, max_tokens, temperature, wait_time
        )
    elif model_source == constants.GOOGLE_SOURCE:
        return get_response_from_google_model(
            model_name, prompt, max_tokens, temperature, wait_time
        )
    elif model_source == constants.MISTRAL_SOURCE:
        return get_response_from_mistral_model(
            model_name, prompt, max_tokens, temperature, wait_time
        )
    elif model_source == constants.XAI_SOURCE:
        return get_response_from_xai_model(model_name, prompt, max_tokens, temperature, wait_time)
    else:
        return "Not a valid model source."


async def get_async_response(
    prompt,
    model_name="gpt-4o-mini",
    temperature=0.0,
    max_tokens=8000,
):
    """
    Asynchronously get a response from the OpenAI API.

    Args:
        prompt (str): Fully specififed prompt to use for the API call.
        model_name (str, optional): Name of the model to use (such as "gpt-3.5-turbo").
        temperature (float, optional): Sampling temperature.
        max_tokens (int, optional): Maximum number of tokens to sample.

    Returns:
        str: Response string from the API call (not the dictionary).
    """
    model_source = infer_model_source(model_name)
    while True:
        try:
            if model_source == constants.OAI_SOURCE:
                response = await oai_async_client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                )
                return response.choices[0].message.content
            elif model_source == constants.ANTHROPIC_SOURCE:
                response = await anthropic_async_client.messages.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return response.content[0].text
            elif model_source == constants.GOOGLE_SOURCE:
                response = await google_ai_client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        candidate_count=1,
                        temperature=temperature,
                    ),
                )
                return response.text
            elif model_source == constants.TOGETHER_AI_SOURCE:
                chat_completion = await asyncio.to_thread(
                    togetherai_client.chat.completions.create,
                    model=model_name,
                    messages=[
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return chat_completion.choices[0].message.content
            else:
                logger.debug("Not a valid model source: {model_source}")
                return ""
        except Exception as e:
            logger.info(f"Exception, erorr message: {e}")
            logger.info("Waiting for 30 seconds before retrying...")
            time.sleep(30)
            continue


def extract_probability(text):
    """
    Extract a probability value from the given text.

    Search through the input text for numeric patterns that could represent probabilities.
    The search checks for plain numbers, percentages, or numbers flanked by asterisks and
    attempts to convert these into a probability value (a float between 0 and 1).
    Ignore exact values of 0.0 and 1.0.

    Args:
        text (str): The text from which to extract the probability.

    Returns:
        float | None: The first valid probability found in the text, or None if no valid
        probability is detected.
    """
    if text is None:
        return None

    pattern = r"(?:\*\s*)?(\d*\.?\d+)%?(?:\s*\*)?"

    matches = re.findall(pattern, text)

    for match in reversed(matches):
        number = float(match)

        surrounding_text = text[max(0, text.find(match) - 1) : text.find(match) + len(match) + 1]
        if "%" in surrounding_text:
            number /= 100

        if 0 <= number <= 1:
            if number == 1.0 or number == 0.0:
                continue  # Skip this match and continue to the next one
            return number

    return None


def convert_string_to_list(string_list):
    """
    Convert a formatted string into a list of floats.

    Strip leading and trailing whitespace from the input string, remove square brackets,
    split the string by commas, and convert each element to a float. Replace non-numeric
    entries (denoted by '*') with 0.5.

    Parameters:
    string_list (str): A string representation of a list of numbers, enclosed in square
                       brackets and separated by commas.

    Returns:
    list: A list of floats, where non-numeric elements are replaced with 0.5.
    """
    # Remove leading and trailing whitespace
    string_list = string_list.strip()
    # Remove square brackets at the beginning and end
    string_list = string_list[1:-1]
    # Split the string by commas and convert each element to a float
    list_values = string_list.split(",")

    actual_list = [
        (
            0.5
            if not re.match(r"^\d*\.\d+$", value.strip().replace("*", ""))
            else float(value.strip().replace("*", ""))
        )
        for value in list_values
    ]

    return actual_list


def reformat_answers(response, prompt="N/A", question="N/A", single=False):
    """
    Reformat the given response based on whether a single response or multiple responses are required.

    This function adjusts the response formatting by using predefined prompt templates and sends
    it to a model for evaluation. Depending on the 'single' flag, it either extracts a probability
    or converts the response to a list.

    Parameters:
    - response (str): The original response from the model that needs to be reformatted.
    - prompt (str, optional): The user prompt to use in the reformatting process. Defaults to 'N/A'.
    - question (str or dict, optional): The question data used to format the response when not single.
      Defaults to 'N/A'.
    - single (bool, optional): Flag to determine if the response should be handled as a single response.
      Defaults to False.

    Returns:
    - str or list: The reformatted model response, either as a probability (if single is True) or as a
      list of responses (if single is False).
    """

    def reformatted_raw_response(
        response, prompt, question, REFORMAT_SINGLE_PROMPT, REFORMAT_PROMPT, single=False
    ):
        if single:
            reformat_prompt = REFORMAT_SINGLE_PROMPT.format(response=response)
        else:
            reformat_prompt = REFORMAT_PROMPT.format(
                user_prompt=prompt,
                model_response=response,
                n_horizons=len(question["resolution_dates"]),
            )
        raw_response = get_response_from_model(
            prompt=reformat_prompt,
            max_tokens=100,
            model_name="gpt-4o-mini",
            temperature=0,
            wait_time=30,
        )
        return raw_response

    raw_response = reformatted_raw_response(
        response,
        prompt,
        question,
        llm_prompts.REFORMAT_SINGLE_PROMPT,
        llm_prompts.REFORMAT_PROMPT,
        single,
    )
    if raw_response == "need_a_new_reformat_prompt":
        raw_response = reformatted_raw_response(
            response,
            prompt,
            question,
            llm_prompts.REFORMAT_SINGLE_PROMPT_2,
            llm_prompts.REFORMAT_PROMPT_2,
            single,
        )

    if single:
        return extract_probability(raw_response)

    return convert_string_to_list(raw_response)


def capitalize_substrings(model_name):
    """
    Capitalize the first letter of each substring in a model name.

    Args:
        model_name (str): The model name to be capitalized.

    Returns:
        str: The capitalized model name.
    """
    model_name = model_name.replace("gpt", "GPT") if "gpt" in model_name else model_name
    substrings = model_name.split("-")
    capitalized_substrings = [
        substr[0].upper() + substr[1:] if substr and not substr[0].isdigit() else substr
        for substr in substrings
    ]
    return "-".join(capitalized_substrings)


def generate_final_forecast_files(forecast_due_date, prompt_type, models, test_or_prod):
    """
    Generate final forecast files for given models, merging individual forecasts into final files.

    Args:
        forecast_due_date (str): The forecast_due_date for the forecast.
        prompt_type (str): The type of prompt used.
        models (dict): A dictionary of models with their information.

    Returns:
        None
    """
    models_to_test = list(models.keys())

    def get_final_dir(with_freeze_values):
        return "final_with_freeze" if with_freeze_values else "final"

    def write_file(model, with_freeze_values, test_or_prod):
        current_model_forecasts = []
        if with_freeze_values:
            dirs = [
                f"{prompt_type}/non_market",
                f"{prompt_type}/market/with_freeze_values",
                f"{prompt_type}/combo_non_market",
                f"{prompt_type}/combo_market/with_freeze_values",
            ]
        else:
            dirs = [
                f"{prompt_type}/non_market",
                f"{prompt_type}/market",
                f"{prompt_type}/combo_non_market",
                f"{prompt_type}/combo_market",
            ]

        if test_or_prod == "TEST":
            dirs = [dir_ + "_test" for dir_ in dirs]

        for test_type in dirs:
            file_path = f"/tmp/{test_type}/{model}.jsonl"
            questions = data_utils.read_jsonl(file_path)
            current_model_forecasts.extend(questions)

        final_dir = get_final_dir(with_freeze_values)
        if test_or_prod == "TEST":
            final_dir += "_test"

        final_file_name = f"/tmp/{prompt_type}/{final_dir}/{model}"
        os.makedirs(os.path.dirname(final_file_name), exist_ok=True)
        with open(final_file_name, "w") as file:
            for entry in current_model_forecasts:
                json_line = json.dumps(entry)
                file.write(json_line + "\n")

    def create_final_file(model, with_freeze_values, test_or_prod):
        final_dir = get_final_dir(with_freeze_values)
        if test_or_prod == "TEST":
            final_dir += "_test"
        file_path = f"/tmp/{prompt_type}/{final_dir}/{model}"
        questions = data_utils.read_jsonl(file_path)
        org = get_model_org(model)

        directory = f"/tmp/{prompt_type}/final_submit"
        if test_or_prod == "TEST":
            directory += "_test"
        os.makedirs(directory, exist_ok=True)

        file_prompt_type = prompt_type
        if with_freeze_values:
            file_prompt_type += "_with_freeze_values"

        # Only possible for some OpenAI models
        if models[model].get("use_web_search", False):
            file_prompt_type += "_with_web_search"

        new_file_name = f"{directory}/{forecast_due_date}.{org}.{model}_{file_prompt_type}.json"
        if test_or_prod == "TEST":
            new_file_name = (
                f"{directory}/{constants.TEST_FORECAST_FILE_PREFIX}.{forecast_due_date}."
                f"{org}.{model}_{file_prompt_type}.json"
            )

        model_name = (
            models[model]["full_name"]
            if "/" not in models[model]["full_name"]
            else models[model]["full_name"].split("/")[1]
        )

        forecast_file = {
            "organization": constants.BENCHMARK_NAME,
            "model": f"{capitalize_substrings(model_name)} ({file_prompt_type.replace('_', ' ')})",
            "model_organization": org,
            "question_set": f"{forecast_due_date}-llm.json",
            "forecast_due_date": forecast_due_date,
            "forecasts": questions,
        }

        with open(new_file_name, "w") as f:
            json.dump(forecast_file, f, indent=4)

    for model in models_to_test:
        write_file(model=model, with_freeze_values=True, test_or_prod=test_or_prod)
        create_final_file(model=model, with_freeze_values=True, test_or_prod=test_or_prod)
        write_file(model=model, with_freeze_values=False, test_or_prod=test_or_prod)
        create_final_file(model=model, with_freeze_values=False, test_or_prod=test_or_prod)


def worker(
    index,
    n_questions,
    model_name,
    save_dict,
    questions_to_eval,
    forecast_due_date,
    prompt_type="zero_shot",
    rate_limit=False,
    market_use_freeze_value=False,
):
    """Worker function for question evaluation."""
    if save_dict[index] != "":
        return

    logger.info(f"Starting {model_name} - {index + 1}/{n_questions}")

    if rate_limit:
        start_time = datetime.now()

    question = questions_to_eval[index]
    is_market_question = question["source"] in question_curation.MARKET_SOURCES
    is_joint_question = question["combination_of"] != "N/A"

    question_type = determine_type(is_market_question, is_joint_question, market_use_freeze_value)

    if not is_market_question and market_use_freeze_value:
        # Don't run for data source questions when market_use_freeze_value is True
        # because we will have already run these requests when it was False.
        return

    if is_market_question:
        if is_joint_question:
            if market_use_freeze_value:  # we don't run superforecaster prompts with freeze values
                prompt = (
                    llm_prompts.ZERO_SHOT_MARKET_JOINT_QUESTION_WITH_FREEZE_VALUE_PROMPT
                    if prompt_type == "zero_shot"
                    else (
                        llm_prompts.SCRATCH_PAD_MARKET_JOINT_QUESTION_WITH_FREEZE_VALUE_PROMPT
                        if prompt_type == "scratchpad"
                        else llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_MARKET_JOINT_QUESTION_WITH_FREEZE_VALUE_PROMPT  # noqa: B950
                    )
                )
            else:
                prompt = (
                    llm_prompts.ZERO_SHOT_MARKET_JOINT_QUESTION_PROMPT
                    if prompt_type == "zero_shot"
                    else (
                        llm_prompts.SCRATCH_PAD_MARKET_JOINT_QUESTION_PROMPT
                        if prompt_type == "scratchpad"
                        else (
                            llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_MARKET_JOINT_QUESTION_PROMPT
                            if prompt_type == "scratchpad_with_news"
                            else (
                                llm_crowd_prompts.SUPERFORECASTER_MARKET_JOINT_QUESTION_PROMPT_1
                                if prompt_type == "superforecaster_with_news_1"
                                else (
                                    llm_crowd_prompts.SUPERFORECASTER_MARKET_JOINT_QUESTION_PROMPT_2
                                    if prompt_type == "superforecaster_with_news_2"
                                    else (
                                        llm_crowd_prompts.SUPERFORECASTER_MARKET_JOINT_QUESTION_PROMPT_3
                                        if prompt_type == "superforecaster_with_news_3"
                                        else None
                                    )
                                )
                            )
                        )
                    )
                )
        else:
            if market_use_freeze_value:  # we don't run superforecaster prompts with freeze values
                prompt = (
                    llm_prompts.ZERO_SHOT_MARKET_WITH_FREEZE_VALUE_PROMPT
                    if prompt_type == "zero_shot"
                    else (
                        llm_prompts.SCRATCH_PAD_MARKET_WITH_FREEZE_VALUE_PROMPT
                        if prompt_type == "scratchpad"
                        else llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_MARKET_WITH_FREEZE_VALUE_PROMPT
                    )
                )
            else:
                prompt = (
                    llm_prompts.ZERO_SHOT_MARKET_PROMPT
                    if prompt_type == "zero_shot"
                    else (
                        llm_prompts.SCRATCH_PAD_MARKET_PROMPT
                        if prompt_type == "scratchpad"
                        else (
                            llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_MARKET_PROMPT
                            if prompt_type == "scratchpad_with_news"
                            else (
                                llm_crowd_prompts.SUPERFORECASTER_MARKET_PROMPT_1
                                if prompt_type == "superforecaster_with_news_1"
                                else (
                                    llm_crowd_prompts.SUPERFORECASTER_MARKET_PROMPT_2
                                    if prompt_type == "superforecaster_with_news_2"
                                    else (
                                        llm_crowd_prompts.SUPERFORECASTER_MARKET_PROMPT_3
                                        if prompt_type == "superforecaster_with_news_3"
                                        else None
                                    )
                                )
                            )
                        )
                    )
                )
    else:
        if is_joint_question:
            prompt = (
                llm_prompts.ZERO_SHOT_NON_MARKET_JOINT_QUESTION_PROMPT
                if prompt_type == "zero_shot"
                else (
                    llm_prompts.SCRATCH_PAD_NON_MARKET_JOINT_QUESTION_PROMPT
                    if prompt_type == "scratchpad"
                    else (
                        llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_NON_MARKET_JOINT_QUESTION_PROMPT
                        if prompt_type == "scratchpad_with_news"
                        else (
                            llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_JOINT_QUESTION_PROMPT_1
                            if prompt_type == "superforecaster_with_news_1"
                            else (
                                llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_JOINT_QUESTION_PROMPT_2
                                if prompt_type == "superforecaster_with_news_2"
                                else (
                                    llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_JOINT_QUESTION_PROMPT_3
                                    if prompt_type == "superforecaster_with_news_3"
                                    else None
                                )
                            )
                        )
                    )
                )
            )
        else:
            prompt = (
                llm_prompts.ZERO_SHOT_NON_MARKET_PROMPT
                if prompt_type == "zero_shot"
                else (
                    llm_prompts.SCRATCH_PAD_NON_MARKET_PROMPT
                    if prompt_type == "scratchpad"
                    else (
                        llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_NON_MARKET_PROMPT
                        if prompt_type == "scratchpad_with_news"
                        else (
                            llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_PROMPT_1
                            if prompt_type == "superforecaster_with_news_1"
                            else (
                                llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_PROMPT_2
                                if prompt_type == "superforecaster_with_news_2"
                                else (
                                    llm_crowd_prompts.SUPERFORECASTER_NON_MARKET_PROMPT_3
                                    if prompt_type == "superforecaster_with_news_3"
                                    else None
                                )
                            )
                        )
                    )
                )
            )

    use_news = True if "with_news" in prompt_type else False
    assert not use_news, "`use_news` should always be False"

    prompt = prompt.format(
        **get_prompt_params(
            question,
            is_market_question,
            is_joint_question,
            forecast_due_date,
            market_use_freeze_value,
            use_news,
        )
    )

    try:
        response = get_response_from_model(
            prompt=prompt,
            max_tokens=(
                100 if prompt_type == "zero_shot" else 2000 if prompt_type == "scratchpad" else 2500
            ),
            model_name=model_name,
            temperature=0,
            wait_time=30,
        )
    except Exception as e:
        logger.error(f"Error in worker: {e}")
        response = None

    logger.info(
        f"IN WORKER: ... {model_name}. {prompt_type}. Is market_question: {is_market_question}."
    )
    if prompt_type == "zero_shot":
        if is_market_question:
            save_dict[index] = {"forecast": extract_probability(response)}
        else:
            save_dict[index] = {
                "forecast": reformat_answers(response=response, prompt=prompt, question=question)
            }
    else:
        if is_market_question:
            save_dict[index] = {
                "forecast": reformat_answers(response=response, single=True),
                "reasoning": response,
            }
        else:
            save_dict[index] = {
                "forecast": reformat_answers(response=response, prompt=prompt, question=question),
                "reasoning": response,
            }

    # if "with_news" not in prompt_type:
    #     # not saving prompts with news because it's too large
    #     save_dict[index]["prompt"] = prompt

    logger.info(
        f"Model: {model_name} | Prompt: {prompt_type} | Question Type: {question_type} | "
        f"Answer: {save_dict[index]['forecast']}"
    )

    if rate_limit:
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)

    return None


def executor(
    model_name,
    save_dict,
    questions_to_eval,
    forecast_due_date,
    prompt_type="zero_shot",
    market_use_freeze_value=False,
):
    """Executor function."""
    with ThreadPoolExecutor(max_workers=env.NUM_CPUS) as executor:
        worker_with_args = partial(
            worker,
            n_questions=len(questions_to_eval),
            model_name=model_name,
            save_dict=save_dict,
            questions_to_eval=questions_to_eval,
            forecast_due_date=forecast_due_date,
            prompt_type=prompt_type,
            market_use_freeze_value=market_use_freeze_value,
        )
        return list(executor.map(worker_with_args, range(len(questions_to_eval))))


def determine_type(is_market_question, is_joint_question, market_use_freeze_value):
    """Determine question type for debugging."""
    if is_market_question:
        if is_joint_question:
            if market_use_freeze_value:
                return "joint market with freeze value"
            else:
                return "joint market"
        else:
            if market_use_freeze_value:
                return "single market with freeze value"
            else:
                return "single market"
    else:
        if is_joint_question:
            return "joint non-market"
        else:
            return "single non-market"


def get_all_retrieved_info(all_retrieved_info):
    """Get all retrieved news."""
    retrieved_info = ""
    for summary in all_retrieved_info:
        retrieved_info += f"Article title: {summary['title']}" + "\n"
        retrieved_info += f"Summary: {summary['summary']}" + "\n\n"
    return retrieved_info


def get_prompt_params(
    question,
    is_market_question,
    is_joint_question,
    forecast_due_date,
    market_use_freeze_value,
    use_news,
):
    """Get prompt parameters."""

    def formatted_question(question):
        question = question["question"].replace("{forecast_due_date}", forecast_due_date)
        question = question.replace(
            "{resolution_date}", "each of the resolution dates provided below"
        )
        return question

    base_params = {
        "question": formatted_question(question),
        "background": question["background"] + "\n" + question["market_info_resolution_criteria"],
        "resolution_criteria": question["resolution_criteria"],
        "today_date": TODAY_DATE,
    }

    if is_market_question:
        base_params["resolution_date"] = question["market_info_close_datetime"]
        if market_use_freeze_value:
            base_params.update(
                {
                    "freeze_datetime": question["freeze_datetime"],
                    "freeze_datetime_value": question["freeze_datetime_value"],
                }
            )
    else:
        base_params.update(
            {
                "freeze_datetime": question["freeze_datetime"],
                "freeze_datetime_value": question["freeze_datetime_value"],
                "freeze_datetime_value_explanation": question["freeze_datetime_value_explanation"],
                "list_of_resolution_dates": question["resolution_dates"],
            }
        )

    if use_news and not is_joint_question:
        base_params.update(
            {
                "retrieved_info": get_all_retrieved_info(question["news"]),
            }
        )

    if is_joint_question:
        joint_params = {
            "human_prompt": HUMAN_JOINT_PROMPTS[question["combo_index"]],
            "question_1": formatted_question(question["combination_of"][0]),
            "question_2": formatted_question(question["combination_of"][1]),
            "background_1": question["combination_of"][0]["background"]
            + "\n"
            + question["combination_of"][0]["market_info_resolution_criteria"],
            "background_2": question["combination_of"][1]["background"]
            + "\n"
            + question["combination_of"][1]["market_info_resolution_criteria"],
            "resolution_criteria_1": question["combination_of"][0]["resolution_criteria"],
            "resolution_criteria_2": question["combination_of"][1]["resolution_criteria"],
            "today_date": TODAY_DATE,
        }

        if use_news:
            joint_params.update(
                {
                    "retrieved_info_1": get_all_retrieved_info(
                        question["combination_of"][0]["news"]
                    ),
                    "retrieved_info_2": get_all_retrieved_info(
                        question["combination_of"][1]["news"]
                    ),
                }
            )

        if is_market_question:
            joint_params["resolution_date"] = max(
                question["combination_of"][0]["market_info_close_datetime"],
                question["combination_of"][1]["market_info_close_datetime"],
            )
            if market_use_freeze_value:
                joint_params.update(
                    {
                        "freeze_datetime_1": question["combination_of"][0]["freeze_datetime"],
                        "freeze_datetime_2": question["combination_of"][1]["freeze_datetime"],
                        "freeze_datetime_value_1": question["combination_of"][0][
                            "freeze_datetime_value"
                        ],
                        "freeze_datetime_value_2": question["combination_of"][1][
                            "freeze_datetime_value"
                        ],
                    }
                )
        else:
            joint_params.update(
                {
                    "freeze_datetime_1": question["combination_of"][0]["freeze_datetime"],
                    "freeze_datetime_2": question["combination_of"][1]["freeze_datetime"],
                    "freeze_datetime_value_1": question["combination_of"][0][
                        "freeze_datetime_value"
                    ],
                    "freeze_datetime_value_2": question["combination_of"][1][
                        "freeze_datetime_value"
                    ],
                    "freeze_datetime_value_explanation_1": question["combination_of"][0][
                        "freeze_datetime_value_explanation"
                    ],
                    "freeze_datetime_value_explanation_2": question["combination_of"][1][
                        "freeze_datetime_value_explanation"
                    ],
                    "list_of_resolution_dates": question["resolution_dates"],
                }
            )
        return joint_params
    else:
        return base_params


def download_and_read_saved_forecasts(filename, base_file_path):
    """Download saved forecasts from cloud storage."""
    local_filename = "/tmp/" + filename.replace(base_file_path + "/", "")

    # Ensure the directory exists
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.FORECAST_SETS_BUCKET,
        filename=filename,
        local_filename=local_filename,
    )
    return data_utils.read_jsonl(local_filename)


def process_model(
    model,
    models,
    test_type,
    results,
    questions_to_eval,
    forecast_due_date,
    prompt_type,
    market_use_freeze_value,
    base_file_path,
):
    """Process a single model for the given questions."""
    logger.info(f"{model} is using {env.NUM_CPUS} workers.")
    executor(
        models[model]["full_name"],
        results[model],
        questions_to_eval,
        forecast_due_date,
        prompt_type=prompt_type,
        market_use_freeze_value=market_use_freeze_value,
    )

    current_model_forecasts = generate_forecasts(model, results, questions_to_eval, prompt_type)
    save_and_upload_results(current_model_forecasts, test_type, model, base_file_path)


def determine_test_type(question_set, prompt_type, market_use_freeze_value, test_or_prod):
    """Determine the test type based on the question set and prompt type."""
    if question_set[0]["source"] in question_curation.MARKET_SOURCES:
        base_type = "market" if question_set[0]["combination_of"] == "N/A" else "combo_market"
        if market_use_freeze_value:
            base_type += "/with_freeze_values"
    else:
        base_type = (
            "non_market" if question_set[0]["combination_of"] == "N/A" else "combo_non_market"
        )
    return f"{prompt_type}/{base_type}" + ("_test" if test_or_prod == "TEST" else "")


def generate_forecasts(model, results, questions_to_eval, prompt_type):
    """Generate forecasts for the current model."""
    forecasts = []
    for index, question in enumerate(questions_to_eval):
        if question["source"] in question_curation.DATA_SOURCES:
            forecasts.extend(
                generate_data_source_forecasts(model, results, question, index, prompt_type)
            )
        else:
            forecasts.append(
                generate_non_data_source_forecast(model, results, question, index, prompt_type)
            )
    return forecasts


def generate_data_source_forecasts(model, results, question, index, prompt_type):
    """Generate forecasts for questions from data sources."""
    forecasts = []
    model_results = results[model][index]["forecast"]
    for forecast, resolution_date in zip(model_results, question["resolution_dates"]):
        forecast_data = {
            "id": question["id"],
            "source": question["source"],
            "forecast": forecast,
            "resolution_date": resolution_date,
            "reasoning": None if prompt_type == "zero_shot" else results[model][index]["reasoning"],
        }
        # if "with_news" not in prompt_type:
        #     forecast_data["prompt"] = results[model][index]["prompt"]
        forecast_data["direction"] = None
        if question["combination_of"] != "N/A":
            forecast_data["direction"] = get_direction(question["combo_index"])

        forecasts.append(forecast_data)
    return forecasts


def generate_non_data_source_forecast(model, results, question, index, prompt_type):
    """Generate a forecast for questions not from data sources."""
    forecast_data = {
        "id": question["id"],
        "source": question["source"],
        "forecast": results[model][index]["forecast"],
        "resolution_date": None,
        "reasoning": None if prompt_type == "zero_shot" else results[model][index]["reasoning"],
    }
    # if "with_news" not in prompt_type:
    #     forecast_data["prompt"] = results[model][index]["prompt"]
    forecast_data["direction"] = None
    if question["combination_of"] != "N/A":
        forecast_data["direction"] = get_direction(question["combo_index"])

    return forecast_data


def get_direction(combo_index):
    """Get the direction based on the combo index."""
    directions = {0: [1, 1], 1: [1, -1], 2: [-1, 1], 3: [-1, -1]}
    return directions.get(combo_index, [0, 0])


def save_and_upload_results(forecasts, test_type, model, base_file_path):
    """Save results locally and upload to GCP."""
    local_filename = f"/tmp/{test_type}/{model}.jsonl"
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    with open(local_filename, "w") as file:
        for entry in forecasts:
            json_line = json.dumps(entry)
            file.write(json_line + "\n")

    remote_filename = local_filename.replace("/tmp/", "")
    gcp.storage.upload(
        bucket_name=env.FORECAST_SETS_BUCKET,
        local_filename=local_filename,
        filename=f"{base_file_path}/{remote_filename}",
    )


def process_questions(questions_file, num_per_source=None):
    """
    Process questions from a JSON file and categorize them.

    Load questions from the specified JSON file. Categorize them into single and combo
    questions for both market and non-market sources. Unroll combo questions.
    Optionally limit the number of questions per source.
    """
    with open(questions_file, "r") as file:
        questions_data = json.load(file)

    questions = questions_data["questions"]

    single_market_questions = [
        q
        for q in questions
        if q["combination_of"] == "N/A" and q["source"] in question_curation.MARKET_SOURCES
    ]
    single_non_market_questions = [
        q
        for q in questions
        if q["combination_of"] == "N/A" and q["source"] in question_curation.DATA_SOURCES
    ]

    combo_market_questions = [
        q
        for q in questions
        if q["combination_of"] != "N/A" and q["source"] in question_curation.MARKET_SOURCES
    ]
    combo_non_market_questions = [
        q
        for q in questions
        if q["combination_of"] != "N/A" and q["source"] in question_curation.DATA_SOURCES
    ]

    def unroll(combo_questions):
        """Unroll combo questions by directions."""
        combo_questions_unrolled = []
        for q in combo_questions:
            for i in range(4):
                new_q = q.copy()
                new_q["combo_index"] = i
                combo_questions_unrolled.append(new_q)
        return combo_questions_unrolled

    combo_market_questions_unrolled = unroll(combo_market_questions)
    combo_non_market_questions_unrolled = unroll(combo_non_market_questions)

    if num_per_source is not None:
        single_market_questions = single_market_questions[:num_per_source]
        single_non_market_questions = single_non_market_questions[:num_per_source]
        combo_market_questions_unrolled = combo_market_questions_unrolled[:num_per_source]
        combo_non_market_questions_unrolled = combo_non_market_questions_unrolled[:num_per_source]

    return (
        single_market_questions,
        single_non_market_questions,
        combo_market_questions_unrolled,
        combo_non_market_questions_unrolled,
    )
