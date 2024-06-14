"""LLM-related util."""

import asyncio
import logging
import re
import time

import anthropic
import google.generativeai as google_ai
import openai
import together
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage

from . import constants, keys, llm_prompts

anthropic_console = anthropic.Anthropic(api_key=keys.API_KEY_ANTHROPIC)
anthropic_async_client = anthropic.AsyncAnthropic(api_key=keys.API_KEY_ANTHROPIC)
oai_async_client = openai.AsyncOpenAI(api_key=keys.API_KEY_OPENAI)
oai = openai.OpenAI(api_key=keys.API_KEY_OPENAI)
together.api_key = keys.API_KEY_TOGETHERAI
google_ai.configure(api_key=keys.API_KEY_GOOGLE)
client = openai.OpenAI(
    api_key=keys.API_KEY_TOGETHERAI,
    base_url="https://api.together.xyz/v1",
)
mistral_client = MistralClient(api_key=keys.API_KEY_MISTRAL)


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        model_input = [{"role": "system", "content": system_prompt}] if system_prompt else []
        model_input.append({"role": "user", "content": prompt})
        response = oai.chat.completions.create(
            model=model_name,
            messages=model_input,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        # logger.info(f"full prompt: {prompt}")
        return response.choices[0].message.content

    if (
        get_response_with_retry(api_call, wait_time, "OpenAI API request exceeded rate limit.")
        == "need_a_new_reformat_prompt"
    ):
        return "need_a_new_reformat_prompt"
    else:
        return get_response_with_retry(
            api_call, wait_time, "OpenAI API request exceeded rate limit."
        )


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
    if max_tokens > 4096:
        max_tokens = 4096

    def api_call():
        completion = anthropic_console.messages.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens,
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
        chat_completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        response = chat_completion.choices[0].message.content

        return response

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
    model = google_ai.GenerativeModel(model_name)

    response = model.generate_content(
        prompt,
        generation_config=google_ai.types.GenerationConfig(
            candidate_count=1,
            max_output_tokens=max_tokens,
            temperature=temperature,
        ),
    )
    return response.text


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
    else:
        return "Not a valid model source."


async def get_async_response(
    prompt,
    model_name="gpt-3.5-turbo-1106",
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
                    max_tokens=4096,
                )
                return response.content[0].text
            elif model_source == constants.GOOGLE_SOURCE:
                model = google_ai.GenerativeModel(model_name)
                response = await model.generate_content_async(
                    prompt,
                    generation_config=google_ai.types.GenerationConfig(
                        candidate_count=1,
                        max_output_tokens=max_tokens,
                        temperature=temperature,
                    ),
                )
                return response.text
            elif model_source == constants.TOGETHER_AI_SOURCE:
                chat_completion = await asyncio.to_thread(
                    client.chat.completions.create,
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
                n_horizons=len(question["forecast_horizons"]),
            )
        raw_response = get_response_from_model(
            prompt=reformat_prompt,
            max_tokens=100,
            model_name="gpt-3.5-turbo-0125",
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
