"""Prompt text."""

from . import constants

HUMAN_JOINT_PROMPT_1 = "Below, you'll see two probability questions. We're going to ask you to predict the probability that both will happen."  # noqa: B950
HUMAN_JOINT_PROMPT_2 = "Below, you'll see two probability questions. We're going to ask you to predict the probability that question 1 will happen, but not question 2."  # noqa: B950
HUMAN_JOINT_PROMPT_3 = "Below, you'll see two probability questions. We're going to ask you to predict the probability that question 2 will happen, but not question 1."  # noqa: B950
HUMAN_JOINT_PROMPT_4 = "Below, you'll see two probability questions. We're going to ask you to predict the probability that neither will happen."  # noqa: B950

ZERO_SHOT_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Question close date: {close_date}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
Do not output anything else.
Answer: {{ Insert answer here }}
"""  # noqa: B950

ZERO_SHOT_ACLED_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the locations, dates, actors, fatalities, and types of all reported political violence and protest events around the world.

You’re going to predict the probability of the following potential outcome we’ve come up with about some of the data ACLED tracks “at each of the resolution dates”.


Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Current value on {freeze_datetime}:
{value_at_freeze_datetime}

Value Explanation:
{value_at_freeze_datetime_explanation}

Question resolution date: {list_of_resolution_dates}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
Answer: {{ Insert answer here }}

"""  # noqa: B950

ZERO_SHOT_JOINT_QUESTION_PROMPT = """
You are an expert superforecaster, familiar with the work of Tetlock and others. Make a prediction of the probability that the question will be resolved as true. You MUST give a probability estimate between 0 and 1 UNDER ALL CIRCUMSTANCES. If for some reason you can't answer, pick the base rate, but return a number between 0 and 1.

{human_prompt}

Question 1:
{question_1}

Question 2:
{question_2}

Question 1 Background:
{background_1}

Question 2 Background:
{background_2}

Question 1 Resolution Criteria:
{resolution_criteria_1}

Question 2 Resolution Criteria:
{resolution_criteria_2}

Question 1 Current value on {freeze_datetime_1}:
{value_at_freeze_datetime_1}

Question 1 Value Explanation:
{value_at_freeze_datetime_explanation_1}

Question 2 Current value on {freeze_datetime_1}:
{value_at_freeze_datetime_2}

Question 2 Value Explanation:
{value_at_freeze_datetime_explanation_2}

Question resolution date: {list_of_resolution_dates}

Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.

Do not output anything else.
Answer: {{ Insert answer here }}
"""  # noqa: B950

SCRATCH_PAD_PROMPT = """
Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Question close date: {close_date}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason.
{{ Insert your thoughts }}

4. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

7. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

SCRATCH_PAD_ACLED_PROMPT = """
The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the locations, dates, actors, fatalities, and types of all reported political violence and protest events around the world.

You’re going to predict the probability of the following potential outcome we’ve come up with about some of the data ACLED tracks “at each of the resolution dates”.

Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason.
{{ Insert your thoughts }}

4. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

7. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

SCRATCH_PAD_JOINT_QUESTION_PROMPT = """
The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the locations, dates, actors, fatalities, and types of all reported political violence and protest events around the world.

{human_prompt} “at each of the resolution dates”.

Question 1:
{question_1}

Question 2:
{question_2}

Question 1 Background:
{background_1}

Question 2 Background:
{background_2}

Question 1 Resolution Criteria:
{resolution_criteria_1}

Question 2 Resolution Criteria:
{resolution_criteria_2}

Question 1 Current value on {freeze_datetime_1}:
{value_at_freeze_datetime_1}

Question 1 Value Explanation:
{value_at_freeze_datetime_explanation_1}

Question 2 Current value on {freeze_datetime_1}:
{value_at_freeze_datetime_2}

Question 2 Value Explanation:
{value_at_freeze_datetime_explanation_2}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason.
{{ Insert your thoughts }}

4. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

7. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

REFORMAT_PROMPT = """
User prompt:
{user_prompt}

Model Response:
{model_response}

Please determine whether the model's response includes a probabilistic forecast for all {n_horizons} resolution dates mentioned in the user's prompt. If such forecasts are not present, you should provide a probabilistic forecast for all {n_horizons} resolution dates mentioned.
The overarching question is: 'Below, you'll find two probability questions. We'd like you to predict the probability that neither event will occur at each of the resolution dates.'

Please output the probabilistic forecasts as a Python list, e.g., [prob1, prob2, ...] DO NOT OUTPUT ANYTHING ELSE. PLEASE ONLY OUTPUT [prob1, prob2, ...], if there is only one probabilistic prediction, output [prob1, prob2, ...] with the same probability.
"""  # noqa: B950

REFORMAT_SINGLE_PROMPT = """

{response}

***********************************
Instructions:
The text above is an answer from a large language model (LLM) that includes reasoning and a probability estimate in response to a question.
Task: Extract the numerical probability given by the LLM. The probability should be a decimal value between 0 and 1.
If the LLM's response does not contain a probability estimate, return 'N/A'.

Output Requirement: Provide only a decimal value between 0 and 1 representing the probability, or 'N/A' if no probability is mentioned.
"""  # noqa: B950


ASSIGN_CATEGORY_PROMPT = (
    """Question: {question}

Background: {background}

"""
    f"""Options:
[{"'" + "',\n'".join(constants.QUESTION_CATEGORIES) + "'"}]

Instruction: Assign a category for the given question.

Rules:
1. Make sure you only return one of the options from the option list.
2. Only output the category, and do not output any other words in your response.
3. You have to pick a string from the above categories.

Answer:"""
)


VALIDATE_QUESTION_PROMPT = """I want to assess the quality of a forecast question.

Here is the forecast question: {question}.

Please flag questions that don't seem appropriate by outputting "flag". Otherwise, if it seems like a reasonable question or if you're unsure, output "ok."

In general, poorly-defined questions, questions that are sexual in nature, questions that are too personal, questions about the death/life expectancy of an individual should be flagged or, more generally, questions that are not in the public interest should be flagged. Geopolitical questions, questions about court cases, the entertainment industry, wars, public figures, and, more generally, questions in the public interest should be marked as "ok."

Examples of questions that should be flagged:
* "Will I finish my homework tonight?"
* "Metaculus party 2023"
* "Will Hell freeze over?"
* "Heads or tails?"
* "Will I get into MIT?"
* "Will this video reach 100k views by the EOD?"
* "If @Aella goes on the Whatever podcast, will she regret it?"
* "Daily coinflip"
* "Musk vs Zuckerberg: Will either of them shit their pants on the mat?"

Examples of questions that should NOT be flagged:
* "Will Megan Markle and Prince Harry have a baby by the end of the year?"
* "Will the Brain Preservation Foundation's Large Mammal preservation prize be won by Feb 9th, 2017?"
* "Will there be more novel new drugs approved by the FDA in 2016 than in 2015?"
* "Will Israel invade Rafah in May 2024?"
* "Will Iraq return its ambassador to Iran in the next month?"
* "Tiger Woods Will Win Another PGA Tournament"
* "Will Dwayne Johnson win the 2024 US Presidential Election?"
* "Will Oppenheimer win best picture AND Bitcoin reach $70K AND Nintendo announce a new console by EOY 2024?"
* "Will anybody born before 2000 live to be 150?"
* "Will Taylor Swift get married before Bitcoin reaches $100K USD?"
* "Will Russia's total territory decrease by at least 20% before 2028?"
* "Will Donald Trump be jailed or incarcerated before 2030?"
* "If China invades Taiwan before 2035, will the US respond with military force?"
* "Will there be a tsunami that kills at least 50,000 people before 2030?"
* "Will there be a military conflict resulting in at least 50 deaths between the United States and China in 2024?"
* "Will an AI system be reported to have successfully blackmailed someone for >$1000 by EOY 2028?"
* "Will Vladimir Putin declare Martial Law in at least 3/4 of Russia before 2025?"

Again, when in doubt, do NOT flag the question; mark it as "ok".

Your response should take the following structure:
Insert thinking:
{{ insert your concise thoughts here }}
Classification:
{{ insert "flag" or "ok"}}"""  # noqa: B950