"""Metadata prompt text."""

from . import constants

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
* "Metaculus party c2023"
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
