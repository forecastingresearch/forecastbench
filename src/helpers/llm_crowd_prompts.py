"""LLM Crowd prompts."""

from . import llm_prompts

# The following SCRATCH_PAD_WITH_SUMMARIES prompt 1 and 2 are mainly from paper:
# "Approaching Human-Level Forecasting with Language Models" by Halawi et al. (2024)
# Some are modified versions in order to adapt to our needs
# https://arxiv.org/pdf/2402.18563v1
SCRATCH_PAD_WITH_SUMMARIES_MARKET_PROMPT_1 = llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_MARKET_PROMPT
SCRATCH_PAD_WITH_SUMMARIES_NON_MARKET_PROMPT_1 = (
    llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_NON_MARKET_PROMPT
)
SCRATCH_PAD_WITH_SUMMARIES_JOINT_QUESTION_PROMPT_1 = (
    llm_prompts.SCRATCH_PAD_WITH_SUMMARIES_JOINT_QUESTION_PROMPT
)

SCRATCH_PAD_WITH_SUMMARIES_MARKET_PROMPT_2 = """
Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Question close date: {close_date}

We have retrieved the following information for this question:
{retrieved_info}

Instructions:
1. Write down any additional relevant information that is not included above. This should be specific facts that you already know the answer to, rather than information that needs to be looked up.
{{ Insert additional information }}

2. Provide at least 3 reasons why the answer might be no.
{{ Insert your thoughts }}

3. Provide at least 3 reasons why the answer might be yes.
{{ Insert your thoughts }}

4. Rate the strength of each of the reasons given in the last two responses. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your rating of the strength of each reason }}

5. Aggregate your considerations.
{{ Insert your aggregated considerations }}

6. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
{{ Insert your answer }}
"""  # noqa: B950

SCRATCH_PAD_WITH_SUMMARIES_NON_MARKET_PROMPT_2 = """
You’re going to predict the probability of the following potential outcome “at each of the resolution dates”.

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

We have retrieved the following information for this question:
{retrieved_info}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Write down any additional relevant information that is not included above. This should be specific facts that you already know the answer to, rather than information that needs to be looked up.
{{ Insert additional information }}

2. Provide at least 3 reasons why the answer might be no.
{{ Insert your thoughts }}

3. Provide at least 3 reasons why the answer might be yes.
{{ Insert your thoughts }}

4. Rate the strength of each of the reasons given in the last two responses. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your rating of the strength of each reason }}

5. Aggregate your considerations.
{{ Insert your aggregated considerations }}

6. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

SCRATCH_PAD_WITH_SUMMARIES_JOINT_QUESTION_PROMPT_2 = """
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

We have retrieved the following information for Question 1:
{retrieved_info_1}

We have retrieved the following information for Question 2:
{retrieved_info_2}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Write down any additional relevant information that is not included above. This should be specific facts that you already know the answer to, rather than information that needs to be looked up.
{{ Insert additional information }}

2. Provide at least 3 reasons why the answer might be no.
{{ Insert your thoughts }}

3. Provide at least 3 reasons why the answer might be yes.
{{ Insert your thoughts }}

4. Rate the strength of each of the reasons given in the last two responses. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your rating of the strength of each reason }}

5. Aggregate your considerations.
{{ Insert your aggregated considerations }}

6. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

# Molly's + Sam's
SUPERFORECASTER_MARKET_PROMPT_3 = """
Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Question close date: {close_date}

We have retrieved the following information for this question:
{retrieved_info}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Let’s start by coming up with a base-rate that could be helpful for forecasting this question. Come up with the best reference-class you can for this sort of event, and give a general base-rate that doesn’t take into account factors unique to this question.

For instance, if the question were about the probability of a new technology being widely adopted within five years, you might look at historical data on the adoption rates of similar technologies as a reference class. Come up with a base-rate that could be relevant for this question.

The base-rate must be formatted as a clear probability (or number, in cases where you believe that to be more useful than a probability). For instance, imagine you are forecasting the probability that an incumbent president will be re-elected in an upcoming election in a hypothetical country. The past data shows that the incumbent has been elected 60% of the time.

Here, you would write ‘The reference class I have chosen is the incumbent being elected. My base-rate is that the probability of the incumbent being re-elected is 0.6.’ Give a justification for the base-rate, as well as a clear number.

Importantly, the base-rate should be as specific as it’s possible to be without losing confidence that the number is correct. For instance, if you were forecasting on the probability of a hypothetical democratic country going to war in the next year, you should ideally produce a base-rate for a democratic country going to war in a given year, rather than simply thinking about a given country going to war.

{{ Insert your base rate }}

3. Now, let’s think about factors specific to this question that may give us a good reason to deviate from the base-rate. Please give some reasons that the probability of this question resolving positively may be higher than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

4. Now, let’s think about reasons that the probability of this question resolving positively may be lower than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

5. Consider any other factors that may affect the probability of this question resolving positively or negatively, that you have not already discussed in the previous two steps.
{{ Insert your thoughts }}

6. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver). Give a ranking to each consideration based on how much you believe it ought to affect your forecast.
{{ Insert your aggregated considerations }}

7. Are there any ways in which the question could resolve positively or negatively that you haven’t considered yet, or that require some outside-the-box thinking? For example, if the question was ‘Will Microsoft have a market capitalization of over $5tn by 2030’, you might consider questions like:

How likely is it that Microsoft no longer exists in 2030?
How likely is it that inflation erodes that value of the dollar as such that $5n is worth significantly less than it is today?
How likely is it that there is a merger between Microsoft and another large company?
How likely is it that Microsoft is broken up, as it is perceived to have monopoly power?

Here, we’re thinking about things that are probably quite unlikely to happen, but should still be integrated into your forecast. Write up some possibilities and consider how they should be integrated into your final forecast.
{{Insert your thoughts and considerations about how this should affect your forecast}}

8. Output an initial probability (prediction) given steps 1-7.
{{ Insert initial probability. }}

9. Okay, now let’s think about some other ways to consider how to forecast on this question.  What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. This odds ratio probably shouldn’t be purely on the basis of the considerations in the previous steps, but you should think again about what you would expect to see if you could fast-forward into the future. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

10. Given your rephrased statement from step 1, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

11. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement, insert new odds for the original statement. }}

12. Now consider each of your odds from the previous steps(steps 9 - 11), and come up with your all-things-considered odds ratio for the original statement.
{{ Insert final odds for the original statement. }}

13. Now, convert that odds ratio to a probability between 0 and 1.
{{Insert a probability}}

14. Now, consider the probability that you came up with in step 8, as well as the probability that you came up with in step 13. Which of these probabilities do you lean towards? How do you weigh them against one another? Write up your thoughts on which probability is more likely to be “correct”, and then decide on a FINAL probability that will be used as your forecast.
{{Insert your thoughts AND a final probability}}

15. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
{{ Insert your answer }}

"""  # noqa: B950

SUPERFORECASTER_NON_MARKET_PROMPT_3 = """
You’re going to predict the probability of the following potential outcome “at each of the resolution dates”.

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

We have retrieved the following information for this question:
{retrieved_info}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Let’s start by coming up with a base-rate that could be helpful for forecasting this question. Come up with the best reference-class you can for this sort of event, and give a general base-rate that doesn’t take into account factors unique to this question.

For instance, if the question were about the probability of a new technology being widely adopted within five years, you might look at historical data on the adoption rates of similar technologies as a reference class. Come up with a base-rate that could be relevant for this question.

The base-rate must be formatted as a clear probability (or number, in cases where you believe that to be more useful than a probability). For instance, imagine you are forecasting the probability that an incumbent president will be re-elected in an upcoming election in a hypothetical country. The past data shows that the incumbent has been elected 60% of the time.

Here, you would write ‘The reference class I have chosen is the incumbent being elected. My base-rate is that the probability of the incumbent being re-elected is 0.6.’ Give a justification for the base-rate, as well as a clear number.

Importantly, the base-rate should be as specific as it’s possible to be without losing confidence that the number is correct. For instance, if you were forecasting on the probability of a hypothetical democratic country going to war in the next year, you should ideally produce a base-rate for a democratic country going to war in a given year, rather than simply thinking about a given country going to war.

{{ Insert your base rate }}

3. Now, let’s think about factors specific to this question that may give us a good reason to deviate from the base-rate. Please give some reasons that the probability of this question resolving positively may be higher than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

4. Now, let’s think about reasons that the probability of this question resolving positively may be lower than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

5. Consider any other factors that may affect the probability of this question resolving positively or negatively, that you have not already discussed in the previous two steps.
{{ Insert your thoughts }}

6. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver). Give a ranking to each consideration based on how much you believe it ought to affect your forecast.
{{ Insert your aggregated considerations }}

7. Are there any ways in which the question could resolve positively or negatively that you haven’t considered yet, or that require some outside-the-box thinking? For example, if the question was ‘Will Microsoft have a market capitalization of over $5tn by 2030’, you might consider questions like:

How likely is it that Microsoft no longer exists in 2030?
How likely is it that inflation erodes that value of the dollar as such that $5n is worth significantly less than it is today?
How likely is it that there is a merger between Microsoft and another large company?
How likely is it that Microsoft is broken up, as it is perceived to have monopoly power?

Here, we’re thinking about things that are probably quite unlikely to happen, but should still be integrated into your forecast. Write up some possibilities and consider how they should be integrated into your final forecast.
{{Insert your thoughts and considerations about how this should affect your forecast}}

8. Output an initial probability (prediction) given steps 1-7.
{{ Insert initial probability. }}

9. Okay, now let’s think about some other ways to consider how to forecast on this question.  What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. This odds ratio probably shouldn’t be purely on the basis of the considerations in the previous steps, but you should think again about what you would expect to see if you could fast-forward into the future. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

10. Given your rephrased statement from step 1, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

11. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement insert new odds for the original statement. }}

12. Now consider each of your odds from the previous steps(steps 9 - 11), and come up with your all-things-considered odds ratio for the original statement.
{{ Insert final odds for the original statement. }}

13. Now, convert that odds ratio to a probability between 0 and 1.
{{ Insert a probability }}

14. Now, consider the probability that you came up with in step 8, as well as the probability that you came up with in step 13. Which of these probabilities do you lean towards? How do you weigh them against one another? Write up your thoughts on which probability is more likely to be “correct”, and then decide on a FINAL probability that will be used as your forecast.
{{Insert your thoughts AND a final probability}}

15. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}

"""  # noqa: B950

SUPERFORECASTER_JOINT_QUESTION_PROMPT_3 = """
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

We have retrieved the following information for Question 1:
{retrieved_info_1}

We have retrieved the following information for Question 2:
{retrieved_info_2}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Let’s start by coming up with a base-rate that could be helpful for forecasting this question. Come up with the best reference-class you can for this sort of event, and give a general base-rate that doesn’t take into account factors unique to this question.

For instance, if the question were about the probability of a new technology being widely adopted within five years, you might look at historical data on the adoption rates of similar technologies as a reference class. Come up with a base-rate that could be relevant for this question.

The base-rate must be formatted as a clear probability (or number, in cases where you believe that to be more useful than a probability). For instance, imagine you are forecasting the probability that an incumbent president will be re-elected in an upcoming election in a hypothetical country. The past data shows that the incumbent has been elected 60% of the time.

Here, you would write ‘The reference class I have chosen is the incumbent being elected. My base-rate is that the probability of the incumbent being re-elected is 0.6.’ Give a justification for the base-rate, as well as a clear number.

Importantly, the base-rate should be as specific as it’s possible to be without losing confidence that the number is correct. For instance, if you were forecasting on the probability of a hypothetical democratic country going to war in the next year, you should ideally produce a base-rate for a democratic country going to war in a given year, rather than simply thinking about a given country going to war.

{{ Insert your base rate }}

3. Now, let’s think about factors specific to this question that may give us a good reason to deviate from the base-rate. Please give some reasons that the probability of this question resolving positively may be higher than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

4. Now, let’s think about reasons that the probability of this question resolving positively may be lower than the base rate. Please note specifically how they affect your forecast in terms of percentage point change.
{{ Insert your thoughts }}

5. Consider any other factors that may affect the probability of this question resolving positively or negatively, that you have not already discussed in the previous two steps.
{{ Insert your thoughts }}

6. Aggregate your considerations. Think like a superforecaster (e.g. Nate Silver). Give a ranking to each consideration based on how much you believe it ought to affect your forecast.
{{ Insert your aggregated considerations }}

7. Are there any ways in which the question could resolve positively or negatively that you haven’t considered yet, or that require some outside-the-box thinking? For example, if the question was ‘Will Microsoft have a market capitalization of over $5tn by 2030’, you might consider questions like:

How likely is it that Microsoft no longer exists in 2030?
How likely is it that inflation erodes that value of the dollar as such that $5n is worth significantly less than it is today?
How likely is it that there is a merger between Microsoft and another large company?
How likely is it that Microsoft is broken up, as it is perceived to have monopoly power?

Here, we’re thinking about things that are probably quite unlikely to happen, but should still be integrated into your forecast. Write up some possibilities and consider how they should be integrated into your final forecast.
{{Insert your thoughts and considerations about how this should affect your forecast}}

8. Output an initial probability (prediction) given steps 1-7.
{{ Insert initial probability. }}

9. Okay, now let’s think about some other ways to consider how to forecast on this question.  What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. This odds ratio probably shouldn’t be purely on the basis of the considerations in the previous steps, but you should think again about what you would expect to see if you could fast-forward into the future. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

10. Given your rephrased statement from step 1, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

11. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement insert new odds for the original statement. }}

12. Now consider each of your odds from the previous steps(steps 9 - 11), and come up with your all-things-considered odds ratio for the original statement.
{{ Insert final odds for the original statement. }}

13. Now, convert that odds ratio to a probability between 0 and 1.
{{Insert a probability}}

14. Now, consider the probability that you came up with in step 8, as well as the probability that you came up with in step 13. Which of these probabilities do you lean towards? How do you weigh them against one another? Write up your thoughts on which probability is more likely to be “correct”, and then decide on a FINAL probability that will be used as your forecast.
{{Insert your thoughts AND a final probability}}

15. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

# Molly's
SUPERFORECASTER_MARKET_PROMPT_4 = """
Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Here’s some related information from the news that I’ve collected for you:
{retrieved_info}

Question close date: {close_date}

Instructions:
1. Rephrase the question as a statement about the future, e.g. you would rephrase “Will Biden be the U.S. president on January 1 2025?” as “Biden is the U.S. president on January 1 2025.”
{{ Insert question rephrased as a statement. }}

2. What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

2. Given your rephrased statement, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

3. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement insert new odds for the original statement. }}

4. Now consider each of your odds from the previous steps and come up with your all-things-considered odds ratio for the original statement.
Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal.
{{ Insert final odds for the original statement. }}
"""  # noqa: B950

SUPERFORECASTER_NON_MARKET_PROMPT_4 = """
You’re going to predict the probability of the following potential outcome “at each of the resolution dates”.

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

Here’s some related information from the news that I’ve collected for you:
{retrieved_info}

Question resolution date: {list_of_resolution_dates}

Instructions:

1. Rephrase the question as a statement about the future, e.g. you would rephrase “Will Biden be the U.S. president on January 1 2025?” as “Biden is the U.S. president on January 1 2025.”
{{ Insert question rephrased as a statement. }}

2. What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

2. Given your rephrased statement, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

3. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement insert new odds for the original statement. }}

4. Now consider each of your odds from the previous steps and come up with your all-things-considered odds ratio for the original statement.
Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert final odds for the original statement. }}

"""  # noqa: B950

SUPERFORECASTER_JOINT_QUESTION_PROMPT_4 = """
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

Here’s some related information from the news that I’ve collected for Question 1:
{retrieved_info_1}

Here’s some related information from the news that I’ve collected for Question 2:
{retrieved_info_2}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Rephrase the question as a statement about the future, e.g. you would rephrase “Will Biden be the U.S. president on January 1 2025?” as “Biden is the U.S. president on January 1 2025.”
{{ Insert question rephrased as a statement. }}

2. What would you say are the odds that if you could fast-forward and find out whether that statement is true or false, you would find out it’s true? You must give an odds ratio. If it helps, imagine that you’re taking a bet.
{{ Insert your odds ratio. }}

2. Given your rephrased statement, think of 2-3 statements that if you conditioned on their being TRUE, you would think it more or less likely that your statement would be TRUE as well. These statements must not DETERMINE OR BE LOGICALLY EQUIVALENT to the original statement. Be creative!
{{ Insert 2 to 3 related statements. }}

3. For each of your related statements, give new odds of the original statement conditional on the related statement being TRUE.
{{ For each related statement insert new odds for the original statement. }}

4. Now consider each of your odds from the previous steps and come up with your all-things-considered odds ratio for the original statement.
Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert final odds for the original statement. }}
"""  # noqa: B950

# SAM's
SUPERFORECASTER_MARKET_PROMPT_5 = """
Question:
{question}

Question Background:
{background}

Resolution Criteria:
{resolution_criteria}

Relevant information we retrieved from news articles:
{retrieved_info}

Question close date: {close_date}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

4. Aggregate the considerations you developed in the previous steps. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Now, consider the perspectives, ideas, and evidence that was provided in the retrieved news articles. How should these affect your judgment of the probability of the question resolving positively? List all reasons why these news articles might increase the probability of the question resolving positively.
{{Insert your thoughts}}

7. Now, let’s focus on how the ideas, perspectives, and evidence provided in the news articles might decrease the probability of the question resolving positively.
{{Insert your thoughts}}

8. Given what you’ve thought about in the previous two steps, update your probability from the initial probability you gave in step 5.
{{Insert updated probability}}

9. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

10. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. Do not output anything else.
{{ Insert your answer }}
"""  # noqa: B950

SUPERFORECASTER_NON_MARKET_PROMPT_5 = """
You’re going to predict the probability of the following potential outcome “at each of the resolution dates”.

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

Here’s some related information from the news that I’ve collected for you:
{retrieved_info}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

4. Aggregate the considerations you developed in the previous steps. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Now, consider the perspectives, ideas, and evidence that was provided in the retrieved news articles. How should these affect your judgment of the probability of the question resolving positively? List all reasons why these news articles might increase the probability of the question resolving positively.
{{Insert your thoughts}}

7. Now, let’s focus on how the ideas, perspectives, and evidence provided in the news articles might decrease the probability of the question resolving positively.
{{Insert your thoughts}}

8. Given what you’ve thought about in the previous two steps, update your probability from the initial probability you gave in step 5.
{{Insert updated probability}}

9. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

10. Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert final odds for the original statement. }}

"""  # noqa: B950

SUPERFORECASTER_JOINT_QUESTION_PROMPT_5 = """
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

Here’s some related information from the news that I’ve collected for Question 1:
{retrieved_info_1}

Here’s some related information from the news that I’ve collected for Question 2:
{retrieved_info_2}

Question resolution date: {list_of_resolution_dates}

Instructions:
1. Given the above question, rephrase and expand it to help you do better answering. Maintain all information in the original question.
{{ Insert rephrased and expanded question.}}

2. Provide a few reasons why the answer might be no. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

3. Provide a few reasons why the answer might be yes. Rate the strength of each reason. For now, ignore the evidence, ideas, and perspectives contained in the attached news articles.
{{ Insert your thoughts }}

4. Aggregate the considerations you developed in the previous steps. Think like a superforecaster (e.g. Nate Silver).
{{ Insert your aggregated considerations }}

5. Output an initial probability (prediction) given steps 1-4.
{{ Insert initial probability. }}

6. Now, consider the perspectives, ideas, and evidence that was provided in the retrieved news articles. How should these affect your judgment of the probability of the question resolving positively? List all reasons why these news articles might increase the probability of the question resolving positively.
{{Insert your thoughts}}

7. Now, let’s focus on how the ideas, perspectives, and evidence provided in the news articles might decrease the probability of the question resolving positively.
{{Insert your thoughts}}

8. Given what you’ve thought about in the previous two steps, update your probability from the initial probability you gave in step 5.
{{Insert updated probability}}

9. Evaluate whether your calculated probability is excessively confident or not confident enough. Also, consider anything else that might affect the forecast that you did not before consider.
{{ Insert your thoughts }}

10. Now consider each of your odds from the previous steps and come up with your all-things-considered odds ratio for the original statement.
Output your answer (a number between 0 and 1) with an asterisk at the beginning and end of the decimal. (For example, if there are n resolution dates, you would output different *p* for each resolution date) Do not output anything else.
{{ Insert final odds for the original statement. }}
"""  # noqa: B950
