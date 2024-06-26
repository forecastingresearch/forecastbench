"""Metaculus-specific variables."""

CATEGORIES = [
    "geopolitics",
    "natural-sciences",
    "sports-entertainment",
    "health-pandemics",
    "law",
    "computing-and-math",
]

SOURCE_INTRO = (
    "We would like you to predict the outcome of a prediction market. A prediction market, in this "
    "context, is the aggregate of predictions submitted by users on the website Metaculus. "
    "You're going to predict the probability that the market will resolve as 'Yes'."
)


RESOLUTION_CRITERIA = "Resolves to the outcome of the question found at {url}."
