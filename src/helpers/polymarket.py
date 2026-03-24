"""Polymarket-specific variables."""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sources.polymarket import NULLIFIED_QUESTION_IDS  # noqa: F401, E402

SOURCE_INTRO = (
    "We would like you to predict the outcome of a prediction market. A prediction market, in this "
    "context, is the aggregate of predictions submitted by users on the website Polymarket. "
    "You're going to predict the probability that the market will resolve as 'Yes'."
)

RESOLUTION_CRITERIA = "Resolves to the outcome of the question found at {url}."
