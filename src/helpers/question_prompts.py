"""Prompt text."""

market = (
    "For the following question, we’re asking you to predict the outcome of a prediction market "
    "at a particular time. A prediction market, in this context, is the aggregate of "
    "predictions submitted by users on the website {f_string_value}. You’re going to predict "
    "what these users will say is the probability of the question outcome (the market value) by "
    "the resolution date listed below. For most of the below resolution dates, you are not "
    "predicting the outcome of the question itself, but the community prediction on the listed "
    "resolution date; if the listed resolution date is after the market close date, you are "
    "predicting the outcome of the question itself."
)

acled = (
    "The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the "
    "locations, dates, actors, fatalities, and types of all reported political violence and "
    "protest events around the world. You’re going to predict the probability of the following "
    "potential outcome we’ve come up with about some of the data ACLED tracks."
)

combination = (
    "Below, you'll see two probability questions. We're going to ask you to predict the probability "
    "that both will happen, that one will happen but not the other, and that neither will happen."
)
