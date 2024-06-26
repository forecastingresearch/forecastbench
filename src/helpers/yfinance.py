"""Yfinance-specific variables."""

SOURCE_INTRO = (
    "Yahoo Finance provides financial data on stocks, bonds, and currencies and also offers news, "
    "commentary and tools for personal financial management. You're going to predict how questions "
    "based on this data will resolve."
)

RESOLUTION_CRITERIA = (
    "Resolves to the market close price at {url} for the resolution date. If the resolution date "
    "coincides with a day the market is closed (weekend, holiday, etc.) the previous market close "
    "price is used."
)
