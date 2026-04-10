"""Yfinance-specific variables. Delegates to sources._metadata."""

from sources._metadata import SOURCE_METADATA

SOURCE_INTRO = SOURCE_METADATA["yfinance"]["source_intro"]
RESOLUTION_CRITERIA = SOURCE_METADATA["yfinance"]["resolution_criteria"]
