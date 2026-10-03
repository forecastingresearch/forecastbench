import pytest

from llm_forecaster import parsing


class FakeExtractModel:
    def __init__(self, response="[0.2, 0.3]"):
        self.response = response
        self.prompts = []

    def get_response(self, prompt):
        self.prompts.append(prompt)
        return self.response


def test_parse_market_forecast_accepts_only_a_stripped_probability_token():
    forecast_extraction_model = FakeExtractModel("[0.9]")

    assert parsing.parse_market_forecast("0.3", forecast_extraction_model) == 0.3
    assert parsing.parse_market_forecast("  *.3*\n", forecast_extraction_model) == 0.3
    assert parsing.parse_market_forecast("*0*", forecast_extraction_model) == 0.0
    assert parsing.parse_market_forecast("*1*", forecast_extraction_model) == 1.0
    assert forecast_extraction_model.prompts == []


def test_parse_market_forecast_rejects_bad_extraction_output():
    with pytest.raises(ValueError, match="Expected 1 extracted market forecast, got 0"):
        parsing.parse_market_forecast("Answer: *0.3*", FakeExtractModel("[]"))
    with pytest.raises(ValueError, match="Expected 1 extracted market forecast, got 0"):
        parsing.parse_market_forecast("0.3 0.4", FakeExtractModel("[]"))
    with pytest.raises(ValueError, match="Expected 1 extracted market forecast, got 0"):
        parsing.parse_market_forecast("61%", FakeExtractModel("[]"))
    with pytest.raises(ValueError, match="Expected 1 extracted market forecast, got 0"):
        parsing.parse_market_forecast("2026-07-01", FakeExtractModel("[]"))
    with pytest.raises(ValueError, match="Expected 1 market forecast, got 0"):
        parsing.parse_market_forecast(None, FakeExtractModel("[]"))


def test_parse_market_forecast_requires_extraction_model():
    with pytest.raises(TypeError, match="forecast_extraction_model"):
        parsing.parse_market_forecast("0.3")


def test_parse_market_forecast_extracts_unparseable_response():
    forecast_extraction_model = FakeExtractModel("[0.3]")

    assert (
        parsing.parse_market_forecast(
            response="Answer: *0.3*",
            forecast_extraction_model=forecast_extraction_model,
        )
        == 0.3
    )


def test_parse_market_forecast_does_not_extract_empty_response():
    forecast_extraction_model = FakeExtractModel("[0.3]")

    with pytest.raises(ValueError, match="Expected 1 market forecast, got 0"):
        parsing.parse_market_forecast(
            response=" \n\t",
            forecast_extraction_model=forecast_extraction_model,
        )
    with pytest.raises(ValueError, match="Expected 1 market forecast, got 0"):
        parsing.parse_market_forecast(
            response=None,
            forecast_extraction_model=forecast_extraction_model,
        )
    assert forecast_extraction_model.prompts == []


def test_parse_dataset_forecast_accepts_a_strict_probability_token_list():
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}
    forecast_extraction_model = FakeExtractModel()

    assert parsing.parse_dataset_forecast("*0.2* *0.3*", question, forecast_extraction_model) == [
        0.2,
        0.3,
    ]
    assert parsing.parse_dataset_forecast("  .2\n*0.3*\n", question, forecast_extraction_model) == [
        0.2,
        0.3,
    ]
    assert parsing.parse_dataset_forecast("*0.2*, *0.3*", question, forecast_extraction_model) == [
        0.2,
        0.3,
    ]
    assert forecast_extraction_model.prompts == []


def test_parse_dataset_forecast_accepts_adjacent_starred_probability_tokens():
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}
    forecast_extraction_model = FakeExtractModel()

    assert parsing.parse_dataset_forecast("*0.2*0.3*", question, forecast_extraction_model) == [
        0.2,
        0.3,
    ]
    assert parsing.parse_dataset_forecast("*0.2**0.3*", question, forecast_extraction_model) == [
        0.2,
        0.3,
    ]


def test_parse_dataset_forecast_extracts_extra_text_and_rejects_bad_extraction_output():
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}

    assert parsing.parse_dataset_forecast("Answer: *0.2* *0.3*", question, FakeExtractModel()) == [
        0.2,
        0.3,
    ]
    with pytest.raises(ValueError, match="Expected a Python list of numeric probabilities"):
        parsing.parse_dataset_forecast("*0.2* nope *0.3*", question, FakeExtractModel("not a list"))
    with pytest.raises(ValueError, match="Expected a Python list of numeric probabilities"):
        parsing.parse_dataset_forecast("61%", question, FakeExtractModel("not a list"))
    with pytest.raises(ValueError, match="Expected a Python list of numeric probabilities"):
        parsing.parse_dataset_forecast("2026-07-01", question, FakeExtractModel("not a list"))


def test_parse_dataset_forecast_does_not_extract_empty_response():
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}
    forecast_extraction_model = FakeExtractModel("[0.2, 0.3]")

    with pytest.raises(ValueError, match="Expected 2 dataset forecasts, got 0"):
        parsing.parse_dataset_forecast(" \n\t", question, forecast_extraction_model)
    with pytest.raises(ValueError, match="Expected 2 dataset forecasts, got 0"):
        parsing.parse_dataset_forecast(None, question, forecast_extraction_model)
    assert forecast_extraction_model.prompts == []


def test_parse_dataset_forecast_rejects_wrong_extract_forecast_count():
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}

    with pytest.raises(ValueError, match="Expected 2 dataset forecasts, got 1"):
        parsing.parse_dataset_forecast(
            "Answer: *0.2*",
            question,
            FakeExtractModel("[0.2]"),
        )


def test_parse_dataset_forecast_extracts_text_with_final_probability_lines_once():
    response = """Reasoning first.

*0.48*
*0.48*
"""
    question = {"resolution_dates": ["2026-06-01", "2026-07-01"]}
    forecast_extraction_model = FakeExtractModel("[0.48, 0.48]")

    assert parsing.parse_dataset_forecast(response, question, forecast_extraction_model) == [
        0.48,
        0.48,
    ]
    assert len(forecast_extraction_model.prompts) == 1
    assert "Reasoning first." in forecast_extraction_model.prompts[0]
    assert "Expected number of probabilities: 2" in forecast_extraction_model.prompts[0]
    assert "original prompt" not in forecast_extraction_model.prompts[0]
    assert "USER PROMPT" not in forecast_extraction_model.prompts[0]
    assert "user's prompt" not in forecast_extraction_model.prompts[0]
