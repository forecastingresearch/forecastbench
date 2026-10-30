import pytest

from llm_forecaster import question_set as question_set_module


def test_question_set_module_name_matches_domain():
    assert question_set_module.__file__.endswith("question_set.py")


def test_split_questions_uses_source_registry_names():
    items = [
        {"id": "fred-1", "source": "fred"},
        {"id": "market-1", "source": "metaculus"},
    ]

    dataset, market = question_set_module.split_questions(items)

    assert dataset == [{"id": "fred-1", "source": "fred"}]
    assert market == [{"id": "market-1", "source": "metaculus"}]


def test_split_questions_rejects_unknown_source():
    with pytest.raises(ValueError, match="Unknown question sources"):
        question_set_module.split_questions([{"id": "q1", "source": "unknown"}])


def test_limit_questions_for_test_mode_limits_each_type():
    dataset = [{"id": f"d{i}", "source": "fred"} for i in range(4)]
    market = [{"id": f"m{i}", "source": "metaculus"} for i in range(4)]

    assert question_set_module.limit_questions_for_test_mode(dataset, market, limit_per_type=2) == (
        dataset[:2],
        market[:2],
    )


def test_question_set_from_question_set_json():
    question_set = question_set_module.QuestionSet.from_question_set_json(
        {
            "forecast_due_date": "2026-05-10",
            "question_set": "2026-05-10-llm.json",
            "questions": [{"id": "q1", "source": "fred"}],
        }
    )

    assert question_set.forecast_due_date == "2026-05-10"
    assert question_set.question_set_filename == "2026-05-10-llm.json"
    assert question_set.questions == [{"id": "q1", "source": "fred"}]


def test_question_set_from_question_set_json_defaults_missing_question_set():
    question_set = question_set_module.QuestionSet.from_question_set_json(
        {
            "forecast_due_date": "2026-05-10",
            "questions": [{"id": "q1", "source": "fred"}],
        }
    )

    assert question_set.question_set_filename == "2026-05-10-llm.json"


def test_question_set_module_does_not_load_through_boundary_reader():
    assert not hasattr(question_set_module, "QuestionSetJsonReader")
    assert not hasattr(question_set_module, "load_question_set")
