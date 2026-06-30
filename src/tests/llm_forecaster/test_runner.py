import ast
import inspect
import json
import logging
import threading
from types import SimpleNamespace

import pytest
from utils.llm.provider_registry import PROVIDERS

from _schemas import ForecastFrame
from llm_forecaster import model_run_transcripts, runner
from llm_forecaster.forecast_variants import (
    ZERO_SHOT,
    ZERO_SHOT_WITH_FREEZE_VALUES,
    DatasetForecastSharingVariantGroup,
)
from llm_forecaster.question_set import QuestionSet


class FakeRun:
    model_run_key = "test-model-run-variant-01"
    slug = "test-model"
    provider_model_id = "test-provider-model-id"
    lab = SimpleNamespace(name="Test Lab")
    provider = PROVIDERS["OpenAI"]

    def __init__(self):
        self.prompts = []

    def get_response(self, prompt):
        self.prompts.append(prompt)
        return "*0.4*"


class FakeExtractRun(FakeRun):
    model_run_key = "extract-model-run-variant-01"
    slug = "extract-model"
    provider_model_id = "extract-model-id"

    def get_response(self, prompt):
        self.prompts.append(prompt)
        return "[0.2, 0.3]"


class FakeMarketExtractRun(FakeRun):
    model_run_key = "extract-model-run-variant-01"
    slug = "extract-model"
    provider_model_id = "extract-model-id"

    def get_response(self, prompt):
        self.prompts.append(prompt)
        return "[0.4]"


class BlockingRun(FakeRun):
    provider = object()

    def __init__(self, expected_calls):
        super().__init__()
        self.expected_calls = expected_calls
        self.started_calls = 0
        self.lock = threading.Lock()
        self.all_started = threading.Event()

    def get_response(self, prompt):
        with self.lock:
            self.prompts.append(prompt)
            self.started_calls += 1
            if self.started_calls >= self.expected_calls:
                self.all_started.set()

        if not self.all_started.wait(timeout=1):
            raise AssertionError("questions were not forecast concurrently")
        return "*0.4*"


def _dataset_question():
    return {
        "id": "dataset-1",
        "source": "fred",
        "url": "https://example.com/dataset-1",
        "question": "Will value rise after {forecast_due_date} by {resolution_date}?",
        "background": "Dataset background",
        "resolution_criteria": "Dataset criteria",
        "market_info_resolution_criteria": "N/A",
        "freeze_datetime": "2026-05-05",
        "freeze_datetime_value": "100",
        "freeze_datetime_value_explanation": "Latest observed value.",
        "resolution_dates": ["2026-06-01", "2026-07-01"],
    }


def _market_question():
    return {
        "id": "market-1",
        "source": "metaculus",
        "url": "https://example.com/market-1",
        "question": "Will the market question resolve true?",
        "background": "Market background",
        "resolution_criteria": "Market criteria",
        "market_info_resolution_criteria": "N/A",
        "market_info_close_datetime": "2026-06-15",
        "freeze_datetime": "2026-05-05",
        "freeze_datetime_value": "0.33",
    }


def _question_set():
    return QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[_dataset_question(), _market_question()],
    )


def _transcript(tmp_path):
    return model_run_transcripts.LLMCallTranscript(tmp_path / "calls")


def _forecast_frame(rows):
    return runner._validated_forecast_frame_from_rows(rows)


def _forecast_records(forecast_frame):
    assert forecast_frame is not None
    return forecast_frame.to_dict(orient="records")


def test_runner_helpers_do_not_force_keyword_only_arguments():
    functions = (
        model_run_transcripts.LLMCallTranscript.record,
        model_run_transcripts.TranscriptRecordingModelRun.__init__,
        runner._prompt_params,
        runner.render_prompt,
        runner._forecast_dataset_questions,
        runner._forecast_market_questions,
        runner._sorted_forecast_rows,
        runner._validated_forecast_frame_from_rows,
    )

    offenders = []
    for function in functions:
        signature = inspect.signature(function)
        for parameter in signature.parameters.values():
            if parameter.kind == inspect.Parameter.KEYWORD_ONLY:
                offenders.append(f"{function.__qualname__}.{parameter.name}")

    assert offenders == []


def test_runner_helper_calls_pass_arguments_by_name():
    helper_names = {
        "LLMCallTranscript",
        "TranscriptRecordingModelRun",
        "_background",
        "_forecast_dataset_questions",
        "_forecast_market_questions",
        "_forecast_questions",
        "_formatted_question",
        "_llm_call_transcript_base_filename",
        "_max_workers_for_questions",
        "_prompt_params",
        "_prompt_template",
        "_sorted_forecast_rows",
        "_validated_forecast_frame_from_rows",
        "render_prompt",
    }
    tree = ast.parse(inspect.getsource(runner))
    offenders = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id in helper_names and node.args:
            offenders.append(node.func.id)

    assert offenders == []


def test_prompt_rendering_helpers_are_not_cached_by_question_phase():
    assert not hasattr(runner, "_render_dataset_prompts")
    assert not hasattr(runner, "_render_market_prompts")
    assert not hasattr(runner, "_render_prompts")

    dataset_prompt = runner.render_prompt(
        question=_dataset_question(),
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
    )
    zero_shot_market_prompt = runner.render_prompt(
        question=_market_question(),
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
    )
    freeze_market_prompt = runner.render_prompt(
        question=_market_question(),
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT_WITH_FREEZE_VALUES,
    )

    assert "Dataset background" in dataset_prompt
    assert "Market background" in zero_shot_market_prompt
    assert "Market value on 2026-05-05" not in zero_shot_market_prompt
    assert "Market value on 2026-05-05" in freeze_market_prompt


def test_prompt_template_maps_match_dataset_forecast_sharing_groups():
    dataset_prompt_variants = {
        variant_group.dataset_prompt_variant
        for variant_group in runner.DATASET_FORECAST_SHARING_VARIANT_GROUPS
    }
    output_variants = {
        variant
        for variant_group in runner.DATASET_FORECAST_SHARING_VARIANT_GROUPS
        for variant in variant_group.output_variants
    }

    assert set(runner.DATASET_PROMPTS_BY_VARIANT) == dataset_prompt_variants
    assert set(runner.MARKET_PROMPTS_BY_VARIANT) == output_variants


def test_dataset_prompt_selection_requires_dataset_prompt_variant():
    with pytest.raises(
        KeyError, match="No dataset prompt template for zero-shot-with-freeze-values"
    ):
        runner.render_prompt(
            question=_dataset_question(),
            forecast_due_date="2026-05-10",
            today_date="2026-05-06",
            variant=ZERO_SHOT_WITH_FREEZE_VALUES,
        )


def test_question_text_template_formats_forecast_date_and_generic_resolution_date():
    question = _dataset_question()

    formatted = runner._formatted_question(
        question=question,
        forecast_due_date="2026-05-10",
    )

    assert formatted == (
        "Will value rise after 2026-05-10 by each of the resolution dates provided below?"
    )


def test_dataset_questions_are_forecast_concurrently_and_returned_in_order(
    monkeypatch,
    tmp_path,
):
    first_question = _dataset_question()
    first_question["resolution_dates"] = ["2026-06-01"]
    second_question = {**_dataset_question(), "id": "dataset-2"}
    second_question["resolution_dates"] = ["2026-08-01"]
    model_run = BlockingRun(expected_calls=2)

    monkeypatch.setitem(runner.fb_model_runs.PROVIDER_MAX_WORKERS, model_run.provider, 2)
    monkeypatch.setattr(runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2])

    rows = runner._forecast_dataset_questions(
        model_run=model_run,
        dataset_questions=[first_question, second_question],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
        raise_on_question_error=True,
    )

    ForecastFrame.validate(rows)
    assert rows["id"].tolist() == ["dataset-1", "dataset-2"]
    assert rows["reasoning"].tolist() == ["", ""]


def test_dataset_questions_extract_unparseable_response(monkeypatch, tmp_path):
    class ProseRun(FakeRun):
        def get_response(self, prompt):
            self.prompts.append(prompt)
            return "Reasoning first.\n\n*0.4*\n*0.5*"

    forecast_extraction_run = FakeExtractRun()
    monkeypatch.setattr(runner.fb_model_runs, "FORECAST_EXTRACTION_MODEL", forecast_extraction_run)

    rows = runner._forecast_dataset_questions(
        model_run=ProseRun(),
        dataset_questions=[_dataset_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
        raise_on_question_error=True,
    )

    ForecastFrame.validate(rows)
    assert rows["forecast"].tolist() == [0.2, 0.3]
    assert len(forecast_extraction_run.prompts) == 1
    assert "Reasoning first." in forecast_extraction_run.prompts[0]
    assert "Dataset background" not in forecast_extraction_run.prompts[0]
    assert "MODEL RESPONSE" in forecast_extraction_run.prompts[0]
    assert "Do not make a forecast." in forecast_extraction_run.prompts[0]
    assert rows["reasoning"].tolist() == ["", ""]


def test_dataset_question_empty_response_does_not_call_extraction_model(monkeypatch, tmp_path):
    class EmptyRun(FakeRun):
        def get_response(self, prompt):
            self.prompts.append(prompt)
            return " \n\t"

    forecast_extraction_run = FakeExtractRun()
    monkeypatch.setattr(runner.fb_model_runs, "FORECAST_EXTRACTION_MODEL", forecast_extraction_run)
    transcript = _transcript(tmp_path)

    rows = runner._forecast_dataset_questions(
        model_run=EmptyRun(),
        dataset_questions=[_dataset_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=transcript,
        raise_on_question_error=False,
    )

    assert rows is None
    assert forecast_extraction_run.prompts == []
    transcript_text = transcript.markdown_filename.read_text()
    assert "## Call 1: forecast" in transcript_text
    assert "extract" not in transcript_text


def test_market_questions_extract_unparseable_response(monkeypatch, tmp_path):
    class ProseRun(FakeRun):
        def get_response(self, prompt):
            self.prompts.append(prompt)
            return "Answer: *0.4*"

    forecast_extraction_run = FakeMarketExtractRun()
    monkeypatch.setattr(runner.fb_model_runs, "FORECAST_EXTRACTION_MODEL", forecast_extraction_run)

    rows = runner._forecast_market_questions(
        model_run=ProseRun(),
        market_questions=[_market_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
        raise_on_question_error=True,
    )

    ForecastFrame.validate(rows)
    assert _forecast_records(rows) == [
        {
            "id": "market-1",
            "source": "metaculus",
            "forecast": 0.4,
            "resolution_date": None,
            "reasoning": "",
        }
    ]
    assert len(forecast_extraction_run.prompts) == 1
    assert "Answer: *0.4*" in forecast_extraction_run.prompts[0]
    assert "Market background" not in forecast_extraction_run.prompts[0]
    assert "MODEL RESPONSE" in forecast_extraction_run.prompts[0]
    assert "Do not make a forecast." in forecast_extraction_run.prompts[0]


def test_market_question_empty_response_does_not_call_extraction_model(monkeypatch, tmp_path):
    class EmptyRun(FakeRun):
        def get_response(self, prompt):
            self.prompts.append(prompt)
            return " \n\t"

    forecast_extraction_run = FakeMarketExtractRun()
    monkeypatch.setattr(runner.fb_model_runs, "FORECAST_EXTRACTION_MODEL", forecast_extraction_run)
    transcript = _transcript(tmp_path)

    rows = runner._forecast_market_questions(
        model_run=EmptyRun(),
        market_questions=[_market_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=transcript,
        raise_on_question_error=False,
    )

    assert rows is None
    assert forecast_extraction_run.prompts == []
    transcript_text = transcript.markdown_filename.read_text()
    assert "## Call 1: forecast" in transcript_text
    assert "extract" not in transcript_text


def test_dataset_questions_require_explicit_variant(tmp_path):
    with pytest.raises(TypeError, match="variant"):
        runner._forecast_dataset_questions(
            model_run=FakeRun(),
            dataset_questions=[_dataset_question()],
            forecast_due_date="2026-05-10",
            today_date="2026-05-06",
            transcript=_transcript(tmp_path),
        )


def test_market_questions_are_forecast_concurrently_and_returned_in_order(
    monkeypatch,
    tmp_path,
):
    first_question = _market_question()
    second_question = {**_market_question(), "id": "market-2"}
    model_run = BlockingRun(expected_calls=2)

    monkeypatch.setitem(runner.fb_model_runs.PROVIDER_MAX_WORKERS, model_run.provider, 2)
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)

    rows = runner._forecast_market_questions(
        model_run=model_run,
        market_questions=[first_question, second_question],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
        raise_on_question_error=True,
    )

    ForecastFrame.validate(rows)
    assert rows["id"].tolist() == ["market-1", "market-2"]
    assert rows["reasoning"].tolist() == ["", ""]


def test_unknown_provider_uses_default_max_workers():
    unknown_provider_run = SimpleNamespace(provider=object())

    assert (
        runner._max_workers_for_questions(
            model_run=unknown_provider_run,
            question_count=10,
        )
        == runner.fb_model_runs.DEFAULT_PROVIDER_MAX_WORKERS
    )


def test_market_parse_miss_skips_question_by_default(monkeypatch, caplog, tmp_path):
    def raise_parse_error(*args, **kwargs):
        raise ValueError("Expected 1 market forecast, got 0")

    monkeypatch.setattr(runner.parsing, "parse_market_forecast", raise_parse_error)
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    rows = runner._forecast_market_questions(
        model_run=FakeRun(),
        market_questions=[_market_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
    )

    assert rows is None
    assert "LLM response before error for market-1:\n*0.4*" in caplog.text


def test_question_error_logs_empty_response_unambiguously(caplog):
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    runner._handle_question_error(
        question={"id": "empty-response-1"},
        raise_on_question_error=False,
        response="",
    )

    assert "LLM response before error for empty-response-1:\n''" in caplog.text


def test_market_parse_miss_raises_when_fail_fast(monkeypatch, caplog, tmp_path):
    def raise_parse_error(*args, **kwargs):
        raise ValueError("Expected 1 market forecast, got 0")

    monkeypatch.setattr(runner.parsing, "parse_market_forecast", raise_parse_error)
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    with pytest.raises(ValueError, match="Expected 1 market forecast, got 0"):
        runner._forecast_market_questions(
            model_run=FakeRun(),
            market_questions=[_market_question()],
            forecast_due_date="2026-05-10",
            today_date="2026-05-06",
            variant=ZERO_SHOT,
            transcript=_transcript(tmp_path),
            raise_on_question_error=True,
        )

    assert "LLM response before error for market-1:\n*0.4*" in caplog.text


def test_validated_forecast_frame_from_rows_returns_none_for_empty_rows():
    assert runner._validated_forecast_frame_from_rows([]) is None


def test_iter_model_forecasts_yields_zero_shot_before_freeze_values(monkeypatch, tmp_path):
    events = []
    original_forecast_market_questions = runner._forecast_market_questions

    monkeypatch.setattr(
        runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2, 0.3]
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)

    def fake_forecast_market_questions(*args, **kwargs):
        variant = kwargs["variant"]
        if variant == ZERO_SHOT_WITH_FREEZE_VALUES:
            events.append(("freeze-started", variant.key))
            raise RuntimeError("freeze failed")
        return original_forecast_market_questions(*args, **kwargs)

    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    forecast_results = runner.iter_model_forecasts(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
        raise_on_question_error=True,
    )
    zero_shot_result = next(forecast_results)
    events.append(("yield", zero_shot_result.variant.key))

    with pytest.raises(RuntimeError, match="freeze failed"):
        next(forecast_results)

    assert events[:2] == [
        ("yield", "zero-shot"),
        ("freeze-started", "zero-shot-with-freeze-values"),
    ]


def test_run_model_logs_forecast_result_before_later_variant_fails(
    monkeypatch,
    tmp_path,
    caplog,
):
    original_forecast_market_questions = runner._forecast_market_questions

    monkeypatch.setattr(
        runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2, 0.3]
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)
    caplog.set_level(logging.INFO, logger=runner.logger.name)

    def fake_forecast_market_questions(*args, **kwargs):
        variant = kwargs["variant"]
        if variant == ZERO_SHOT_WITH_FREEZE_VALUES:
            raise RuntimeError("freeze failed")
        return original_forecast_market_questions(*args, **kwargs)

    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    with pytest.raises(RuntimeError, match="freeze failed"):
        runner.run_model(
            FakeRun(),
            _question_set(),
            tmp_path,
            is_test=True,
            today_date="2026-05-06",
            raise_on_question_error=True,
        )

    assert "Generated LLM forecasts variant=zero-shot" in caplog.text
    assert "forecast_file=" not in caplog.text
    assert "zero-shot-with-freeze-values" not in caplog.text


def test_run_model_logs_question_progress_by_forecast_phase(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(
        runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2, 0.3]
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)
    caplog.set_level(logging.INFO, logger=runner.logger.name)

    runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=False,
        today_date="2026-05-06",
    )

    assert [
        record.message
        for record in caplog.records
        if record.message.startswith(("LLM forecast phase ", "LLM forecast progress "))
    ] == [
        "LLM forecast phase dataset starting: 1 question(s).",
        "LLM forecast progress phase=dataset completed=1/1 question_id=dataset-1",
        "LLM forecast phase zero-shot starting: 1 question(s).",
        "LLM forecast progress phase=zero-shot completed=1/1 question_id=market-1",
        "LLM forecast phase zero-shot-with-freeze-values starting: 1 question(s).",
        (
            "LLM forecast progress phase=zero-shot-with-freeze-values "
            "completed=1/1 question_id=market-1"
        ),
    ]


def test_dataset_rows_are_reused_across_variants(monkeypatch, tmp_path):
    calls = {"dataset": 0}
    dataset_row = {
        "id": "dataset-1",
        "source": "fred",
        "forecast": 0.2,
        "resolution_date": "2026-06-01",
        "reasoning": "",
    }

    def fake_forecast_dataset_questions(*args, **kwargs):
        calls["dataset"] += 1
        return _forecast_frame([dataset_row])

    def fake_forecast_market_questions(*args, **kwargs):
        variant = kwargs["variant"]
        return _forecast_frame(
            [
                {
                    "id": variant.key,
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
            ]
        )

    monkeypatch.setattr(runner, "_forecast_dataset_questions", fake_forecast_dataset_questions)
    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    written_files = runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert [written_file.variant for written_file in written_files] == [
        ZERO_SHOT,
        ZERO_SHOT_WITH_FREEZE_VALUES,
    ]
    assert calls == {"dataset": 1}
    assert _forecast_records(written_files[0].rows)[0] == dataset_row
    assert _forecast_records(written_files[1].rows)[0] == dataset_row


def test_dataset_rows_are_reused_within_each_variant_group(monkeypatch, tmp_path):
    dataset_calls = []

    def fake_forecast_dataset_questions(*args, **kwargs):
        variant = kwargs["variant"]
        dataset_calls.append(variant)
        return _forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": len(dataset_calls) / 10,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                }
            ]
        )

    def fake_forecast_market_questions(*args, **kwargs):
        variant = kwargs["variant"]
        return _forecast_frame(
            [
                {
                    "id": variant.key,
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
            ]
        )

    monkeypatch.setattr(
        runner,
        "DATASET_FORECAST_SHARING_VARIANT_GROUPS",
        (
            DatasetForecastSharingVariantGroup(
                dataset_prompt_variant=ZERO_SHOT,
                output_variants=(ZERO_SHOT, ZERO_SHOT_WITH_FREEZE_VALUES),
            ),
        ),
    )
    monkeypatch.setattr(runner, "_forecast_dataset_questions", fake_forecast_dataset_questions)
    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    written_files = runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert dataset_calls == [ZERO_SHOT]
    assert [
        _forecast_records(written_file.rows)[0]["forecast"] for written_file in written_files
    ] == [
        0.1,
        0.1,
    ]


def test_run_model_does_not_expose_timing_recorder_argument():
    assert "timing_recorder" not in inspect.signature(runner.run_model).parameters
    assert "timing_recorder" not in inspect.signature(runner.iter_model_forecasts).parameters


def test_run_model_logs_successful_forecast_counts(monkeypatch, tmp_path, caplog):
    first_dataset_question = _dataset_question()
    second_dataset_question = {**_dataset_question(), "id": "dataset-2"}
    second_dataset_question["resolution_dates"] = [
        "2026-08-01",
        "2026-09-01",
        "2026-10-01",
    ]
    second_market_question = {**_market_question(), "id": "market-2"}
    question_set = QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[
            first_dataset_question,
            second_dataset_question,
            _market_question(),
            second_market_question,
        ],
    )

    def fake_forecast_dataset_questions(*args, **kwargs):
        return _forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.2,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                },
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.3,
                    "resolution_date": "2026-07-01",
                    "reasoning": "",
                },
                {
                    "id": "dataset-2",
                    "source": "fred",
                    "forecast": 0.4,
                    "resolution_date": "2026-08-01",
                    "reasoning": "",
                },
            ]
        )

    def fake_forecast_market_questions(*args, **kwargs):
        variant = kwargs["variant"]
        if variant == ZERO_SHOT:
            market_question_ids = ["market-1"]
        else:
            market_question_ids = ["market-1", "market-2"]
        return _forecast_frame(
            [
                {
                    "id": question_id,
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
                for question_id in market_question_ids
            ]
        )

    monkeypatch.setattr(runner, "_forecast_dataset_questions", fake_forecast_dataset_questions)
    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)
    caplog.set_level(logging.INFO, logger=runner.logger.name)

    runner.run_model(
        FakeRun(),
        question_set,
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert [
        record.message
        for record in caplog.records
        if record.message.startswith("LLM forecast success ")
    ] == [
        "LLM forecast success zero shot: 1/2",
        "LLM forecast success zero shot with freeze values: 2/2",
        "LLM forecast success dataset: 3/5",
    ]


def test_run_model_fails_when_dataset_forecasts_are_empty(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(runner, "_forecast_dataset_questions", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="No dataset forecasts were produced"):
        runner.run_model(
            FakeRun(),
            _question_set(),
            tmp_path,
            is_test=True,
            today_date="2026-05-06",
        )


def test_run_model_fails_when_market_forecasts_are_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runner,
        "_forecast_dataset_questions",
        lambda *args, **kwargs: _forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.2,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        runner,
        "_forecast_market_questions",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(
        RuntimeError,
        match="No market forecasts were produced for variant zero-shot",
    ):
        runner.run_model(
            FakeRun(),
            _question_set(),
            tmp_path,
            is_test=True,
            today_date="2026-05-06",
        )


def test_run_model_sorts_dataset_and_market_rows_before_returning(monkeypatch, tmp_path):
    def fake_forecast_dataset_questions(*args, **kwargs):
        return _forecast_frame(
            [
                {
                    "id": "b",
                    "source": "fred",
                    "forecast": 0.2,
                    "resolution_date": "2026-07-01",
                    "reasoning": "",
                },
                {
                    "id": "a",
                    "source": "acled",
                    "forecast": 0.3,
                    "resolution_date": "2026-08-01",
                    "reasoning": "",
                },
                {
                    "id": "a",
                    "source": "acled",
                    "forecast": 0.4,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                },
            ]
        )

    def fake_forecast_market_questions(*args, **kwargs):
        return _forecast_frame(
            [
                {
                    "id": "b",
                    "source": "metaculus",
                    "forecast": 0.5,
                    "resolution_date": None,
                    "reasoning": "",
                },
                {
                    "id": "a",
                    "source": "manifold",
                    "forecast": 0.6,
                    "resolution_date": None,
                    "reasoning": "",
                },
            ]
        )

    monkeypatch.setattr(runner, "_forecast_dataset_questions", fake_forecast_dataset_questions)
    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    written_file = runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )[0]

    assert [
        (row["source"], row["id"], row["resolution_date"])
        for row in _forecast_records(written_file.rows)
    ] == [
        ("acled", "a", "2026-06-01"),
        ("acled", "a", "2026-08-01"),
        ("fred", "b", "2026-07-01"),
        ("manifold", "a", None),
        ("metaculus", "b", None),
    ]


def test_runner_does_not_expose_final_forecast_file_persistence_helpers():
    assert not hasattr(runner, "WrittenForecastFile")
    assert not hasattr(runner, "_write_final_file")
    assert not hasattr(runner, "final_forecast_set_destination_blob_names")


def test_run_model_does_not_preload_prompts(monkeypatch, tmp_path):
    def fail_render_prompt(*args, **kwargs):
        raise AssertionError("run_model should not render prompts before forecasting questions")

    def fake_forecast_dataset_questions(*args, **kwargs):
        return _forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.1,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                }
            ]
        )

    def fake_forecast_market_questions(*args, **kwargs):
        return _forecast_frame(
            [
                {
                    "id": "market-1",
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
            ]
        )

    monkeypatch.setattr(runner, "render_prompt", fail_render_prompt)
    monkeypatch.setattr(runner, "_forecast_dataset_questions", fake_forecast_dataset_questions)
    monkeypatch.setattr(runner, "_forecast_market_questions", fake_forecast_market_questions)

    written_files = runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert [written_file.variant for written_file in written_files] == [
        ZERO_SHOT,
        ZERO_SHOT_WITH_FREEZE_VALUES,
    ]


def test_run_model_writes_test_llm_call_transcript(monkeypatch, tmp_path):
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)
    monkeypatch.setattr(runner.fb_model_runs, "FORECAST_EXTRACTION_MODEL", FakeExtractRun())
    question_set = _question_set()
    question_set.questions[0]["url"] = "https://example.com/dataset-question"
    question_set.questions[1]["url"] = "https://example.com/market-question"

    runner.run_model(
        FakeRun(),
        question_set,
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    transcript_files = list(tmp_path.glob("TEST.*.llm-calls.md"))
    assert len(transcript_files) == 1
    transcript = transcript_files[0].read_text(encoding="utf-8")

    assert transcript.startswith("# LLM Call Transcript\n")
    assert transcript.count("## Call ") == 4
    assert "## Call 1: forecast (zero-shot)" in transcript
    assert "- Model run slug: test-model" in transcript
    assert "- Model run key: test-model-run-variant-01" in transcript
    assert "- Question Source: fred" in transcript
    assert "- Question ID: dataset-1" in transcript
    assert "- Question URL: https://example.com/dataset-question" in transcript
    assert "- Question URL: https://example.com/market-question" in transcript
    assert "- Variant: zero-shot-with-freeze-values" in transcript
    assert "- Expected forecasts: 2" in transcript
    assert "- Expected forecasts: 1" in transcript
    assert "- Role:" not in transcript
    assert "- Model run slug: extract-model" in transcript
    assert "- Model run key: extract-model-run-variant-01" in transcript
    assert "### Prompt\n\n```text\n" in transcript
    assert "Dataset background" in transcript
    assert "Market value on 2026-05-05" in transcript
    assert "Extract only probabilities explicitly stated in the MODEL RESPONSE." in transcript
    assert "MODEL RESPONSE:\n```text\n*0.4*\n```" in transcript
    assert "### Response\n\n```text\n*0.4*\n```" in transcript
    assert "### Response\n\n```text\n[0.2, 0.3]\n```" in transcript

    jsonl_files = list(tmp_path.glob("TEST.*.llm-calls.jsonl"))
    assert len(jsonl_files) == 1
    records = [json.loads(line) for line in jsonl_files[0].read_text(encoding="utf-8").splitlines()]
    assert len(records) == 4
    assert records[0]["call_index"] == 1
    assert records[0]["role"] == "forecast"
    assert records[0]["variant"] == "zero-shot"
    assert records[0]["provider"] == "OpenAI"
    assert records[0]["model_run_key"] == "test-model-run-variant-01"
    assert records[0]["model_run_slug"] == "test-model"
    assert records[0]["question_source"] == "fred"
    assert records[0]["question_id"] == "dataset-1"
    assert records[0]["question_url"] == "https://example.com/dataset-question"
    assert records[0]["expected_forecasts"] == 2
    assert records[0]["prompt"]
    assert records[0]["response"] == "*0.4*"
    assert records[0]["error"] is None
    assert records[1]["expected_forecasts"] == 2
    assert records[2]["expected_forecasts"] == 1
    assert records[2]["question_url"] == "https://example.com/market-question"


def test_llm_call_transcript_writes_local_markdown_and_jsonl_files(tmp_path):
    path = tmp_path / "calls"

    transcript = model_run_transcripts.LLMCallTranscript(path)
    transcript.record(
        role="forecast",
        model_run=FakeRun(),
        question={"id": "q1", "source": "fred", "url": "https://example.com/q1"},
        variant=ZERO_SHOT,
        prompt="Prompt",
        response="*0.4*",
        expected_forecasts=1,
    )

    markdown_path = tmp_path / "calls.llm-calls.md"
    jsonl_path = tmp_path / "calls.llm-calls.jsonl"
    markdown = markdown_path.read_text(encoding="utf-8")
    jsonl = jsonl_path.read_text(encoding="utf-8")
    assert markdown.startswith("# LLM Call Transcript\n")
    assert "## Call 1: forecast (zero-shot)" in markdown
    assert "- Question URL: https://example.com/q1" in markdown
    assert "- Expected forecasts: 1" in markdown
    assert json.loads(jsonl)["response"] == "*0.4*"
    assert json.loads(jsonl)["question_url"] == "https://example.com/q1"
    assert json.loads(jsonl)["expected_forecasts"] == 1


def test_llm_call_transcript_requires_question_url(tmp_path):
    transcript = model_run_transcripts.LLMCallTranscript(tmp_path / "calls")

    with pytest.raises(KeyError, match="url"):
        transcript.record(
            role="forecast",
            model_run=FakeRun(),
            question={"id": "q1", "source": "fred"},
            variant=ZERO_SHOT,
            prompt="Prompt",
            response="*0.4*",
            expected_forecasts=1,
        )


def test_llm_call_transcript_owns_file_write_helpers():
    assert not hasattr(model_run_transcripts, "_write_text_file")
    assert not hasattr(model_run_transcripts, "_append_text_file")
    assert isinstance(
        inspect.getattr_static(
            model_run_transcripts.LLMCallTranscript,
            "_write_text_file",
        ),
        staticmethod,
    )
    assert isinstance(
        inspect.getattr_static(
            model_run_transcripts.LLMCallTranscript,
            "_append_text_file",
        ),
        staticmethod,
    )


def test_model_run_transcripts_does_not_expose_upload_target_builder():
    assert not hasattr(model_run_transcripts, "llm_call_transcript_upload_targets")


def test_run_model_writes_llm_call_transcript_in_prod_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2, 0.3]
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)

    runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=False,
        today_date="2026-05-06",
    )

    transcript_files = list(tmp_path.glob("*.llm-calls.md"))
    assert len(transcript_files) == 1
    assert transcript_files[0].name == (
        "2026-05-10.ForecastBench.OpenAI.Test Lab." "test-model-run-variant-01.llm-calls.md"
    )


def test_run_model_does_not_write_local_final_files(
    monkeypatch,
    tmp_path,
):
    existing_paths = [
        tmp_path / runner.output.final_filename("2026-05-10", FakeRun(), variant, is_test=True)
        for variant in [ZERO_SHOT, ZERO_SHOT_WITH_FREEZE_VALUES]
    ]
    for existing_path in existing_paths:
        existing_path.write_text(f"existing {existing_path.name}", encoding="utf-8")

    monkeypatch.setattr(
        runner.parsing, "parse_dataset_forecast", lambda *args, **kwargs: [0.2, 0.3]
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)

    forecast_results = runner.run_model(
        FakeRun(),
        _question_set(),
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert len(forecast_results) == 2
    for existing_path in existing_paths:
        assert existing_path.read_text(encoding="utf-8") == f"existing {existing_path.name}"
    assert list(tmp_path.glob("*.llm-calls.md")) != []


def test_run_model_fails_when_dataset_forecast_result_is_none(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        runner,
        "_forecast_dataset_questions",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        runner,
        "_forecast_market_questions",
        lambda *args, **kwargs: _forecast_frame(
            [
                {
                    "id": "market-1",
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="No dataset forecasts were produced"):
        runner.run_model(
            FakeRun(),
            _question_set(),
            tmp_path,
            is_test=True,
            today_date="2026-05-06",
        )


def test_missing_required_dataset_prompt_field_skips_question_and_keeps_forecasting(
    monkeypatch,
    tmp_path,
    caplog,
):
    malformed_question = _dataset_question()
    del malformed_question["background"]
    valid_question = {**_dataset_question(), "id": "dataset-2"}
    context = QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[malformed_question, valid_question, _market_question()],
    )

    monkeypatch.setattr(
        runner.parsing,
        "parse_dataset_forecast",
        lambda *args, **kwargs: [0.2, 0.3],
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    written_files = runner.run_model(
        FakeRun(),
        context,
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    assert "Skipping LLM forecast question after error: dataset-1" in caplog.text
    assert [written_file.variant for written_file in written_files] == [
        ZERO_SHOT,
        ZERO_SHOT_WITH_FREEZE_VALUES,
    ]
    first_result_records = [
        record for record in _forecast_records(written_files[0].rows) if record["source"] == "fred"
    ]
    assert all(row["id"] == "dataset-2" for row in first_result_records)
    assert [row["forecast"] for row in first_result_records] == [0.2, 0.3]


def test_market_question_missing_freeze_value_preserves_zero_shot_result(monkeypatch, tmp_path):
    malformed_question = _market_question()
    del malformed_question["freeze_datetime_value"]
    context = QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[_dataset_question(), malformed_question],
    )

    monkeypatch.setattr(
        runner,
        "_forecast_dataset_questions",
        lambda *args, **kwargs: _forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.2,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                }
            ]
        ),
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)
    model_run = FakeRun()

    forecast_results = runner.iter_model_forecasts(
        model_run,
        context,
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )
    zero_shot_result = next(forecast_results)

    with pytest.raises(
        RuntimeError,
        match="No market forecasts were produced for variant zero-shot-with-freeze-values",
    ):
        next(forecast_results)

    assert len(model_run.prompts) == 1
    assert zero_shot_result.variant == ZERO_SHOT


def test_llm_call_transcript_upload_targets_match_written_transcript_files(
    monkeypatch,
    tmp_path,
):
    context = QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[_dataset_question(), _market_question()],
    )

    monkeypatch.setattr(
        runner.parsing,
        "parse_dataset_forecast",
        lambda *args, **kwargs: [0.2, 0.3],
    )
    monkeypatch.setattr(runner.parsing, "parse_market_forecast", lambda *args, **kwargs: 0.4)

    runner.run_model(
        FakeRun(),
        context,
        tmp_path,
        is_test=True,
        today_date="2026-05-06",
    )

    targets = runner.llm_call_transcript_upload_targets(
        forecast_due_date="2026-05-10",
        model_run=FakeRun(),
        output_dir=tmp_path,
        is_test=True,
    )
    assert [target.local_filename.exists() for target in targets] == [True, True]
    assert [target.destination_blob_name for target in targets] == [
        (
            "2026-05-10/test/"
            "TEST.2026-05-10.ForecastBench.OpenAI.Test Lab."
            "test-model-run-variant-01.llm-calls.md"
        ),
        (
            "2026-05-10/test/"
            "TEST.2026-05-10.ForecastBench.OpenAI.Test Lab."
            "test-model-run-variant-01.llm-calls.jsonl"
        ),
    ]


@pytest.mark.parametrize("forecasts", [[0.2], [0.2, 0.3, 0.4]])
def test_dataset_forecast_length_mismatch_skips_whole_question_by_default(
    monkeypatch,
    caplog,
    forecasts,
    tmp_path,
):
    monkeypatch.setattr(
        runner.parsing,
        "parse_dataset_forecast",
        lambda *args, **kwargs: forecasts,
    )
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    rows = runner._forecast_dataset_questions(
        model_run=FakeRun(),
        dataset_questions=[_dataset_question()],
        forecast_due_date="2026-05-10",
        today_date="2026-05-06",
        variant=ZERO_SHOT,
        transcript=_transcript(tmp_path),
    )

    assert rows is None
    assert "LLM response before error for dataset-1:\n*0.4*" in caplog.text


@pytest.mark.parametrize("forecasts", [[0.2], [0.2, 0.3, 0.4]])
def test_dataset_forecast_length_mismatch_raises_when_fail_fast(
    monkeypatch,
    caplog,
    forecasts,
    tmp_path,
):
    monkeypatch.setattr(
        runner.parsing,
        "parse_dataset_forecast",
        lambda *args, **kwargs: forecasts,
    )
    caplog.set_level(logging.ERROR, logger=runner.logger.name)

    with pytest.raises(ValueError, match="Expected 2 dataset forecasts"):
        runner._forecast_dataset_questions(
            model_run=FakeRun(),
            dataset_questions=[_dataset_question()],
            forecast_due_date="2026-05-10",
            today_date="2026-05-06",
            variant=ZERO_SHOT,
            transcript=_transcript(tmp_path),
            raise_on_question_error=True,
        )

    assert "LLM response before error for dataset-1:\n*0.4*" in caplog.text
