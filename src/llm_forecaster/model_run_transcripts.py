"""Record model-run prompts and responses."""

import json
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from llm_forecaster import fb_model_runs
from llm_forecaster.forecast_variants import ForecastVariant


@dataclass(frozen=True)
class TranscriptUploadTarget:
    """Local transcript file and its remote destination name."""

    local_filename: Path
    destination_blob_name: str


class LLMCallTranscript:
    """Write LLM prompts and responses to transcript files."""

    def __init__(self, local_filename: str | Path) -> None:
        """Create a fresh transcript file for this runner invocation."""
        self.base_filename = Path(local_filename)
        self.markdown_filename = Path(f"{self.base_filename}.llm-calls.md")
        self.jsonl_filename = Path(f"{self.base_filename}.llm-calls.jsonl")
        self.local_filename = self.markdown_filename
        self._write_text_file(self.markdown_filename, "# LLM Call Transcript\n")
        self._write_text_file(self.jsonl_filename, "")
        self._lock = Lock()
        self._next_call_index = 1

    @staticmethod
    def _write_text_file(local_filename: str | Path, text: str) -> None:
        path = Path(local_filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    @staticmethod
    def _append_text_file(local_filename: str | Path, text: str) -> None:
        path = Path(local_filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(text)

    @staticmethod
    def _fenced_text(value: str | None) -> str:
        """Return text in a Markdown fence without escaping prompt newlines."""
        if value is None:
            value = ""
        fence = "```"
        while fence in value:
            fence += "`"
        return f"{fence}text\n{value}\n{fence}"

    def record(
        self,
        role: str,
        model_run: fb_model_runs.ModelRun,
        question: dict[str, Any],
        variant: ForecastVariant,
        prompt: str,
        expected_forecasts: int,
        response: str | None = None,
        error: str | None = None,
    ) -> None:
        """Append one completed LLM call to the transcript."""
        provider = getattr(model_run, "provider", None)

        with self._lock:
            call_index = self._next_call_index
            self._next_call_index += 1
            section = [
                "",
                f"## Call {call_index}: {role} ({variant.key})",
                "",
                f"- Provider: {getattr(provider, 'name', str(provider))}",
                f"- Model run key: {model_run.model_run_key}",
                f"- Model run slug: {model_run.slug}",
                f"- Provider model ID: {model_run.provider_model_id}",
                f"- Question Source: {question['source']}",
                f"- Question ID: {question['id']}",
                f"- Question URL: {question['url']}",
                f"- Variant: {variant.key}",
                f"- Expected forecasts: {expected_forecasts}",
                "",
                "### Prompt",
                "",
                self._fenced_text(prompt),
                "",
                "### Response",
                "",
                self._fenced_text(response),
            ]
            if error is not None:
                section.extend(
                    [
                        "",
                        "### Error",
                        "",
                        self._fenced_text(error),
                    ]
                )

            record = {
                "call_index": call_index,
                "role": role,
                "variant": variant.key,
                "provider": getattr(provider, "name", str(provider)),
                "lab": model_run.lab.name,
                "model_run_key": model_run.model_run_key,
                "model_run_slug": model_run.slug,
                "provider_model_id": model_run.provider_model_id,
                "question_source": question["source"],
                "question_id": question["id"],
                "question_url": question["url"],
                "expected_forecasts": expected_forecasts,
                "prompt": prompt,
                "response": response,
                "error": error,
            }

            self._append_text_file(self.markdown_filename, "\n".join(section) + "\n")
            self._append_text_file(
                self.jsonl_filename,
                f"{json.dumps(record, ensure_ascii=False)}\n",
            )


class TranscriptRecordingModelRun:
    """Wrap a model run so LLM calls are recorded."""

    def __init__(
        self,
        model_run: fb_model_runs.ModelRun,
        transcript: LLMCallTranscript,
        question: dict[str, Any],
        variant: ForecastVariant,
        role: str,
        expected_forecasts: int,
    ) -> None:
        """Store the call context used when recording wrapped model responses."""
        self._model_run = model_run
        self._transcript = transcript
        self._question = question
        self._variant = variant
        self._role = role
        self._expected_forecasts = expected_forecasts

    def __getattr__(self, name: str) -> Any:
        """Delegate model-run attributes to the wrapped model run."""
        return getattr(self._model_run, name)

    def get_response(self, prompt: str) -> str:
        """Request and record one model response."""
        try:
            response = self._model_run.get_response(prompt)
        except Exception as exc:
            self._transcript.record(
                role=self._role,
                model_run=self._model_run,
                question=self._question,
                variant=self._variant,
                prompt=prompt,
                error=f"{type(exc).__name__}: {exc}",
                expected_forecasts=self._expected_forecasts,
            )
            raise

        self._transcript.record(
            role=self._role,
            model_run=self._model_run,
            question=self._question,
            variant=self._variant,
            prompt=prompt,
            response=response,
            expected_forecasts=self._expected_forecasts,
        )
        return response
