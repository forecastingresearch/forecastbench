"""Run-mode parsing and behavior."""

from enum import Enum


class RunMode(str, Enum):
    """Run modes for code execution.

    - TEST: Test/dev runs; use to reduce costs when running models.
    - PROD: Full production runs; execute all models with full question set.

    Construction is case-insensitive (e.g., RunMode("teST") --> RunMode.TEST).
    Invalid values raise ValueError.
    """

    TEST = "TEST"
    PROD = "PROD"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            return cls.__members__.get(value.upper())
        return None

    @classmethod
    def from_string(cls, value: str | None) -> "RunMode":
        """Parse a run mode string, defaulting to TEST for missing or invalid values."""
        try:
            return cls(value)
        except ValueError:
            return cls.TEST

    @property
    def is_test(self) -> bool:
        """Return whether this mode should run test-sized workloads."""
        return self is RunMode.TEST

    @property
    def is_prod(self) -> bool:
        """Return whether this mode should run production workloads."""
        return self is RunMode.PROD

    @property
    def output_file_prefix(self) -> str:
        """Return the file prefix for outputs written in this run mode."""
        if self.is_test:
            return f"{self.value}."
        return ""
