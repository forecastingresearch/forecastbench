"""Question metadata: categorise (tag) and validate the questions pulled into the question bank."""

import importlib

__all__ = ["tag"]

# Each job ships only its own requirements, so the package must not import every submodule on
# `import metadata`: the validate job has no Jev SDK, which `_tag` imports at module level.
# TODO(python 3.15): replace with `lazy from ._tag import tag` (PEP 810).
_EXPORTS = {
    "tag": "._tag",
}


def __getattr__(name: str):
    """Import the submodule that defines `name` on first access."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
