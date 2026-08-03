"""Import modules under stand-in dependencies so tests can run credential-free."""

import importlib
import sys
from contextlib import contextmanager

_MISSING = object()


@contextmanager
def stubbed_modules(stubs):
    """Install `stubs` in `sys.modules` for the duration of the block.

    Each stub is also set on its parent package, because `from pkg import mod` reads the
    attribute the parent already holds rather than `sys.modules`. On exit both `sys.modules`
    and the parent attributes are restored, including attributes the import machinery sets as
    a side effect while the stubs are in place.

    Args:
        stubs (dict): Mapping of dotted module path to stand-in module.
    """
    previous_modules = {name: sys.modules.get(name, _MISSING) for name in stubs}
    previous_attrs = {}
    for name in stubs:
        parent, attr = _parent_and_attr(name)
        if parent is not None:
            previous_attrs[(parent, attr)] = getattr(parent, attr, _MISSING)

    try:
        for name, stub in stubs.items():
            sys.modules[name] = stub
            parent, attr = _parent_and_attr(name)
            if parent is not None:
                setattr(parent, attr, stub)
        yield
    finally:
        for name, previous in previous_modules.items():
            if previous is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
        for (parent, attr), previous in previous_attrs.items():
            if previous is _MISSING:
                if hasattr(parent, attr):
                    delattr(parent, attr)
            else:
                setattr(parent, attr, previous)


@contextmanager
def reset_modules(*names):
    """Drop `names` from `sys.modules` for the duration of the block, restoring them on exit.

    Anything imported under those names inside the block is discarded, so each block gets a
    fresh import.

    Args:
        names (str): Dotted module paths to re-import from scratch.
    """
    previous_modules = {name: sys.modules.get(name, _MISSING) for name in names}
    for name in names:
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name, previous in previous_modules.items():
            sys.modules.pop(name, None)
            if previous is not _MISSING:
                sys.modules[name] = previous


@contextmanager
def imported_with_stubs(module_name, stubs):
    """Import `module_name` with `stubs` standing in for its dependencies.

    Args:
        module_name (str): Dotted path of the module under test.
        stubs (dict): Mapping of dotted module path to stand-in module.
    """
    with stubbed_modules(stubs), reset_modules(module_name):
        yield importlib.import_module(module_name)


def _parent_and_attr(name):
    """Return the parent package of dotted `name` and the attribute it is bound to.

    The parent is None for top-level modules and for packages that are not yet imported.

    Args:
        name (str): Dotted module path.
    """
    if "." not in name:
        return None, None
    parent_name, attr = name.rsplit(".", maxsplit=1)
    return sys.modules.get(parent_name), attr
