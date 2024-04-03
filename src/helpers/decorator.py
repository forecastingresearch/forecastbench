"""Decorators."""

import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def log_runtime(func):
    """Print the runtime of a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Runtime of {func.__name__}: {int(end_time - start_time)} seconds")
        return result

    return wrapper
