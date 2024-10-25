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
        elapsed_time = int(time.time() - start_time)

        minutes = elapsed_time // 60
        seconds = elapsed_time % 60

        logger.info(
            f"Runtime of {func.__name__}: "
            + (f"{minutes}m" if minutes > 0 else "")
            + f"{seconds}s."
        )
        return result

    return wrapper
