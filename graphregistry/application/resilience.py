# graphregistry/application/resilience.py
"""Resilience utilities for application services.

These decorators live in the application layer because they orchestrate retry
policies around business operations, independent of any specific adapter.
"""
from __future__ import annotations
import functools
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar
from graphregistry.domain.exceptions import ConnectionExhaustedError, LockWaitTimeoutError

# Define generic type variables used by the retry decorator signature.
P = ParamSpec("P")
T = TypeVar("T")

# Domain errors that are considered transient and may succeed on retry.
_TRANSIENT_DB_ERRORS: tuple[type[Exception], ...] = (
    ConnectionExhaustedError,
    LockWaitTimeoutError,
)

#-----------------------------------------------------------------------#
# Public Method: Build a decorator that retries a function on transient #
#-----------------------------------------------------------------------#
def retry_on_transient_db_error(
    *,
    max_retries    : int = 3,
    retry_delay    : float = 1.0,
    backoff_factor : float = 2.0,
    on_retry       : Callable[[Exception, int], None] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
#-----------------------------------------------------------------------#
    """Retry a function when a transient database error occurs.

    The retry boundary is the decorated function call. Because a MySQL lock
    wait timeout rolls back the current transaction, the function must own its
    own UnitOfWork so the entire business operation can be replayed.

    Args:
        max_retries: Maximum number of retry attempts after the initial failure.
        retry_delay: Base delay in seconds before the first retry.
        backoff_factor: Multiplier applied to the delay between retries.
        on_retry: Optional callback invoked with (exception, attempt_number)
            before each retry.
    """

    # Validate configuration early to fail fast on bad inputs.
    def decorator(fn: Callable[P, T]) -> Callable[P, T]:

        # Preserve the original function's metadata on the wrapper.
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:

            # Track the last exception so we can re-raise it if retries run out.
            last_exception: Exception | None = None

            # Retry the decorated function up to max_retries times after the
            # initial attempt.
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except _TRANSIENT_DB_ERRORS as exc:
                    last_exception = exc
                    if attempt < max_retries:
                        wait = retry_delay * (backoff_factor ** attempt)
                        if on_retry is not None:
                            on_retry(exc, attempt + 1)
                        time.sleep(wait)
                        continue
                    raise

            # Guard against an unexpected loop exit by surfacing the last error.
            if last_exception is not None:
                raise last_exception
            raise RuntimeError("retry loop exited without result or exception")

        # Return the retry-aware wrapper to install on the decorated function.
        return wrapper

    # Return the decorator factory so it can be used with or without arguments.
    return decorator
