# tests/unit_tests/application/test_resilience.py
"""Unit tests for application-layer resilience utilities."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from graphregistry.application.resilience import retry_on_transient_db_error
from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    LockWaitTimeoutError,
    PersistenceError,
)


class TestRetryOnTransientDbError:
    def test_succeeds_on_first_attempt(self) -> None:
        fn = MagicMock(return_value="ok")
        wrapped = retry_on_transient_db_error()(fn)

        assert wrapped("arg", kw="value") == "ok"
        fn.assert_called_once_with("arg", kw="value")

    def test_retries_on_connection_exhausted(self) -> None:
        fn = MagicMock(side_effect=[
            ConnectionExhaustedError("too many connections"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=2, retry_delay=0.0)
        result = wrapped(fn)()

        assert result == "ok"
        assert fn.call_count == 2

    def test_retries_on_lock_wait_timeout(self) -> None:
        fn = MagicMock(side_effect=[
            LockWaitTimeoutError("lock wait timeout"),
            LockWaitTimeoutError("lock wait timeout"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=3, retry_delay=0.0)
        result = wrapped(fn)()

        assert result == "ok"
        assert fn.call_count == 3

    def test_reraises_after_exhaustion(self) -> None:
        fn = MagicMock(side_effect=ConnectionExhaustedError("too many connections"))
        wrapped = retry_on_transient_db_error(max_retries=1, retry_delay=0.0)(fn)

        with pytest.raises(ConnectionExhaustedError):
            wrapped()

        assert fn.call_count == 2  # initial + 1 retry

    def test_does_not_retry_non_transient_persistence_error(self) -> None:
        fn = MagicMock(side_effect=PersistenceError("syntax error"))
        wrapped = retry_on_transient_db_error(max_retries=3, retry_delay=0.0)(fn)

        with pytest.raises(PersistenceError):
            wrapped()

        fn.assert_called_once()
