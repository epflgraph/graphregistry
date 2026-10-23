# graphregistry/tests/unit_tests/application/test_resilience.py
"""Unit tests for application-layer resilience utilities."""
from __future__ import annotations
from unittest.mock import MagicMock
import pytest
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.resilience import retry_on_transient_db_error
from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    DeadlockError,
    DuplicateKeyError,
    LockWaitTimeoutError,
    PersistenceError,
    RecordChangedError,
    TransientPersistenceError,
)
from graphregistry.domain.models.entities.mdl_node import NodeList
from tests.conftest import make_node

#==================#
# Class Definition #
#==================#
class TestRetryOnTransientDbError:
    """Tests for the retry_on_transient_db_error decorator."""

    # Public Method: The decorator returns immediately when the wrapped function succeeds.
    def test_succeeds_on_first_attempt(self) -> None:
        fn = MagicMock(return_value="ok")
        wrapped = retry_on_transient_db_error()(fn)
        assert wrapped("arg", kw="value") == "ok"
        fn.assert_called_once_with("arg", kw="value")

    # Public Method: The decorator retries on ConnectionExhaustedError and then succeeds.
    def test_retries_on_connection_exhausted(self) -> None:
        fn = MagicMock(side_effect=[
            ConnectionExhaustedError("too many connections"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=2, retry_delay=0.0)
        result = wrapped(fn)()
        assert result == "ok"
        assert fn.call_count == 2

    # Public Method: The decorator retries multiple times on LockWaitTimeoutError and then succeeds.
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

    # Public Method: The decorator re-raises the transient error after retries are exhausted.
    def test_reraises_after_exhaustion(self) -> None:
        fn = MagicMock(side_effect=ConnectionExhaustedError("too many connections"))
        wrapped = retry_on_transient_db_error(max_retries=1, retry_delay=0.0)(fn)
        with pytest.raises(ConnectionExhaustedError):
            wrapped()
        assert fn.call_count == 2  # initial + 1 retry

    # Public Method: The decorator does not retry non-transient persistence errors.
    def test_does_not_retry_non_transient_persistence_error(self) -> None:
        fn = MagicMock(side_effect=PersistenceError("syntax error"))
        wrapped = retry_on_transient_db_error(max_retries=3, retry_delay=0.0)(fn)
        with pytest.raises(PersistenceError):
            wrapped()
        fn.assert_called_once()

    # Public Method: The decorator retries on DeadlockError and then succeeds.
    def test_retries_on_deadlock(self) -> None:
        fn = MagicMock(side_effect=[
            DeadlockError("deadlock found"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=2, retry_delay=0.0)
        result = wrapped(fn)()
        assert result == "ok"
        assert fn.call_count == 2

    # Public Method: The decorator retries on RecordChangedError and then succeeds.
    def test_retries_on_record_changed(self) -> None:
        fn = MagicMock(side_effect=[
            RecordChangedError("record has changed since last read"),
            RecordChangedError("record has changed since last read"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=3, retry_delay=0.0)
        result = wrapped(fn)()
        assert result == "ok"
        assert fn.call_count == 3

    # Public Method: The decorator does not retry non-transient DuplicateKeyError.
    def test_does_not_retry_duplicate_key(self) -> None:
        fn = MagicMock(side_effect=DuplicateKeyError("duplicate entry"))
        wrapped = retry_on_transient_db_error(max_retries=3, retry_delay=0.0)(fn)
        with pytest.raises(DuplicateKeyError):
            wrapped()
        fn.assert_called_once()

    # Public Method: All transient persistence errors share the TransientPersistenceError marker.
    def test_transient_error_hierarchy(self) -> None:
        assert issubclass(DeadlockError, TransientPersistenceError)
        assert issubclass(RecordChangedError, TransientPersistenceError)
        assert issubclass(LockWaitTimeoutError, TransientPersistenceError)
        assert issubclass(ConnectionExhaustedError, TransientPersistenceError)
        assert issubclass(TransientPersistenceError, PersistenceError)
        assert not issubclass(DuplicateKeyError, TransientPersistenceError)

    # Public Method: The backoff wait is drawn from a full-jitter range capped by the base delay.
    def test_backoff_uses_full_jitter(self, monkeypatch) -> None:
        caps: list[float] = []

        # Internal Function: Capture the jitter upper bound instead of sleeping.
        def fake_uniform(low: float, high: float) -> float:
            caps.append(high)
            return low

        # Patch the jitter draw and the sleep so the test asserts on the
        # requested range rather than on wall-clock timing.
        monkeypatch.setattr("graphregistry.application.resilience.random.uniform", fake_uniform)
        monkeypatch.setattr("graphregistry.application.resilience.time.sleep", lambda _s: None)
        fn = MagicMock(side_effect=[
            DeadlockError("deadlock found"),
            DeadlockError("deadlock found"),
            "ok",
        ])
        wrapped = retry_on_transient_db_error(max_retries=2, retry_delay=1.0, backoff_factor=2.0)(fn)
        result = wrapped()
        assert result == "ok"
        assert caps == [1.0, 2.0]

    # Public Method: A production operation replays its whole UnitOfWork on a transient failure.
    def test_ops_level_retry_replays_uow(self, monkeypatch) -> None:
        attempts: list[int] = []

        #==================#
        # Class Definition #
        #==================#
        class FlakyNodeRepo:
            """Fake node repository that deadlocks on its first save_many call."""

            # Public Method: Fail the first save_many with a deadlock, then succeed.
            def save_many(self, node_list, actions=("commit",)):
                attempts.append(1)
                if len(attempts) == 1:
                    raise DeadlockError("deadlock found")
                return node_list

        #==================#
        # Class Definition #
        #==================#
        class FlakyUoW:
            """Fake unit of work exposing the flaky node repository."""

            # Public Method: Enter the unit-of-work context with a fresh flaky repository.
            def __enter__(self):
                self.nodes = FlakyNodeRepo()
                return self

            # Internal Method: Exit the unit-of-work context without persistence side effects.
            def __exit__(self, exc_type, exc_val, exc_tb):
                return None

        # Neutralize the retry backoff sleep so the wiring test runs instantly.
        monkeypatch.setattr("graphregistry.application.resilience.time.sleep", lambda _s: None)

        # Run the production operations against the flaky UoW and verify the replay.
        node_ops = NodeOperations(uow_factory=FlakyUoW)
        node_list = NodeList(item_list=[make_node(object_id="CS-433")])
        saved = node_ops.save_many(node_list)
        assert len(attempts) == 2  # first attempt deadlocked, second replayed
        assert saved.item_list == node_list.item_list
