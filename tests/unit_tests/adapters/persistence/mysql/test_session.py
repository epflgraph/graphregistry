# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_session.py
"""Unit tests for the MySQL session adapter error mapping."""
from __future__ import annotations
from unittest.mock import MagicMock
import pymysql.err as pymysql_err
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from graphregistry.adapters.persistence.mysql.session import MySQLSession, _map_sqlalchemy_error
from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    DeadlockError,
    DuplicateKeyError,
    LockWaitTimeoutError,
    PersistenceError,
    RecordChangedError,
    TransientPersistenceError,
)

# Messages mirroring the raw pymysql errors observed in production logs.
_DEADLOCK_MSG = "Deadlock found when trying to get lock; try restarting transaction"
_RECORD_CHANGED_MSG = "Record has changed since last read in table 'Nodes_N_Object'"

# Internal Function: Build a SQLAlchemy error wrapping a pymysql error with the given code.
def _sqlalchemy_error(code: int, message: str) -> OperationalError:
    orig = pymysql_err.OperationalError(code, message)
    return OperationalError("UPDATE `schema`.`Nodes_N_Object` SET record_deleted = 1", {}, orig)

#==================#
# Class Definition #
#==================#
class TestMapSqlalchemyError:
    """Tests for the SQLAlchemy-to-domain persistence error mapping."""

    # Public Method: A MySQL deadlock is mapped to the transient DeadlockError.
    def test_maps_deadlock_1213(self) -> None:
        mapped = _map_sqlalchemy_error(_sqlalchemy_error(1213, _DEADLOCK_MSG))
        assert isinstance(mapped, DeadlockError)
        assert isinstance(mapped, TransientPersistenceError)
        assert mapped.dbapi_code == 1213
        assert "Deadlock" in mapped.dbapi_msg

    # Public Method: A MySQL record-changed error maps to the transient RecordChangedError.
    def test_maps_record_changed_1020(self) -> None:
        mapped = _map_sqlalchemy_error(_sqlalchemy_error(1020, _RECORD_CHANGED_MSG))
        assert isinstance(mapped, RecordChangedError)
        assert isinstance(mapped, TransientPersistenceError)
        assert mapped.dbapi_code == 1020
        assert "Nodes_N_Object" in mapped.dbapi_msg

    # Public Method: A lock wait timeout keeps its existing transient mapping.
    def test_maps_lock_wait_timeout_1205(self) -> None:
        mapped = _map_sqlalchemy_error(_sqlalchemy_error(1205, "Lock wait timeout exceeded"))
        assert isinstance(mapped, LockWaitTimeoutError)
        assert isinstance(mapped, TransientPersistenceError)

    # Public Method: Connection exhaustion keeps its existing transient mapping.
    def test_maps_connection_exhausted_1040(self) -> None:
        mapped = _map_sqlalchemy_error(_sqlalchemy_error(1040, "Too many connections"))
        assert isinstance(mapped, ConnectionExhaustedError)
        assert isinstance(mapped, TransientPersistenceError)

    # Public Method: A duplicate key error is not classified as transient.
    def test_maps_duplicate_key_1062(self) -> None:
        mapped = _map_sqlalchemy_error(_sqlalchemy_error(1062, "Duplicate entry '1' for key 'PRIMARY'"))
        assert isinstance(mapped, DuplicateKeyError)
        assert not isinstance(mapped, TransientPersistenceError)

    # Public Method: An integrity error without a recognized code maps to DuplicateKeyError.
    def test_maps_integrity_error(self) -> None:
        orig = pymysql_err.IntegrityError(1451, "Cannot delete or update a parent row")
        exc = IntegrityError("DELETE FROM t WHERE id = 1", {}, orig)
        mapped = _map_sqlalchemy_error(exc)
        assert isinstance(mapped, DuplicateKeyError)
        assert not isinstance(mapped, TransientPersistenceError)

    # Public Method: A generic SQLAlchemy error falls back to the non-transient PersistenceError.
    def test_maps_generic_sqlalchemy_error(self) -> None:
        mapped = _map_sqlalchemy_error(SQLAlchemyError("something unexpected"))
        assert type(mapped) is PersistenceError
        assert not isinstance(mapped, TransientPersistenceError)

#==================#
# Class Definition #
#==================#
class TestMySQLSessionExecute:
    """Tests for error propagation through the session execute wrapper."""

    # Public Method: execute() maps a raw deadlock raised by the connection into DeadlockError.
    def test_execute_maps_deadlock_error(self) -> None:
        session = MySQLSession(MagicMock(), "main")
        session._connection = MagicMock()
        session._connection.execute.side_effect = _sqlalchemy_error(1213, _DEADLOCK_MSG)
        with pytest.raises(DeadlockError):
            session.execute("UPDATE `schema`.`Nodes_N_Object` SET record_deleted = 1")

    # Public Method: execute() maps a raw record-changed error into RecordChangedError.
    def test_execute_maps_record_changed_error(self) -> None:
        session = MySQLSession(MagicMock(), "main")
        session._connection = MagicMock()
        session._connection.execute.side_effect = _sqlalchemy_error(1020, _RECORD_CHANGED_MSG)
        with pytest.raises(RecordChangedError):
            session.execute("DELETE FROM `schema`.`Data_N_Object_T_CustomFields`")

