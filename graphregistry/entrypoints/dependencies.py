# graphregistry/entrypoints/dependencies.py
"""Shared dependency builders for API and CLI entrypoints.

These functions wire concrete adapters (MySQL UnitOfWork, repositories) without
leaking adapter details into application services. They live in the entrypoints
layer because they know about config paths, environment variables, and concrete
implementations.
"""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Callable
from graphdb.client import Environments
from graphdb.domain.models.mdl_config import GraphDBConfig
from graphregistry.adapters.persistence.mysql.databaseclient import MySQLDatabaseClient
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork
from graphregistry.application.ports.repositories.prt_databaseclient import DatabaseClient
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.common.config import GlobalConfig

#================================================================#
# Function Group: Database client builders                       #
#================================================================#

# Internal Function: Cache the Environments composition point process-wide.
_ENVIRONMENTS: Environments | None = None

# Public Method: Create the single database client for this process.
def build_db(config_path: Path | str | None = None, *, config: "GraphDBConfig | None" = None) -> DatabaseClient:
    """Create the single database client for this process.

    The client wraps the graphdb-client Environments composition point, whose
    per-environment gateways are built lazily on first access and cached, so
    repeated calls reuse the same engine pools instead of duplicating them.
    """
    global _ENVIRONMENTS

    # Reject ambiguous configuration sources.
    if config is not None and config_path is not None:
        raise ValueError("Provide either config_path= or config=, not both.")

    # Load the configuration from disk when it was not passed explicitly.
    if config is None:
        if config_path is None:
            from graphregistry.common.paths import CONFIG_DB_PATH

            # Fall back to the default database configuration path.
            config_path = CONFIG_DB_PATH
        config = GraphDBConfig.from_file(str(config_path))

    # Build the Environments composition point once per process.
    if _ENVIRONMENTS is None:
        _ENVIRONMENTS = Environments(config)

    # Return the client adapter over the shared composition point.
    return MySQLDatabaseClient(_ENVIRONMENTS)

#================================================================#
# Function Group: Unit of Work factory builders                  #
#================================================================#

# Internal Function: Cache schema resolvers because they are stateless.
@lru_cache(maxsize=8)
def _schema_resolver(engine_name: str) -> DefaultSchemaResolver:
    """Cache schema resolvers because they are stateless.

    The cache is bounded to a small number of environments to avoid unbounded
    growth if many engine names are used in tests.
    """
    return DefaultSchemaResolver(engine_name=engine_name, glbcfg=GlobalConfig())

# Public Method: Return a factory that creates a fresh UnitOfWork for engine_name.
def build_uow_factory(db: GraphDB, engine_name: str, verbose: bool = False) -> Callable[[], UnitOfWork]:
    """Return a factory that creates a fresh UnitOfWork for engine_name."""
    schema_resolver = _schema_resolver(engine_name)

    # Internal Function: factory
    def _factory() -> UnitOfWork:
        return MySQLUnitOfWork(db=db, schema_resolver=schema_resolver, verbose=verbose)

    # Return the closure that creates a fresh UnitOfWork per transaction.
    return _factory
