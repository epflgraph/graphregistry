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

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.common.config import GlobalConfig


#================================================================#
# Function Group: Database client builders                       #
#================================================================#

# Function: Create the single GraphDB client for this process.
def build_db(
    config_path: Path | str | None = None,
    *,
    config: "GraphDBConfig | None" = None,
) -> GraphDB:
    """Create the single GraphDB client for this process.

    Because GraphDB is a singleton class, repeated calls return the same
    underlying instance, but this function ensures it is initialized exactly
    once with the right configuration.
    """
    if config is not None and config_path is not None:
        raise ValueError("Provide either config_path= or config=, not both.")

    if config is None:
        if config_path is None:
            from graphregistry.common.paths import CONFIG_DB_PATH

            config_path = CONFIG_DB_PATH
        config = GraphDBConfig.from_file(str(config_path))

    return GraphDB(config=config)


#================================================================#
# Function Group: Unit of Work factory builders                  #
#================================================================#

# Function: Cache schema resolvers because they are stateless.
@lru_cache(maxsize=8)
def _schema_resolver(engine_name: str) -> DefaultSchemaResolver:
    """Cache schema resolvers because they are stateless.

    The cache is bounded to a small number of environments to avoid unbounded
    growth if many engine names are used in tests.
    """
    return DefaultSchemaResolver(engine_name=engine_name, glbcfg=GlobalConfig())


# Function: Return a factory that creates a fresh UnitOfWork for engine_name.
def build_uow_factory(db: GraphDB, engine_name: str) -> Callable[[], UnitOfWork]:
    """Return a factory that creates a fresh UnitOfWork for engine_name."""
    schema_resolver = _schema_resolver(engine_name)

    def _factory() -> UnitOfWork:
        return MySQLUnitOfWork(db=db, schema_resolver=schema_resolver)

    return _factory
