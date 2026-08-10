# graphregistry/domain/repositories/rpo_indexdeploy.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class IndexTableSpec:
    """Specification for a single index table to sync."""

    table_type: str  # 'doc' | 'doclink' | 'page_profile'
    doc_type: str | None = None
    link_type: str | None = None
    link_subtype: str | None = None  # 'SEM' | 'ORG'
    special_suffix: str = ""


@runtime_checkable
class IndexDeployRepository(Protocol):
    """Port for deploying / syncing GraphSearch index tables across environments."""

    def create_patch(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...],
        schema_overrides: dict[str, str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """
        Evaluate the diff for the given tables.
        Returns a mapping of table name -> {insert: n, delete: n, replace: n}.
        """
        ...

    def apply_patch(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...],
        schema_overrides: dict[str, str] | None = None,
    ) -> None:
        """Apply the replace / insert / delete operations to the target engine."""
        ...
