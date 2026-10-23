# graphregistry/application/ports/repositories/prt_formula.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class FormulaRepository(Protocol):
    """Persistence port for the cache materialisation formulas: the SQL
    template files under database/formulas and the materialised views that
    populate the graph cache from the registry sources.

    The formula templates are the business logic, stored as versioned SQL
    files with [[schema]] placeholders; the repository resolves the
    placeholders against the configured schema names and executes each
    formula, dispatching between safe-insert upserts (SELECT templates) and
    direct execution (INSERT/REPLACE/UPDATE/DELETE/DDL templates). The
    formula families and the materialised views are the platform's own
    vocabulary, kept from the legacy cache management commands.
    """

    # Public Method: Apply every formula of a folder relative to
    # database/formulas, returning the applied formula names.
    def apply_formulas_from_folder(self, local_path: str, actions: ActionSet = ('commit',)) -> list[str]:
        ...

    # Public Method: Apply one formula by its path relative to
    # database/formulas, resolving the folder aliases.
    def apply_formula_by_path(self, formula_path: str, actions: ActionSet = ('eval',)) -> None:
        ...

    # Public Method: Apply the data reset formulas in row_id chunks.
    def apply_data_reset_formulas(self, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Apply the calculated-field formulas of objects and
    # object pairs.
    def apply_calculated_field_formulas(self, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Apply the graph traversal formulas.
    def apply_traversals(self, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Apply the scoring formulas: the object-to-ontology
    # concept and category scores, their unions, and the degree scores.
    def apply_scoring_formulas(self, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Materialize the cache views: the symmetric all-fields,
    # page-profile, all-fields, and parent-child projections of the flagged
    # registry rows.
    def materialize_views(self, actions: ActionSet = ('commit',)) -> None:
        ...
