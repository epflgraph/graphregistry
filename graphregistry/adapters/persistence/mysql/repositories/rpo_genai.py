# graphregistry/adapters/persistence/mysql/repositories/rpo_genai.py
from __future__ import annotations
import json
from typing import TYPE_CHECKING, Any, cast
from graphregistry.adapters.persistence.mysql.mappers.map_genai import (
    MySQLGenAICompletionMapper,
    MySQLGenAIKeywordsMapper,
    MySQLGenAIRawContentMapper,
    MySQLGenAITranslationMapper,
    MySQLGenAIWikiSearchMapper,
    MySQLGenAIWikifyMapper,
)
from graphregistry.adapters.persistence.mysql.repositories.helpers import qualified_table, upsert_rows
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_genai import GenAIRepository
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.domain.models.entities.mdl_genai import (
    GenAICompletion,
    GenAIKeywords,
    GenAIRawContent,
    GenAITranslation,
    GenAIWikiSearchLookup,
    GenAIWikifyLookup,
)

# Import DatabaseClient and SchemaResolver only during type checking to avoid a runtime dependency.
if TYPE_CHECKING:
    from graphregistry.application.ports.repositories.prt_databaseclient import DatabaseClient
    from graphregistry.adapters.persistence.mysql.repositories.resolvers import SchemaResolver

#==================#
# Class Definition #
#==================#
class MySQLGenAIRepository(GenAIRepository):
    """MySQL adapter for the graph_genai outputs database.

    The repository resolves the graph_genai schema through the schema
    resolver and opens one standalone session per write, so concurrent
    callers (the parallel chunk calls of the enrichment gateway) never share
    a connection. All writes are safe upserts that keep the record_deleted
    soft-delete convention of the graph_genai tables.
    """

    # Public Method: Initialise the repository with a DatabaseClient client and schema resolver.
    def __init__(self, db: "DatabaseClient", schema_resolver: "SchemaResolver", verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.verbose = verbose

    #================================================================#
    # Method Group: Internal helpers                                 #
    #================================================================#

    # Internal Function: Open a standalone session for the graph_genai engine.
    def _session(self) -> MySQLSession:
        """Open a standalone session for the graph_genai engine."""

        # Resolve the engine from the schema resolver and open the session.
        engine_name, _ = self.schema_resolver.for_genai()
        session = MySQLSession(self.db, engine_name, verbose=self.verbose)
        session.begin()
        return session

    # Internal Function: Execute a read query and return the results as a list of tuples.
    def _execute_read(self, query: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a read query on a standalone session."""

        # Run the query and always close the standalone session afterwards.
        session = self._session()
        try:
            return session.execute(query, params)
        finally:
            session.close()

    # Internal Function: Upsert one row into a graph_genai table and commit.
    def _upsert_and_commit(self, table_name: str, key_column_names: list[str], upd_column_names: list[str], rows: list[dict[str, Any]]) -> None:
        """Upsert one batch of rows into a graph_genai table and commit."""

        # Empty batches have nothing to write.
        if not rows:
            return

        # Resolve the schema-qualified table path for the genai schema.
        _, schema_name = self.schema_resolver.for_genai()
        table_path = qualified_table(schema_name, table_name)

        # Open a standalone session, upsert the batch, and commit.
        session = self._session()
        try:
            upsert_rows(
                session          = session,
                table_path       = table_path,
                key_column_names = key_column_names,
                upd_column_names = upd_column_names,
                rows             = rows,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    #================================================================#
    # Method Group: Raw content operations                           #
    #================================================================#

    # Public Method: Store the raw composed input of one enrichment task.
    def save_raw_content(self, raw: GenAIRawContent) -> None:
        """Store the raw composed input of one enrichment task."""

        # Build the upsert row through the mapper and write it.
        row = MySQLGenAIRawContentMapper.to_row(raw)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Object_T_RawContent",
            key_column_names = ["object_type", "object_id"],
            upd_column_names = ["raw_content"],
            rows             = [row],
        )

    #================================================================#
    # Method Group: Completion cache operations                      #
    #================================================================#

    # Public Method: Store one cached LLM completion.
    def save_completion(self, completion: GenAICompletion) -> None:
        """Store one cached LLM completion."""

        # Build the upsert row through the mapper and write it.
        row = MySQLGenAICompletionMapper.to_row(completion)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Object_T_CachedCompletions",
            key_column_names = ["object_type", "object_id", "model_used", "input_hash"],
            upd_column_names = ["call_role", "chunk_num", "input_text", "output_text"],
            rows             = [row],
        )

    # Public Method: Fetch one cached LLM completion by its dedup key, or None when absent.
    def get_completion(self, object_type: str, object_id: str, model_used: str, input_hash: str) -> GenAICompletion | None:
        """Fetch one cached LLM completion by its dedup key."""

        # Resolve the read query template and substitute the key values.
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['queries']['genai']['completion_get'],
            genai       = self.schema_resolver.for_genai()[1],
            object_type = object_type,
            object_id   = object_id,
            model_used  = model_used,
            input_hash  = input_hash,
        )

        # Execute the query; rebuild the record with its stored call metadata.
        rows = cast(list[tuple[Any, ...]], self._execute_read(sql_query))
        if not rows:
            return None
        (call_role, chunk_num, output_text) = rows[0]
        return GenAICompletion(
            object_type = object_type,
            object_id   = object_id,
            model_used  = model_used,
            call_role   = call_role,
            chunk_num   = chunk_num,
            input_hash  = input_hash,
            input_text  = "",
            output_text = output_text,
        )

    #================================================================#
    # Method Group: Keywords operations                              #
    #================================================================#

    # Public Method: Store one batch of keyword list records.
    def save_keywords(self, rows: list[GenAIKeywords]) -> None:
        """Store one batch of keyword list records."""

        # Build the upsert rows through the mapper and write them.
        upsert_rows_data = MySQLGenAIKeywordsMapper.to_rows(rows)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Object_T_Keywords",
            key_column_names = ["object_type", "object_id", "completion_hash", "coverage"],
            upd_column_names = ["keywords_list"],
            rows             = upsert_rows_data,
        )

    #================================================================#
    # Method Group: Concept-lookup cache operations                  #
    #================================================================#

    # Public Method: Fetch one cached wiki_search lookup by keyword hash, or None when absent.
    def get_wiki_search(self, keyword_hash: str) -> GenAIWikiSearchLookup | None:
        """Fetch one cached wiki_search lookup by keyword hash."""

        # Resolve the read query template and substitute the key value.
        sql_query = resolve_sql_query(
            file_path    = sql_queries_paths['queries']['genai']['wikisearch_get'],
            genai        = self.schema_resolver.for_genai()[1],
            keyword_hash = keyword_hash,
        )

        # Execute the query; rebuild the lookup with its known key fields.
        rows = cast(list[tuple[Any, ...]], self._execute_read(sql_query))
        if not rows:
            return None
        (suggestions_json,) = rows[0]
        return GenAIWikiSearchLookup(
            keyword_hash     = keyword_hash,
            keyword          = "",
            suggestions_list = json.loads(suggestions_json),
        )

    # Public Method: Store one cached wiki_search lookup.
    def save_wiki_search(self, lookup: GenAIWikiSearchLookup) -> None:
        """Store one cached wiki_search lookup."""

        # Build the upsert row through the mapper and write it.
        row = MySQLGenAIWikiSearchMapper.to_row(lookup)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Keyword_T_WikiSearch",
            key_column_names = ["keyword_hash"],
            upd_column_names = ["keyword", "suggestions_list"],
            rows             = [row],
        )

    # Public Method: Fetch one cached wikify lookup by input hash, or None when absent.
    def get_wikify(self, input_hash: str) -> GenAIWikifyLookup | None:
        """Fetch one cached wikify lookup by input hash."""

        # Resolve the read query template and substitute the key value.
        sql_query = resolve_sql_query(
            file_path  = sql_queries_paths['queries']['genai']['wikify_get'],
            genai      = self.schema_resolver.for_genai()[1],
            input_hash = input_hash,
        )

        # Execute the query; rebuild the lookup with its known key fields.
        rows = cast(list[tuple[Any, ...]], self._execute_read(sql_query))
        if not rows:
            return None
        (output_text,) = rows[0]
        return GenAIWikifyLookup(
            input_hash  = input_hash,
            input_text  = "",
            output_text = output_text,
        )

    # Public Method: Store one cached wikify lookup.
    def save_wikify(self, lookup: GenAIWikifyLookup) -> None:
        """Store one cached wikify lookup."""

        # Build the upsert row through the mapper and write it.
        row = MySQLGenAIWikifyMapper.to_row(lookup)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Text_T_Wikify",
            key_column_names = ["input_hash"],
            upd_column_names = ["input_text", "output_text"],
            rows             = [row],
        )

    #================================================================#
    # Method Group: Translation operations                           #
    #================================================================#

    # Public Method: Store one batch of translation records.
    def save_translations(self, rows: list[GenAITranslation]) -> None:
        """Store one batch of translation records."""

        # Build the upsert rows through the mapper and write them.
        upsert_rows_data = MySQLGenAITranslationMapper.to_rows(rows)
        self._upsert_and_commit(
            table_name       = "GenAI_N_Object_T_Translations",
            key_column_names = ["object_type", "object_id", "completion_hash", "translated_to"],
            upd_column_names = ["translated_from", "name_value", "description_short_value", "description_medium_value", "description_long_value"],
            rows             = upsert_rows_data,
        )
