# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_indexdoclink.py
"""Unit tests for MySQLIndexDocLinkRepository using an in-memory GraphDB fake.

The fake records every query, write, and chunked update so the tests verify
the legacy query sequences, the branch selection of both patches, the
execution-strategy split of the vertical patch, and the scratch-table
lifecycle of the horizontal patch. DDL writes carry their side effects:
created tables and views start existing, dropped views stop existing.
"""
from __future__ import annotations
import re
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdoclink import MySQLIndexDocLinkRepository
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_policies import LinkSelectionPolicy, OrderRule

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and canned results.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.chunked_calls = []

        # Tables exist unless explicitly listed as missing, so the table
        # creation helper short-circuits without exercising DDL.
        self.missing_tables = set()
        self.results_by_query_id = {}

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, verbose: bool = False, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query and apply its DDL
    # side effects: created tables and views start existing, dropped views
    # stop existing.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        create_match = re.search(r'CREATE (?:TABLE IF NOT EXISTS|OR REPLACE VIEW) (\S+)\.(\S+)', query)
        if create_match:
            self.missing_tables.discard(f"{create_match.group(1)}.{create_match.group(2)}")
        drop_match = re.search(r'DROP VIEW IF EXISTS (\S+)\.(\S+)', query)
        if drop_match:
            self.missing_tables.add(f"{drop_match.group(1)}.{drop_match.group(2)}")

    # Public Method: Record a chunked update and its chunk filter.
    def execute_query_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, chunk_filter: str | None = None, chunk_size: int | None = None, row_id_name: str | None = None, show_progress: bool = False, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.chunked_calls.append({
            "schema"       : schema_name,
            "table"        : table_name,
            "query"        : query,
            "chunk_filter" : chunk_filter,
            "chunk_size"   : chunk_size,
            "row_id_name"  : row_id_name,
            "query_id"     : query_id,
        })

    # Public Method: Report whether a schema-qualified table exists,
    # with views reported like tables when they are not excluded.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str, exclude_views: bool = True) -> bool:
        return f"{schema_name}.{table_name}" not in self.missing_tables

    # Public Method: Return the canned column list of a table.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list[str]:
        return getattr(self, "get_column_names_result", ["doc_type", "doc_id", "row_id"])

    # Public Method: Report that the databases always exist, so the table
    # creation helper short-circuits before any DDL.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        return True

    # Public Method: Record a database creation that the tests never reach.
    def create_database(self, engine_name: str, schema_name: str) -> None:
        self.write_calls.append({"engine": engine_name, "query": f"CREATE DATABASE {schema_name}", "query_id": None})

    # Public Method: Return the query ids of all captured writes in order.
    def write_query_ids(self) -> list[str | None]:
        return [call["query_id"] for call in self.write_calls]

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow, cache, and search schemas."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and graph cache schema.
    def for_graph_cache(self) -> tuple[str, str]:
        return ("coresrv", "graph_cache")

    # Public Method: Return the canned engine and graphsearch test schema.
    def for_graphsearch_test(self) -> tuple[str, str]:
        return ("coresrv", "graphsearch_test")

    # Public Method: Return the canned engine and Elasticsearch cache schema.
    def for_es_cache(self) -> tuple[str, str]:
        return ("coresrv", "es_cache_test")

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration carrying link fields, contexts, and types."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "graphsearch" : {
                "fields"  : {"links" : {
                    "default"      : {"Course" : ["link_name_en", "link_name_fr"], "Person" : ["link_name_en"]},
                    "parent_child" : {"Person" : {"Course" : ["context_note"]}},
                }},
            },
            "elasticsearch" : {
                "fields"  : {"links" : {"Course" : ["external_url_en"]}},
                "filters" : {"links" : {"Course" : []}},
            },
            "edge_selection_contexts" : {("Course", "Person") : "teacher"},
            "data_types"              : {"degree_score" : "int", "link_name_en" : "char"},
        }

#==================#
# Class Definition #
#==================#
class FakeScoresConfig:
    """Fixed scores configuration carrying the matrix mapping."""

    # Public Method: Initialize the fake with the scores settings.
    def __init__(self) -> None:
        self.settings = {
            'scored_edge_tuple_to_class_mapping' : {
                ('Course', 'Person') : 'education',
                ('Course', 'Course') : 'education',
            },
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo() -> tuple[MySQLIndexDocLinkRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLIndexDocLinkRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        index_config    = FakeIndexConfig(),
        scores_config   = FakeScoresConfig(),
    )
    return repo, db

# Public Function: Build a semantic selection policy with a cast ordering rule.
def _sem_policy() -> LinkSelectionPolicy:
    return LinkSelectionPolicy(
        score_type     = "semantic_score",
        rank_threshold = 32,
        ordering       = [OrderRule(field="degree_score", direction="DESC")],
    )

#================================================================#
# Function Group: Vertical patch tests                           #
#================================================================#

# Public Function: Verify the SEM vertical patch runs small patches directly.
def test_vertical_patch_sem_small_patch() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["hLdNx8Hb"] = [(100, 5)]
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    # Patch the link fields in eval and commit modes.
    stats = repo.vertical_patch(key=key, actions=('eval', 'commit'))

    # The eval counts the drifted rows and the commit patches them directly.
    assert stats.rows_flagged == 5
    assert stats.target == "graphsearch_test.Index_D_Person_L_Course_T_SEM"
    assert db.write_query_ids() == ['FCQgBmb2']
    assert db.chunked_calls == []
    commit = db.write_calls[0]["query"]
    assert "i.link_name_en  = b.link_name_en" in commit
    assert 'COALESCE(i.link_name_en, "__null__") != COALESCE(b.link_name_en, "__null__")' in commit
    assert "b.to_process = 1" in commit

# Public Function: Verify the SEM vertical patch chunks large patches.
def test_vertical_patch_sem_large_patch_chunks() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["hLdNx8Hb"] = [(100000, 50000)]
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    # Patch the link fields in commit mode only, above the small-patch bound.
    repo.vertical_patch(key=key, actions=('commit',))

    # Large patches run chunked with the buildup-existence chunk filter.
    assert db.write_calls == []
    assert len(db.chunked_calls) == 1
    chunked = db.chunked_calls[0]
    assert chunked["query_id"] == 'FCQgBmb2'
    assert chunked["chunk_filter"].startswith("EXISTS (SELECT 1 FROM graph_cache.IndexBuildup_Fields_Docs_Course b")
    assert chunked["row_id_name"] == 'i.row_id'

# Public Function: Verify the ORG vertical patch joins the parent-child fields.
def test_vertical_patch_org_parentchild() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["of8T3uCG"] = [(100, 8)]
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="ORG")

    # Patch the parent-child link fields in commit mode only.
    stats = repo.vertical_patch(key=key, actions=('commit',))

    # The ORG patch always runs chunked, joining the obj2obj buildup.
    assert stats.rows_flagged == 8
    assert db.write_query_ids() == []
    assert [call["query_id"] for call in db.chunked_calls] == ['sxUZ7wER']
    chunked = db.chunked_calls[0]
    assert "LEFT JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_Course_Person l" in chunked["query"]
    assert "i.context_note  = l.context_note" in chunked["query"]

# Public Function: Verify the vertical patch skips unconfigured fields.
def test_vertical_patch_no_fields_skips() -> None:
    repo, db = _make_repo()
    key = DocLinkTypeKey(doc_type="Person", link_type="Lecture", partition="SEM")

    # Lecture has no configured default fields, so nothing runs.
    stats = repo.vertical_patch(key=key, actions=('commit',))
    assert stats.rows_flagged == 0
    assert db.read_calls == [] and db.write_calls == []

#================================================================#
# Function Group: Horizontal patch tests                         #
#================================================================#

# Public Function: Verify the ORG horizontal patch deletes and re-inserts.
def test_horizontal_patch_org() -> None:
    repo, db = _make_repo()
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="ORG")

    # Rebuild the link membership and ranking in commit mode only.
    stats = repo.horizontal_patch(key=key, policy=LinkSelectionPolicy.default_for_partition("ORG"), actions=('commit',))

    # The delete runs before the forward insert; the flipped insert does not
    # exist for the organisational partition.
    assert db.write_query_ids() == ['Del6hv6h', 'InsFwdKuT']
    delete = db.write_calls[0]["query"]
    insert = db.write_calls[1]["query"]
    assert "'teacher'" in delete and "to_process = 1" in delete
    assert "Edges_N_Object_N_Object_T_ParentChildSymmetric p" in insert
    assert "row_rank <= 9999999" in insert
    assert "LEFT JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_Person_Course bl" in insert
    assert "bd.degree_score IS NOT NULL" in insert

# Public Function: Verify the ORG patch skips pairs without a context.
def test_horizontal_patch_org_no_context_skips() -> None:
    repo, db = _make_repo()
    key = DocLinkTypeKey(doc_type="Person", link_type="Publication", partition="ORG")

    # The (Publication, Person) pair has no configured edge context.
    stats = repo.horizontal_patch(key=key, policy=LinkSelectionPolicy.default_for_partition("ORG"), actions=('commit',))
    assert stats.rows_flagged == 0
    assert db.write_calls == []

# Public Function: Verify the SEM ontology patch reads the final scores.
def test_horizontal_patch_sem_ontology() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["z0rFNfM5"] = [(4, 2)]
    db.results_by_query_id["oxyoF81R"] = [(0, 0)]

    # Patch the Concept-Person semantic pair, driven by the concept final
    # scores rather than the scores matrix.
    key = DocLinkTypeKey(doc_type="Concept", link_type="Person", partition="SEM")

    # Rebuild the pair in eval and commit modes.
    stats = repo.horizontal_patch(key=key, policy=_sem_policy(), actions=('eval', 'commit'))

    # The ontology branch joins the concept final scores with the Person
    # buildup and ranks per concept id; no flipped insert or scratch table.
    assert db.write_query_ids() == ['Del6hv6h', 'InsFwdKuT']
    insert = db.write_calls[1]["query"]
    assert "graph_cache.Edges_N_Object_N_Concept_T_FinalScores fs" in insert
    assert "'Concept' AS doc_type" in insert and "'Semantic' AS link_subtype" in insert
    assert "PARTITION BY fs.concept_id" in insert
    assert "semantic_score >= 0.1" in insert
    assert stats.rows_flagged == 6

# Public Function: Verify the SEM matrix patch manages the scratch table.
def test_horizontal_patch_sem_matrix() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["z0rFNfM5"] = [(3, 1)]
    db.results_by_query_id["oxyoF81R"] = [(2, 1)]

    # Patch the Person-Course semantic pair from the education matrix.
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    # Rebuild the pair in eval and commit modes.
    stats = repo.horizontal_patch(key=key, policy=_sem_policy(), actions=('eval', 'commit'))

    # The scratch table is created and dropped around the delete and both
    # directional inserts.
    assert db.write_query_ids() == ['TmpHpCreate', 'Del6hv6h', 'InsFwdKuT', 'InsFlippedT1B', 'TmpHpDrop']
    create = db.write_calls[0]["query"]
    insert = db.write_calls[2]["query"]
    flipped = db.write_calls[3]["query"]
    assert "_tmp_hp_Person_Course" in create
    assert "Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS" in create
    assert 'ON (s.from_object_type, s.to_object_type, s.to_object_id) = ("Person", "Course", i.doc_id)' in insert
    assert 'ON (s.to_object_type, s.from_object_type, s.from_object_id) = ("Person", "Course", i.doc_id)' in flipped
    assert "row_rank <= 32" in insert and "row_rank <= 32" in flipped
    assert stats.rows_flagged == 7

# Public Function: Verify the SEM matrix patch drops the flipped self-loop.
def test_horizontal_patch_sem_selfloop() -> None:
    repo, db = _make_repo()

    # Patch the Course-Course self-loop semantic pair.
    key = DocLinkTypeKey(doc_type="Course", link_type="Course", partition="SEM")

    # Rebuild the self-loop pair in commit mode only.
    repo.horizontal_patch(key=key, policy=_sem_policy(), actions=('commit',))

    # Self-loop edges keep only the forward insert to protect the unique key.
    assert db.write_query_ids() == ['TmpHpCreate', 'Del6hv6h', 'InsFwdKuT', 'TmpHpDrop']

# Public Function: Verify the eval mode manages the scratch table but writes
# no patch rows.
def test_horizontal_patch_eval_only() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["z0rFNfM5"] = [(3, 1)]
    db.results_by_query_id["oxyoF81R"] = [(0, 0)]

    # Evaluate the Person-Course semantic pair without committing.
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")
    stats = repo.horizontal_patch(key=key, policy=_sem_policy(), actions=('eval',))

    # The scratch table lifecycle is preserved in eval mode, but neither the
    # delete nor the inserts run.
    assert db.write_query_ids() == ['TmpHpCreate', 'TmpHpDrop']
    assert [call["query_id"] for call in db.read_calls] == ['z0rFNfM5', 'oxyoF81R']
    assert stats.rows_flagged == 4

# Public Function: Verify the ordering expression applies the cast rules.
def test_order_by_expression_applies_casts() -> None:
    repo, _db = _make_repo()
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")
    order_by = repo._order_by_expression(key, _sem_policy())

    # The configured rule is cast per its data type and precedes the defaults.
    assert order_by == "CAST(degree_score AS UNSIGNED) DESC, semantic_score DESC, link_id ASC"


#================================================================#
# Function Group: Elasticsearch cache patch tests                #
#================================================================#

# Public Function: Verify the es_cache vertical patch refreshes the names.
def test_vertical_patch_es_cache() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["XL1265bE"] = [(50, 12)]
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    stats = repo.vertical_patch_es_cache(key=key, actions=('eval', 'commit'))

    # The eval counts the drifted presentation columns and the commit runs
    # chunked with the profile-join chunk filter.
    assert stats.rows_flagged == 12
    assert stats.target == "es_cache_test.Index_D_Person_L_Course"
    assert db.write_query_ids() == []
    assert [call["query_id"] for call in db.chunked_calls] == ['Z16jRm9j']
    chunked = db.chunked_calls[0]

    # The commit SET clause refreshes the presentation columns from the
    # profile and buildup values, including the configured link field.
    assert "SET t.link_name_en = IF(l.include_code_in_name=1" in chunked["query"]
    assert "t.link_short_description_fr = p.description_short_fr_value" in chunked["query"]
    assert "t.external_url_en = l.external_url_en" in chunked["query"]

# Public Function: Verify the es_cache vertical patch skips unconfigured fields.
def test_vertical_patch_es_cache_no_fields_skips() -> None:
    repo, db = _make_repo()
    key = DocLinkTypeKey(doc_type="Person", link_type="Lecture", partition="SEM")

    # Lecture has no configured Elasticsearch link fields, so nothing runs.
    stats = repo.vertical_patch_es_cache(key=key, actions=('commit',))
    assert stats.rows_flagged == 0
    assert db.read_calls == [] and db.write_calls == []

# Public Function: Verify the es_cache horizontal patch uses the mixed view.
def test_horizontal_patch_es_cache_mixed_view() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["FJ9HVCLW"] = [(3, 2)]

    # Register the pair as configured for mixed scoring and make all source
    # tables exist, so the mixed view is created and used.
    repo.scores_config.settings['mixed_scoring_tuples'] = [('Course', 'Person'), ('Person', 'Course')]
    db.get_column_names_result = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id', 'semantic_score', 'row_score', 'row_rank', 'row_id']
    db.missing_tables.add("graphsearch_test.Index_D_Person_L_Course_T_MIX")
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    stats = repo.horizontal_patch_es_cache(key=key, policy=_sem_policy(), actions=('eval', 'commit'))

    # The mixed view is created and used as the source with the adjusted
    # rank column, without the FORCE INDEX hint.
    assert db.write_query_ids() == ['tb1Vdfyq', 'zwRx2b8a']
    create_view = db.write_calls[0]["query"]
    assert "CREATE OR REPLACE VIEW graphsearch_test.Index_D_Person_L_Course_T_MIX" in create_view
    assert "(s.row_rank + COALESCE(o.max_row_rank, 0)) AS adjusted_row_rank" in create_view
    commit = db.write_calls[1]["query"]
    assert "Index_D_Person_L_Course_T_MIX dl" in commit
    assert "dl.adjusted_row_rank AS link_rank" in commit
    assert "FORCE INDEX" not in db.read_calls[0]["query"]
    assert stats.rows_flagged == 5

# Public Function: Verify the es_cache horizontal patch drops stale views.
def test_horizontal_patch_es_cache_drops_stale_view() -> None:
    repo, db = _make_repo()

    # The pair is not configured for mixed scoring but carries a stale view;
    # no source table remains after the drop, so the patch stops there.
    db.missing_tables.update([
        "graphsearch_test.Index_D_Person_L_Course_T_ORG",
        "graphsearch_test.Index_D_Person_L_Course_T_SEM",
    ])
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="SEM")

    stats = repo.horizontal_patch_es_cache(key=key, policy=_sem_policy(), actions=('commit',))

    # The stale view is dropped and no patch runs.
    assert db.write_query_ids() == ['xY7gHv2K']
    assert stats.rows_flagged == 0

# Public Function: Verify the es_cache horizontal patch falls back to ORG.
def test_horizontal_patch_es_cache_org_source() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["FJ9HVCLW"] = [(5, 1)]
    db.missing_tables.update([
        "graphsearch_test.Index_D_Person_L_Course_T_SEM",
        "graphsearch_test.Index_D_Person_L_Course_T_MIX",
    ])
    key = DocLinkTypeKey(doc_type="Person", link_type="Course", partition="ORG")

    stats = repo.horizontal_patch_es_cache(key=key, policy=LinkSelectionPolicy.default_for_partition("ORG"), actions=('commit',))

    # The organisational source reads the parent-child edges and applies
    # the FORCE INDEX hint.
    assert db.write_query_ids() == ['zwRx2b8a']
    commit = db.write_calls[0]["query"]
    assert "Index_D_Person_L_Course_T_ORG dl" in commit
    assert "dl.row_rank AS link_rank" in commit
    assert "Edges_N_Object_N_Object_T_ParentChildSymmetric" in commit
    assert "FORCE INDEX (idx_doc_rank_link)" in db.read_calls[0]["query"]
    assert stats.rows_flagged == 6


# Public Function: Verify the SEM matrix insert stays valid without fields.
def test_horizontal_patch_sem_matrix_without_fields(tmp_placeholder=None) -> None:
    repo, db = _make_repo()
    repo.scores_config.settings['scored_edge_tuple_to_class_mapping'][('Lecture', 'Person')] = 'education'
    key = DocLinkTypeKey(doc_type="Person", link_type="Lecture", partition="SEM")

    # Lecture carries no configured default link fields, so the field join
    # is empty; the insert must not contain a dangling comma.
    repo.horizontal_patch(key=key, policy=_sem_policy(), actions=('commit',))

    insert = db.write_calls[db.write_query_ids().index('InsFwdKuT')]["query"]
    assert "s.score AS semantic_score" in insert

    # No dangling comma: never two commas separated only by whitespace.
    import re as _re
    assert not _re.search(r",\s*,", insert), "dangling comma in the insert query"
    assert "s.to_object_id AS link_id,\n" in insert or "AS link_id," in insert
