# graphregistry/tests/end2end_tests/test_api_stress_save_many.py
"""End-to-end stress test for chunked bulk save_many against a real database.

Drives the full HTTP stack (uvicorn -> FastAPI -> repository -> MySQL) with
large synthetic Publication batches that cross the 500-row statement-chunk
boundary multiple times per request, with deliberately varied field presence so
the page-profile column union (and therefore the statement shape) differs
between waves. Verifies that every row round-trips correctly, that re-saving is
idempotent, and that cleanup removes everything; it also reports the server's
RSS growth as a coarse tripwire against allocator-ratchet regressions.

Prerequisites (same as the other e2e tests): dev-mode database config in
``config/environment/config_graphdb.yml`` and the ``_0_PYTESTS_`` test schemas
present. Run explicitly with ``-m e2e``.
"""
from __future__ import annotations
import gc
import json
import os
import subprocess
import sys
import time
from typing import Any
import pytest
import requests
from graphregistry.entrypoints.mappers import SpecMapper
from tests.end2end_tests.test_api_endpoints import (
    ENGINE_NAME,
    api_base_url,
    assert_response_ok,
    db,
    delete_nodes,
    test_config,
)
from tests.helpers.db_checks import fetch_node_basic_row

# Batch geometry: 1,250 nodes per request produce statement chunks of
# 500/500/250 per table, so every save crosses two chunk boundaries inside one
# transaction.
NODES_PER_WAVE = 1250
WAVES = 3

# Chunk-boundary sample positions within one 1,250-node wave, plus the shared
# id prefix that lets a single list query count every synthetic node.
SAMPLE_INDICES = (0, 499, 500, 999, 1000, 1249)
ID_PREFIX = "E2E-STRESS"

# Coarse ceiling for server RSS growth during the stress section: the chunked
# path should stay in the low hundreds of MB, so a multi-GB growth means the
# allocator ratchet or a per-request retention has regressed.
RSS_GROWTH_LIMIT_MB = 2000

# Internal Function: Build one synthetic Publication node spec with varied shapes.
def _stress_node(wave: int, index: int) -> dict[str, Any]:
    node_id = f"{ID_PREFIX}-W{wave}-{index:05d}"

    # Base spec carries three custom fields so every node exercises the
    # custom-fields delete-and-rewrite path of the chunked save.
    node: dict[str, Any] = {
        "type"          : "Publication",
        "id"            : node_id,
        "short_code"    : f"stress-{wave}-{index:05d}",
        "title"         : f"Stress publication {wave}-{index:05d}: chunked bulk ingestion",
        "url"           : f"https://example.org/stress/{wave}/{index:05d}",
        "custom_fields" : [
            {"field_language": "en", "field_name": "authors", "field_value": f"A. Author{index}; B. Author"},
            {"field_language": "en", "field_name": "year", "field_value": str(2020 + index % 6)},
            {"field_language": "en", "field_name": "doi", "field_value": f"10.1000/stress.{wave}.{index:05d}"},
        ],
    }

    # Step the description size (roughly 1/2/3/4 KB) across the wave so payload
    # size and raw_text storage differ between nodes, like real publications.
    filler = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
    repeats = max(1, ((index % 4) * 2000) // len(filler))
    node["description"] = f"Abstract {wave}-{index:05d}. " + filler * repeats

    # Multilingual subtype on every third node and multilingual title on every
    # fifth node change which page-profile columns a wave needs, so consecutive
    # waves produce different statement shapes for the same chunk geometry.
    if index % 3 == 0:
        node["subtype"] = [
            {"language": "en", "text": "article"},
            {"language": "fr", "text": "article"},
        ]
    if index % 5 == 0:
        node["title"] = [
            {"language": "en", "text": f"Stress publication {wave}-{index:05d}"},
            {"language": "de", "text": f"Stresspublikation {wave}-{index:05d}"},
        ]
    return node

# Internal Function: Build the full node list for one stress wave.
def _stress_wave(wave: int) -> list[dict[str, Any]]:
    return [_stress_node(wave, index) for index in range(NODES_PER_WAVE)]

# Internal Function: Build API key dicts for every node of one wave.
def _wave_keys(wave: int) -> list[dict[str, str]]:
    return [{"type": "Publication", "id": f"{ID_PREFIX}-W{wave}-{index:05d}"} for index in range(NODES_PER_WAVE)]

# Internal Function: Return the current process RSS in MB; the API server runs
# in-process during e2e tests, so this covers the server's allocations.
def _rss_mb() -> float:
    with open("/proc/self/status") as status_file:
        for line in status_file:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return 0.0

# Internal Function: Extract the description text from a returned node spec,
# normalizing the multilingual list and bucketed-dict shapes back into plain
# text so saved plain-string descriptions can be compared for equality.
def _description_text(node_json: dict[str, Any]) -> str:
    description = node_json.get("description")
    if description is None:
        return ""
    if isinstance(description, str):
        return description
    if isinstance(description, list):
        return "".join(item["text"] for item in description)
    return "".join(item["text"] for bucket in description.values() for item in bucket)

# Internal Function: Normalize a title (plain string, multilingual list, or
# missing) into a language-to-text map so titles can be compared across the
# API round-trip, which stores plain titles as English page-profile entries.
def _title_map(node_json: dict[str, Any]) -> dict[str, str]:
    title = node_json.get("title")
    if title is None:
        return {}
    if isinstance(title, str):
        return {"en": title}
    return {item["language"]: item["text"] for item in title}

# Function: Bulk save_many crosses chunk boundaries with varied shapes and round-trips fully.
@pytest.mark.e2e
def test_api_bulk_save_many_chunked_stress_e2e(test_config: tuple, db: Any) -> None:
    # Build every wave up front together with its API keys, so cleanup can
    # remove the full synthetic set even when a wave fails mid-test.
    config_path, schema_name = test_config
    waves = [_stress_wave(wave) for wave in range(1, WAVES + 1)]
    all_keys = [_wave_keys(wave) for wave in range(1, WAVES + 1)]
    flat_keys = [key for wave_keys in all_keys for key in wave_keys]

    # Run the whole stress section against one in-process API server wired to
    # the temporary test schemas.
    with api_base_url(config_path, db) as base_url:
        # Defensive cleanup so leftovers from an earlier failed run cannot skew counts.
        delete_nodes(base_url, flat_keys)

        # Sample the server RSS before the stress section for the growth tripwire.
        gc.collect()
        rss_before = _rss_mb()
        try:
            # Push each wave as ONE request so a single transaction spans all
            # chunk boundaries of that wave.
            for wave, nodes in enumerate(waves, start=1):
                response = requests.post(
                    f"{base_url}/api/nodes/save_many",
                    json    = {"node_list": nodes},
                    timeout = 600,
                )
                assert_response_ok(
                    response,
                    count_key      = "count",
                    expected_count = NODES_PER_WAVE,
                    saved_key      = "saved_keys",
                )

            # Re-save wave 1: exercises the UPDATE path of the chunked upsert
            # (every row hits ON DUPLICATE KEY UPDATE) and must stay idempotent.
            response = requests.post(
                f"{base_url}/api/nodes/save_many",
                json    = {"node_list": waves[0]},
                timeout = 600,
            )
            assert_response_ok(
                response,
                count_key      = "count",
                expected_count = NODES_PER_WAVE,
                saved_key      = "saved_keys",
            )

            # Count survivors with a single list query using the id prefix.
            list_response = requests.post(
                f"{base_url}/api/nodes/list",
                json    = {"type": "Publication", "id_pattern": f"{ID_PREFIX}-*"},
                timeout = 120,
            )
            assert list_response.status_code == 200, list_response.text
            assert list_response.json()["count"] == WAVES * NODES_PER_WAVE

            # Round-trip the chunk-boundary sample of every wave through get_many.
            sample_keys = [
                {"type": "Publication", "id": f"{ID_PREFIX}-W{wave}-{index:05d}"}
                for wave in range(1, WAVES + 1)
                for index in SAMPLE_INDICES
            ]
            get_response = requests.post(
                f"{base_url}/api/nodes/get_many",
                json    = {"key_list": sample_keys},
                timeout = 120,
            )
            assert get_response.status_code == 200, get_response.text
            get_body = get_response.json()
            assert get_body["count"] == len(sample_keys)
            assert all(get_body["found_keys"]), "Some chunk-boundary nodes were not found after save_many"

            # Compare each sampled node against its original spec, normalizing
            # both title and description to their language-mapped text form:
            # the API stores plain titles/descriptions as page-profile entries
            # and returns them as multilingual lists regardless of input shape.
            sample_nodes = [waves[wave - 1][index] for wave in range(1, WAVES + 1) for index in SAMPLE_INDICES]
            for position, (key, node_json) in enumerate(zip(sample_keys, sample_nodes)):
                saved_node = get_body["nodes"][position]
                assert saved_node is not None, f"Missing node for {key}"
                assert saved_node["id"] == key["id"]
                assert _title_map(saved_node) == _title_map(node_json)
                assert _description_text(saved_node) == node_json["description"]

            # Sentinel check directly in MySQL: the basic row must carry the
            # mapped title and raw_text of the first node of each wave.
            for wave in range(1, WAVES + 1):
                node_json = waves[wave - 1][0]
                expected_node = SpecMapper.from_node_spec(node_json)
                basic_row = fetch_node_basic_row(db, schema_name, ENGINE_NAME, node_json)
                assert basic_row is not None, f"Missing basic row for stress node wave {wave}"
                assert basic_row[0] == expected_node.title
                assert (basic_row[2] or "") == (expected_node.raw_text or "")

            # Report server memory growth as a coarse ratchet tripwire.
            gc.collect()
            rss_after = _rss_mb()
            growth = rss_after - rss_before
            print(
                f"\n[stress] {WAVES * NODES_PER_WAVE} nodes in {WAVES + 1} requests: "
                f"RSS {rss_before:.0f} MB -> {rss_after:.0f} MB (+{growth:.0f} MB)"
            )
            assert growth < RSS_GROWTH_LIMIT_MB, (
                f"Server RSS grew by {growth:.0f} MB during bulk ingestion; "
                "the chunked statement path or allocator pinning has regressed."
            )

        # The cleanup must roll back every stress row through the same chunked
        # soft-delete path, leaving no synthetic data behind in the test schema.
        finally:
            # Chunked soft-delete of every stress key through the API.
            delete_nodes(base_url, flat_keys)

        # Nothing may survive the cleanup.
        list_response = requests.post(
            f"{base_url}/api/nodes/list",
            json    = {"type": "Publication", "id_pattern": f"{ID_PREFIX}-*"},
            timeout = 120,
        )
        assert list_response.status_code == 200, list_response.text
        assert list_response.json()["count"] == 0

# Internal Function: Run the graphregistry CLI in a fresh subprocess with the
# test config, returning the completed process.
def _run_cli(config_path: Any, *args: str) -> "subprocess.CompletedProcess[str]":
    cli_env = {**os.environ, "GRAPH_REGISTRY_CONFIG_GLOBAL": str(config_path)}
    return subprocess.run(
        [sys.executable, "-c", "from graphregistry.entrypoints.cli.main import main; main()", *args],
        capture_output = True,
        text           = True,
        timeout        = 900,
        env            = cli_env,
        check          = False,
    )

# Internal Function: Count non-deleted stress publications directly in MySQL.
def _count_stress_nodes(db: Any, schema_name: str) -> int:
    rows = db.execute_query(
        engine_name = ENGINE_NAME,
        query       = (
            f"SELECT COUNT(*) FROM {schema_name}.Nodes_N_Object "
            f"WHERE object_type = 'Publication' AND object_id LIKE '{ID_PREFIX}-%' AND record_deleted = 0"
        ),
    )
    return int(rows[0][0])

# Function: CLI bulk save/delete crosses chunk boundaries and round-trips via direct DB checks.
@pytest.mark.e2e
def test_cli_bulk_save_many_chunked_stress_e2e(test_config: tuple, db: Any, tmp_path: Any) -> None:
    # Build every wave up front and persist the wave and key payloads as JSON
    # files so the CLI reads them exactly like a real bulk-ingestion caller.
    config_path, schema_name = test_config
    waves = [_stress_wave(wave) for wave in range(1, WAVES + 1)]
    all_keys = [_wave_keys(wave) for wave in range(1, WAVES + 1)]
    flat_keys = [key for wave_keys in all_keys for key in wave_keys]

    # Persist the wave and key payloads as JSON files so the CLI reads them
    # exactly like a real bulk-ingestion caller would.
    wave_files = []
    for wave, nodes in enumerate(waves, start=1):
        wave_file = tmp_path / f"stress_wave_{wave}.json"
        wave_file.write_text(json.dumps({"node_list": nodes}), encoding="utf-8")
        wave_files.append(wave_file)
    keys_file = tmp_path / "stress_keys.json"
    keys_file.write_text(json.dumps({"key_list": flat_keys}), encoding="utf-8")

    # Defensive CLI cleanup so leftovers from an earlier failed run cannot skew counts.
    cleanup = _run_cli(config_path, "data", "delete", str(keys_file))
    assert cleanup.returncode == 0, cleanup.stderr[-2000:]

    # Run the timed save invocations; every assertion failure must still
    # trigger the final CLI cleanup through the finally block.
    durations: list[float] = []
    try:
        # Push each wave as ONE CLI invocation so a single process and
        # transaction span all chunk boundaries of that wave.
        for wave_file in wave_files:
            started = time.perf_counter()
            completed = _run_cli(config_path, "data", "save", str(wave_file))
            durations.append(time.perf_counter() - started)
            assert completed.returncode == 0, completed.stderr[-2000:]

        # Re-save wave 1: exercises the UPDATE path of the chunked upsert
        # (every row hits ON DUPLICATE KEY UPDATE) and must stay idempotent.
        completed = _run_cli(config_path, "data", "save", str(wave_files[0]))
        assert completed.returncode == 0, completed.stderr[-2000:]

        # All waves plus the idempotent re-save must leave exactly one live row
        # per synthetic node, verified directly in MySQL with a single query.
        assert _count_stress_nodes(db, schema_name) == WAVES * NODES_PER_WAVE

        # Sentinel check directly in MySQL: the basic row must carry the
        # mapped title and raw_text of the first node of each wave.
        for wave in range(1, WAVES + 1):
            node_json = waves[wave - 1][0]
            expected_node = SpecMapper.from_node_spec(node_json)
            basic_row = fetch_node_basic_row(db, schema_name, ENGINE_NAME, node_json)
            assert basic_row is not None, f"Missing basic row for stress node wave {wave}"
            assert basic_row[0] == expected_node.title
            assert (basic_row[2] or "") == (expected_node.raw_text or "")

        # Report per-invocation CLI durations for the bulk operations.
        save_times = ", ".join(f"{duration:.1f}s" for duration in durations)
        print(f"\n[stress-cli] {WAVES} save invocations of {NODES_PER_WAVE} nodes: {save_times}")

    # The cleanup must roll back every stress row through the same chunked
    # soft-delete path, leaving no synthetic data behind in the test schema.
    finally:
        # Chunked soft-delete of every stress key through the CLI.
        cleanup = _run_cli(config_path, "data", "delete", str(keys_file))
        assert cleanup.returncode == 0, cleanup.stderr[-2000:]

    # Nothing may survive the cleanup.
    assert _count_stress_nodes(db, schema_name) == 0
