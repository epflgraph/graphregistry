# tests/end2end_tests/test_full_pipeline.py
"""End-to-end pipeline test for the full GraphRegistry workflow.

This test runs ``tests/end2end_tests/helpers/run_cli_sequence.sh``, which
mutates the configured ``_1_DEV_*`` MySQL schemas and writes output datasets to
``tests/end2end_tests/data/test_output/``.  It then compares those outputs to
``tests/end2end_tests/data/ground_truth/``.

This test is marked ``e2e`` and ``manual``.  It is skipped unless the
environment variable ``GRAPHREGISTRY_RUN_MANUAL_E2E`` is set to ``1``.

Run it manually from the repo root:

    ./scripts/run_manual_e2e_test.sh

or directly with pytest:

    GRAPHREGISTRY_RUN_MANUAL_E2E=1 \
        .venv.registry/bin/pytest tests/end2end_tests/test_full_pipeline.py -v -s
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from graphregistry.common.config import GlobalConfig
from tests.end2end_tests.helpers.compare_outputs import compare_output_directories


# Tolerances for comparing computed scores between runs.  Two sources of drift:
#   1. Floating-point accumulation across aggregated score tables (last digits).
#   2. GraphAI concept-detection score drift: the live wikify service is not
#      bit-stable between runs; observed drift is ~7% on individual concept
#      scores (e.g. 0.142816 vs 0.152535), propagating into derived tables.
# A 10% relative tolerance absorbs both while still catching real regressions
# (missing/extra detections, large scoring changes, rank reshuffles).
# NOTE: for fully deterministic e2e runs, pin or replay the GraphAI detection
# results instead of relying on tolerance alone.
SCORE_RTOL = 1e-1
SCORE_ATOL = 1e-4


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.manual,
    pytest.mark.skipif(
        os.environ.get("GRAPHREGISTRY_RUN_MANUAL_E2E") != "1",
        reason="Manual e2e pipeline. Set GRAPHREGISTRY_RUN_MANUAL_E2E=1 to run.",
    ),
]


def _assert_dev_mode() -> None:
    """Hard-fail if the configured environment is not a dev sandbox."""
    glbcfg = GlobalConfig()

    if glbcfg.mysql_execution_mode != "dev":
        raise RuntimeError(
            f"Refusing to run the destructive e2e pipeline in "
            f"'{glbcfg.mysql_execution_mode}' execution mode. "
            f"Set database.mode to 'dev' in your test config."
        )

    if not glbcfg.schema_registry.startswith("_1_DEV_"):
        raise RuntimeError(
            "Expected dev-prefixed schemas (_1_DEV_*). "
            "The current config points to non-dev schemas."
        )


def _assert_script_targets_dev_schemas(script_path: Path) -> None:
    """Hard-fail if the pipeline script exports from non-dev schemas."""
    content = script_path.read_text(encoding="utf-8")
    non_dev_schemas: list[str] = []

    for line in content.splitlines():
        line = line.strip()
        if "--schema_name" not in line:
            continue

        # Extract the schema token that follows --schema_name.
        parts = line.split("--schema_name", 1)[1].split()
        if not parts:
            continue
        schema_name = parts[0].strip()

        if not schema_name.startswith("_1_DEV_"):
            non_dev_schemas.append(schema_name)

    if non_dev_schemas:
        raise RuntimeError(
            "Refusing to run pipeline script: it targets non-dev schema(s): "
            f"{', '.join(non_dev_schemas)}. "
            "All --schema_name exports must use _1_DEV_* schemas."
        )


@pytest.fixture(scope="module", autouse=True)
def _require_dev_mode() -> None:
    """Auto-applied guard: every manual pipeline run must verify dev mode."""
    _assert_dev_mode()


@pytest.fixture(scope="module")
def repo_root() -> Path:
    """Return the repository root (two levels above this file)."""
    return Path(__file__).resolve().parents[2]


def test_full_pipeline_matches_ground_truth(repo_root: Path) -> None:
    """Run the full pipeline and compare outputs to the ground truth snapshot.

    Set ``GRAPHREGISTRY_E2E_COMPARE_ONLY=1`` (or pass ``--compare-only`` to
    ``scripts/run_e2e_test.sh``) to skip the pipeline execution and compare the
    existing ``test_output/`` directory to ``ground_truth/``.  This is useful
    when iterating on comparison tolerance or ground-truth snapshots without
    re-running the long pipeline.
    """
    ground_truth_dir = repo_root / "tests" / "end2end_tests" / "data" / "ground_truth"
    test_output_dir = repo_root / "tests" / "end2end_tests" / "data" / "test_output"

    compare_only = os.environ.get("GRAPHREGISTRY_E2E_COMPARE_ONLY") == "1"
    if compare_only:
        print("\n🔄 Compare-only mode: skipping pipeline execution.")
        if not test_output_dir.exists():
            raise FileNotFoundError(
                f"Compare-only mode requested but test output directory does not exist: {test_output_dir}"
            )
        print(f"📁 Comparing existing output: {test_output_dir}")
    else:
        print("\n🚀 Running full e2e pipeline...")
        script_path = repo_root / "tests" / "end2end_tests" / "helpers" / "run_cli_sequence.sh"
        if not script_path.exists():
            raise FileNotFoundError(f"Pipeline script not found: {script_path}")

        # Extra safety net: make sure the script only exports from dev schemas.
        _assert_script_targets_dev_schemas(script_path)

        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=str(repo_root),
            env=os.environ.copy(),
            text=True,
            capture_output=False,
            check=False,
        )
        assert result.returncode == 0, (
            f"Pipeline script failed with exit code {result.returncode}: {script_path}"
        )
        print("\n✅ Pipeline finished successfully. Comparing outputs to ground truth...")

    mismatches = compare_output_directories(
        ground_truth_dir,
        test_output_dir,
        ignore_row_id=True,
        ignore_date_columns=True,
        rtol=SCORE_RTOL,
        atol=SCORE_ATOL,
        ignore_json_keys=["exported_at"],
    )

    assert not mismatches, "Output does not match ground truth:\n" + "\n".join(mismatches)
