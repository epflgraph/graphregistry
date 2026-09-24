# tests/end2end_tests/helpers/compare_outputs.py
"""Tolerant comparison of GraphRegistry e2e output datasets.

Compares SQL dumps and Elasticsearch JSON exports between a ground-truth
snapshot and a fresh test run.  Floats are compared with ``math.isclose``,
row IDs are ignored, and nested JSON is compared recursively with
order-independent list handling.
"""
from __future__ import annotations

import ast
import json
import math
import re
from pathlib import Path
from typing import Any


# Regex for MariaDB/MySQL-style dates and timestamps such as '2026-09-18'
# (DATE) or '2026-09-18 09:07:24' (DATETIME).  Also accepts ISO 'T'
# separators and optional fractional seconds.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?$")


def _is_date_like(value: Any) -> bool:
    """Return True if the value looks like a SQL date or timestamp string."""
    return isinstance(value, str) and bool(_DATE_RE.match(value))


def _detect_date_columns(
    gt_rows: list[tuple[Any, ...]],
    act_rows: list[tuple[Any, ...]],
) -> set[int]:
    """Return column indices where every non-None value looks like a date."""
    if not gt_rows or not act_rows:
        return set()

    n_cols = len(gt_rows[0])
    date_cols: set[int] = set()

    for col_idx in range(n_cols):
        is_date = True
        for row in gt_rows + act_rows:
            if col_idx >= len(row):
                is_date = False
                break
            val = row[col_idx]
            if val is not None and not _is_date_like(val):
                is_date = False
                break
        if is_date:
            date_cols.add(col_idx)

    return date_cols


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def compare_output_directories(
    ground_truth_dir: Path,
    actual_dir: Path,
    *,
    ignore_row_id: bool = True,
    ignore_date_columns: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    ignore_json_keys: list[str] | None = None,
    verbose: bool = True,
) -> list[str]:
    """Compare a ground-truth output tree with a fresh test output tree.

    Returns a list of human-readable mismatch messages.  An empty list means
    the outputs are equivalent within the configured tolerance.
    """
    mismatches: list[str] = []

    # --------------------------- SQL dumps --------------------------------- #
    gt_sql_dir = ground_truth_dir / "sql"
    act_sql_dir = actual_dir / "sql"
    if gt_sql_dir.exists() or act_sql_dir.exists():
        gt_tables = _collect_sql_tables(gt_sql_dir)
        act_tables = _collect_sql_tables(act_sql_dir)

        if verbose:
            print(f"🔍 Comparing {len(set(gt_tables) | set(act_tables))} SQL table export(s)...")

        for key in sorted(set(gt_tables) | set(act_tables)):
            schema, table = key
            if key not in gt_tables:
                mismatches.append(f"SQL table missing in ground truth: {schema}.{table}")
                continue
            if key not in act_tables:
                mismatches.append(f"SQL table missing in actual output: {schema}.{table}")
                continue
            if verbose:
                print(f"   📂 {schema}.{table}")
            mismatches.extend(
                compare_sql_table(
                    gt_tables[key],
                    act_tables[key],
                    schema_table=f"{schema}.{table}",
                    ignore_row_id=ignore_row_id,
                    ignore_date_columns=ignore_date_columns,
                    rtol=rtol,
                    atol=atol,
                    verbose=verbose,
                )
            )

    # --------------------------- JSON exports ------------------------------ #
    gt_json_dir = ground_truth_dir / "json"
    act_json_dir = actual_dir / "json"
    if gt_json_dir.exists() or act_json_dir.exists():
        gt_indices = _collect_json_indices(gt_json_dir)
        act_indices = _collect_json_indices(act_json_dir)

        if verbose:
            print(f"🔍 Comparing {len(set(gt_indices) | set(act_indices))} Elasticsearch index export(s)...")

        for idx in sorted(set(gt_indices) | set(act_indices)):
            if idx not in gt_indices:
                mismatches.append(f"JSON index missing in ground truth: {idx}")
                continue
            if idx not in act_indices:
                mismatches.append(f"JSON index missing in actual output: {idx}")
                continue

            gt_idx_dir = gt_indices[idx]
            act_idx_dir = act_indices[idx]

            if verbose:
                print(f"   📂 {idx}")

            documents_gt = gt_idx_dir / "documents.jsonl"
            documents_act = act_idx_dir / "documents.jsonl"
            if documents_gt.exists() or documents_act.exists():
                if not documents_gt.exists():
                    mismatches.append(f"Missing ground-truth documents.jsonl for index {idx}")
                elif not documents_act.exists():
                    mismatches.append(f"Missing actual documents.jsonl for index {idx}")
                else:
                    if verbose:
                        print(f"      📄 documents.jsonl")
                    mismatches.extend(
                        compare_documents_jsonl(
                            documents_gt,
                            documents_act,
                            index_name=idx,
                            rtol=rtol,
                            atol=atol,
                            ignore_keys=ignore_json_keys,
                        )
                    )

            settings_gt = gt_idx_dir / "settings_mappings.json"
            settings_act = act_idx_dir / "settings_mappings.json"
            if settings_gt.exists() or settings_act.exists():
                if not settings_gt.exists():
                    mismatches.append(f"Missing ground-truth settings_mappings.json for index {idx}")
                elif not settings_act.exists():
                    mismatches.append(f"Missing actual settings_mappings.json for index {idx}")
                else:
                    if verbose:
                        print(f"      📄 settings_mappings.json")
                    mismatches.extend(
                        compare_settings_mappings(
                            settings_gt,
                            settings_act,
                            index_name=idx,
                            ignore_keys=ignore_json_keys or ["exported_at"],
                        )
                    )

    if verbose:
        if mismatches:
            print(f"❌ Found {len(mismatches)} mismatch(es).")
        else:
            print("✅ All outputs match ground truth.")

    return mismatches


def compare_sql_table(
    gt_table_dir: Path,
    actual_table_dir: Path,
    *,
    schema_table: str,
    ignore_row_id: bool = True,
    ignore_date_columns: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    verbose: bool = True,
) -> list[str]:
    """Compare all SQL chunk files for a single table."""
    gt_rows = _collect_sql_rows(gt_table_dir)
    act_rows = _collect_sql_rows(actual_table_dir)

    if ignore_row_id and gt_rows and act_rows:
        gt_rows = [row[:-1] for row in gt_rows]
        act_rows = [row[:-1] for row in act_rows]

    ignored_cols: set[int] = set()
    if ignore_date_columns and gt_rows and act_rows:
        ignored_cols = _detect_date_columns(gt_rows, act_rows)
        if verbose and ignored_cols:
            print(
                f"      ⏱️  Ignoring date column(s) {sorted(ignored_cols)} in {schema_table}"
            )

    if len(gt_rows) != len(act_rows):
        return [f"{schema_table}: row count mismatch ({len(gt_rows)} vs {len(act_rows)})"]

    gt_rows.sort(key=_sortable_row)
    act_rows.sort(key=_sortable_row)

    mismatches: list[str] = []
    for gt_row, act_row in zip(gt_rows, act_rows):
        if len(gt_row) != len(act_row):
            mismatches.append(
                f"{schema_table}: column count mismatch ({len(gt_row)} vs {len(act_row)})"
            )
            continue

        for col_idx, (a, b) in enumerate(zip(gt_row, act_row)):
            if col_idx in ignored_cols:
                continue
            if not _values_close(a, b, rtol=rtol, atol=atol):
                mismatches.append(
                    f"{schema_table} col {col_idx}: {a!r} != {b!r}"
                )
                # Report only the first column mismatch per row to keep output readable.
                break

    return mismatches


def compare_documents_jsonl(
    gt_path: Path,
    actual_path: Path,
    *,
    index_name: str,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    ignore_keys: list[str] | None = None,
) -> list[str]:
    """Compare Elasticsearch ``documents.jsonl`` exports by ``_id``."""
    gt_docs = _load_jsonl_documents(gt_path)
    act_docs = _load_jsonl_documents(actual_path)

    mismatches: list[str] = []
    missing = set(gt_docs) - set(act_docs)
    extra = set(act_docs) - set(gt_docs)

    for doc_id in sorted(missing):
        mismatches.append(f"{index_name}/documents.jsonl missing _id: {doc_id}")
    for doc_id in sorted(extra):
        mismatches.append(f"{index_name}/documents.jsonl extra _id: {doc_id}")

    for doc_id in sorted(set(gt_docs) & set(act_docs)):
        mismatches.extend(
            _compare_json_values(
                gt_docs[doc_id],
                act_docs[doc_id],
                path=f"{index_name}[{doc_id}]",
                rtol=rtol,
                atol=atol,
                ignore_keys=ignore_keys,
            )
        )

    return mismatches


def compare_settings_mappings(
    gt_path: Path,
    actual_path: Path,
    *,
    index_name: str,
    ignore_keys: list[str] | None = None,
) -> list[str]:
    """Compare Elasticsearch ``settings_mappings.json`` exports."""
    gt_data = json.loads(gt_path.read_text(encoding="utf-8"))
    act_data = json.loads(actual_path.read_text(encoding="utf-8"))

    return _compare_json_values(
        gt_data,
        act_data,
        path=f"{index_name}/settings_mappings.json",
        rtol=0.0,
        atol=0.0,
        ignore_keys=ignore_keys or ["exported_at"],
    )


# --------------------------------------------------------------------------- #
# Internal helpers                                                            #
# --------------------------------------------------------------------------- #

def _collect_sql_tables(base_dir: Path) -> dict[tuple[str, str], Path]:
    """Return a map of (schema_name, table_name) -> table directory."""
    tables: dict[tuple[str, str], Path] = {}
    if not base_dir.exists():
        return tables

    for schema_dir in base_dir.iterdir():
        if not schema_dir.is_dir():
            continue
        for table_dir in schema_dir.iterdir():
            if table_dir.is_dir() and any(table_dir.glob("*.sql")):
                tables[(schema_dir.name, table_dir.name)] = table_dir
    return tables


def _collect_sql_rows(table_dir: Path) -> list[tuple[Any, ...]]:
    """Parse all INSERT row tuples from the SQL chunk files in a table dir."""
    rows: list[tuple[Any, ...]] = []
    for sql_file in sorted(table_dir.glob("*.sql")):
        text = sql_file.read_text(encoding="utf-8")
        for line in text.splitlines():
            row = _parse_sql_row_line(line)
            if row is not None:
                rows.append(row)
    return rows


def _parse_sql_row_line(line: str) -> tuple[Any, ...] | None:
    """Parse a single ``INSERT INTO ... VALUES`` row line into a Python tuple."""
    line = line.strip()
    if not line.startswith("("):
        return None

    # Strip trailing comma (more rows follow) or semicolon (statement end).
    if line.endswith(","):
        line = line[:-1]
    elif line.endswith(";"):
        line = line[:-1]

    # SQL NULL -> Python None.  Use word boundaries to avoid replacing NULL
    # inside string literals.
    line = re.sub(r"\bNULL\b", "None", line)

    try:
        return ast.literal_eval(line)
    except Exception:
        return None


def _collect_json_indices(base_dir: Path) -> dict[str, Path]:
    """Return a map of index_name -> index directory."""
    indices: dict[str, Path] = {}
    if not base_dir.exists():
        return indices
    for idx_dir in base_dir.iterdir():
        if idx_dir.is_dir():
            indices[idx_dir.name] = idx_dir
    return indices


def _load_jsonl_documents(path: Path) -> dict[str, Any]:
    """Load a ``documents.jsonl`` file keyed by ``_id``."""
    docs: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        doc = json.loads(line)
        docs[doc["_id"]] = doc["_source"]
    return docs


def _values_close(a: Any, b: Any, *, rtol: float, atol: float) -> bool:
    """Return True if two scalar values are close enough."""
    if a is None or b is None:
        return a is b
    if _is_number(a) and _is_number(b):
        return math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=atol)
    return a == b


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _sortable_row(row: tuple[Any, ...]) -> tuple[str, ...]:
    """Convert a row tuple into a sortable string tuple."""
    return tuple("" if v is None else str(v) for v in row)


def _sort_key(item: Any) -> Any:
    """Stable sort key for mixed list contents."""
    if isinstance(item, dict):
        return (0, json.dumps(item, sort_keys=True, default=str, ensure_ascii=False))
    if isinstance(item, list):
        return (1, json.dumps(item, default=str, ensure_ascii=False))
    return (2, str(item))


def _compare_json_values(
    a: Any,
    b: Any,
    *,
    path: str,
    rtol: float,
    atol: float,
    ignore_keys: list[str] | None,
) -> list[str]:
    """Recursively compare two JSON values with float tolerance."""
    if a is None or b is None:
        if a is not b:
            return [f"{path}: None mismatch ({a!r} vs {b!r})"]
        return []

    if _is_number(a) and _is_number(b):
        if not math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=atol):
            return [f"{path}: {a!r} != {b!r}"]
        return []

    if isinstance(a, dict) and isinstance(b, dict):
        keys_a = set(a.keys()) - set(ignore_keys or [])
        keys_b = set(b.keys()) - set(ignore_keys or [])
        if keys_a != keys_b:
            return [f"{path}: key mismatch (missing={keys_a - keys_b}, extra={keys_b - keys_a})"]

        mismatches: list[str] = []
        for key in sorted(keys_a):
            mismatches.extend(
                _compare_json_values(
                    a[key],
                    b[key],
                    path=f"{path}.{key}",
                    rtol=rtol,
                    atol=atol,
                    ignore_keys=ignore_keys,
                )
            )
        return mismatches

    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{path}: list length mismatch ({len(a)} vs {len(b)})"]

        # Sort both lists for order-independent comparison.
        try:
            sorted_a = sorted(a, key=_sort_key)
            sorted_b = sorted(b, key=_sort_key)
        except Exception:
            sorted_a, sorted_b = a, b

        mismatches = []
        for i, (x, y) in enumerate(zip(sorted_a, sorted_b)):
            mismatches.extend(
                _compare_json_values(
                    x,
                    y,
                    path=f"{path}[{i}]",
                    rtol=rtol,
                    atol=atol,
                    ignore_keys=ignore_keys,
                )
            )
        return mismatches

    if a != b:
        return [f"{path}: {a!r} != {b!r}"]

    return []
