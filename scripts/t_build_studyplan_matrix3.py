#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from collections import defaultdict
from statistics import fmean
from typing import Any

import networkx as nx
from sqlalchemy import text

from graphdb.core.graphdb import GraphDB


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _canonical_pair(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def _mean_or_zero(values: list[float]) -> float:
    return float(fmean(values)) if values else 0.0


def _compute_clusters(
    pair_scores: dict[tuple[str, str], float],
    node_ids: set[str],
    threshold: float,
    resolution: float,
    seed: int,
) -> dict[str, int]:
    graph = nx.Graph()
    graph.add_nodes_from(node_ids)

    for (a, b), score in pair_scores.items():
        if a == b:
            continue
        if score >= threshold:
            graph.add_edge(a, b, weight=score)

    try:
        from networkx.algorithms.community import louvain_communities

        communities = louvain_communities(
            graph,
            weight="weight",
            resolution=resolution,
            seed=seed,
        )
    except Exception:
        from networkx.algorithms.community import greedy_modularity_communities

        communities = list(greedy_modularity_communities(graph, weight="weight"))

    # Deterministic cluster ids: biggest communities first, then lexical smallest member.
    communities_sorted = sorted(
        communities,
        key=lambda c: (-len(c), min(c) if c else ""),
    )

    cluster_map: dict[str, int] = {}
    next_cluster_id = 1

    for community in communities_sorted:
        for node_id in sorted(community):
            cluster_map[node_id] = next_cluster_id
        next_cluster_id += 1

    # Safety fallback (should already be covered by communities):
    for node_id in sorted(node_ids):
        if node_id not in cluster_map:
            cluster_map[node_id] = next_cluster_id
            next_cluster_id += 1

    return cluster_map


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build graph_analytics.Flourish_StudyPlan_NormalizedScoreMatrix_3 from _2: "
            "symmetric matrix + study plan similarity clusters."
        )
    )
    parser.add_argument("--engine", default="xaas_coresrv", help="GraphDB engine name")
    parser.add_argument("--schema", default="graph_analytics", help="Schema name")
    parser.add_argument(
        "--source-table",
        default="Flourish_StudyPlan_NormalizedScoreMatrix_2",
        help="Input table name",
    )
    parser.add_argument(
        "--target-table",
        default="Flourish_StudyPlan_NormalizedScoreMatrix_3",
        help="Output table name",
    )
    parser.add_argument(
        "--cluster-threshold",
        type=float,
        default=0.05,
        help="Minimum normalized_score used as an edge for clustering",
    )
    parser.add_argument(
        "--resolution",
        type=float,
        default=1.0,
        help="Louvain resolution parameter",
    )
    parser.add_argument("--seed", type=int, default=42, help="Louvain random seed")
    args = parser.parse_args()

    db = GraphDB()

    if not db.database_exists(engine_name=args.engine, schema_name=args.schema):
        raise SystemExit(f"Schema does not exist: {args.schema}")
    if not db.table_exists(engine_name=args.engine, schema_name=args.schema, table_name=args.source_table):
        raise SystemExit(f"Table does not exist: {args.schema}.{args.source_table}")

    query = f"""
    SELECT
        from_study_plan_id,
        to_study_plan_id,
        from_study_plan_name,
        to_study_plan_name,
        raw_score,
        course_pair_count,
        from_self_score,
        to_self_score,
        normalized_score
    FROM {args.schema}.{args.source_table}
    """

    rows = db.execute_query(engine_name=args.engine, query=query)

    # Aggregate pairwise values in an undirected form.
    pair_raw_scores: dict[tuple[str, str], list[float]] = defaultdict(list)
    pair_norm_scores: dict[tuple[str, str], list[float]] = defaultdict(list)
    pair_course_count_max: dict[tuple[str, str], int] = defaultdict(int)

    study_plan_name: dict[str, str] = {}
    self_score: dict[str, float] = {}
    node_ids: set[str] = set()

    for row in rows:
        (
            from_id,
            to_id,
            from_name,
            to_name,
            raw_score,
            course_pair_count,
            from_self_score,
            to_self_score,
            normalized_score,
        ) = row

        f_id = _as_str(from_id)
        t_id = _as_str(to_id)
        if not f_id or not t_id:
            continue

        node_ids.add(f_id)
        node_ids.add(t_id)

        f_name = _as_str(from_name)
        t_name = _as_str(to_name)
        if f_name:
            study_plan_name[f_id] = f_name
        if t_name:
            study_plan_name[t_id] = t_name

        f_self = _as_float(from_self_score)
        t_self = _as_float(to_self_score)
        if f_self is not None:
            self_score[f_id] = f_self
        if t_self is not None:
            self_score[t_id] = t_self

        pair = _canonical_pair(f_id, t_id)

        raw = _as_float(raw_score)
        if raw is not None:
            pair_raw_scores[pair].append(raw)

        norm = _as_float(normalized_score)
        if norm is not None:
            pair_norm_scores[pair].append(norm)

        pair_course_count_max[pair] = max(pair_course_count_max[pair], _as_int(course_pair_count))

    # Undirected pair score map used for clustering.
    pair_norm_mean: dict[tuple[str, str], float] = {
        pair: _mean_or_zero(values)
        for pair, values in pair_norm_scores.items()
    }

    cluster_by_plan = _compute_clusters(
        pair_scores=pair_norm_mean,
        node_ids=node_ids,
        threshold=args.cluster_threshold,
        resolution=args.resolution,
        seed=args.seed,
    )

    # Re-expand into a symmetric directed matrix (both A->B and B->A), plus cluster ids.
    out_rows: list[dict[str, Any]] = []
    for pair in sorted(pair_course_count_max.keys()):
        a, b = pair

        raw_mean = _mean_or_zero(pair_raw_scores[pair])
        norm_mean = _mean_or_zero(pair_norm_scores[pair])
        course_count = pair_course_count_max[pair]

        directions = [(a, b)] if a == b else [(a, b), (b, a)]

        for src, dst in directions:
            src_cluster = cluster_by_plan[src]
            dst_cluster = cluster_by_plan[dst]

            out_rows.append(
                {
                    "from_study_plan_id": src,
                    "to_study_plan_id": dst,
                    "from_study_plan_name": study_plan_name.get(src, ""),
                    "to_study_plan_name": study_plan_name.get(dst, ""),
                    "raw_score": raw_mean,
                    "course_pair_count": course_count,
                    "from_self_score": self_score.get(src, 0.0),
                    "to_self_score": self_score.get(dst, 0.0),
                    "normalized_score": norm_mean,
                    "from_cluster_id": src_cluster,
                    "to_cluster_id": dst_cluster,
                    "same_cluster": 1 if src_cluster == dst_cluster else 0,
                }
            )

    drop_sql = f"DROP TABLE IF EXISTS {args.schema}.{args.target_table}"
    create_sql = f"""
    CREATE TABLE {args.schema}.{args.target_table} (
      from_study_plan_id   VARCHAR(255) NOT NULL,
      to_study_plan_id     VARCHAR(255) NOT NULL,
      from_study_plan_name TEXT NULL,
      to_study_plan_name   TEXT NULL,
      raw_score            DOUBLE NULL,
      course_pair_count    BIGINT NOT NULL DEFAULT 0,
      from_self_score      DOUBLE NULL,
      to_self_score        DOUBLE NULL,
      normalized_score     DOUBLE NULL,
      from_cluster_id      INT NOT NULL,
      to_cluster_id        INT NOT NULL,
      same_cluster         TINYINT NOT NULL DEFAULT 0,
      row_id               INT NOT NULL AUTO_INCREMENT,
      PRIMARY KEY (row_id),
      KEY idx_from_plan (from_study_plan_id),
      KEY idx_to_plan (to_study_plan_id),
      KEY idx_from_cluster (from_cluster_id),
      KEY idx_to_cluster (to_cluster_id),
      KEY idx_same_cluster (same_cluster),
      KEY idx_norm_score (normalized_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

    db.execute_query(engine_name=args.engine, query=drop_sql)
    db.execute_query(engine_name=args.engine, query=create_sql)

    insert_sql = text(
        f"""
        INSERT INTO {args.schema}.{args.target_table} (
            from_study_plan_id,
            to_study_plan_id,
            from_study_plan_name,
            to_study_plan_name,
            raw_score,
            course_pair_count,
            from_self_score,
            to_self_score,
            normalized_score,
            from_cluster_id,
            to_cluster_id,
            same_cluster
        ) VALUES (
            :from_study_plan_id,
            :to_study_plan_id,
            :from_study_plan_name,
            :to_study_plan_name,
            :raw_score,
            :course_pair_count,
            :from_self_score,
            :to_self_score,
            :normalized_score,
            :from_cluster_id,
            :to_cluster_id,
            :same_cluster
        )
        """
    )

    with db.engine[args.engine].begin() as conn:
        conn.execute(insert_sql, out_rows)

    print(f"✅ Created {args.schema}.{args.target_table}")
    print(f"Rows written: {len(out_rows):,}")
    print(f"Study plans clustered: {len(cluster_by_plan):,}")
    print(f"Clusters: {len(set(cluster_by_plan.values())):,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
