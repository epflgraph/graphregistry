#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from typing import Any

import networkx as nx

from graphdb.core.graphdb import GraphDB


SELECT_COLUMNS = [
    "root",
    "category_name_1",
    "category_name_2",
    "category_name_3",
    "category_name_4",
    "cluster_id",
    "course_code",
    "course_name",
    "concept_id",
    "concept_name",
    "is_cs_119_concept",
    "study_plan_id",
    "study_plan_name",
    "study_plan_level",
    "score",
]

NODE_COLORS = {
    "category": {"r": 55, "g": 126, "b": 184},
    "course": {"r": 77, "g": 175, "b": 74},
    "study_plan": {"r": 228, "g": 26, "b": 28},
}


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_score(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _node_attrs(node_type: str) -> dict[str, Any]:
    color = NODE_COLORS[node_type]
    return {
        "node_type": node_type,
        "gephi_group": node_type,
        "color_r": color["r"],
        "color_g": color["g"],
        "color_b": color["b"],
        "viz": {"color": {"r": color["r"], "g": color["g"], "b": color["b"], "a": 1.0}},
    }


def _add_node(graph: nx.Graph, node_id: str, label: str, node_type: str, **attrs: Any) -> None:
    if graph.has_node(node_id):
        return
    graph.add_node(node_id, label=label, **_node_attrs(node_type), **attrs)


def _add_edge(graph: nx.Graph, source: str, target: str, edge_type: str, score: float) -> None:
    if graph.has_edge(source, target):
        graph[source][target]["weight"] += score
        graph[source][target]["score_sum"] += score
        graph[source][target]["count"] += 1
        return
    graph.add_edge(
        source,
        target,
        edge_type=edge_type,
        weight=score,
        score_sum=score,
        count=1,
    )


def _add_hierarchy_edge(graph: nx.Graph, source: str, target: str) -> None:
    """Add a category hierarchy edge once with fixed weight."""
    if graph.has_edge(source, target):
        return
    graph.add_edge(
        source,
        target,
        edge_type="category_hierarchy",
        weight=1.0,
        score_sum=1.0,
        count=1,
    )


def _build_graph(rows: list[tuple[Any, ...]], min_score: float) -> nx.Graph:
    graph = nx.Graph()

    for row in rows:
        data = dict(zip(SELECT_COLUMNS, row, strict=True))

        root = _as_str(data["root"])
        cat1 = _as_str(data["category_name_1"])
        cat2 = _as_str(data["category_name_2"])
        cat3 = _as_str(data["category_name_3"])
        cat4 = _as_str(data["category_name_4"])
        cluster_id = _as_str(data["cluster_id"])

        course_code = _as_str(data["course_code"])
        course_name = _as_str(data["course_name"])

        study_plan_id_raw = _as_str(data["study_plan_id"])
        study_plan_name = _as_str(data["study_plan_name"])
        study_plan_level = _as_str(data["study_plan_level"])

        score = _as_score(data["score"])
        if score < min_score:
            continue
        if int(data["is_cs_119_concept"] or 0) != 1:
            continue

        # Mandatory identifiers for requested node types.
        if not (root and cat1 and cat2 and cat3 and cat4 and course_code and study_plan_id_raw):
            continue

        root_id = f"category:0:{root}"
        cat1_id = f"category:1:{cat1}"
        cat2_id = f"category:2:{cat2}"
        cat3_id = f"category:3:{cat3}"
        cat4_id = f"category:4:{cat4}"

        course_id = f"course:{course_code}"
        study_plan_id = f"study_plan:{study_plan_id_raw}"

        _add_node(graph, root_id, root, "category", category_level=0, category_name=root)
        _add_node(graph, cat1_id, cat1, "category", category_level=1, category_name=cat1)
        _add_node(graph, cat2_id, cat2, "category", category_level=2, category_name=cat2)
        _add_node(graph, cat3_id, cat3, "category", category_level=3, category_name=cat3)
        _add_node(
            graph,
            cat4_id,
            cat4,
            "category",
            category_level=4,
            category_name=cat4,
            cluster_id=cluster_id,
        )

        _add_node(
            graph,
            course_id,
            course_code,
            "course",
            course_code=course_code,
            course_name=course_name,
            is_cs_119_course=1 if course_code.startswith("CS-119") else 0,
        )

        _add_node(
            graph,
            study_plan_id,
            study_plan_name or study_plan_id_raw,
            "study_plan",
            study_plan_id=study_plan_id_raw,
            study_plan_name=study_plan_name,
            study_plan_level=study_plan_level,
        )

        # Score-accumulating edges requested.
        _add_edge(graph, cat4_id, course_id, "category_contains_course", score)
        _add_edge(graph, study_plan_id, course_id, "study_plan_contains_course", score)

    orphan_nodes = [node_id for node_id, deg in graph.degree() if deg == 0]
    if orphan_nodes:
        graph.remove_nodes_from(orphan_nodes)

    nx.set_node_attributes(graph, dict(graph.degree()), "degree")
    nx.set_node_attributes(graph, dict(graph.degree(weight="weight")), "weighted_degree")

    return graph


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export Flourish-style table data to a NetworkX graph (category/course/study_plan) "
            "and save it as GEXF for Gephi."
        )
    )
    parser.add_argument("--engine", default="xaas_coresrv", help="GraphDB engine name")
    parser.add_argument("--schema", required=True, help="MySQL schema name")
    parser.add_argument("--table", default="Flourish_v3", help="MySQL table name")
    parser.add_argument("--output", default="flourish_network.gexf", help="Output GEXF path")
    parser.add_argument("--min-score", type=float, default=0.0, help="Drop rows with score < min-score")
    args = parser.parse_args()

    db = GraphDB()

    if not db.database_exists(engine_name=args.engine, schema_name=args.schema):
        raise SystemExit(f"Schema does not exist: {args.schema}")
    if not db.table_exists(engine_name=args.engine, schema_name=args.schema, table_name=args.table):
        raise SystemExit(f"Table does not exist: {args.schema}.{args.table}")

    sql_query = f"""
    SELECT
        {", ".join(SELECT_COLUMNS)}
    FROM {args.schema}.{args.table}
    """

    rows = db.execute_query(engine_name=args.engine, query=sql_query)
    graph = _build_graph(rows, min_score=args.min_score)

    nx.write_gexf(graph, args.output)

    print(f"Exported graph to {args.output}")
    print(f"Nodes: {graph.number_of_nodes():,}")
    print(f"Edges: {graph.number_of_edges():,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
