# Indexing

GraphRegistry builds Elasticsearch indexes that power GraphSearch. The index layout, fields, and ranking rules are controlled by `config/config_index.json`.

## Index lifecycle

| Step | CLI command | Purpose |
|------|-------------|---------|
| Build cache tables | `graphregistry run` / cache commands | Compute fields, scores, and doc-link tables. |
| Generate index patches | `graphregistry index build` | Produce SQL patches for index tables. |
| Deploy to Elasticsearch | `graphregistry index deploy` | Push documents to ES indexes. |

## Configuring index rules

`config_index.json` is large and specific to each deployment. Start from the provided example and adjust:

- `doc_types` — which node types become searchable documents.
- `links` — parent/child and semantic relationships to include.
- `fields` — which node fields are indexed and how they are weighted.
- `ranking` — recommendation list ordering.

## Deploying an index

```bash
graphregistry index build --actions=commit
graphregistry index deploy --actions=commit
```

## Score matrices

Semantic similarity between nodes is stored in scores matrix tables:

- `Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS`
- `Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS`
- `Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS`

These are populated by scoring formulas in `database/formulas/scores/` and consumed by the indexing step.

## Monitoring

Watch the logs for progress bars, per-table row counts, and any `to_process` flags that remain set after a cycle.
