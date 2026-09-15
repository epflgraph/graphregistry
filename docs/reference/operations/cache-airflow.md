# Cache and airflow

GraphRegistry uses airflow and cache tables to track which graph objects need reprocessing during incremental rebuilds.

## Airflow tables

The `graph_airflow` schema contains flag tables:

- `Operations_N_Object_T_TypeFlags` — which node/edge types are active for each cycle.
- `Operations_N_Object_T_FieldsChanged` — nodes whose fields changed.
- `Operations_N_Object_T_ScoresExpired` — nodes whose scores expired.
- `Operations_N_Object_N_Object_T_FieldsChanged` — edges whose fields changed.

These flags are driven by the `config/application/config_airflow.json` configuration.

## Resetting flags

Reset all flags before a clean run:

```bash
graphregistry airflow reset --options typeflags airflow cache traversals
```

Options:

- `typeflags` — reset active type flags.
- `airflow` — reset flags in `graph_airflow` tables.
- `cache` — reset `to_process` flags in `graph_cache` tables.
- `traversals` — reset `to_process` flags in `graph_traversals` tables.

## Propagating flags

After ingest or field changes, propagate airflow flags to downstream cache tables:

```bash
graphregistry airflow propagate --actions=commit
```

This marks rows in cache and index-buildup tables that need to be refreshed.

## Expiring scores

Force score recalculation for selected object types:

```bash
graphregistry airflow expire --include_nodes --include_edges --object_types Course Lecture
```

## Typical cycle

1. Ingest new nodes/edges.
2. `airflow propagate` to flag affected cache rows.
3. Run cache rebuilds to compute fields and scores.
4. Build or update Elasticsearch indexes.

!!! tip
    Use `actions=eval` to preview the SQL that would run without committing it.
