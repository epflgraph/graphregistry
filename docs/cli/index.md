# CLI reference

GraphRegistry provides a single `graphregistry` command with subcommands grouped by domain.

## Global options

```bash
graphregistry -h
```

The CLI loads `config/environment/config_registry.yml` and creates a `CLIContext` with shared configuration objects. Heavy clients are initialised lazily.

## Domains

| Domain | Purpose |
|--------|---------|
| `config` | Validate and display configuration. |
| `data` | Insert, update, and delete nodes/edges from JSON files. |
| `es` | Elasticsearch operations. |
| `ai` | GraphAI-related commands (legacy path). |
| `airflow` | Manage airflow flags and propagation. |
| `cache` | Cache table operations. |
| `run` | Execute orchestrated processing cycles. |
| `index` | Build and deploy search indexes. |
| `setup` | Database and schema setup. |

## Common patterns

### Validate configuration

```bash
graphregistry config validate
graphregistry config show
```

### Insert data

```bash
graphregistry data insert \
  --node_list=@nodes.json \
  --edge_list=@edges.json \
  --actions=commit
```

### Manage airflow flags

```bash
graphregistry airflow reset --options typeflags airflow cache
graphregistry airflow propagate --actions=commit
```

### Run a processing cycle

```bash
graphregistry run --full --actions=commit
```

### Build and deploy the search index

```bash
graphregistry index build --actions=commit
graphregistry index deploy --actions=commit
```

## Actions

Many commands accept an `--actions` flag:

- `eval` — preview what would happen.
- `commit` — execute the changes.
- `print` — print generated SQL or payloads.

Multiple actions can be combined, for example `--actions=eval,print,commit`.

## Environment

Most commands accept `--env` to select a MySQL environment defined in `config/environment/config_graphdb.yml`.

```bash
graphregistry data insert --node_list=@nodes.json --env=prod_env --actions=commit
```
