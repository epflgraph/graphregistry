# Graph Registry — Agent Notes

Compact instructions for OpenCode sessions working in the EPFL Graph Registry repo.

## Project overview

- Graph Registry is the ingestion / ETL layer of the EPFL Graph Data Platform.
- It exposes a FastAPI REST API and a `graphregistry` CLI for building the knowledge graph.
- Architecture is hexagonal-ish: domain models → application operations → adapter repositories/gateways → entrypoints (API + CLI).
- Shared skills are in `.opencode/skills/` (graph-project, hexagonal-architecture). Read them when touching architecture.

## Entrypoints

- API factory: `graphregistry.entrypoints.api.main:create_app`
- CLI entrypoint: `graphregistry.entrypoints.cli.main:main` (script name: `graphregistry`)
- Key CLI domains: `data`, `es`, `airflow`, `cache`, `index`, `run`, `ai`, `setup`, `config`

## Running locally

### API

```bash
# From repo root, with the virtualenv activated
uvicorn graphregistry.entrypoints.api.main:create_app --reload --factory
```

- Convenience script: `./api.sh` (uses `.venv.registry`, binds `127.0.0.1:9999`).
- Docker entrypoint uses `GRAPHREGISTRY_ROLE=api` and exposes port `28800`.

### CLI

```bash
# Install in editable mode so the `graphregistry` script exists
pip install -e .
graphregistry --help
```

## Configuration

- Required config files live under `config/` (gitignored; templates are committed):
  - `config_global.yaml` (from `config_global.template.yaml`)
  - `config_db.yaml`
  - `config_index.json` (from `config_index.template.json`)
  - `config_scores.json` (from `config_scores.template.json`)
- Optional: `config_graphai_client.json` for GraphAI credentials.
- `GRAPHREGISTRY_ROOT` env var overrides repo-root auto-discovery. `PYTHONPATH` should include the repo root.
- Dev mode in `config_global.yaml` (`mysql.mode: dev`) prefixes schema names with `_1_DEV_` automatically.

## Tests

```bash
# Default run: unit tests only (integration/e2e are excluded by default)
pytest

# Run a specific test
pytest tests/unit_tests/api/test_router.py -v

# Include integration tests (needs real MySQL / external systems)
pytest -m integration

# Include e2e tests (needs real systems + CLI)
pytest -m e2e
```

- Default `pytest` excludes `integration` and `e2e` markers (configured in `pyproject.toml`).
- Unit tests use fake in-memory repositories from `tests/conftest.py`; they do not need MySQL.
- API router tests override FastAPI dependencies (`get_node_ops`, `get_edge_ops`) to inject fakes.

## Type checking

- Pyright config: `pyrightconfig.json` (uses `.venv.registry`).

```bash
pyright
```

## Useful CLI examples

```bash
# Import a sample subgraph from JSON
graphregistry data import --input_file examples/sample_sets/sample_epfl_node_list.json --import_method list --actions commit

# Save nodes / edges from example files
graphregistry data save --node examples/entrypoints/node_save/request.json --actions commit
graphregistry data save --edge_list examples/entrypoints/edge_save_many/request.json --actions commit

# List nodes of a type
graphregistry data list --node_request examples/entrypoints/node_list/request.json
```

- CLI file args accept either `path/to/file.json` or `@path/to/file.json`.
- `--actions` is comma-separated: `print,eval,commit`. Default is `eval`; add `commit` to persist.

## Important gotchas

- The API router builds a real MySQL-backed repository on every request unless dependencies are overridden (as in tests). Do not assume API tests hit the fake repo unless `dependency_overrides` is set.
- `GlobalConfig` loads at app/CLI startup from `config/config_global.yaml`; importing `graphregistry.common.config` does not read the file, but instantiating `GlobalConfig()` does.
- Many scripts and commands require `PYTHONPATH` and `GRAPHREGISTRY_ROOT` to be set correctly; use `./api.sh` or the Docker entrypoint to avoid surprises.
- `package.json` is only for `elasticdump` (index backup/restore); it is not a Node.js application.
- Git-tracked templates are the source of truth for config shape; the active `.yaml`/`.json` files are gitignored.
