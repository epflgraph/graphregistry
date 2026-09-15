# Entrypoints

Entrypoints are thin adapters that connect the application to the outside world. They validate input, map to domain models, call application operations, and map results back to the transport format.

## REST API

The API is built with FastAPI.

| File | Responsibility |
|------|----------------|
| `entrypoints/api/main.py` | Application factory, exception handlers, OpenAPI customisation. |
| `entrypoints/api/router.py` | Route definitions and dependency injection. |
| `entrypoints/api/schemas.py` | Pydantic request/response models. |
| `entrypoints/mappers.py` | Maps between API schemas and domain models. |
| `entrypoints/dependencies.py` | Shared dependency builders for API and CLI. |

### Routes

All routes are mounted under `/api` and grouped into **system**, **nodes**, and **edges**.

Nodes:

- `POST /api/nodes/list`
- `POST /api/nodes/exists`
- `POST /api/nodes/exists_many`
- `POST /api/nodes/get`
- `POST /api/nodes/get_many`
- `POST /api/nodes/save`
- `POST /api/nodes/save_many`
- `POST /api/nodes/delete`
- `POST /api/nodes/delete_many`

Edges:

- `POST /api/edges/list`
- `POST /api/edges/exists`
- `POST /api/edges/exists_many`
- `POST /api/edges/get`
- `POST /api/edges/get_many`
- `POST /api/edges/save`
- `POST /api/edges/save_many`
- `POST /api/edges/delete`
- `POST /api/edges/delete_many`

### Exception mapping

The API maps domain and infrastructure exceptions to structured HTTP responses:

| Exception | HTTP status |
|-----------|-------------|
| `DisallowedTypeError` | 400 |
| `ValueError` (expected) | 400 |
| `DuplicateKeyError` | 409 |
| `ConnectionExhaustedError` | 503 |
| `LockWaitTimeoutError` | 503 |
| `PersistenceError` | 500 |
| Unhandled exceptions | 500 |

### Dependency injection

FastAPI dependencies build operations wired to a `UnitOfWork` factory backed by a process-scoped GraphDB client:

```python
def get_node_ops(uow_factory: Callable[[], UnitOfWork] = Depends(_get_uow_factory)) -> NodeOperations:
    return NodeOperations(uow_factory=uow_factory)
```

Tests can override `_get_uow_factory` via `app.dependency_overrides` to inject a fake unit of work.

## CLI

The CLI is built with `argparse` and exposed through the `graphregistry` console script.

| File | Responsibility |
|------|----------------|
| `entrypoints/cli/main.py` | Argument parser, context creation, command dispatch. |
| `entrypoints/cli/register.py` | Subcommand registration. |
| `entrypoints/cli/context.py` | Shared CLI context (configs, clients). |
| `entrypoints/cli/dependencies.py` | CLI-specific operation builders. |
| `entrypoints/cli/cmd_*.py` | Command handlers per domain. |

### Domains

- `config` — validate and show configuration.
- `data` — insert, update, and delete nodes/edges.
- `es` — Elasticsearch index operations.
- `ai` — GraphAI-related commands (legacy path).
- `airflow` — airflow flag management.
- `cache` — cache table operations.
- `run` — orchestrated processing cycles.
- `index` — index building and deployment.
- `setup` — database and schema setup.

### Context

`CLIContext` holds shared configuration objects. Heavy clients such as MySQL and Elasticsearch are initialised lazily so commands that do not need them start quickly.
