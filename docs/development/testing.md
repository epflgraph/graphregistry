# Testing

GraphRegistry uses `pytest`. The goal is to test domain and application logic without requiring live databases or external services.

## Running tests

```bash
pytest
```

By default, integration and end-to-end tests are excluded:

```bash
pytest -m "not integration and not e2e"
```

To run integration tests against real systems:

```bash
pytest -m integration
```

## Test markers

| Marker | Meaning |
|--------|---------|
| `integration` | Hits real external systems (DB, APIs, etc.). |
| `e2e` | End-to-end tests hitting real systems. |

## Writing tests

Use fake repositories and gateways for unit tests:

```python
class FakeNodeRepository:
    def __init__(self):
        self.nodes = {}

    def save(self, node, actions):
        self.nodes[node.key] = node
        return node

class FakeUnitOfWork:
    def __init__(self):
        self.nodes = FakeNodeRepository()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

node_ops = NodeOperations(uow_factory=FakeUnitOfWork)
saved = node_ops.save(Node(key=NodeKey(object_type="Course", object_id="c1")))
assert saved.key.object_id == "c1"
```

## Coverage

Coverage is configured in `pyproject.toml`:

```toml
[tool.coverage.run]
source = ["graphregistry"]
```

Generate a coverage report:

```bash
pytest --cov=graphregistry --cov-report=term-missing
```

## Continuous integration

Make sure tests pass and coverage is reasonable before opening a pull request.
