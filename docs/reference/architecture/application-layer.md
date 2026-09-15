# Application layer

The application layer contains use cases (operations) and the ports they depend on. It orchestrates workflows without containing business rules (those belong in the domain) or infrastructure details (those belong in adapters).

## Operations

Operations are the primary application services. Each operation owns a vertical slice of functionality.

| Operation | File | Responsibility |
|-----------|------|----------------|
| `NodeOperations` | `ops_node.py` | CRUD, concept enrichment, and diagnostics for nodes. |
| `EdgeOperations` | `ops_edge.py` | CRUD, upsert semantics, and bulk operations for edges. |
| `LectureOperations` | `ops_lecture.py` | Video/audio/slide ingestion and transcript processing. |
| `IndexOperations` | `ops_index.py` | Elasticsearch index building and deployment. |
| `TextOperations` | `ops_text.py` | Translation and text-related workflows. |

## Unit of Work

`UnitOfWork` is the persistence boundary. Application services receive a factory that produces a fresh `UnitOfWork` per transaction.

```python
class UnitOfWork(Protocol):
    @property
    def nodes(self) -> NodeRepository: ...
    @property
    def edges(self) -> EdgeRepository: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
```

Benefits:

- Transaction scope is explicit.
- The same operation can run against MySQL or an in-memory fake.
- Retry and resilience logic can wrap the operation without touching repositories.

## Repositories as ports

Repository interfaces are defined in `graphregistry/application/ports/repositories/`.

- `NodeRepository`
- `EdgeRepository`
- `LectureRepository`
- `IndexDeployRepository`

Concrete implementations live in `graphregistry/adapters/persistence/mysql/repositories/`.

## Gateways as ports

External capabilities are exposed through gateway ports in `graphregistry/application/ports/gateways/`.

- `ConceptDetectionGateway`
- `TextTranslationGateway`
- `VideoProcessingGateway`
- `VoiceProcessingGateway`
- `ImageProcessingGateway`
- `EmbeddingGateway`
- `TextGenerationGateway`
- `LectureEnrichmentGateway`

## Resilience

The application layer applies retry decorators for transient database errors so individual operations do not need to handle lock waits or connection exhaustion.

```python
@retry_on_transient_db_error()
def save(self, node: Node, actions: ActionSet = ("commit",)) -> Node:
    ...
```
