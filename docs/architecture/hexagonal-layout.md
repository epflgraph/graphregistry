# Hexagonal layout

GraphRegistry follows a hexagonal (ports-and-adapters) architecture. The central idea is to isolate domain logic from infrastructure so that databases, external APIs, and delivery mechanisms can change without affecting the business rules.

## Ports and adapters

A **port** is an interface defined in the application or domain layer. An **adapter** is a concrete implementation of that interface.

| Port | Example adapters |
|------|------------------|
| `UnitOfWork` | `MySQLUnitOfWork` |
| `NodeRepository` | `MySQLNodeRepository` |
| `EdgeRepository` | `MySQLEdgeRepository` |
| `ConceptDetectionGateway` | `GraphAIConceptDetectionGateway` |
| `TextTranslationGateway` | `GraphAITextTranslationGateway` |
| `VideoProcessingGateway` | `GraphAIVideoGateway` |
| `VoiceProcessingGateway` | `GraphAIVoiceGateway` |

## Why this matters

- **Testability**: Domain and application tests can use fake repositories and gateways.
- **Swapability**: MySQL can be replaced with another store by writing a new adapter.
- **Clarity**: Each layer has a single responsibility.

## Boundary rules

1. Domain models cross layer boundaries; infrastructure payloads do not.
2. Adapters map SQL rows, HTTP JSON, or CLI arguments to domain models.
3. Application services never import FastAPI, SQLAlchemy, `requests`, or other framework libraries.
4. Entrypoints stay thin: validation, mapping, and delegation only.

## Example: saving a node

```python
# Entrypoint: API router
@router.post("/api/nodes/save")
def nodes_save(request: APINodesSaveRequest, node_ops: NodeOperations = Depends(get_node_ops)):
    node = SpecMapper.from_node_spec(request.node)
    saved_node = node_ops.save(node, actions=("commit",))
    return APINodesSaveResponse(saved_key=...)

# Application service
class NodeOperations:
    def save(self, node: Node, actions):
        with self.uow_factory() as uow:
            return uow.nodes.save(node, actions=actions)

# Adapter
class MySQLNodeRepository:
    def save(self, node: Node, actions):
        row = NodeMapper.to_row(node)
        ...
```

## Adapter conventions

- Do not leak `login_info`, SQL rows, or HTTP payloads into domain/application logic.
- Treat gateways as ports for external systems.
- Preserve graceful-degradation behaviour: language fallback, text chunking, list-shape reconstruction.
- Provide `launch_only=True` variants for long-running async tasks.
