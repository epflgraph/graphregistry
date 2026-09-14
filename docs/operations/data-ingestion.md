# Data ingestion

GraphRegistry can ingest data through the REST API or JSON files imported via the CLI.

## Node format

A node is identified by `(type, id)` and may include fields, page profile, and concepts.

```json
{
  "type": "Course",
  "id": "graph-101",
  "subtype": "lecture series",
  "short_code": "GRAPH101",
  "title": "Introduction to Knowledge Graphs",
  "description": "Foundations of graph data models and semantic networks.",
  "url": "https://example.com/courses/graph-101",
  "custom_fields": [
    {
      "field_language": "en",
      "field_name": "credits",
      "field_value": "4"
    }
  ]
}
```

## Edge format

An edge is identified by `(from_type, from_id, to_type, to_id, context)`.

```json
{
  "from_type": "Person",
  "from_id": "alice-smith",
  "to_type": "Course",
  "to_id": "graph-101",
  "context": "teacher",
  "custom_fields": [
    {
      "field_language": "n/a",
      "field_name": "role_order",
      "field_value": "1"
    }
  ]
}
```

## Bulk import via CLI

```bash
graphregistry data insert \
  --node_list=@path/to/nodes.json \
  --edge_list=@path/to/edges.json \
  --actions=commit
```

Use `--actions=eval` first to validate the input without writing to the database.

## API ingestion

Save a single node:

```bash
curl -X POST http://127.0.0.1:9999/api/nodes/save \
  -H "Content-Type: application/json" \
  -d @node.json
```

Save many nodes:

```bash
curl -X POST http://127.0.0.1:9999/api/nodes/save_many \
  -H "Content-Type: application/json" \
  -d @nodes.json
```

The API enforces the allow-list configured in `config/config_api.json`.

## Concept detection

When a `ConceptDetectionGateway` is configured, `NodeOperations.enrich_with_concepts` can detect concepts from a node's title and raw text.

```python
from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.application.operations.ops_node import NodeOperations

node_ops = NodeOperations(
    uow_factory=...,
    concept_detection_gateway=GraphAIConceptDetectionGateway(),
)
node_ops.enrich_with_concepts(node)
```

## Validation

- Node and edge types must be allowed by `config_api.json`.
- Field keys must be consistent with their parent node/edge key.
- Duplicate keys are rejected with a `DuplicateKeyError` (HTTP 409 in the API).
