# Adapters

Adapters implement the ports defined in the application layer. They translate between domain models and external infrastructure formats.

## Persistence adapters

### MySQL repositories

Located in `graphregistry/adapters/persistence/mysql/`.

| Component | Responsibility |
|-----------|----------------|
| `MySQLUnitOfWork` | Manages a transaction and exposes repositories. |
| `MySQLNodeRepository` | Maps `Node` to/from MySQL rows. |
| `MySQLLectureRepository` | Persists lecture-related entities. |
| `DefaultSchemaResolver` | Resolves schema names from `GlobalConfig`. |
| Mappers (`map_node.py`, `map_edge.py`, `map_lecture.py`, ...) | Convert between domain models and SQL records. |

The unit of work coordinates repository instances so that a single business operation can read and write nodes, edges, and lecture data atomically.

## GraphAI gateways

Located in `graphregistry/adapters/gateways/graphai/`, these gateways wrap the GraphAI service.

| Gateway | Port | Key capabilities |
|---------|------|------------------|
| `GraphAIBaseGateway` | Base class | Auth, retry, async polling, token refresh. |
| `GraphAIConceptDetectionGateway` | `ConceptDetectionGateway` | `wiki_search`, `detect_concepts`. |
| `GraphAITextTranslationGateway` | `TextTranslationGateway` | Direct and via-English translation pairs, chunking, list-shape reconstruction. |
| `GraphAIEmbeddingGateway` | `EmbeddingGateway` | Text embedding with chunking and averaging. |
| `GraphAIImageGateway` | `ImageProcessingGateway` | OCR text extraction, image fingerprinting. |
| `GraphAIVideoGateway` | `VideoProcessingGateway` | Video metadata, fingerprinting, audio/slide extraction, file download. |
| `GraphAIVoiceGateway` | `VoiceProcessingGateway` | Transcription, language detection, audio fingerprinting. |

### Base gateway behaviour

`GraphAIBaseGateway` provides:

- Lazy login and bearer-token caching.
- HTTP retry with exponential backoff on `408/425/429/5xx`.
- Automatic token refresh on `401`.
- Async endpoint submission and polling.
- `launch_only=True` for fire-and-forget task orchestration.

### Example: concept detection

```python
from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway

gtw = GraphAIConceptDetectionGateway()
concepts = gtw.detect_concepts("Graph databases are useful for knowledge graphs.")
for scored in concepts.item_list:
    print(scored.concept.id, scored.concept.name, scored.score)
```

## GenAI gateways

Located in `graphregistry/adapters/gateways/genai/`, these adapters integrate optional LLM services for lecture enrichment and text generation.

## Elasticsearch client

`graphregistry/adapters/clients/elasticsearch.py` wraps the Elasticsearch client used for building search indexes. It is a concrete client rather than a hexagonal gateway because it is consumed directly by indexing operations.
