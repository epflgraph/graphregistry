# Domain models

The `graphregistry/domain/models/` package contains the business entities and value objects used throughout the service. These models are plain Pydantic objects with no framework or database dependencies.

## Core entities

### Node

A `Node` represents a single object in the knowledge graph.

```python
class Node(BaseModel):
    key: NodeKey
    title: str = ""
    text_source: str | None = None
    raw_text: str | None = None
    field_list: NodeFieldList
    page_profile: PageProfile | None = None
    concepts: NodeConceptList
```

`NodeKey` is the immutable identifier `(object_type, object_id)`.

### Edge

An `Edge` connects two nodes.

```python
class Edge(BaseModel):
    key: EdgeKey
    field_list: EdgeFieldList
```

`EdgeKey` is `(from_object_type, from_object_id, to_object_type, to_object_id, context)`.

### Concepts

Concepts attach semantic meaning to nodes.

```python
class Concept(BaseModel):
    id: str
    name: str

class ScoredConcept(BaseModel):
    concept: Concept
    score: float

class ScoredConceptList(BaseModel):
    item_list: list[ScoredConcept]
```

`NodeConceptList` groups concepts by provenance:

- `detected` — automatically detected.
- `ai_validated` — validated by an AI step.
- `manually_mapped` — curated by a human.

### Lectures and media

Lecture processing introduces media domain objects:

```python
class Video(BaseModel): ...
class Voice(BaseModel): ...
class Slide(BaseModel): ...
class SlideList(BaseModel): item_list: list[Slide]
class Transcript(BaseModel): ...
class TranscriptSegment(BaseModel): ...
```

These models live in `graphregistry/domain/models/entities/mdl_lecture.py`.

## Value objects

- `ObjectType` — literal of allowed node/edge types.
- `FieldLanguage` — language code or `"n/a"`.
- `ActionSet` — tuple of actions such as `("eval",)` or `("commit",)`.

## List wrappers

Many domain objects have a corresponding list wrapper to keep bulk operations explicit:

```python
class NodeList(BaseModel): item_list: list[Node]
class EdgeList(BaseModel): item_list: list[Edge]
class NodeKeyList(BaseModel): item_list: list[NodeKey]
class EdgeKeyList(BaseModel): item_list: list[EdgeKey]
```

## Tasks

Task models represent units of work submitted to external AI services:

- `ConceptDetectionTask`
- `TranslationTask`
- `LectureEnrichmentTask`

These live in `graphregistry/domain/models/tasks/`.
