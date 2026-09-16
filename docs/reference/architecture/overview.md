# Architecture overview

GraphRegistry is organised as a hexagonal-ish application. The goal is to keep business logic independent of frameworks, databases, and external services while still providing concrete adapters for real-world deployment.

## High-level layers

```
                    +------------------+
                    |   Entrypoints    |
                    |  API / CLI / ... |
                    +--------+---------+
                             |
        +--------------------+--------------------+
        |                                         |
+-------v---------+                     +---------v-------+
|    Adapters     |                     |   Adapters      |
| (persistence,   |<--domain models-->  | (gateways /     |
|  MySQL repos)   |                     |  external AI)   |
+--------+--------+                     +---------+-------+
         |                                        |
         |            +----------------+          |
         +----------->|  Application   |<---------+
                      |   operations   |
                      +--------+-------+
                               |
                      +--------v--------+
                      |     Domain      |
                      |    models       |
                      +-----------------+
```

## Dependency rule

Dependencies always point inward:

- Entrypoints depend on application services and domain models.
- Adapters depend on application ports and domain models.
- Application services depend only on domain models and ports.
- Domain models depend on nothing outside themselves.

This means you can test domain logic and use cases without a database, Elasticsearch, or GraphAI instance.

## Key packages

| Package | Responsibility |
|---------|----------------|
| `graphregistry/domain/models` | Business entities and value objects: `Node`, `Edge`, `Concept`, `Video`, ... |
| `graphregistry/application/operations` | Use cases / application services: `NodeOperations`, `EdgeOperations`, `LectureOperations`, ... |
| `graphregistry/application/ports` | Ports (interfaces) for repositories and external gateways. |
| `graphregistry/adapters` | Concrete adapters: MySQL repositories, GraphAI gateways, Elasticsearch clients, GenAI gateways. |
| `graphregistry/entrypoints` | Thin API routers, CLI commands, and request/response schemas. |
| `graphregistry/common` | Configuration, logging, paths, and shared utilities. |

## Configuration boundary

`GlobalConfig` loads `config/environment/config_registry.yml` at startup. Concrete adapters read service-specific configuration files (for example, `config/environment/config_graphai.yml` for GraphAI, `config/environment/config_graphdb.yml` for MySQL). Application and domain code never read files directly; they receive configured objects through dependency injection.

## Lifecycle of a request

1. An entrypoint (API or CLI) receives input.
2. It maps the input to a domain model using a mapper.
3. It calls an application operation with the model.
4. The operation orchestrates the workflow and talks to repositories or gateways through ports.
5. Adapters implement the ports, translating domain models to MySQL rows, GraphAI HTTP calls, or Elasticsearch documents.
6. The operation returns a domain result, which the entrypoint maps back to an API/CLI response.
