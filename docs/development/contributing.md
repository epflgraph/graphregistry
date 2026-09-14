# Contributing

Contributions to GraphRegistry are welcome. The project is organised around hexagonal architecture principles, so please keep dependency direction in mind when adding code.

## Layer rules

1. **Domain** — pure business models and invariants. No framework imports.
2. **Application** — use cases and ports. Depends only on domain.
3. **Adapters** — concrete implementations. Depends on application/domain.
4. **Entrypoints** — API/CLI wiring. Depends on application/domain/adapters.

## Adding a feature

1. Start with the domain model if the feature introduces new business concepts.
2. Define a port in `graphregistry/application/ports/` if the feature needs persistence or an external service.
3. Implement the port in `graphregistry/adapters/`.
4. Add a thin entrypoint in `graphregistry/entrypoints/api/` or `graphregistry/entrypoints/cli/`.
5. Add tests for domain and application logic without real databases or external services.

## Code style

- Use type hints.
- Keep public methods documented.
- Prefer explicit dependency injection over global state.
- Use domain models at layer boundaries; do not leak SQL rows or HTTP payloads.

## Pull requests

1. Open an issue to discuss large changes.
2. Keep pull requests focused on a single concern.
3. Ensure existing tests pass.
4. Update documentation if the change affects user-facing behaviour.

## Repository

[github.com/epflgraph/graphregistry](https://github.com/epflgraph/graphregistry)
