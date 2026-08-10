#!/usr/bin/env python3
"""Example: compute an embedding vector for a single string."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.gtw_embedding import GraphAIEmbeddingGateway


def main() -> None:
    text = "Machine learning is a branch of artificial intelligence."

    gateway = GraphAIEmbeddingGateway()
    # Use max_tries=1 in the example so an unreachable GraphAI service fails
    # fast rather than retrying for minutes. Remove it in production code.
    embedding = gateway.embed_text_str(text=text, max_tries=1)

    if embedding is None:
        print("Embedding failed.")
        return

    if isinstance(embedding, str):
        print(f"Async task launched; task_id={embedding}")
        return

    print(f"Embedding dimension: {len(embedding)}")
    print(f"First 5 values: {embedding[:5]}")


if __name__ == "__main__":
    main()
