#!/usr/bin/env python3
"""Example: compute embedding vectors for a list of strings."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_embedding import GraphAIEmbeddingGateway


def main() -> None:
    texts = [
        "Machine learning is a branch of artificial intelligence.",
        "Natural language processing enables machines to understand text.",
        None,  # Placeholders are preserved in the output.
    ]

    gateway = GraphAIEmbeddingGateway()
    embeddings = gateway.embed_text_list(list_of_texts=texts)

    if embeddings is None:
        print("Embedding failed.")
        return

    for idx, embedding in enumerate(embeddings):
        if embedding is None:
            print(f"[{idx}] None")
        elif isinstance(embedding, str):
            print(f"[{idx}] task_id={embedding}")
        else:
            print(f"[{idx}] dim={len(embedding)}, first 3={embedding[:3]}")


if __name__ == "__main__":
    main()
