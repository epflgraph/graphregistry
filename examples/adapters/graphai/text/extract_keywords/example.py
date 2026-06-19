#!/usr/bin/env python3
"""Example: extract keywords from a text."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway


def main() -> None:
    text = "Machine learning is a branch of artificial intelligence."

    gateway = GraphAIConceptDetectionGateway()
    keywords = gateway.extract_keywords(text=text)

    print("Extracted keywords:")
    for keyword in keywords:
        print(f"  {keyword}")


if __name__ == "__main__":
    main()
