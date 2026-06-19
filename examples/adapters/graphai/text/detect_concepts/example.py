#!/usr/bin/env python3
"""Example: detect Wikipedia concepts in a text or list of texts."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway


def main() -> None:
    text = "Machine learning is a branch of artificial intelligence."

    gateway = GraphAIConceptDetectionGateway()
    concepts = gateway.detect_concepts(text=text)

    print("Detected concepts:")
    for scored in concepts.item_list:
        print(f"  {scored.concept.id}: {scored.concept.name} (score={scored.score:.3f})")


if __name__ == "__main__":
    main()
