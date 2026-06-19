#!/usr/bin/env python3
"""Example: search Wikipedia concepts by name."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway


def main() -> None:
    search_term = "machine learning"

    gateway = GraphAIConceptDetectionGateway()
    suggestions = gateway.wiki_search(search_term=search_term)

    print(f"Wiki suggestions for '{search_term}':")
    for suggestion in suggestions:
        print(f"  {suggestion.get('concept_id')}: {suggestion.get('concept_name')}")


if __name__ == "__main__":
    main()
