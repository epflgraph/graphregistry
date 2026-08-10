#!/usr/bin/env python3
"""Example: translate a single string between two languages."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway


def main() -> None:
    text = "Bonjour le monde"
    source_language = "fr"
    target_language = "en"

    gateway = GraphAITextTranslationGateway()
    translated = gateway.translate_text(
        text=text,
        source_language=source_language,
        target_language=target_language,
    )

    print(f"[{source_language}] {text}")
    print(f"[{target_language}] {translated}")


if __name__ == "__main__":
    main()
