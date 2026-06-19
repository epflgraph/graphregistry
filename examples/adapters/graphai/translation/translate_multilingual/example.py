#!/usr/bin/env python3
"""Example: translate a MultilingualText object into several languages."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.domain.models.entities.mdl_text import MultilingualText


def main() -> None:
    text = MultilingualText(item_map={"fr": "Bonjour le monde"})

    gateway = GraphAITextTranslationGateway()
    translated = gateway.translate_multilingual(
        text=text,
        source_language="fr",
        target_languages=("en", "de"),
    )

    print("Translations:")
    for lang in translated.keys():
        print(f"  [{lang}] {translated.get(lang)}")


if __name__ == "__main__":
    main()
