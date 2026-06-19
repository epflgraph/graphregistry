#!/usr/bin/env python3
"""Example: process slides (fingerprint, OCR, language fallback, translation)."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video token returned by get_video().
    video_token = "VIDEO-TOKEN-PLACEHOLDER"

    video_gateway = GraphAIVideoGateway()
    translation_gateway = GraphAITextTranslationGateway()

    language, slides = video_gateway.process_slides(
        video_token=video_token,
        destination_languages=("en", "fr"),
        translation_gateway=translation_gateway,
    )

    print(f"Detected slide language: {language}")
    for slide in slides.item_list:
        print(f"  timestamp={slide.timestamp}")
        print(f"  fingerprint={slide.fingerprint}")
        print(f"  text={slide.text}")
        print(f"  translations={slide.translations}")


if __name__ == "__main__":
    main()
