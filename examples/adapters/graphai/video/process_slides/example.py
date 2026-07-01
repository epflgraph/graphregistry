#!/usr/bin/env python3
"""Example: process slides (fingerprint, OCR, language fallback, translation).

Reads the Google Cloud API key from the ``GOOGLE_CLOUD_API_KEY`` variable in
``.env`` at the repo root.
"""
from __future__ import annotations

import os
from pathlib import Path

from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


def _load_google_api_key() -> str | None:
    """Return GOOGLE_CLOUD_API_KEY from the environment or from ``.env``."""
    env_key = os.environ.get("GOOGLE_CLOUD_API_KEY")
    if env_key:
        return env_key

    env_path = Path(__file__).resolve().parents[5] / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == "GOOGLE_CLOUD_API_KEY":
            return value.strip().strip('"').strip("'")

    return None


def main() -> None:
    # Video token from the get_video example.
    video_token = "177581471705447505981400.mp4"

    google_api_token = _load_google_api_key()
    if google_api_token is None:
        print("GOOGLE_CLOUD_API_KEY not found in environment or .env")
        raise SystemExit(1)

    video_gateway = GraphAIVideoGateway()
    translation_gateway = GraphAITextTranslationGateway()

    language, slides = video_gateway.process_slides(
        video_token=video_token,
        destination_languages=("en", "fr"),
        translation_gateway=translation_gateway,
        google_api_token=google_api_token,
    )

    print(f"Detected slide language: {language}")
    for slide in slides.item_list:
        print(f"  timestamp={slide.timestamp}")
        print(f"  fingerprint={slide.fingerprint}")
        print(f"  text={slide.text}")
        print(f"  translations={slide.translations}")


if __name__ == "__main__":
    main()
