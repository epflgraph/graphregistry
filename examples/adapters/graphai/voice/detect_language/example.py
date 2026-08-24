#!/usr/bin/env python3
"""Example: detect the spoken language of an audio token."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.gtw_voice import GraphAIVoiceGateway


def main() -> None:
    # Replace with a real audio token returned by GraphAIVideoGateway.extract_audio().
    audio_token = "175887418976912501285267.mp4_audio.ogg"

    gateway = GraphAIVoiceGateway()
    language = gateway.detect_language(audio_token=audio_token)

    if language is None:
        print("Language detection failed.")
        return

    print(f"Detected audio language: {language}")


if __name__ == "__main__":
    main()
