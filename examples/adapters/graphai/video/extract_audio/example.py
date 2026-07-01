#!/usr/bin/env python3
"""Example: extract audio from a video and fingerprint it."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video token returned by get_video().
    video_token = "177581471705447505981400.mp4"

    gateway = GraphAIVideoGateway()
    voice = gateway.extract_audio(video_token=video_token)

    if voice is None:
        print("Audio extraction failed.")
        return

    if isinstance(voice, str):
        print(f"Async task launched; task_id={voice}")
        return

    print("Audio extracted successfully:")
    print(f"  token      = {voice.token}")
    print(f"  fingerprint= {voice.fingerprint}")
    print(f"  duration   = {voice.duration}")


if __name__ == "__main__":
    main()
