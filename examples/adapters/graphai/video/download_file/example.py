#!/usr/bin/env python3
"""Example: download the video file bytes for a video token."""
from __future__ import annotations

from pathlib import Path
from tempfile import gettempdir

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video token returned by get_video().
    video_token = "VIDEO-TOKEN-PLACEHOLDER"

    output_path = Path(gettempdir()) / "graphai_video_example.mp4"

    gateway = GraphAIVideoGateway()
    result = gateway.download_file(token=video_token, file_path=output_path)

    if result is None:
        print("Download failed.")
        return

    print(f"Downloaded video to {result} ({result.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
