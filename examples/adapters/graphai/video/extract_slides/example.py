#!/usr/bin/env python3
"""Example: extract slide keyframes from a video."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video token returned by get_video().
    video_token = "177581471705447505981400.mp4"

    gateway = GraphAIVideoGateway()
    slides = gateway.extract_slides(video_token=video_token)

    if slides is None:
        print("Slide detection failed.")
        return

    if isinstance(slides, str):
        print(f"Async task launched; task_id={slides}")
        return

    print(f"Detected {len(slides.item_list)} slides:")
    for slide in slides.item_list:
        print(f"  timestamp={slide.timestamp}, token={slide.token}")


if __name__ == "__main__":
    main()
