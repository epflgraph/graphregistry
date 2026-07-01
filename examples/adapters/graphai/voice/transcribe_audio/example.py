#!/usr/bin/env python3
"""Example: transcribe audio and optionally translate subtitles."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway


def main() -> None:
    # Replace with a real audio token returned by GraphAIVideoGateway.extract_audio().
    audio_token = "175887418976912501285267.mp4_audio.ogg"

    voice_gateway = GraphAIVoiceGateway()
    translation_gateway = GraphAITextTranslationGateway()

    transcript = voice_gateway.transcribe_audio(
        audio_token=audio_token,
        destination_languages=("en", "fr"),
        translation_gateway=translation_gateway,
    )

    if transcript is None:
        print("Transcription failed.")
        return

    if isinstance(transcript, str):
        print(f"Async task launched; task_id={transcript}")
        return

    print(f"Transcript language: {transcript.language}")
    print(f"Full text: {transcript.full_text}")
    for segment in transcript.item_list:
        print(f"  [{segment.start:.2f} -> {segment.end:.2f}] {segment.text}")
        if segment.translations:
            for lang, text in segment.translations.items():
                print(f"    [{lang}] {text}")


if __name__ == "__main__":
    main()
