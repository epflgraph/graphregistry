#!/usr/bin/env python
"""Simple chat client for the EPFL inference server.

Usage:
    python scripts/chat.py --model MODEL_NAME --prompt textfile.txt

Reads the inference URL and API key from the ``EPFL_RCP_API_URL`` and
``EPFL_RCP_API_KEY`` variables in the nearest ``.env`` file (walking up
from this script's directory), sends the prompt text to the
OpenAI-compatible ``/chat/completions`` endpoint, and prints the model's
output.
"""

import argparse
import os
import sys
from pathlib import Path

import requests


def load_env() -> None:
    """Load variables from the nearest .env file into os.environ (no overwrite)."""
    current = Path(__file__).resolve().parent
    for candidate in [current, *current.parents]:
        env_file = candidate / ".env"
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                os.environ.setdefault(key, value)
            break


def read_prompt(path: str) -> str:
    prompt_file = Path(path)
    if not prompt_file.is_file():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return prompt_file.read_text(encoding="utf-8")


def chat(model: str, prompt: str) -> str:
    url = os.environ.get("EPFL_RCP_API_URL")
    key = os.environ.get("EPFL_RCP_API_KEY")
    if not url or not key:
        raise RuntimeError(
            "EPFL_RCP_API_URL and EPFL_RCP_API_KEY must be set (in .env or environment)"
        )

    response = requests.post(
        f"{url.rstrip('/')}/completions",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "model": model,
            "prompt": prompt,
            "max_tokens": 128,
            "temperature": 0,
        },
        timeout=120,
    )

    response.raise_for_status()
    payload = response.json()
    return payload["choices"][0]["text"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat with a model on the EPFL inference server")
    parser.add_argument("--model", required=True, help="Model name, e.g. gpt-4o")
    parser.add_argument("--prompt", required=True, help="Path to a text file containing the prompt")
    args = parser.parse_args()

    load_env()
    prompt = read_prompt(args.prompt)
    try:
        output = chat(args.model, prompt)
    except requests.HTTPError as exc:
        sys.exit(f"HTTP error: {exc} - {exc.response.text}")
    print(output)


if __name__ == "__main__":
    main()
