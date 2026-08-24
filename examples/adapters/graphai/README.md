# GraphAI Adapter Examples

This directory contains runnable examples for the GraphAI hexagonal adapters in
`graphregistry/adapters/gateways/graphai/`.

## Layout

Each endpoint has its own folder containing:

- `example.py` — a small Python script showing how to instantiate the adapter
  and call the endpoint.
- `run.sh` — convenience shell wrapper that runs `example.py`.

Endpoint groups mirror the GraphAI domains:

```
examples/adapters/graphai/
├── video/
│   ├── get_video/
│   ├── get_token_from_url/
│   ├── fingerprint/
│   ├── extract_audio/
│   ├── extract_slides/
│   ├── process_slides/
│   └── download_file/
├── voice/
│   ├── transcribe_audio/
│   ├── detect_language/
│   └── fingerprint/
├── image/
│   ├── extract_text/
│   └── fingerprint/
├── text/
│   ├── wiki_search/
│   ├── detect_concepts/
│   └── extract_keywords/
├── translation/
│   ├── translate_text/
│   └── translate_multilingual/
└── embedding/
    ├── embed_text_str/
    └── embed_text_list/
```

## Running an example

Make sure the package is installed in editable mode so the adapters are
importable:

```bash
pip install -e .
```

Then run any example:

```bash
./examples/adapters/graphai/video/get_video/run.sh
```

Most examples use placeholder tokens/URLs. Replace them with real values from
your GraphAI instance before running against a live service. The active GraphAI
configuration is read from `config/config_graphai_client.json` via
`GlobalConfig`.

## Notes

- Examples that produce media tokens (`get_video`, `extract_audio`,
  `extract_slides`, etc.) print placeholder tokens by default. Wire them
  together to build a full pipeline:
  1. `get_token_from_url` → video token (minimal URL-to-token step)
  2. `get_video` → full `Video` object (use when you also need metadata)
  3. `extract_audio` / `extract_slides` → audio/slide tokens
  4. `voice/transcribe_audio` or `image/extract_text` → text
- `process_slides` demonstrates the high-level orchestration that combines
  slide detection, fingerprinting, OCR, language fallback, and translation.
- The GraphAI bearer token is cached both in-memory and on disk
  (`~/.cache/graphregistry/graphai_tokens.json`) so the first example run pays
  the authentication cost (~10-20s in some environments) and subsequent runs
  reuse the token.
