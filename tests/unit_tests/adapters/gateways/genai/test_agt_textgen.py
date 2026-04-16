from __future__ import annotations

import pytest

from graphregistry.adapters.gateways.genai.agt_textgen import GenAITextGenerationGateway


def test_genai_text_generation_gateway_is_explicit_placeholder() -> None:
    gateway = GenAITextGenerationGateway()

    with pytest.raises(NotImplementedError, match="placeholder"):
        gateway.generate_text(prompt="hello", language="en")
