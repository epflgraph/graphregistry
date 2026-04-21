# graphregistry/domain/models/mdl_translation.py
from __future__ import annotations

from pydantic import BaseModel, Field

from graphregistry.domain.models.entities.mdl_text import LanguageCode


class TranslationTask(BaseModel):
    # What GraphAI accepts:
    # - one string
    # - or a list of strings
    text: str | list[str]

    # Use aliases so model_dump(by_alias=True) produces the exact API keys:
    # {"source": "...", "target": "..."}
    source_language: LanguageCode = Field(default="fr", alias="source")
    target_language: LanguageCode = Field(default="en", alias="target")

    # Request flags supported by the endpoint
    force: bool = False
    no_cache: bool = False

    # If set to true, passes the entire text directly to the translation model without sentence segmentation
    # (i.e. breaking the text up on full stops). Increases the risk of 'text too large' errors.
    skip_segmentation: bool = False

    # If set to true, the sentence segmenter will also clean up the text.
    clean: bool = False

    # Local execution/result fields (not part of the request payload)
    result: str | list[str] | None = None
    successful: bool = False
    error_message: str | None = None

    model_config = {
        "populate_by_name": True,
    }

    def to_api_payload(self) -> dict:
        """
        Return only the fields that belong to the GraphAI request payload,
        using the endpoint's expected key names.
        """
        return {
            "text": self.text,
            "source": self.source_language,
            "target": self.target_language,
            "force": self.force,
            "no_cache": self.no_cache,
            "skip_segmentation": self.skip_segmentation,
            "clean": self.clean,
        }