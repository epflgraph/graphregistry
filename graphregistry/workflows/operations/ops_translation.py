# graphregistry/workflows/operations/ops_translation.py
from __future__ import annotations
from typing import cast
from graphregistry.domain.interfaces.gateways.gtw_translation import TextTranslationGateway
from graphregistry.domain.models.mdl_translation import TranslationTask

# Class definition
class TranslationOperations:
    def __init__(self, gateway: TextTranslationGateway) -> None:
        self.gateway = gateway

    def execute(self, task: TranslationTask) -> TranslationTask:
        try:
            result = self.gateway.translate_text(
                text=cast(str, task.text),
                source_language=task.source_language,
                target_language=task.target_language,
                # force=task.force,
                # no_cache=task.no_cache,
                # clean_and_segment=task.clean_and_segment,
            )

            if not result:
                return task.model_copy(
                    update={
                        "successful": False,
                        "error_message": f"Translation failed for {task.source_language}->{task.target_language}",
                    }
                )

            return task.model_copy(
                update={
                    "result": result,
                    "successful": True,
                    "error_message": None,
                }
            )

        except Exception as e:
            return task.model_copy(
                update={
                    "successful": False,
                    "error_message": str(e),
                }
            )
