from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from graphregistry.common.config import GlobalConfig, REPO_ROOT
from graphregistry.domain.interfaces.gateways.gtw_texttranslate import TextTranslationGateway
from graphregistry.domain.models.mdl_gentext import MultilingualText


LoginFn = Callable[[str], dict[str, Any]]
TranslateFn = Callable[[str, str, str, dict[str, Any]], str]


class GraphAITextTranslationGateway(TextTranslationGateway):
    def __init__(
        self,
        graph_api_json: str | Path | None = None,
        login_info: dict[str, Any] | None = None,
        login_fn: LoginFn | None = None,
        translate_fn: TranslateFn | None = None,
    ) -> None:
        self.graph_api_json = self._resolve_graph_api_json(graph_api_json)
        self._login_info = login_info
        self._login_fn = login_fn
        self._translate_fn = translate_fn

    def translate_text(self, text: str, source_language: str, target_language: str) -> str:
        if not text or source_language == target_language:
            return text
        login_info = self._ensure_login_info()
        return self._get_translate_fn()(text, source_language, target_language, login_info)

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: str,
        target_languages: tuple[str, ...] = ("en", "fr", "de", "it"),
    ) -> MultilingualText:
        out = text.model_copy(deep=True)
        source_value = getattr(text, source_language, None)
        if source_value is None:
            return out

        for lang in target_languages:
            if lang == source_language:
                continue
            current = getattr(out, lang, None)
            if current is None or len(current.strip()) == 0:
                translated = self.translate_text(source_value, source_language=source_language, target_language=lang)
                setattr(out, lang, translated)
        return out

    def _ensure_login_info(self) -> dict[str, Any]:
        if self._login_info is not None:
            return self._login_info

        login_info = self._get_login_fn()(str(self.graph_api_json))
        if not isinstance(login_info, dict) or not login_info:
            raise ValueError("Failed to obtain valid GraphAI login info.")
        self._login_info = login_info
        return login_info

    def _get_login_fn(self) -> LoginFn:
        if self._login_fn is not None:
            return self._login_fn

        from graphai_client.client import login as graphai_login

        self._login_fn = graphai_login
        return self._login_fn

    def _get_translate_fn(self) -> TranslateFn:
        if self._translate_fn is not None:
            return self._translate_fn

        from graphai_client.client_api.translation import translate_text_str

        def _translate(text: str, source_language: str, target_language: str, login_info: dict[str, Any]) -> str:
            return translate_text_str(
                text=text,
                source_language=source_language,
                target_language=target_language,
                force=True,
                login_info=login_info,
            ).strip()

        self._translate_fn = _translate
        return self._translate_fn

    @staticmethod
    def _resolve_graph_api_json(graph_api_json: str | Path | None) -> Path:
        if graph_api_json is None:
            glbcfg = GlobalConfig()
            graph_api_json = glbcfg.settings["graphai"]["client_config_file"]
        p = Path(graph_api_json)
        return p if p.is_absolute() else (REPO_ROOT / p)
