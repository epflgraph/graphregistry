import unittest

from graphregistry.adapters.graphai.adp_translationgatw import GraphAITextTranslationGateway
from graphregistry.domain.models.mdl_text import MultilingualText


class GraphAITextTranslationGatewayTests(unittest.TestCase):
    def test_translate_multilingual_fills_missing_languages(self) -> None:
        gateway = GraphAITextTranslationGateway(
            login_fn=lambda _cfg: {"token": "ok"},
            translate_fn=lambda text, source, target, _login: f"{text}:{source}->{target}",
        )

        out = gateway.translate_multilingual(
            text=MultilingualText(en="hello"),
            source_language="en",
            target_languages=("en", "fr", "de"),
        )

        self.assertEqual(out.en, "hello")
        self.assertEqual(out.fr, "hello:en->fr")
        self.assertEqual(out.de, "hello:en->de")


if __name__ == "__main__":
    unittest.main()
