import unittest

from graphregistry.adapters.graphai.adp_conceptgatw import GraphAIConceptGateway


class GraphAIConceptGatewayTests(unittest.TestCase):
    def test_detect_concepts_maps_graphai_payload(self) -> None:
        def fake_login(graph_api_json: str) -> dict[str, str]:
            return {"token": "ok", "config": graph_api_json}

        def fake_extract(text: str, login_info: dict[str, str]) -> list[dict[str, object]]:
            self.assertEqual(text, "Sample text")
            self.assertIn("token", login_info)
            return [
                {"concept_id": "C1", "score": 0.8},
                {"id": "C2", "confidence": "0.9", "text_source": "raw_text"},
                {"concept": "C3"},
                {"score": 0.5},  # skipped (no concept id)
            ]

        gateway = GraphAIConceptGateway(login_fn=fake_login, extract_fn=fake_extract)
        out = gateway.detect_concepts("Sample text")

        self.assertEqual(len(out.concept_list), 3)
        self.assertEqual(out.concept_list[0].concept_id, "C1")
        self.assertEqual(out.concept_list[0].score, 0.8)
        self.assertEqual(out.concept_list[1].concept_id, "C2")
        self.assertEqual(out.concept_list[1].score, 0.9)
        self.assertEqual(out.concept_list[1].text_source, "raw_text")
        self.assertEqual(out.concept_list[2].concept_id, "C3")
        self.assertEqual(out.concept_list[2].score, 1.0)

    def test_detect_concepts_empty_text_returns_empty_list(self) -> None:
        gateway = GraphAIConceptGateway(
            login_fn=lambda _: {"token": "ok"},
            extract_fn=lambda _text, _login: [{"concept_id": "C1", "score": 0.7}],
        )
        out = gateway.detect_concepts("   ")
        self.assertEqual(out.concept_list, [])


if __name__ == "__main__":
    unittest.main()
