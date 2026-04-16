from __future__ import annotations

from graphregistry.domain.models.mdl_concept import DetectedConcept, DetectedConceptList


class DummyConceptGateway:
    def detect_concepts(self, text: str) -> DetectedConceptList:
        return DetectedConceptList(concept_list=[DetectedConcept(concept_id="x", score=1.0, text_source=text)])


def test_concept_gateway_shape_is_usable() -> None:
    out = DummyConceptGateway().detect_concepts("raw")
    assert out.concept_list[0].concept_id == "x"
    assert out.concept_list[0].text_source == "raw"
