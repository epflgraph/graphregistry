from __future__ import annotations

from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_concept import DetectedConcept, DetectedConceptList


def test_detected_concept_from_json_and_to_json() -> None:
    payload = {
        "object_key": {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "CS-101",
        },
        "concept_id": "autonomous-systems",
        "text_source": "summary",
        "score": 0.92,
    }

    concept = DetectedConcept.from_json(payload)

    assert concept.object_key == NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101")
    assert concept.to_json()["concept_id"] == "autonomous-systems"
    assert concept.to_json()["score"] == 0.92


def test_detected_concept_list_from_json_handles_none() -> None:
    out = DetectedConceptList.from_json(None)  # type: ignore[arg-type]

    assert out.concept_list == []
    assert out.to_json() == []


def test_detected_concept_list_round_trip() -> None:
    docs = [
        {"concept_id": "a", "score": 1.0},
        {"concept_id": "b", "score": 0.5, "text_source": "raw_text"},
    ]

    out = DetectedConceptList.from_json(docs)

    assert [item.concept_id for item in out.concept_list] == ["a", "b"]
    assert out.to_json() == [
        {"object_key": None, "concept_id": "a", "text_source": None, "score": 1.0},
        {"object_key": None, "concept_id": "b", "text_source": "raw_text", "score": 0.5},
    ]
