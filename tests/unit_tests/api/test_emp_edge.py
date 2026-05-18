# tests/unit_tests/api/test_emp_edge.py
import json
from pathlib import Path
from typing import Any
import pytest
from graphregistry.entrypoints.api import schemas
from graphregistry.entrypoints.api.mappers.emp_edge import APIEdgeMapper

FIXTURE_PATH = (Path(__file__).resolve().parents[1] / "fixtures" / "unit_api_edge_mapper_inout.json")

def load_samples() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open(encoding="utf-8") as fixture_file:
        data = json.load(fixture_file)
    samples = data.get("samples")
    if not isinstance(samples, list):
        raise AssertionError("Expected unit_api_edge_mapper fixture to contain a samples list.")
    return samples

@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["edge"]["from_id"]+" -> "+sample["input"]["edge"]["to_id"])
def test_api_edge_mapper_from_save_request(sample: dict[str, Any]) -> None:
    edge = APIEdgeMapper.from_save_request(sample["input"])
    assert edge.to_json() == sample["output"]

@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["edge"]["from_id"]+" -> "+sample["input"]["edge"]["to_id"])
def test_api_edge_mapper_to_get_request(sample: dict[str, Any]) -> None:
    edge = APIEdgeMapper.from_save_request(sample["input"])
    output = APIEdgeMapper.to_get_request(edge)
    expected = schemas.EdgeMinimalFormat.model_validate(sample["input"]["edge"])
    assert output.model_dump(mode="json") == expected.model_dump(mode="json")
