# tests/unit_tests/api/test_emp_node.py
import json
from pathlib import Path
from typing import Any
import pytest
from graphregistry.entrypoints.api.mappers.emp_node import APINodeMapper

FIXTURE_PATH = (Path(__file__).resolve().parents[1] / "fixtures" / "unit_api_node_mapper.json")

def load_samples() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open(encoding="utf-8") as fixture_file:
        data = json.load(fixture_file)
    samples = data.get("samples")
    if not isinstance(samples, list):
        raise AssertionError("Expected unit_api_node_mapper fixture to contain a samples list.")
    return samples

@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["node"]["id"])
def test_api_node_mapper_from_save_request(sample: dict[str, Any]) -> None:
    node = APINodeMapper.from_save_request(sample["input"])
    assert node.to_json() == sample["output"]
