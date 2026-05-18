# tests/unit_tests/api/test_emp_node.py
import json
from pathlib import Path
from typing import Any

import pytest

from graphregistry.entrypoints.mappers import SpecMapper
from graphregistry.entrypoints.schemas import NodeSpec

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "unit_tests"
    / "unit_api_node_mapper_inout.json"
)


def load_samples() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open(encoding="utf-8") as fixture_file:
        data = json.load(fixture_file)
    samples = data.get("samples")
    if not isinstance(samples, list):
        raise AssertionError("Expected unit_api_node_mapper fixture to contain a samples list.")
    return samples


@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["node"]["id"])
def test_spec_mapper_from_node_spec(sample: dict[str, Any]) -> None:
    node = SpecMapper.from_node_spec(sample["input"]["node"])
    assert node.to_json() == sample["output"]


@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["node"]["id"])
def test_spec_mapper_to_node_spec(sample: dict[str, Any]) -> None:
    node = SpecMapper.from_node_spec(sample["input"]["node"])
    output = SpecMapper.to_node_spec(node)
    expected = NodeSpec.model_validate(sample["input"]["node"])
    assert output.model_dump(mode="json", exclude_none=True) == expected.model_dump(mode="json", exclude_none=True)
