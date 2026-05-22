# tests/unit_tests/api/test_emp_node.py
import json
from copy import deepcopy
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


def expected_short_code(node_input: dict[str, Any]) -> str:
    return node_input["short_code"] if node_input.get("short_code") is not None else node_input["id"]


@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["node"]["id"])
def test_spec_mapper_from_node_spec(sample: dict[str, Any]) -> None:
    node = SpecMapper.from_node_spec(sample["input"]["node"])
    expected_output = deepcopy(sample["output"])
    expected_output["page_profile"]["short_code"] = expected_short_code(sample["input"]["node"])
    assert node.to_json() == expected_output


@pytest.mark.parametrize("sample", load_samples(), ids=lambda sample: sample["input"]["node"]["id"])
def test_spec_mapper_to_node_spec(sample: dict[str, Any]) -> None:
    node = SpecMapper.from_node_spec(sample["input"]["node"])
    output = SpecMapper.to_node_spec(node)
    expected = NodeSpec.model_validate(sample["input"]["node"])
    output_dump = output.model_dump(mode="json", exclude_none=True)
    expected_dump = expected.model_dump(mode="json", exclude_none=True)
    expected_dump["short_code"] = expected_short_code(sample["input"]["node"])
    assert output_dump == expected_dump
