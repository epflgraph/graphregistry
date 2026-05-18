# tests/unit_tests/api/test_emp_edge.py
import json
from pathlib import Path
from typing import Any

import pytest

from graphregistry.domain.models.entities.mdl_edge import Edge
from graphregistry.entrypoints.mappers import SpecMapper
from graphregistry.entrypoints.schemas import EdgeSpec

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "unit_tests"
    / "unit_api_edge_mapper_inout.json"
)


def load_samples() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open(encoding="utf-8") as fixture_file:
        data = json.load(fixture_file)
    samples = data.get("samples")
    if not isinstance(samples, list):
        raise AssertionError("Expected unit_api_edge_mapper fixture to contain a samples list.")
    return samples


@pytest.mark.parametrize(
    "sample",
    load_samples(),
    ids=lambda sample: f"{sample['input']['edge']['from_id']} -> {sample['input']['edge']['to_id']}",
)
def test_spec_mapper_from_edge_spec(sample: dict[str, Any]) -> None:
    edge = SpecMapper.from_edge_spec(sample["input"]["edge"])
    assert edge.to_json() == sample["output"]


@pytest.mark.parametrize(
    "sample",
    load_samples(),
    ids=lambda sample: f"{sample['input']['edge']['from_id']} -> {sample['input']['edge']['to_id']}",
)
def test_spec_mapper_to_edge_spec(sample: dict[str, Any]) -> None:
    edge = Edge.model_validate(sample["output"])
    output = SpecMapper.to_edge_spec(edge)
    expected = EdgeSpec.model_validate(sample["input"]["edge"])
    assert output.model_dump(mode="json", exclude_none=True) == expected.model_dump(mode="json", exclude_none=True)
