from __future__ import annotations

from typing import get_args

from graphregistry.domain.interfaces.types import ActionName


def test_action_name_literal_values_are_stable() -> None:
    assert set(get_args(ActionName)) == {"print", "eval", "commit"}
