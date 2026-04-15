# graphregistry/domain/interfaces/types.py
from __future__ import annotations
from typing import Literal, TypeAlias

ActionName: TypeAlias = Literal["print", "eval", "commit"]
ActionSet: TypeAlias = tuple[ActionName, ...]