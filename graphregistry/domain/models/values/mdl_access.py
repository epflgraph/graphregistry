# graphregistry/domain/models/values/mdl_access.py
"""Access-control value objects shared between application and entrypoints."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


#==================#
# Class Definition #
#==================#
class AccessAction(str, Enum):
    """Vocabulary of checked actions (mirrors GraphAuth's action set)."""
    READ  = "read"
    WRITE = "write"


#==================#
# Class Definition #
#==================#
class EnforcementMode(str, Enum):
    """How GraphRegistry treats GraphAuth decisions.

    - OFF:     no GraphAuth calls at all (pre-integration behaviour).
    - LOG:     decisions are consulted and logged, never blocking (dry-run).
    - ENFORCE: denials block the operation (403 via AccessDeniedError).
    """
    OFF     = "off"
    LOG     = "log"
    ENFORCE = "enforce"


#==================#
# Class Definition #
#==================#
@dataclass(frozen=True)
class AccessDecision:
    """Outcome of a GraphAuth verify call for one key."""
    allowed  : bool
    principal: str | None = None
    reason   : str | None = None
