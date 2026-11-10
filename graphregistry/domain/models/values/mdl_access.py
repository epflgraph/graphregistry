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
    """Outcome of a GraphAuth verify call for one key.

    auth_failed marks credential problems (GraphAuth answered 401): in
    enforce mode these must surface as 401 (bad credential), not 403
    (known principal, forbidden action).
    """
    allowed     : bool
    principal   : str | None = None
    reason      : str | None = None
    auth_failed : bool = False
