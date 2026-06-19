# graphregistry/domain/types.py
from __future__ import annotations
from typing import Literal

# Types of concept maps that can be associated with a Node.
ConceptMapType = Literal['detected', 'ai_validated', 'manually_mapped']
