# graphregistry/domain/exceptions.py
"""Domain-level exceptions used across the application and entrypoints."""

from __future__ import annotations


class DisallowedTypeError(ValueError):
    """Raised when a node or edge type is not allowed by API configuration."""
