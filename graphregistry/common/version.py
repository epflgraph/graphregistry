# graphregistry/common/version.py
"""Single source of truth for the Registry API version.

The version is read from the installed package metadata so that it stays in
sync with the version declared in pyproject.toml.
"""

from __future__ import annotations

from importlib.metadata import version

REGISTRY_API_VERSION: str = version("graphregistry")
