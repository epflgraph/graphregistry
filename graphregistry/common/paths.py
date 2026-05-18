# graphregistry/common/paths.py
from __future__ import annotations

import os
from pathlib import Path


# Environment variable override, useful in production, tests, Docker, systemd, etc.
GRAPHREGISTRY_ROOT_ENV = "GRAPHREGISTRY_ROOT"


def find_repo_root(start: Path | None = None) -> Path:
    """
    Find GraphRegistry project root.

    Resolution order:
      1. GRAPHREGISTRY_ROOT env var
      2. Walk upward from `start` or this file until a project marker is found

    This makes paths independent of the shell's current working directory.
    """

    # 1. Explicit environment override
    env_root = os.environ.get(GRAPHREGISTRY_ROOT_ENV)
    if env_root:
        root = Path(env_root).expanduser().resolve()
        if not root.exists():
            raise FileNotFoundError(
                f"{GRAPHREGISTRY_ROOT_ENV} points to a non-existing path: {root}"
            )
        return root

    # 2. Auto-discover by walking upward
    start_path = (start or Path(__file__)).resolve()

    candidates = [start_path]
    if start_path.is_file():
        candidates = [start_path.parent, *start_path.parents]
    else:
        candidates = [start_path, *start_path.parents]

    for parent in candidates:
        if (
            (parent / "graphregistry").is_dir()
            and (parent / "config").is_dir()
            and (parent / "database").is_dir()
        ):
            return parent

    raise RuntimeError(f"Could not find GraphRegistry repo root from: {start_path}")


REPO_ROOT = find_repo_root()

CONFIG_DIR = REPO_ROOT / "config"
DATABASE_DIR = REPO_ROOT / "database"
EXAMPLES_DIR = REPO_ROOT / "examples"
RESOURCES_DIR = REPO_ROOT / "resources"
SCRIPTS_DIR = REPO_ROOT / "scripts"


CONFIG_DB_PATH = CONFIG_DIR / "config_db.yaml"
CONFIG_GLOBAL_PATH = CONFIG_DIR / "config_global.yaml"
CONFIG_GLOBAL_TEMPLATE_PATH = CONFIG_DIR / "config_global.template.yaml"
CONFIG_INDEX_PATH = CONFIG_DIR / "config_index.json"
CONFIG_INDEX_TEMPLATE_PATH = CONFIG_DIR / "config_index.template.json"
CONFIG_SCORES_PATH = CONFIG_DIR / "config_scores.json"
CONFIG_SCORES_TEMPLATE_PATH = CONFIG_DIR / "config_scores.template.json"
CONFIG_GRAPHAI_CLIENT_PATH = CONFIG_DIR / "config_graphai_client.json"


DATABASE_QUERIES_DIR = DATABASE_DIR / "queries"
DATABASE_INIT_DIR = DATABASE_DIR / "init"
DATABASE_CONFIG_DATATYPES_PATH = DATABASE_INIT_DIR / "config" / "config_datatypes.json"


def repo_path(*parts: str | Path) -> Path:
    """
    Build an absolute path relative to the GraphRegistry repo root.
    """
    return REPO_ROOT.joinpath(*map(Path, parts))


def config_path(*parts: str | Path) -> Path:
    """
    Build an absolute path relative to config/.
    """
    return CONFIG_DIR.joinpath(*map(Path, parts))


def database_path(*parts: str | Path) -> Path:
    """
    Build an absolute path relative to database/.
    """
    return DATABASE_DIR.joinpath(*map(Path, parts))


def resolve_cli_input_path(path_arg: str) -> Path:
    """
    Resolve a CLI file argument.

    Supports:
      request.json
      @request.json
      examples/entrypoints/node_save/request.json
      @examples/entrypoints/node_save/request.json
      ~/file.json
      /absolute/file.json

    Resolution order for relative paths:
      1. current working directory
      2. repo root
    """

    raw = path_arg[1:] if path_arg.startswith("@") else path_arg
    path = Path(raw).expanduser()

    if path.is_absolute():
        return path.resolve(strict=True)

    candidates = [
        Path.cwd() / path,
        REPO_ROOT / path,
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve(strict=True)

    checked = "\n".join(
        f"  - {candidate.resolve(strict=False)}"
        for candidate in candidates
    )

    raise FileNotFoundError(
        f"Input file not found: {path_arg}\n"
        f"Checked:\n{checked}"
    )