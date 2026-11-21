"""Code provenance for training runs: which source produced a model version.

The submit script hashes the files it uploads and passes the git SHA; the job
hashes the files it actually runs. Both are logged as run params, so a run can
be tied to a commit and checked for local edits (``code_dirty``) or a
mismatched upload (``source_hash`` != ``source_hash_runtime``).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

# Mirrors scripts/submit_risk_model_training.py: ml/ plus these iot_twin packages.
SOURCE_PACKAGES = ("domain", "ml", "scenario_engine")


def source_files(repo_root: Path) -> list[Path]:
    """The Python files a training run executes, in a stable order."""
    files = sorted((repo_root / "ml").glob("*.py"))
    files.append(repo_root / "src" / "iot_twin" / "__init__.py")
    for package in SOURCE_PACKAGES:
        files.extend(sorted((repo_root / "src" / "iot_twin" / package).glob("*.py")))
    return [path for path in files if path.is_file()]


def source_hash(repo_root: Path) -> str:
    """sha256 over each file's repo-relative path and bytes."""
    digest = hashlib.sha256()
    for path in source_files(repo_root):
        digest.update(path.relative_to(repo_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
