"""Throwaway local Postgres for schema-migration tests.

Starts a private cluster (unix socket only, trust auth) in a temp directory
when ``initdb``/``pg_ctl`` are available; tests that use it skip otherwise.
Nothing here talks to a remote database.
"""

from __future__ import annotations

import glob
import os
import shutil
import socket
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

_CANDIDATE_BIN_GLOBS = (
    "/opt/homebrew/opt/postgresql*/bin",
    "/usr/local/opt/postgresql*/bin",
    "/usr/lib/postgresql/*/bin",
)


def _pg_bin() -> Path | None:
    explicit = os.getenv("PG_BIN")
    candidates = [explicit] if explicit else []
    found = shutil.which("initdb")
    if found:
        candidates.append(str(Path(found).parent))
    for pattern in _CANDIDATE_BIN_GLOBS:
        candidates.extend(sorted(glob.glob(pattern), reverse=True))
    for candidate in candidates:
        if candidate and (Path(candidate) / "initdb").exists():
            return Path(candidate)
    return None


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass(frozen=True)
class LocalPostgres:
    socket_dir: str
    port: int
    superuser: str = "postgres"


@contextmanager
def local_postgres() -> Iterator[LocalPostgres | None]:
    """Yield a running local cluster, or None when Postgres binaries are absent."""
    bin_dir = _pg_bin()
    if bin_dir is None:
        yield None
        return
    root = tempfile.mkdtemp(prefix="iot-pg-")
    data = os.path.join(root, "data")
    port = _free_port()
    env = {**os.environ, "LC_ALL": "C", "LANG": "C"}
    try:
        subprocess.run(
            [str(bin_dir / "initdb"), "-D", data, "-U", "postgres", "-A", "trust", "-E", "UTF8"],
            check=True,
            capture_output=True,
            env=env,
        )
        subprocess.run(
            [
                str(bin_dir / "pg_ctl"),
                "-D",
                data,
                "-w",
                "-l",
                os.path.join(root, "log"),
                "-o",
                f"-k {root} -p {port} -c listen_addresses=''",
                "start",
            ],
            check=True,
            capture_output=True,
            env=env,
        )
    except (OSError, subprocess.CalledProcessError):
        shutil.rmtree(root, ignore_errors=True)
        yield None
        return
    version = subprocess.run(
        [str(bin_dir / "postgres"), "--version"], capture_output=True, text=True, env=env
    ).stdout.strip()
    print(f"[local_pg] started {version or 'postgres (unknown version)'} from {bin_dir}")
    try:
        yield LocalPostgres(socket_dir=root, port=port)
    finally:
        subprocess.run(
            [str(bin_dir / "pg_ctl"), "-D", data, "-m", "immediate", "stop"],
            capture_output=True,
            env=env,
        )
        shutil.rmtree(root, ignore_errors=True)
