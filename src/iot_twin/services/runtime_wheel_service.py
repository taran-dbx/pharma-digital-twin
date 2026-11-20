"""Augment the bundled simulator wheel with repo-owned producer packages.

Deployed notebooks ``%pip install`` a single workspace wheel path. The
bundled ``iot_data_simulator`` artifact does not include ``domain`` or
``scenario_engine``, so this service copies the wheel bytes and adds those
tracked package sources with valid PEP 376 RECORD hashes.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import os
import re
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

from iot_twin.core.paths import LIB_DIR, PACKAGE_ROOT

_RECORD_BASENAMES = frozenset({"RECORD", "RECORD.jws", "RECORD.p7s"})

WHEEL_FILENAME = "iot_data_simulator-0.1.0-py3-none-any.whl"
# Job clusters import these as TOP-LEVEL packages (``import domain``), so wheel
# members are named ``domain/...`` / ``scenario_engine/...`` (relative to
# ``PACKAGE_ROOT``) and their intra-repo imports are rewritten accordingly.
_PACKAGE_DIRS = ("domain", "scenario_engine")
_RUNTIME_IMPORT_RE = re.compile(r"\biot_twin\.(domain|scenario_engine)\b")
MAX_WHEEL_MEMBERS = 2000
MAX_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
MAX_MEMBER_BYTES = 8 * 1024 * 1024
RUNTIME_WHEEL_SHA256_ENV = "RUNTIME_WHEEL_SHA256"


class RuntimeWheelError(ValueError):
    """Raised when the source wheel fails integrity or zip safety checks."""


def bundled_wheel_path() -> Path:
    """Return the packaged simulator wheel under ``lib/``."""
    return LIB_DIR / WHEEL_FILENAME


def _record_digest(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _to_toplevel_imports(source: str) -> bytes:
    """Rewrite ``iot_twin.domain`` / ``iot_twin.scenario_engine`` to top-level names."""
    rewritten = _RUNTIME_IMPORT_RE.sub(r"\1", source)
    if "iot_twin" in rewritten:
        raise RuntimeWheelError("runtime packages must not reference iot_twin outside imports")
    return rewritten.encode("utf-8")


def _package_files() -> list[tuple[str, bytes]]:
    entries: list[tuple[str, bytes]] = []
    for package in _PACKAGE_DIRS:
        root = PACKAGE_ROOT / package
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(PACKAGE_ROOT).as_posix()
            entries.append((rel, _to_toplevel_imports(path.read_text(encoding="utf-8"))))
    return entries


def expected_source_sha256(source: Path) -> str | None:
    env = (os.getenv(RUNTIME_WHEEL_SHA256_ENV) or "").strip().lower()
    if env:
        return env
    sidecar = Path(str(source) + ".sha256")
    if sidecar.is_file():
        text = sidecar.read_text(encoding="utf-8").strip().split()[0].lower()
        return text or None
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_symlink(info: zipfile.ZipInfo) -> bool:
    return (info.external_attr >> 16) & 0o170000 == 0o120000


def _reject_member_name(name: str) -> str | None:
    if not name or name.startswith("/") or name.startswith("\\") or ":" in name[:3]:
        return "absolute path"
    parts = name.replace("\\", "/").split("/")
    if any(part == ".." for part in parts):
        return "path traversal"
    return None


def inspect_source_wheel(source: Path) -> None:
    """Reject traversal, symlinks, duplicates, zip bombs, and unexpected dist-info."""
    if source.is_symlink() or not source.is_file():
        raise RuntimeWheelError("invalid wheel source")
    expected = expected_source_sha256(source)
    if expected:
        actual = _sha256_file(source)
        if actual != expected:
            raise RuntimeWheelError("wheel digest mismatch")
    try:
        with zipfile.ZipFile(source, "r") as src:
            names = src.namelist()
            if len(names) > MAX_WHEEL_MEMBERS:
                raise RuntimeWheelError("too many archive members")
            seen: set[str] = set()
            dist_infos: set[str] = set()
            uncompressed = 0
            for info in src.infolist():
                name = info.filename.replace("\\", "/")
                reason = _reject_member_name(name)
                if reason:
                    raise RuntimeWheelError(reason)
                if _is_symlink(info):
                    raise RuntimeWheelError("symlink member")
                if name in seen:
                    raise RuntimeWheelError("duplicate member")
                seen.add(name)
                size = int(info.file_size or 0)
                if size > MAX_MEMBER_BYTES:
                    raise RuntimeWheelError("member too large")
                uncompressed += size
                if uncompressed > MAX_UNCOMPRESSED_BYTES:
                    raise RuntimeWheelError("uncompressed size limit")
                if ".dist-info/" in name:
                    dist_infos.add(name.split(".dist-info/", 1)[0] + ".dist-info")
            if len(dist_infos) > 1:
                raise RuntimeWheelError("unexpected dist-info trees")
    except zipfile.BadZipFile as exc:
        raise RuntimeWheelError("invalid zip") from exc


def augment_runtime_wheel(source: Path) -> bytes:
    """Copy ``source`` wheel bytes and add domain + scenario_engine sources."""
    inspect_source_wheel(source)
    extra = dict(_package_files())
    buffer = BytesIO()
    with zipfile.ZipFile(source, "r") as src, zipfile.ZipFile(buffer, "w") as dest:
        contents: dict[str, bytes] = {}
        for info in src.infolist():
            name = info.filename
            basename = name.rsplit("/", 1)[-1]
            if name.endswith("/") or basename in _RECORD_BASENAMES:
                continue
            data = src.read(info)
            dest.writestr(info, data)
            contents[name] = data
        for name, data in extra.items():
            dest.writestr(name, data)
            contents[name] = data
        dist_info = next(
            name.rsplit("/", 1)[0]
            for name in contents
            if name.endswith(".dist-info/WHEEL") or name.endswith(".dist-info/METADATA")
        )
        record_name = f"{dist_info}/RECORD"
        buf = StringIO()
        writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for name, data in sorted(contents.items()):
            writer.writerow([name, _record_digest(data), str(len(data))])
        writer.writerow([record_name, "", ""])
        dest.writestr(record_name, buf.getvalue())
    return buffer.getvalue()
