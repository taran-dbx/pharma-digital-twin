"""Runtime wheel augmentation — domain + scenario_engine packaged into the simulator wheel [qa]."""

from __future__ import annotations

import ast
import base64
import csv
import hashlib
import importlib
import io
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from iot_twin.services import runtime_wheel_service as rws


def _sha256_record(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _minimal_simulator_wheel(path: Path) -> Path:
    dist = "iot_data_simulator-0.1.0.dist-info"
    files = {
        "iot_simulator/__init__.py": b"class Simulator:\n    pass\n",
        f"{dist}/METADATA": b"Name: iot-data-simulator\nVersion: 0.1.0\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    lines = [f"{name},{_sha256_record(content)},{len(content)}" for name, content in files.items()]
    lines.append(f"{dist}/RECORD,,")
    files[f"{dist}/RECORD"] = ("\n".join(lines) + "\n").encode("utf-8")
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return path


def _record_map(archive: zipfile.ZipFile) -> dict[str, tuple[str, str]]:
    dist_info = next(name for name in archive.namelist() if name.endswith(".dist-info/RECORD"))
    mapping: dict[str, tuple[str, str]] = {}
    for line in archive.read(dist_info).decode("utf-8").splitlines():
        if not line.strip():
            continue
        name, digest, size = line.split(",")
        mapping[name] = (digest, size)
    return mapping


class TestRuntimeWheelAugmentation:
    def test_augmented_wheel_contains_and_imports_packages_c7315_regression(
        self, tmp_path: Path
    ) -> None:
        """[C7315][regression] Augmented wheel ships domain + scenario_engine with valid RECORD."""
        source = _minimal_simulator_wheel(tmp_path / rws.WHEEL_FILENAME)
        payload = rws.augment_runtime_wheel(source)
        dest = tmp_path / "augmented" / rws.WHEEL_FILENAME
        dest.parent.mkdir()
        dest.write_bytes(payload)

        with zipfile.ZipFile(dest) as archive:
            names = set(archive.namelist())
            assert "iot_simulator/__init__.py" in names
            assert "domain/__init__.py" in names
            assert "domain/catalog.py" in names
            assert "scenario_engine/__init__.py" in names
            assert "scenario_engine/engine.py" in names
            assert "scenario_engine/cadence.py" in names
            records = _record_map(archive)
            for name in (
                "domain/__init__.py",
                "domain/catalog.py",
                "scenario_engine/__init__.py",
                "scenario_engine/engine.py",
                "scenario_engine/cadence.py",
                "iot_simulator/__init__.py",
            ):
                digest, size = records[name]
                data = archive.read(name)
                assert digest == _sha256_record(data)
                assert size == str(len(data))

        extract = tmp_path / "extract"
        extract.mkdir()
        with zipfile.ZipFile(dest) as archive:
            archive.extractall(extract)
        sys.path.insert(0, str(extract))
        try:
            for module in ("domain", "domain.catalog", "scenario_engine", "iot_simulator"):
                sys.modules.pop(module, None)
            domain = importlib.import_module("domain")
            scenario = importlib.import_module("scenario_engine")
            assert hasattr(domain, "get_domain")
            assert hasattr(scenario, "ScenarioEngine")
            assert hasattr(scenario, "CadenceClock")
        finally:
            sys.path.remove(str(extract))
            for module in list(sys.modules):
                ours = (
                    module == "domain"
                    or module.startswith("domain.")
                    or module == "scenario_engine"
                    or module.startswith("scenario_engine.")
                )
                if ours and str(extract) in getattr(sys.modules[module], "__file__", ""):
                    sys.modules.pop(module, None)

    def test_wheel_filename_stays_pip_installable_c7315_smoke(self) -> None:
        """[C7315][smoke] Uploaded artifact keeps the bundled simulator wheel filename."""
        assert rws.WHEEL_FILENAME == "iot_data_simulator-0.1.0-py3-none-any.whl"
        path = rws.bundled_wheel_path()
        assert path.name == rws.WHEEL_FILENAME
        assert path.parent.name == "lib"

    def test_service_has_no_inline_imports_c7315_defect(self) -> None:
        """[C7315][defect] Runtime wheel service keeps imports at module top."""
        src = Path(rws.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for child in ast.walk(node):
                    if child is node:
                        continue
                    if isinstance(child, (ast.Import, ast.ImportFrom)):
                        pytest.fail(f"inline import in {node.name}")

    def test_strips_signed_record_and_quotes_csv_c7315_defect(self, tmp_path: Path) -> None:
        """[C7315][defect] RECORD.jws/p7s are dropped and RECORD is csv.writer encoded."""
        source = _minimal_simulator_wheel(tmp_path / rws.WHEEL_FILENAME)
        dist = "iot_data_simulator-0.1.0.dist-info"
        with zipfile.ZipFile(source, "a") as archive:
            archive.writestr(f"{dist}/RECORD.jws", b"signed-jws")
            archive.writestr(f"{dist}/RECORD.p7s", b"signed-p7s")
            archive.writestr("iot_simulator/odd,name.py", b'print("quoted")\n')
        payload = rws.augment_runtime_wheel(source)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = archive.namelist()
            assert f"{dist}/RECORD.jws" not in names
            assert f"{dist}/RECORD.p7s" not in names
            record = archive.read(f"{dist}/RECORD").decode("utf-8")
            parsed = list(csv.reader(io.StringIO(record)))
            paths = {row[0] for row in parsed if row}
            assert "iot_simulator/odd,name.py" in paths
            quoted_line = next(line for line in record.splitlines() if "odd,name.py" in line)
            assert quoted_line.startswith('"iot_simulator/odd,name.py"') or "," in quoted_line
            assert rws.__file__
            src = Path(rws.__file__).read_text(encoding="utf-8")
            assert "csv.writer" in src

    def test_rejects_traversal_and_duplicate_members_c7315_defect(self, tmp_path: Path) -> None:
        """[C7315][defect] Traversal, duplicates, and extra dist-info trees are rejected."""
        source = tmp_path / rws.WHEEL_FILENAME
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("../evil.py", b"nope")
        with pytest.raises(rws.RuntimeWheelError):
            rws.augment_runtime_wheel(source)

        source2 = _minimal_simulator_wheel(tmp_path / "dup.whl")
        with zipfile.ZipFile(source2, "a") as archive:
            archive.writestr("other-1.0.dist-info/METADATA", b"Name: other\n")
        with pytest.raises(rws.RuntimeWheelError):
            rws.augment_runtime_wheel(source2)

    def test_verifies_configured_sha256_c7315_regression(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7315][regression] Configured source wheel SHA-256 is checked before augment."""
        source = _minimal_simulator_wheel(tmp_path / rws.WHEEL_FILENAME)
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        monkeypatch.setenv("RUNTIME_WHEEL_SHA256", digest)
        payload = rws.augment_runtime_wheel(source)
        assert payload
        monkeypatch.setenv("RUNTIME_WHEEL_SHA256", "0" * 64)
        with pytest.raises(rws.RuntimeWheelError):
            rws.augment_runtime_wheel(source)


_SUBPROCESS_IMPORT_SCRIPT = """
import sys

sys.path.insert(0, sys.argv[1])
import domain
import scenario_engine
import domain.catalog
import scenario_engine.cadence
import scenario_engine.engine

assert "iot_twin" not in sys.modules, "wheel code must not import the iot_twin package"
for module in (domain, scenario_engine, domain.catalog, scenario_engine.engine):
    assert module.__file__.startswith(sys.argv[1]), module.__file__
assert hasattr(domain, "get_domain")
assert hasattr(scenario_engine, "ScenarioEngine")
print("ok")
"""


def _source_package_members() -> set[str]:
    return {
        path.relative_to(rws.PACKAGE_ROOT).as_posix()
        for package in ("domain", "scenario_engine")
        for path in (rws.PACKAGE_ROOT / package).rglob("*.py")
        if "__pycache__" not in path.parts
    }


class TestRuntimeWheelLayout:
    """Job clusters import ``domain`` / ``scenario_engine`` as top-level packages."""

    def _built_wheel(self, tmp_path: Path) -> Path:
        source = _minimal_simulator_wheel(tmp_path / rws.WHEEL_FILENAME)
        dest = tmp_path / "built" / rws.WHEEL_FILENAME
        dest.parent.mkdir()
        dest.write_bytes(rws.augment_runtime_wheel(source))
        return dest

    def test_members_are_toplevel_packages(self, tmp_path: Path) -> None:
        dest = self._built_wheel(tmp_path)
        with zipfile.ZipFile(dest) as archive:
            names = set(archive.namelist())
        expected = _source_package_members()
        assert {"domain/__init__.py", "domain/catalog.py"} <= expected
        assert expected <= names
        added = {n for n in names if n.startswith(("domain/", "scenario_engine/"))}
        assert added == expected
        assert not [n for n in names if n.startswith(("iot_twin", "src/"))]

    def test_member_sources_do_not_reference_iot_twin(self, tmp_path: Path) -> None:
        dest = self._built_wheel(tmp_path)
        with zipfile.ZipFile(dest) as archive:
            for name in _source_package_members():
                text = archive.read(name).decode("utf-8")
                assert "iot_twin" not in text, name

    def test_extracted_wheel_imports_in_clean_interpreter(self, tmp_path: Path) -> None:
        dest = self._built_wheel(tmp_path)
        extract = tmp_path / "extract"
        extract.mkdir()
        with zipfile.ZipFile(dest) as archive:
            archive.extractall(extract)
        result = subprocess.run(
            [sys.executable, "-I", "-c", _SUBPROCESS_IMPORT_SCRIPT, str(extract)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ok"

    def test_real_bundled_wheel_is_augmentable(self, tmp_path: Path) -> None:
        source = rws.bundled_wheel_path()
        assert source.is_file(), f"bundled simulator wheel missing at {source}"
        payload = rws.augment_runtime_wheel(source)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
        assert _source_package_members() <= names
