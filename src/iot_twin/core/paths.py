"""Filesystem locations, resolved from this file rather than the process cwd.

Layout (repo root == the Databricks Apps sync root)::

    <root>/src/iot_twin/core/paths.py   <- this file
    <root>/src/iot_twin/assets/         <- packaged data (lakets.sql)
    <root>/static/dist/                 <- built SPA served by the app
    <root>/lib/                         <- bundled simulator wheel

``IOT_TWIN_STATIC_DIR`` overrides the static directory (the one containing
``dist/``), e.g. for tests or alternative deployments.
"""

from __future__ import annotations

import os
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

STATIC_DIR_ENV = "IOT_TWIN_STATIC_DIR"
STATIC_DIR = Path(os.getenv(STATIC_DIR_ENV) or REPO_ROOT / "static")
SPA_DIST_DIR = STATIC_DIR / "dist"
SPA_INDEX = SPA_DIST_DIR / "index.html"

LIB_DIR = REPO_ROOT / "lib"

ASSETS_DIR = PACKAGE_ROOT / "assets"
LAKETS_SQL_PATH = ASSETS_DIR / "lakets.sql"
# App-owned LakeTS extension: statement-level INSERT triggers (see the file header).
LAKETS_INSERT_TRIGGERS_SQL_PATH = ASSETS_DIR / "lakets_insert_triggers.sql"
# App-owned LakeTS extension: stall-safe CDF shadow pruning (see the file header).
LAKETS_CDF_PRUNE_SQL_PATH = ASSETS_DIR / "lakets_cdf_prune.sql"
