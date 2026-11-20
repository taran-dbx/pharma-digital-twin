#!/usr/bin/env python3
"""Submit one serverless training run of the Spindle 3 thermal-risk model.

The bundle's ``pharma-twin-risk-model-train`` job is the normal path. This
script is for the bootstrap of a new workspace (the bundle's serving endpoint
needs a registered model version before the first ``bundle deploy``) and for
ad-hoc retrains without deploying the app. It uploads ``ml/``, the
``iot_twin`` packages the job imports and the simulator wheel to a workspace
folder, then submits a one-time run with the same environment as the bundle job
(read from resources/risk_model.yml).

    python scripts/submit_risk_model_training.py --profile <cli-profile> \\
        --catalog <catalog> [--schema pharma_twin] [--endpoint-name pharma-twin-spindle-risk \\
        --create-endpoint] [--wait]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _profile import resolve_profile

REPO_ROOT = Path(__file__).resolve().parents[1]
RESOURCE_FILE = REPO_ROOT / "resources" / "risk_model.yml"
# The iot_twin packages the training job imports (pure Python, no app deps).
SHARED_PACKAGES = ("domain", "ml", "scenario_engine")
RUN_NAME = "pharma-twin-risk-model-train"
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", default="pharma_twin")
    parser.add_argument("--model-name", default="spindle_thermal_risk")
    parser.add_argument("--telemetry-table", default="")
    parser.add_argument("--data-source", default="auto", choices=("auto", "synthetic", "real"))
    parser.add_argument("--endpoint-name", default="")
    parser.add_argument("--create-endpoint", action="store_true")
    parser.add_argument("--workspace-dir", default="", help="default: ~/pharma-twin-risk-model")
    parser.add_argument("--experiment", default="", help="default: <workspace-dir>/experiment")
    parser.add_argument("--wait", action="store_true", help="poll until the run finishes")
    return parser.parse_args(argv)


def job_environment(wheel_path: str) -> tuple[str, list[str]]:
    """The bundle job's ``(client, dependencies)``, with the wheel path rewritten."""
    import yaml

    data = yaml.safe_load(RESOURCE_FILE.read_text(encoding="utf-8"))
    spec = data["resources"]["jobs"]["risk_model_train"]["environments"][0]["spec"]
    deps = [wheel_path if dep.endswith(".whl") else dep for dep in spec["dependencies"]]
    return str(spec["client"]), deps


def files_to_upload() -> list[Path]:
    files = sorted((REPO_ROOT / "ml").glob("*.py"))
    files.append(REPO_ROOT / "src" / "iot_twin" / "__init__.py")
    for package in SHARED_PACKAGES:
        files.extend(sorted((REPO_ROOT / "src" / "iot_twin" / package).glob("*.py")))
    files.extend(sorted((REPO_ROOT / "lib").glob("iot_data_simulator-*.whl")))
    return files


def _upload(workspace, root: str, files: list[Path]) -> None:
    from databricks.sdk.service.workspace import ImportFormat

    made: set[str] = set()
    for path in files:
        target = f"{root}/{path.relative_to(REPO_ROOT).as_posix()}"
        parent = target.rsplit("/", 1)[0]
        if parent not in made:
            workspace.workspace.mkdirs(parent)
            made.add(parent)
        workspace.workspace.upload(
            target, path.read_bytes(), format=ImportFormat.AUTO, overwrite=True
        )


def task_parameters(args: argparse.Namespace, root: str) -> list[str]:
    params = [
        "--src-root",
        f"{root}/src",
        "--catalog",
        args.catalog,
        "--schema",
        args.schema,
        "--model-name",
        args.model_name,
        "--data-source",
        args.data_source,
        "--telemetry-table",
        args.telemetry_table,
        "--endpoint-name",
        args.endpoint_name,
        "--create-endpoint",
        "true" if args.create_endpoint else "false",
        "--experiment",
        args.experiment or f"{root.removeprefix('/Workspace')}/experiment",
    ]
    return params


def main(argv: list[str]) -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import compute, jobs

    profile, rest = resolve_profile(argv, dict(os.environ))
    if not profile:
        print("pass --profile <cli-profile> (or set DATABRICKS_CONFIG_PROFILE)", file=sys.stderr)
        return 2
    args = _args(rest)
    workspace = WorkspaceClient(profile=profile)
    user = workspace.current_user.me().user_name
    root = args.workspace_dir or f"/Workspace/Users/{user}/pharma-twin-risk-model"
    files = files_to_upload()
    _upload(workspace, root, files)
    wheel = next(p for p in files if p.suffix == ".whl")
    client, deps = job_environment(f"{root}/{wheel.relative_to(REPO_ROOT).as_posix()}")
    waiter = workspace.jobs.submit(
        run_name=RUN_NAME,
        tasks=[
            jobs.SubmitTask(
                task_key="train",
                environment_key="ml",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=f"{root}/ml/train_spindle_risk.py",
                    parameters=task_parameters(args, root),
                ),
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="ml",
                spec=compute.Environment(client=client, dependencies=deps),
            )
        ],
    )
    run_id = waiter.run_id
    print(json.dumps({"run_id": run_id, "workspace_dir": root}))
    if not args.wait:
        return 0
    while True:
        run = workspace.jobs.get_run(run_id)
        state = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else ""
        if state in TERMINAL:
            break
        time.sleep(30)
    result = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
    task_run = run.tasks[0].run_id if run.tasks else None
    output = workspace.jobs.get_run_output(task_run) if task_run else None
    print(json.dumps({"run_id": run_id, "result_state": result, "task_run_id": task_run}))
    if output is not None and output.logs:
        print(output.logs[-6000:])
    if output is not None and output.error:
        print(output.error, file=sys.stderr)
    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
