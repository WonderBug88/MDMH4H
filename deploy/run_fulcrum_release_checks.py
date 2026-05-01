from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def _run_step(label: str, command: list[str]) -> int:
    print(f"[check] {label}")
    completed = subprocess.run(command, cwd=REPO_ROOT)
    if completed.returncode != 0:
        print(f"[fail] {label}")
        return completed.returncode
    print(f"[ok] {label}")
    return 0


def _compile_targets() -> list[str]:
    targets = [
        "run_fulcrum_alpha.py",
        "run_fulcrum_scheduler.py",
        "wsgi.py",
        "wsgi_fulcrum.py",
    ]
    targets.extend(
        str(path.relative_to(REPO_ROOT))
        for path in sorted((REPO_ROOT / "app" / "fulcrum").glob("*.py"))
    )
    return targets


def main() -> int:
    compile_targets = _compile_targets()

    steps = [
        (
            "Compile Fulcrum entrypoints and package",
            [PYTHON, "-m", "py_compile", *compile_targets],
        ),
        (
            "Run Fulcrum unit suite",
            [
                PYTHON,
                "-m",
                "unittest",
                "discover",
                "-s",
                "app/fulcrum/tests",
                "-p",
                "test_*.py",
            ],
        ),
    ]

    for label, command in steps:
        exit_code = _run_step(label, command)
        if exit_code:
            return exit_code

    print("[ok] Fulcrum release checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
