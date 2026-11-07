"""Comprobacion de solo lectura del entorno local; sin dependencias externas."""

import configparser
import importlib.metadata
import json
from pathlib import Path
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    expected_venv = root / ".venv"
    expected_python = expected_venv / "Scripts" / "python.exe"
    venv_config = configparser.ConfigParser()
    venv_config.read_string(
        "[venv]\n" + (expected_venv / "pyvenv.cfg").read_text(encoding="utf-8")
    )
    pip_config = configparser.ConfigParser()
    pip_config.read(expected_venv / "pip.ini", encoding="utf-8")

    json_files = [
        root / ".vscode" / "settings.json",
        root / ".vscode" / "extensions.json",
        root / ".vscode" / "tasks.json",
        root / "config" / "project.json",
        root / "config" / "study.example.json",
    ]
    documents = {
        path.relative_to(root).as_posix(): json.loads(path.read_text(encoding="utf-8"))
        for path in json_files
    }
    project = documents["config/project.json"]
    study = documents["config/study.example.json"]
    settings = documents[".vscode/settings.json"]
    checks = {
        "python_3_10": sys.version_info[:2] == (3, 10),
        "virtual_environment": sys.prefix != sys.base_prefix,
        "project_venv": Path(sys.prefix).resolve() == expected_venv.resolve(),
        "project_interpreter": Path(sys.executable).resolve() == expected_python.resolve(),
        "no_global_site_packages": not venv_config.getboolean(
            "venv", "include-system-site-packages", fallback=True
        ),
        "pip_requires_venv": pip_config.getboolean(
            "global", "require-virtualenv", fallback=False
        ),
        "vscode_interpreter": settings["python.defaultInterpreterPath"]
        == "${workspaceFolder}/.venv/Scripts/python.exe",
        "zero_budget": project["budget"]["maximum_new_spend"] == 0,
        "study_selection_pending": study["metropolitan_area"] is None
        and study["category_family"] is None,
        "host_lock_empty": not any(
            line.strip() and not line.lstrip().startswith("#")
            for line in (root / "requirements" / "host.lock.txt")
            .read_text(encoding="utf-8").splitlines()
        ),
        "json_files_valid": len(documents) == len(json_files),
    }
    result = {
        "status": "ok" if all(checks.values()) else "failed",
        "python": sys.version.split()[0],
        "executable": sys.executable,
        "checks": checks,
        "installed_distributions": sorted(
            distribution.metadata["Name"] + "==" + distribution.version
            for distribution in importlib.metadata.distributions()
        ),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
