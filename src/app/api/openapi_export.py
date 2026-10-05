"""Writes or checks ``openapi/openapi.yaml``.

python -m app.api.openapi_export           write the file
python -m app.api.openapi_export --check   fail if the file is not the current contract
"""

import sys
from pathlib import Path

import yaml  # type: ignore[import-untyped]  # no stubs: not worth a new dependency
from fastapi import FastAPI

from app.api.contract import TITLE, build_contract
from app.main import API_VERSION, include_routes

CONTRACT_PATH = Path(__file__).resolve().parents[3] / "openapi" / "openapi.yaml"


def contract_text() -> str:
    """The current contract as YAML text. Same text every time."""
    app = FastAPI(title=TITLE, version=API_VERSION)
    include_routes(app)
    return str(yaml.safe_dump(build_contract(app), sort_keys=True, allow_unicode=True))


def main(argv: list[str]) -> int:
    """Write the file, or check it with ``--check``."""
    text = contract_text()
    if "--check" in argv:
        current = CONTRACT_PATH.read_text(encoding="utf-8") if CONTRACT_PATH.exists() else ""
        if current.replace("\r\n", "\n") != text:
            print(
                "openapi/openapi.yaml is not the current contract: run the export", file=sys.stderr
            )
            return 1
        return 0
    CONTRACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONTRACT_PATH.write_text(text, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
