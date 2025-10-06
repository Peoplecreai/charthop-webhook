#!/usr/bin/env python
"""Compatibilidad CLI para exportar datos de Runn a BigQuery."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from app.services.runn_bq_export import cli_main

    cli_main(sys.argv[1:])


if __name__ == "__main__":
    main()

