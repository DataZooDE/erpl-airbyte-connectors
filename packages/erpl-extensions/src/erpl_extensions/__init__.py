"""The ERPL DuckDB extensions, as files on disk inside an installed package.

DuckDB's ``extension_directory`` expects ``<duckdb_version>/<platform>/`` below
the directory it is given, which is the layout ``bin/build-wheel.sh`` reproduces
when it unpacks the ERPL release into ``_files``.
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["ERPL_VERSION", "extension_dir", "is_populated"]

#: The ERPL release this package carries. Kept in step with the payload by
#: bin/build-wheel.sh, which refuses to build if they disagree.
ERPL_VERSION = "v2026.09.04"


def extension_dir() -> Path:
    """The directory to hand DuckDB as ``extension_directory``."""
    return Path(__file__).resolve().parent / "_files"


def is_populated() -> bool:
    """Whether this installation actually carries the binaries.

    A wheel built without its payload would otherwise install cleanly and fail
    much later, inside DuckDB, with a message about an extension rather than
    about packaging.
    """
    root = extension_dir()
    return root.is_dir() and any(root.rglob("*.duckdb_extension"))
