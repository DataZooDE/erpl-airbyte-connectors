#!/usr/bin/env bash
# Download and pre-warm the ERPL DuckDB extensions.
#
# `erpl` is a trampoline: loading it once unpacks erpl_rfc / erpl_bics / erpl_odp
# and the SAP + ICU shared libraries into the extension directory. After that the
# 180 MB trampoline is dead weight, so it is deleted.
#
# The download runs through Python rather than curl, because the Airbyte
# connector base image ships neither curl nor wget.
set -euo pipefail

TARGET="${1:-${ERPL_EXTENSION_DIR:-/airbyte/duckdb_extensions}}"
export DUCKDB_VERSION="${DUCKDB_VERSION:-v1.5.5}"
export ERPL_PLATFORM="${ERPL_PLATFORM:-linux_amd64}"
export ERPL_REPO="${ERPL_REPO:-http://get.erpl.io}"

python3 - "$TARGET" <<'PY'
import gzip
import os
import pathlib
import shutil
import sys
import urllib.request

target = pathlib.Path(sys.argv[1])
duckdb_version = os.environ["DUCKDB_VERSION"]
platform = os.environ["ERPL_PLATFORM"]
repo = os.environ["ERPL_REPO"].rstrip("/")

dest = target / duckdb_version / platform
dest.mkdir(parents=True, exist_ok=True)

for name in ("erpl", "erpl_web"):
    url = f"{repo}/{duckdb_version}/{platform}/{name}.duckdb_extension.gz"
    print(f"fetching {url}", flush=True)
    with urllib.request.urlopen(url, timeout=300) as response, gzip.GzipFile(
        fileobj=response
    ) as unpacked, open(dest / f"{name}.duckdb_extension", "wb") as out:
        shutil.copyfileobj(unpacked, out)

print("pre-warming the trampoline", flush=True)
import duckdb  # noqa: E402 - only needed once the binaries are on disk

con = duckdb.connect(
    config={"allow_unsigned_extensions": "true", "extension_directory": str(target)}
)
con.load_extension("erpl")  # unpacks erpl_rfc / erpl_bics / erpl_odp + SAP libs
con.load_extension("erpl_web")
con.close()

# The trampoline has served its purpose; the sub-extensions load directly as long
# as LD_LIBRARY_PATH points at this directory.
(dest / "erpl.duckdb_extension").unlink(missing_ok=True)

print(f"extensions ready in {dest}:", flush=True)
for path in sorted(dest.iterdir()):
    print(f"  {path.name}  {path.stat().st_size // 1024 // 1024} MB", flush=True)
PY
